// Command voodu-postgres expands a `postgres "<scope>" "<name>" { … }`
// HCL block into a (asset, statefulset) manifest pair. The asset
// carries the entrypoint script and the postgresql.conf overrides
// the operator declared via `pg_config = { ... }`. The statefulset
// is a single primary postgres pod (M-P1 ships single-primary only;
// streaming replication for replicas>=2 lands in M-P3).
//
// # Status: M-P1 — single primary
//
// Honoured fields (parsed + emitted):
//
//   - image / replicas / database / user / port → statefulset shape
//   - password → statefulset env (auto-gen + persist if empty)
//   - initdb_locale / initdb_encoding → POSTGRES_INITDB_ARGS env
//   - pg_config = { ... } → asset bytes (postgresql.conf override)
//
// Parsed but NOT auto-applied at runtime in M-P1:
//
//   - extensions = [...] → see postgresSpec.Extensions doc.
//     Operators install via app migrations or psql; M-P4 ships
//     `vd postgres:exec` for explicit install.
//   - replicas > 1 → currently a no-op beyond pod count; the
//     standby pods boot but don't enter recovery mode (no
//     primary_conninfo). M-P3 wires streaming replication.
//
// # Plugin contract
//
// stdin: { kind, scope, name, spec, config } — spec is the operator's
// HCL block, config is the controller-pre-fetched merged bucket for
// (scope, name). config carries POSTGRES_PASSWORD across applies so
// the auto-gen value stays stable.
//
// stdout: envelope wrapping { manifests: [asset, statefulset],
// actions: [config_set] } — actions only present on first apply when
// password was generated.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

var version = "dev"

const defaultImage = "postgres:latest"

// expandRequest is the wire shape the controller streams on stdin.
// Config is the merged config bucket for (scope, name) — empty on
// first apply, populated on subsequent applies with whatever
// config_set actions the plugin emitted previously.
type expandRequest struct {
	Kind   string            `json:"kind"`
	Scope  string            `json:"scope,omitempty"`
	Name   string            `json:"name"`
	Spec   json.RawMessage   `json:"spec,omitempty"`
	Config map[string]string `json:"config,omitempty"`
}

type envelope struct {
	Status string `json:"status"`
	Data   any    `json:"data,omitempty"`
	Error  string `json:"error,omitempty"`
}

// manifest is one entry in the fan-out the plugin emits — typically
// an `asset` plus a `statefulset` per expand call.
type manifest struct {
	Kind  string         `json:"kind"`
	Scope string         `json:"scope,omitempty"`
	Name  string         `json:"name"`
	Spec  map[string]any `json:"spec"`
}

// dispatchAction is a side-effect the controller applies after
// the expand or dispatch command returns. Mirrors the wire shape
// the controller's pluginDispatchAction expects — fields are
// kept in lockstep so plugin authors get a single Go struct that
// covers every action type the controller recognises.
//
// Types:
//
//   - `config_set`       — write KV pairs to the (Scope, Name) bucket
//   - `config_unset`     — remove Keys from the (Scope, Name) bucket
//   - `apply_manifest`   — Store.Put the embedded Manifest. Action's
//                          Scope/Name are the OWNER context (used
//                          for audit only — restart fan-out is
//                          suppressed for this action type because
//                          the watch loop reconciles the manifest
//                          via its kind handler).
//   - `delete_manifest`  — Store.Delete (Kind, Scope, Name). Same
//                          fan-out suppression as apply_manifest.
//
// Plugins that materialise runtime resources (e.g. spawning a
// one-shot job for a backup capture) emit apply_manifest +
// delete_manifest instead of touching docker / etcd directly.
// The controller is the single mediator for all writes.
type dispatchAction struct {
	Type  string            `json:"type"`
	Scope string            `json:"scope"`
	Name  string            `json:"name"`
	KV    map[string]string `json:"kv,omitempty"`
	Keys  []string          `json:"keys,omitempty"`

	// SkipRestart asks the controller to apply this config write
	// WITHOUT triggering the usual restart fan-out on (Scope, Name).
	// Default false (omitted in JSON) — config_set normally
	// triggers a rolling restart so the new value takes effect.
	// Ignored by apply_manifest / delete_manifest (which already
	// suppress fan-out by design).
	//
	// Used by:
	//
	//   - cmdLink/cmdUnlink for the linked-consumers tracking
	//     write on the provider — the value isn't consumed by
	//     postgres itself, only by the plugin's password rotation
	//     flow, so no restart needed.
	//
	//   - cmdNewPassword --no-restart for staged rotations.
	SkipRestart bool `json:"skip_restart,omitempty"`

	// Manifest carries the apply_manifest payload — the full
	// resource spec the controller will Store.Put. Empty for
	// other action types.
	Manifest *dispatchManifest `json:"manifest,omitempty"`

	// Kind carries the delete_manifest target kind. Scope/Name
	// come from the top-level fields above. Empty for other
	// action types.
	Kind string `json:"kind,omitempty"`

	// Command carries the exec_local payload — a vector the
	// CLI runs locally on the operator's host with TTY attached.
	// Used by interactive shells (cmdPsql) and any flow that
	// needs the operator's terminal. Controller passes this
	// through verbatim; it does not execute server-side.
	Command []string `json:"command,omitempty"`

	// fetch_file payload — instructs the operator-side CLI to
	// transfer the file at RemotePath (on the controller host)
	// to DestPath (on the operator's filesystem). The CLI picks
	// scp (when running auto-forwarded over SSH) or cp (when
	// running directly on the controller host) — plugins emit
	// the same shape regardless.
	//
	// Why metadata-only and not bytes-in-envelope: streaming via
	// scp reuses the SSH credentials the auto-forward already
	// has, gets native progress for free, and isn't bound by RAM
	// or JSON-envelope size budgets. Bytes-in-dispatch was a dead
	// end above ~few hundred MiB and lacked progress.
	//
	// Controller passes this through verbatim (same model as
	// exec_local) — it never reads or copies the file itself.
	RemotePath string `json:"remote_path,omitempty"`
	DestPath   string `json:"dest_path,omitempty"`
	SizeBytes  int64  `json:"size_bytes,omitempty"`

	// Summary is an optional human-readable label the plugin
	// can attach to ANY action. Used by the controller as the
	// `applied` line the CLI prints (e.g. `✓ <Summary>`). When
	// empty, the controller composes a default summary from
	// the action's structural fields (verbose but always works).
	//
	// Use case: keep the operator's checklist scannable. Default
	// fetch_file summary is `fetch_file <full-remote-path> →
	// <dest> (deferred to CLI)` — fine for debugging but noisy
	// when the plugin already knows a short identifier (a backup
	// ref like `b013`, an asset name, etc.). Plugins set Summary
	// to the short form so the operator sees `fetch_file b013 →
	// bkp/db2.dump` instead of the full host path.
	Summary string `json:"summary,omitempty"`
}

// dispatchManifest is the wire shape for apply_manifest payloads.
// Mirrors the controller's pluginDispatchManifest — kind+scope+
// name+spec, no metadata (controller fills it in on Put).
//
// Spec is map[string]any so plugin authors can compose it via
// literal Go maps without juggling json.RawMessage:
//
//	&dispatchManifest{
//	    Kind:  "job",
//	    Scope: "clowk-lp",
//	    Name:  "db-backup-b008",
//	    Spec: map[string]any{
//	        "image":   "postgres:16",
//	        "command": []string{"bash", "-c", "pg_dump ..."},
//	    },
//	}
type dispatchManifest struct {
	Kind  string         `json:"kind"`
	Scope string         `json:"scope,omitempty"`
	Name  string         `json:"name"`
	Spec  map[string]any `json:"spec"`
}

// expandedPayload is the envelope-data shape the controller's
// dispatcher recognises: { manifests, actions } — actions optional.
type expandedPayload struct {
	Manifests []manifest       `json:"manifests"`
	Actions   []dispatchAction `json:"actions,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		emitErr("usage: voodu-postgres <expand|link|unlink|new-password|info|expose|unexpose|promote|rejoin|psql|backups|backups:capture|backups:restore|backups:download|backups:delete|backups:schedule|backups:cancel|backups:logs|backups:prune|defaults|help|--version>")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "--version", "-v", "version":
		fmt.Println(version)

	case "defaults":
		emitOK(defaultsForInspection())

	case "expand":
		if err := cmdExpand(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "link":
		if err := cmdLink(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "unlink":
		if err := cmdUnlink(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "new-password":
		if err := cmdNewPassword(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "info":
		if err := cmdInfo(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "expose":
		if err := cmdExpose(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "unexpose":
		if err := cmdUnexpose(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "promote", "failover":
		// "promote" is the canonical name (M-P5+); "failover"
		// is the legacy alias kept for backward compat with any
		// scripts that already use vd pg:failover. Both invoke
		// the same handler — plugin owns the pg_promote() SQL,
		// operator never types it.
		if err := cmdPromote(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "rejoin":
		if err := cmdRejoin(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "psql":
		// psql replaces the plugin process via syscall.Exec —
		// only reaches here when the exec fails (rare). When
		// it succeeds, the process is replaced and main never
		// returns.
		if err := cmdPsql(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "backups":
		// `vd pg:backups <ref>` — list backups (Heroku-style,
		// no verb means default action = list).
		if err := cmdBackups(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "backups:capture":
		// `vd pg:backups:capture <ref>` — pg_dump snapshot.
		if err := cmdBackupsCapture(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "backups:restore":
		// `vd pg:backups:restore <ref> <id>` — pg_restore from /backups/<file>.
		if err := cmdBackupsRestore(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "backups:download":
		// `vd pg:backups:download <ref> <id>` — docker cp to host.
		if err := cmdBackupsDownload(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "backups:delete":
		// `vd pg:backups:delete <ref> <id>` — rm /backups/<file>.
		if err := cmdBackupsDelete(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "backups:schedule":
		// `vd pg:backups:schedule <ref> --at <cron>` — print
		// HCL template for an operator-declared cronjob.
		if err := cmdBackupsSchedule(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "backups:cancel":
		// `vd pg:backups:cancel <ref> [<id>]` — docker stop on
		// running backup containers.
		if err := cmdBackupsCancel(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "backups:logs":
		// `vd pg:backups:logs <ref> <id> [--follow]` — docker logs
		// on a backup container.
		if err := cmdBackupsLogs(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "backups:prune":
		// `vd pg:backups:prune <ref> [--keep N] [--max-age D]` —
		// retention policy applied on demand. Same semantics as
		// the auto-prune that runs after a successful capture, but
		// triggerable independently (cleanup, schedule cronjobs).
		if err := cmdBackupsPrune(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "help":
		// `vd postgres -h` reaches us as a "help" subcommand.
		// Plain text on stdout (no envelope) — operator sees the
		// overview verbatim without dispatch unwrapping.
		printPluginOverview()

	default:
		emitErr(fmt.Sprintf("unknown subcommand %q (want expand|link|unlink|new-password|info|expose|unexpose|promote|rejoin|psql|backups|backups:capture|backups:restore|backups:download|backups:delete|backups:schedule|backups:cancel|backups:logs|backups:prune|defaults|help)", os.Args[1]))
		os.Exit(1)
	}
}

// cmdExpand is the M-P1 main path. Reads the controller's expand
// request from stdin, parses + validates the operator's HCL,
// resolves the password (reuse from bucket or generate), composes
// the (asset, statefulset) manifest pair, and emits it.
//
// First-apply flow (config bucket empty):
//
//   - resolveOrGeneratePassword returns isNew=true with a fresh hex
//     string. We bake the password into the statefulset env AND
//     emit a config_set action so the next apply reads it back.
//
// Subsequent-apply flow (bucket has POSTGRES_PASSWORD):
//
//   - resolveOrGeneratePassword reuses the existing value. No
//     action emitted — the asset bytes stay stable, no spurious
//     rolling restart.
func cmdExpand() error {
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("read stdin: %w", err)
	}

	var req expandRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return fmt.Errorf("decode expand request: %w", err)
	}

	if req.Name == "" {
		return fmt.Errorf("expand request missing required field 'name'")
	}

	var operatorSpec map[string]any

	if len(req.Spec) > 0 {
		if err := json.Unmarshal(req.Spec, &operatorSpec); err != nil {
			return fmt.Errorf("decode block spec: %w", err)
		}
	}

	spec, err := parsePostgresSpec(operatorSpec)
	if err != nil {
		return fmt.Errorf("postgres %s: %w", refOrName(req.Scope, req.Name), err)
	}

	if err := validatePostgresSpec(spec); err != nil {
		return fmt.Errorf("postgres %s: %w", refOrName(req.Scope, req.Name), err)
	}

	password, isNew, err := resolveOrGeneratePassword(spec, req.Config)
	if err != nil {
		return fmt.Errorf("postgres %s: resolve password: %w", refOrName(req.Scope, req.Name), err)
	}

	replicationPassword, replIsNew, err := resolveOrGenerateReplicationPassword(req.Config)
	if err != nil {
		return fmt.Errorf("postgres %s: resolve replication password: %w", refOrName(req.Scope, req.Name), err)
	}

	// Primary ordinal: defaults to 0 (the convention is pod-0 =
	// primary, pod-1+ = standbys). `vd postgres:promote --replica
	// N` flips this via the PG_PRIMARY_ORDINAL bucket key — on the
	// next expand the wrapper picks the new value up from req.Config
	// and standbys reconnect to the new primary FQDN.
	primaryOrdinal := 0
	if config := req.Config; config != nil {
		if v, ok := config[primaryOrdinalKey]; ok && v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				primaryOrdinal = n
			}
		}
	}

	entrypointBytes := renderEntrypointScript()
	overridesBytes := renderPgOverridesConf(spec.PgConfig)
	streamingBytes := renderStreamingConf(req.Scope, req.Name, primaryOrdinal, spec.ReplicationUser, replicationPassword)
	initReplicationBytes := renderInitReplicationSH()

	asset := manifest{
		Kind:  "asset",
		Scope: req.Scope,
		Name:  req.Name,
		Spec: map[string]any{
			"files": map[string]any{
				entrypointAssetKey:      entrypointBytes,
				pgOverridesAssetKey:     overridesBytes,
				streamingConfAssetKey:   streamingBytes,
				initReplicationAssetKey: initReplicationBytes,
			},
		},
	}

	exposed := req.Config[exposeFlagKey] == "true"

	defaults := composeStatefulsetDefaults(req.Scope, req.Name, spec, password, replicationPassword, primaryOrdinal, exposed)
	merged := mergeSpec(defaults, operatorSpec)

	stripPluginOwnedFields(merged)

	statefulset := manifest{
		Kind:  "statefulset",
		Scope: req.Scope,
		Name:  req.Name,
		Spec:  merged,
	}

	out := expandedPayload{
		Manifests: []manifest{asset, statefulset},
	}

	// Persist auto-generated state. Two distinct keys, two
	// distinct actions — keeps `vd config get` output readable
	// (one KV per action vs one fat blob). Subsequent applies
	// see both via Config and skip generation.
	if isNew {
		out.Actions = append(out.Actions, dispatchAction{
			Type:  "config_set",
			Scope: req.Scope,
			Name:  req.Name,
			KV:    map[string]string{passwordKey: password},
		})
	}

	if replIsNew {
		out.Actions = append(out.Actions, dispatchAction{
			Type:  "config_set",
			Scope: req.Scope,
			Name:  req.Name,
			KV:    map[string]string{replicationPasswordKey: replicationPassword},
		})
	}

	// Seed the backup retention default on first apply only —
	// subsequent applies leave the operator's `vd config set
	// BACKUP_KEEP=…` (or the deliberate "0 = disable" choice)
	// alone. The explicit isNew gate is the same pattern used
	// for the auto-gen passwords above.
	//
	// We only seed BACKUP_KEEP. BACKUP_MAX_AGE stays empty so
	// operators discover it through the README/help and opt in
	// when they want a time cap on top of the count cap.
	if isNew && req.Config[backupKeepKey] == "" {
		out.Actions = append(out.Actions, dispatchAction{
			Type:  "config_set",
			Scope: req.Scope,
			Name:  req.Name,
			KV:    map[string]string{backupKeepKey: defaultBackupKeep},
		})
	}

	emitOK(out)

	return nil
}

// assetRef composes a voodu asset interpolation ref. Voodu's
// asset kind accepts BOTH 1-label (unscoped) and 2-label (scoped)
// declarations; the matching ref shapes are:
//
//	scoped (scope != ""):    ${asset.<scope>.<name>.<key>}   (4 segments)
//	unscoped (scope == ""):  ${asset.<name>.<key>}           (3 segments)
//
// Using the 4-segment form on an unscoped asset produces
// `${asset..<name>.<key>}` (double dot) which the controller's
// asset resolver doesn't match — "asset not found" at apply time.
//
// Postgres resources can be declared with 0/1/2 labels, so we
// dispatch by scope at expand time. Same logic applies to any
// future plugin emitting asset bind-mounts.
func assetRef(scope, name, key string) string {
	if scope == "" {
		return "${asset." + name + "." + key + "}"
	}

	return "${asset." + scope + "." + name + "." + key + "}"
}

// composeStatefulsetDefaults is the plugin's contribution to the
// statefulset shape. Operator overrides win per-key (alias contract)
// — see mergeSpec for the merge strategy.
//
// Env vars set here:
//
//   - POSTGRES_USER / POSTGRES_DB / POSTGRES_PASSWORD — consumed by
//     the official image's docker-entrypoint.sh on first boot to
//     run initdb with the right superuser + database.
//   - POSTGRES_INITDB_ARGS — the official entrypoint forwards this
//     to initdb verbatim. We compose `--locale=... --encoding=...`.
//   - PGDATA — pinned to a subdirectory of the volume mount so
//     postgres's "PGDATA must be empty" check tolerates volume
//     drivers that pre-create lost+found at the mount root.
//   - PG_PORT — read by our entrypoint wrapper to pass `-p` to
//     postgres. Operator overriding `port = X` in HCL flows here.
//
// Volumes:
//
//   - asset:entrypoint → /usr/local/bin/voodu-postgres-entrypoint
//   - asset:pg_overrides_conf → /etc/postgresql/voodu-overrides.conf
//   - volume_claim "data" → /var/lib/postgresql/data (PGDATA's parent)
//
// Health check: pg_isready against the configured user/db/port.
// Readiness probes after first boot completes (initdb takes a few
// seconds on a fresh volume; the controller's start-grace covers
// the gap).
func composeStatefulsetDefaults(scope, name string, spec *postgresSpec, password, replicationPassword string, primaryOrdinal int, exposed bool) map[string]any {
	env := map[string]any{
		// Official postgres image contract
		"POSTGRES_USER":        spec.User,
		"POSTGRES_DB":          spec.Database,
		"POSTGRES_PASSWORD":    password,
		"POSTGRES_INITDB_ARGS": fmt.Sprintf("--locale=%s --encoding=%s", spec.InitdbLocale, spec.InitdbEncoding),
		"PGDATA":               "/var/lib/postgresql/data/pgdata",

		// voodu-postgres wrapper contract
		"PG_PORT":                  strconv.Itoa(spec.Port),
		"PG_NAME":                  name,
		"PG_SCOPE_SUFFIX":          composeScopeSuffix(scope),
		"PG_PRIMARY_ORDINAL":       strconv.Itoa(primaryOrdinal),
		"PG_REPLICATION_USER":      spec.ReplicationUser,
		"PG_REPLICATION_PASSWORD":  replicationPassword,
	}

	healthCheck := fmt.Sprintf("pg_isready -U %s -d %s -p %d", spec.User, spec.Database, spec.Port)

	// Asset bind-mounts. Three .conf files (wrapper symlinks
	// each into PGDATA/conf.d/ via glob) plus one shell script
	// that postgres's official entrypoint runs once during
	// initdb on the primary's first boot. Standbys never run
	// the init script — they pg_basebackup from primary and
	// inherit the bootstrap state.
	//
	// Asset ref shape depends on whether the resource is
	// scoped: 4-segment ${asset.<scope>.<name>.<key>} for
	// scoped, 3-segment ${asset.<name>.<key>} for unscoped.
	// Voodu's manifest parser registers asset blocks with the
	// matching label count; using the wrong shape produces
	// "asset not found" at apply time.
	volumes := []any{
		assetRef(scope, name, entrypointAssetKey) + ":" + entrypointMountPath + ":ro",
		assetRef(scope, name, pgOverridesAssetKey) + ":" + pgOverridesMountPath + ":ro",
		assetRef(scope, name, streamingConfAssetKey) + ":" + streamingConfMountPath + ":ro",
		assetRef(scope, name, initReplicationAssetKey) + ":" + initReplicationMountPath + ":ro",

		// Backups directory: host bind-mount for pg_dump output.
		// Same path on every pod (primary writes; replicas can
		// mount + read for `vd pg:backups:capture --from-replica`).
		// Operator pre-flight: `mkdir -p /opt/voodu/backups/<scope>/<name>`
		// (chown handled by entrypoint wrapper at boot).
		composeBackupHostPath(scope, name) + ":" + backupsContainerPath + ":rw",
	}

	volumeClaims := []any{
		map[string]any{
			"name":       "data",
			"mount_path": "/var/lib/postgresql/data",
		},
	}

	// Port binding: loopback by default (voodu's "ports are
	// loopback-only" platform invariant). When the operator runs
	// vd postgres:expose, PG_EXPOSE_PUBLIC=true lands in the
	// bucket and we flip ports to 0.0.0.0:<port> — making
	// postgres reachable from outside the host VM.
	portStr := strconv.Itoa(spec.Port)
	if exposed {
		portStr = "0.0.0.0:" + portStr
	}

	return map[string]any{
		"image":         spec.Image,
		"replicas":      spec.Replicas,
		"ports":         []any{portStr},
		"command":       []any{"bash", entrypointMountPath},
		"env":           env,
		"volumes":       volumes,
		"volume_claims": volumeClaims,
		"health_check":  healthCheck,
	}
}

// mergeSpec applies operator overrides on top of plugin defaults.
// Per-key strategy:
//
//   - `env` deep-merges so operator vars and plugin vars coexist
//     by key (operator wins on conflict, mirroring redis pattern).
//   - `volumes` additive-merges by destination path: plugin's
//     defaults always present unless operator declares the same
//     destination, in which case operator wins for that one entry.
//     Avoids docker's "duplicate mount point" error and lets
//     operators selectively replace one of our binds.
//   - everything else: operator-wins outright (alias contract).
//
// Empty-but-present operator values (`volumes = []`) are honoured
// verbatim — that's how operators opt out of a default. Same
// pattern as voodu-redis.
func mergeSpec(defaults, operator map[string]any) map[string]any {
	out := make(map[string]any, len(defaults))

	for k, v := range defaults {
		out[k] = v
	}

	for k, v := range operator {
		switch k {
		case "env":
			out[k] = mergeEnv(out[k], v)

		case "volumes":
			out[k] = mergeVolumes(out[k], v)

		default:
			out[k] = v
		}
	}

	return out
}

// mergeEnv merges operator env on top of plugin env. Operator wins
// per key. Both empty → nil (don't emit an empty env block).
func mergeEnv(defaultEnv, operatorEnv any) any {
	a := envAsMap(defaultEnv)
	b := envAsMap(operatorEnv)

	if len(a) == 0 && len(b) == 0 {
		return nil
	}

	out := make(map[string]any, len(a)+len(b))

	for k, v := range a {
		out[k] = v
	}

	for k, v := range b {
		out[k] = v
	}

	return out
}

// envAsMap normalises env to map[string]any. HCL gives us
// map[string]any directly; YAML round-trips through map[string]any
// as well; map[string]string (Go-side) gets coerced for tests that
// hand-craft the operator spec.
func envAsMap(v any) map[string]any {
	switch x := v.(type) {
	case map[string]any:
		return x

	case map[string]string:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = val
		}

		return out
	}

	return nil
}

// mergeVolumes additive-merges by destination path. Each entry is
// "<src>:<dst>[:<opts>]"; we key by dst so a duplicate-dst from the
// operator REPLACES the plugin's bind for that destination only.
// Other plugin defaults stay.
func mergeVolumes(defaultVols, operatorVols any) any {
	a := volumesAsList(defaultVols)
	b := volumesAsList(operatorVols)

	if len(a) == 0 && len(b) == 0 {
		return nil
	}

	// Index by dst path. Order-preserving — operator entries land
	// after plugin defaults in the final list (so an operator-added
	// fresh dst sorts at the end, predictable for `docker run -v`).
	type entry struct {
		raw string
		dst string
	}

	indexBy := func(list []string) map[string]int {
		out := make(map[string]int, len(list))
		for i, s := range list {
			out[volumeDest(s)] = i
		}

		return out
	}

	defIdx := indexBy(a)

	out := make([]any, 0, len(a)+len(b))
	used := map[string]bool{}

	for _, s := range a {
		dst := volumeDest(s)
		used[dst] = true

		// Replaced by operator → emit operator's version below.
		if _, hit := indexBy(b)[dst]; hit {
			continue
		}

		out = append(out, s)
	}

	// Now operator entries: replacements in their original ORDER
	// (matching the plugin's slot for that dst), then fresh dsts at
	// the tail.
	for _, s := range b {
		dst := volumeDest(s)
		if _, hit := defIdx[dst]; hit {
			out = append(out, s)
		}
	}

	for _, s := range b {
		dst := volumeDest(s)
		if _, hit := defIdx[dst]; !hit && !used[dst] {
			out = append(out, s)
			used[dst] = true
		}
	}

	// Stable order across Go map iteration: sort the operator-
	// replacement segment by dst. (Default-only segment already in
	// declaration order via slice iteration above.)
	sortable := make([]entry, 0, len(out))
	for _, v := range out {
		s, _ := v.(string)
		sortable = append(sortable, entry{raw: s, dst: volumeDest(s)})
	}

	sort.SliceStable(sortable, func(i, j int) bool {
		return sortable[i].dst < sortable[j].dst
	})

	final := make([]any, len(sortable))
	for i, e := range sortable {
		final[i] = e.raw
	}

	return final
}

// volumeDest extracts the destination path from a docker volume
// spec. "<src>:<dst>:<opts>" or "<src>:<dst>" — anything past the
// second colon is mount options. Returns the raw string when the
// shape is unrecognisable (operator typo'd, let docker reject).
func volumeDest(s string) string {
	parts := strings.SplitN(s, ":", 3)
	if len(parts) < 2 {
		return s
	}

	return parts[1]
}

// volumesAsList normalises volumes to []string.
func volumesAsList(v any) []string {
	switch x := v.(type) {
	case []string:
		return append([]string(nil), x...)

	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}

		return out
	}

	return nil
}

// defaultsForInspection is the shape the `defaults` subcommand
// emits. Inspectional only — the real defaults flow through
// composeStatefulsetDefaults. Keeps the surface stable for ops
// tooling that grep'd for fields.
func defaultsForInspection() map[string]any {
	return map[string]any{
		"image":            defaultImage,
		"replicas":         1,
		"database":         "postgres",
		"user":             "postgres",
		"password":         "<auto-generated 32 chars on first apply>",
		"port":             5432,
		"initdb_locale":    "C.UTF-8",
		"initdb_encoding":  "UTF8",
		"pg_config":        map[string]any{},
		"extensions":       []any{},
		"replication_user": "replicator",
	}
}

func emitOK(data any) {
	enc := json.NewEncoder(os.Stdout)

	_ = enc.Encode(envelope{Status: "ok", Data: data})
}

func emitErr(msg string) {
	enc := json.NewEncoder(os.Stderr)

	_ = enc.Encode(envelope{Status: "error", Error: msg})
}
