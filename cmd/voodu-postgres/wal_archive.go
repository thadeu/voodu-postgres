// WAL archive: parse, validate, and dispatch the postgres
// `archive_mode` + `archive_command` config that lets a primary
// stream WAL files out for PITR + standby catch-up.
//
// # Strategy abstraction
//
// The operator picks a strategy via `wal_archive { strategy = "..." }`:
//
//   - "local" (default) — bind-mount a host directory at /wal-archive
//     inside every pod; archive_command does an idempotent local cp.
//     Destination is the host path. Pre-flight: operator mkdirs +
//     chowns to postgres uid 999.
//   - "s3" / "r2" / "nfs" / "rsync" — FUTURE. Each future strategy
//     lives in its own strategy_<name>.go and implements the
//     walArchiveStrategy interface below. Plugin dispatches via
//     selectStrategy(name).
//
// Until the alternative strategies ship, "local" is the only valid
// value. Setting `strategy = "s3"` today returns a "strategy unknown"
// error at apply time.
//
// # Defaults
//
// Block omitted entirely → enabled, strategy = "local", destination =
// /opt/voodu/backups/<scope>/<name>. Operator override is one line:
//
//	wal_archive = {
//	  enabled     = true                # default
//	  strategy    = "local"             # default
//	  destination = "/srv/pg-wal/db"    # override default path
//	}
//
// # Cleanup / tiering
//
// Plugin DOES NOT manage WAL retention or off-host replication. The
// archive grows unbounded under default settings. Operator declares
// cronjobs alongside the postgres resource for retention + sync to
// S3/R2/etc. — see README "WAL archive to S3 + R2 (multi-cloud
// durability)" for the canonical pattern.

package main

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// walArchiveSpec is the parsed view of the operator's
// `wal_archive { ... }` block. nil means "block not declared" —
// caller treats as "use defaults" (enabled, local strategy, default
// destination per strategy).
type walArchiveSpec struct {
	// Enabled toggles the entire archive subsystem. When false:
	//
	//   - No bind-mount / volume emitted (strategy-dependent).
	//   - voodu-00-wal-archive.conf reduces to a comment header.
	//   - postgres runs with default archive_mode (off).
	//
	// Default true.
	Enabled bool

	// Strategy picks the destination shape. Today only "local" is
	// implemented. Future: "s3", "r2", "nfs", "rsync". Each maps
	// to a walArchiveStrategy via selectStrategy().
	//
	// Default "local".
	Strategy string

	// Destination is the strategy-interpreted target of the WAL
	// archive. Concrete shape depends on Strategy:
	//
	//   - local:  absolute host path (/opt/voodu/backups/<scope>/<name>)
	//   - s3:     "s3://bucket/prefix"     (future)
	//   - r2:     "r2://bucket/prefix"     (future)
	//   - nfs:    "nfs://server/export"    (future)
	//   - rsync:  "user@host:/path"        (future)
	//
	// Empty means "use the strategy's default" — strategy.Apply()
	// fills it in via DefaultDestination(scope, name).
	Destination string
}

// walArchivePlan is the strategy's contribution to the statefulset.
// composeStatefulsetDefaults wires the volumes into the spec; the
// archive_command lands in voodu-00-wal-archive.conf via
// renderWALArchiveConf.
type walArchivePlan struct {
	// Volumes are the docker volume specs the strategy adds to
	// every pod's `volumes` list. For "local": one host bind-mount
	// at /wal-archive. For S3/R2 (future): empty (writes go via
	// API/CLI, no mount needed).
	Volumes []string

	// ArchiveCommand is the literal string postgres runs to
	// archive each WAL segment. Substitution placeholders %p
	// (full path of segment) and %f (segment filename) are
	// expanded by postgres at runtime.
	ArchiveCommand string
}

// walArchiveStrategy is the contract every strategy implements.
// Adding a new strategy = new file (strategy_<name>.go) + a case
// in selectStrategy. No changes to wal_archive.go required.
type walArchiveStrategy interface {
	// Name returns the HCL identifier for this strategy
	// ("local", "s3", etc.). Used in error messages.
	Name() string

	// DefaultDestination returns the destination when the
	// operator omits `destination = "..."`. May return "" if
	// the strategy has no sensible default (operator MUST then
	// set destination explicitly — Validate rejects empty).
	DefaultDestination(scope, name string) string

	// Validate checks the operator-supplied destination + any
	// strategy-specific fields. Called after parse, before
	// composeStatefulsetDefaults. Empty Destination is allowed
	// here when DefaultDestination is non-empty (Apply fills it).
	Validate(spec *walArchiveSpec) error

	// Apply renders the strategy's contribution to the
	// statefulset spec. Receives the resolved destination
	// (default already substituted when operator left it empty).
	Apply(spec *walArchiveSpec, scope, name string) walArchivePlan
}

// selectStrategy returns the strategy impl for an HCL name.
// Unknown names produce an error so an operator-typo'd value
// fails loud at apply time instead of silently archiving
// nothing.
func selectStrategy(name string) (walArchiveStrategy, error) {
	switch name {
	case "", "local":
		return localStrategy{}, nil
	default:
		return nil, fmt.Errorf("wal_archive.strategy %q unknown (supported: local)", name)
	}
}

// walArchiveAssetKey is the asset file key + bind-mount path
// for the plugin-side WAL archive config. The mount path is
// chosen so it sorts BEFORE the operator's pg_overrides
// (voodu-99-overrides.conf) — postgres reads include_dir entries
// alphabetically and "last config wins", so operator overrides
// trump plugin defaults cleanly.
const (
	walArchiveAssetKey  = "wal_archive_conf"
	walArchiveConfMountPath = "/etc/postgresql/voodu-00-wal-archive.conf"
)

// defaultWALArchiveSpec returns the spec used when the operator
// omits the `wal_archive { }` block entirely — archiving is on,
// local strategy, destination resolved from scope/name at apply.
func defaultWALArchiveSpec() *walArchiveSpec {
	return &walArchiveSpec{
		Enabled:  true,
		Strategy: "local",
	}
}

// parseWALArchiveSpec extracts the wal_archive sub-map from the
// operator's spec. Returns nil if the block was not declared
// (caller substitutes defaults via defaultWALArchiveSpec).
//
// HCL block decoding gives us a map[string]any; the spec accepts
// `wal_archive = { ... }` (assignment form) which decodes the
// same way as a block. Type errors surface here so apply fails
// loudly instead of silently coercing.
func parseWALArchiveSpec(operatorSpec map[string]any) (*walArchiveSpec, error) {
	v, present := operatorSpec["wal_archive"]
	if !present {
		return nil, nil
	}

	body, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("wal_archive: expected block, got %T", v)
	}

	out := defaultWALArchiveSpec()

	if rv, ok := body["enabled"]; ok {
		b, ok := rv.(bool)
		if !ok {
			return nil, fmt.Errorf("wal_archive.enabled: expected bool, got %T", rv)
		}

		out.Enabled = b
	}

	if rv, ok := body["strategy"]; ok {
		s, ok := rv.(string)
		if !ok {
			return nil, fmt.Errorf("wal_archive.strategy: expected string, got %T", rv)
		}

		if s != "" {
			out.Strategy = s
		}
	}

	if rv, ok := body["destination"]; ok {
		s, ok := rv.(string)
		if !ok {
			return nil, fmt.Errorf("wal_archive.destination: expected string, got %T", rv)
		}

		// Empty string keeps default — caller (cmdExpand)
		// resolves via strategy.DefaultDestination(scope, name).
		out.Destination = s
	}

	return out, nil
}

// validateWALArchiveSpec dispatches to the chosen strategy's
// Validate. Strategy-agnostic checks (enabled toggle) live here;
// strategy-specific checks (path absoluteness, dangerous roots,
// URI schemes) live in each strategy_<name>.go.
func validateWALArchiveSpec(spec *walArchiveSpec) error {
	if spec == nil {
		return nil
	}

	if !spec.Enabled {
		return nil
	}

	strategy, err := selectStrategy(spec.Strategy)
	if err != nil {
		return err
	}

	return strategy.Validate(spec)
}

// renderWALArchiveConf produces the bytes the plugin emits as
// the wal_archive_conf asset file. Postgres parses each include
// in alphabetical order and "last assignment wins" — these
// values can be overridden by the operator's pg_config block
// (which lands in voodu-99-overrides.conf, AFTER ours).
//
// What's written:
//
//   - wal_level = replica  (required for archive + streaming)
//   - archive_mode = on
//   - archive_command = <strategy-supplied>
//   - archive_timeout = 60  (force a WAL switch every minute
//     even when traffic is quiet — bounds the recovery RPO at
//     ~1 minute on a low-write cluster)
//
// archive_command is single-quoted with the postgres-conf escape
// rule (doubled quote) to defend against operator-controlled
// destination strings containing a quote character.
func renderWALArchiveConf(spec *walArchiveSpec, plan walArchivePlan) string {
	if spec == nil || !spec.Enabled {
		// Return the header alone so the asset still emits a
		// file (the wrapper symlinks everything voodu-*.conf —
		// keeping the file present + empty avoids surprising
		// "file disappeared" semantics on a re-apply that
		// flipped enabled false).
		return "# wal_archive disabled — no plugin-side overrides emitted\n"
	}

	// Render parameters in a stable alphabetical order — same
	// reasoning as renderPgOverridesConf (asset digest
	// stability across map iteration noise).
	lines := map[string]string{
		"archive_command": quotePgString(plan.ArchiveCommand),
		"archive_mode":    "on",
		"archive_timeout": "60",
		"wal_level":       "replica",
	}

	keys := make([]string, 0, len(lines))
	for k := range lines {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var b strings.Builder

	b.Grow(256 + len(keys)*48)

	b.WriteString("# generated by voodu-postgres — WAL archive defaults\n")
	b.WriteString("# (operator overrides via pg_config = { archive_command = ... } win)\n")

	for _, k := range keys {
		b.WriteString(k)
		b.WriteString(" = ")
		b.WriteString(lines[k])
		b.WriteByte('\n')
	}

	return b.String()
}

// errEmptyDestination is the canonical error a strategy returns
// when it has no DefaultDestination AND the operator left the
// field blank. Stripped of strategy-specific noise so we can wrap
// it consistently across strategies.
var errEmptyDestination = errors.New("destination is required (no strategy default available)")
