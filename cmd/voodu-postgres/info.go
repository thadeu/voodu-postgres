// `vd postgres:info` — operator-facing snapshot of a postgres
// resource. Reads the manifest spec + config bucket, formats a
// human-readable text dump (default) or JSON (-o json).
//
// What it shows:
//
//   - Cluster topology: image, replicas count, primary ordinal,
//     primary FQDN, list of standby FQDNs.
//   - Auth: superuser name, replication user name. Passwords
//     redacted to first 8 chars + "..." (operator who needs
//     full value uses `vd config <ref> get POSTGRES_PASSWORD`).
//   - WAL archive: enabled/disabled + mount path.
//   - Linked consumers: count + list (refs that received
//     DATABASE_URL via vd postgres:link).
//   - Exposure: loopback (default) vs internet (after
//     vd postgres:expose).
//
// What it does NOT show (deferred):
//
//   - Live cluster state via psql (replication lag, WAL position,
//     pg_stat_replication output). Operator runs `vd shell <ref>
//     psql -c "SELECT * FROM pg_stat_replication"` for that.
//     Plugin-side psql exec adds container-shell complexity not
//     warranted at M-P4.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const infoHelp = `vd postgres:info — show cluster topology + bucket vars.

USAGE
  vd postgres:info <postgres-scope/name> [-o text|json]

ARGUMENTS
  <postgres-scope/name>   The postgres resource to inspect.

FLAGS
  -o, --output            'text' (default, human-readable)
                          'json' (machine-readable, jq-friendly)

DISPLAYS
  - image, replicas count, primary ordinal
  - primary FQDN + standby FQDNs (for psql/replication debugging)
  - super_user, database, port
  - replication_user
  - passwords (redacted: first 8 chars + ...)
  - wal_archive enabled/disabled + mount path
  - exposed (loopback vs 0.0.0.0)
  - linked consumers (refs that received DATABASE_URL via link)

DOES NOT DISPLAY (deferred)
  Live cluster state (replication lag, pg_stat_replication, WAL
  position) — operator runs:
    vd shell <postgres-ref> psql -U postgres -c "SELECT * FROM pg_stat_replication"

EXAMPLES
  vd postgres:info clowk-lp/db
  vd postgres:info clowk-lp/db -o json | jq '.linked_consumers'
  vd postgres:info clowk-lp/db -o json | jq -r '.primary_fqdn'
`

// cmdInfo prints a snapshot of the postgres resource.
func cmdInfo() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(infoHelp)
		return nil
	}

	positional, asJSON := parseInfoFlags(args)

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd postgres:info <postgres-scope/name> [-o json]")
	}

	scope, name := splitScopeName(positional[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", positional[0])
	}

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	client := newControllerClient(ctx.ControllerURL)

	spec, err := client.fetchSpec("statefulset", scope, name)
	if err != nil {
		return fmt.Errorf("describe %s: %w", positional[0], err)
	}

	config, err := client.fetchConfig(scope, name)
	if err != nil {
		return fmt.Errorf("config get %s: %w", positional[0], err)
	}

	snap := composeInfoSnapshot(scope, name, spec, config)

	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")

		return enc.Encode(snap)
	}

	fmt.Print(formatInfoText(snap))

	return nil
}

// infoSnapshot is the data-only shape — formatters consume it.
// Same struct serves text + JSON output paths so both forms
// stay in lockstep.
type infoSnapshot struct {
	Ref             string   `json:"ref"`
	Image           string   `json:"image"`
	Replicas        int      `json:"replicas"`
	PrimaryOrdinal  int      `json:"primary_ordinal"`
	PrimaryFQDN     string   `json:"primary_fqdn"`
	StandbyFQDNs    []string `json:"standby_fqdns,omitempty"`
	SuperUser       string   `json:"super_user"`
	Database        string   `json:"database"`
	Port            int      `json:"port"`
	ReplicationUser string   `json:"replication_user"`
	PasswordRedacted        string   `json:"password_redacted"`
	ReplicationPasswordRedacted string `json:"replication_password_redacted"`
	WALArchiveEnabled bool   `json:"wal_archive_enabled"`
	WALArchiveMountPath string `json:"wal_archive_mount_path,omitempty"`
	Exposed         bool     `json:"exposed"`
	BindAddress     string   `json:"bind_address"`
	LinkedConsumers []string `json:"linked_consumers,omitempty"`
}

// composeInfoSnapshot pulls the relevant fields out of the
// statefulset spec + config bucket. Pure function — no HTTP, no
// IO. Tested in info_test.go.
func composeInfoSnapshot(scope, name string, spec map[string]any, config map[string]string) infoSnapshot {
	user, db, port := readUserDBPort(spec)
	replicas := readReplicas(spec)
	primaryOrdinal := readPrimaryOrdinal(spec)
	exposed := config["PG_EXPOSE_PUBLIC"] == "true"

	bindAddr := "127.0.0.1 (loopback only — use vd postgres:expose to publish)"
	if exposed {
		bindAddr = "0.0.0.0 (internet-facing)"
	}

	snap := infoSnapshot{
		Ref:                         refOrName(scope, name),
		Replicas:                    replicas,
		PrimaryOrdinal:              primaryOrdinal,
		PrimaryFQDN:                 composePrimaryFQDN(scope, name, primaryOrdinal),
		SuperUser:                   user,
		Database:                    db,
		Port:                        port,
		PasswordRedacted:            redactPassword(config[passwordKey]),
		ReplicationPasswordRedacted: redactPassword(config[replicationPasswordKey]),
		Exposed:                     exposed,
		BindAddress:                 bindAddr,
		LinkedConsumers:             parseLinkedConsumers(config),
	}

	if v, ok := spec["image"].(string); ok {
		snap.Image = v
	}

	if env, ok := spec["env"].(map[string]any); ok {
		if v, ok := env["PG_REPLICATION_USER"].(string); ok {
			snap.ReplicationUser = v
		}
	}

	if snap.ReplicationUser == "" {
		snap.ReplicationUser = "replicator"
	}

	// Standby FQDNs: every ordinal except the primary.
	for i := 0; i < replicas; i++ {
		if i == primaryOrdinal {
			continue
		}

		snap.StandbyFQDNs = append(snap.StandbyFQDNs, composePrimaryFQDN(scope, name, i))
	}

	// WAL archive detection: look for the wal-archive
	// volume_claim. Plugin always emits the asset bind, but the
	// claim only appears when wal_archive { enabled = true }
	// (the default).
	if claims, ok := spec["volume_claims"].([]any); ok {
		for _, c := range claims {
			m, ok := c.(map[string]any)
			if !ok {
				continue
			}

			if m["name"] == walArchiveClaimName {
				snap.WALArchiveEnabled = true
				if mp, ok := m["mount_path"].(string); ok {
					snap.WALArchiveMountPath = mp
				}
				break
			}
		}
	}

	return snap
}

// readPrimaryOrdinal pulls PG_PRIMARY_ORDINAL from the
// statefulset env (where the plugin emits it). Default 0.
func readPrimaryOrdinal(spec map[string]any) int {
	env, ok := spec["env"].(map[string]any)
	if !ok {
		return 0
	}

	v, ok := env["PG_PRIMARY_ORDINAL"].(string)
	if !ok || v == "" {
		return 0
	}

	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
		return n
	}

	return 0
}

// redactPassword shows the first 8 chars + "..." so an operator
// can confirm a value is set without leaking the secret. Empty
// → "<unset>".
func redactPassword(pw string) string {
	if pw == "" {
		return "<unset>"
	}

	if len(pw) <= 8 {
		return strings.Repeat("*", len(pw))
	}

	return pw[:8] + "..."
}

// formatInfoText renders the snapshot as human-readable text.
// Layout chosen to fit under 80 cols — easy on `vd info | less`.
func formatInfoText(s infoSnapshot) string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("postgres %s\n", s.Ref))
	b.WriteString(strings.Repeat("─", min(80, len("postgres "+s.Ref))))
	b.WriteString("\n")

	b.WriteString(fmt.Sprintf("  image           %s\n", s.Image))
	b.WriteString(fmt.Sprintf("  replicas        %d (primary=pod-%d)\n", s.Replicas, s.PrimaryOrdinal))
	b.WriteString(fmt.Sprintf("  primary         %s:%d\n", s.PrimaryFQDN, s.Port))

	if len(s.StandbyFQDNs) > 0 {
		b.WriteString("  standbys\n")
		for _, fqdn := range s.StandbyFQDNs {
			b.WriteString(fmt.Sprintf("                  %s:%d\n", fqdn, s.Port))
		}
	}

	b.WriteString("\n")
	b.WriteString(fmt.Sprintf("  database        %s\n", s.Database))
	b.WriteString(fmt.Sprintf("  super_user      %s (password=%s)\n", s.SuperUser, s.PasswordRedacted))
	b.WriteString(fmt.Sprintf("  replication     %s (password=%s)\n", s.ReplicationUser, s.ReplicationPasswordRedacted))

	b.WriteString("\n")

	walState := "disabled"
	if s.WALArchiveEnabled {
		walState = "enabled @ " + s.WALArchiveMountPath
	}

	b.WriteString(fmt.Sprintf("  wal_archive     %s\n", walState))
	b.WriteString(fmt.Sprintf("  bind            %s\n", s.BindAddress))

	if len(s.LinkedConsumers) > 0 {
		b.WriteString(fmt.Sprintf("\n  linked consumers (%d)\n", len(s.LinkedConsumers)))

		for _, c := range s.LinkedConsumers {
			b.WriteString(fmt.Sprintf("                  %s\n", c))
		}
	} else {
		b.WriteString("\n  linked consumers (none) — use vd postgres:link to wire one\n")
	}

	return b.String()
}

// parseInfoFlags pulls the -o/--output flag value out. Default
// is text; only "json" flips to structured output. The value
// after -o is ALWAYS consumed (even if not "json") so that
// `vd postgres:info ref -o text` doesn't accidentally treat
// "text" as a positional arg.
func parseInfoFlags(args []string) (positional []string, asJSON bool) {
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-o", "--output":
			if i+1 < len(args) {
				if strings.EqualFold(args[i+1], "json") {
					asJSON = true
				}
				i++ // always consume value, even if not json
			}
		case "--output=json", "-o=json":
			asJSON = true
		case "-h", "--help":
			// handled
		default:
			positional = append(positional, args[i])
		}
	}

	return positional, asJSON
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}
