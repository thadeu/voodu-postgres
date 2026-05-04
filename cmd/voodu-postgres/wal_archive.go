// WAL archive: parse, validate, and render the postgres
// `archive_mode` + `archive_command` config that lets a primary
// stream WAL files into a separate volume for PITR + standby
// catch-up.
//
// # Default behaviour (M-P2)
//
//   - Enabled by default. Operator who doesn't want the extra
//     volume + I/O overhead opts out via
//     `wal_archive { enabled = false }`.
//
//   - Default mount path is /wal-archive (a sibling volume_claim
//     of the data dir). Operator can override the path but the
//     plugin always provisions the second volume_claim — there's
//     no way to "use my existing dir" because we want PITR
//     guarantees.
//
//   - Default archive_command writes locally with an idempotency
//     guard:
//
//	'test ! -f /wal-archive/%f && cp %p /wal-archive/%f'
//
//     This is the form postgres docs recommend (rejects rewrites
//     that would otherwise corrupt the archive on retry).
//
//   - Operator override for S3/R2/NFS goes via `pg_config = { ... }`
//     setting `archive_command` to an `aws s3 cp` / `rclone copy`
//     / etc. invocation. The env_from = ["aws/cli"] pattern from
//     the shared-config-bucket doc carries credentials.
//
// # Cleanup
//
// Plugin DOES NOT manage WAL retention. The archive grows
// unbounded under default settings. Operator declares a cronjob
// alongside the postgres resource (mirroring the
// voodu-redis backup automation pattern):
//
//	cronjob "clowk-lp" "wal-cleanup" {
//	  schedule = "0 2 * * *"
//	  image    = "postgres:16"
//	  command  = ["bash", "-c",
//	    "find /wal-archive -mmin +10080 -delete"]  # 7 days
//	  volumes  = ["voodu-clowk-lp-db-wal-archive-0:/wal-archive:rw"]
//	}
//
// (The volume name follows voodu's deterministic
// voodu-<scope>-<name>-<claim>-<ordinal> convention; only ordinal
// 0 archives in the single-primary M-P2 scope.)
//
// Future M-P2.5 may add `retention_days = 7` to the block and
// auto-emit the cronjob — deferred because operator cron pattern
// already covers most setups and adds zero plugin code.

package main

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// walArchiveSpec is the parsed view of the operator's
// `wal_archive { ... }` block. nil means "block not declared" —
// caller treats as "use defaults" (enabled, /wal-archive).
type walArchiveSpec struct {
	// Enabled toggles the entire archive subsystem. When false:
	//
	//   - No second volume_claim emitted.
	//   - No 00-wal-archive.conf file in the asset.
	//   - postgres runs with default archive_mode (off).
	//
	// Default true (declared via the spec defaults; nil means
	// "use defaults" which is enabled).
	Enabled bool

	// MountPath is the absolute path inside the container where
	// the second volume_claim is mounted. archive_command writes
	// here. Default /wal-archive.
	MountPath string
}

// walArchiveClaimName is the volume_claim name for the WAL
// archive mount. Hyphenated to match docker volume naming
// conventions (the controller composes
// voodu-<scope>-<name>-<claim>-<ordinal>; underscores would
// land in the docker volume name without issue, but hyphen is
// the convention across the platform).
const walArchiveClaimName = "wal-archive"

// walArchiveAssetKey is the asset file key + bind-mount path
// for the plugin-side WAL archive config. The mount path is
// chosen so it sorts BEFORE the operator's pg_overrides
// (voodu-99-overrides.conf) — postgres reads include_dir entries
// alphabetically and "last config wins", so operator overrides
// trump plugin defaults cleanly.
const (
	walArchiveAssetKey  = "wal_archive_conf"
	walArchiveMountPath = "/etc/postgresql/voodu-00-wal-archive.conf"
)

// defaultWALArchiveSpec returns the spec used when the operator
// omits the `wal_archive { }` block — i.e. archiving is on with
// the local /wal-archive default.
func defaultWALArchiveSpec() *walArchiveSpec {
	return &walArchiveSpec{
		Enabled:   true,
		MountPath: "/wal-archive",
	}
}

// parseWALArchiveSpec extracts the wal_archive sub-map from the
// operator's spec. Returns nil if the block was not declared
// (caller substitutes defaults).
//
// HCL block decoding gives us a map[string]any with the
// (single) bool/string body. Type errors surface here so apply
// fails loudly instead of silently coercing — same posture as
// parsePostgresSpec.
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

	if rv, ok := body["mount_path"]; ok {
		s, ok := rv.(string)
		if !ok {
			return nil, fmt.Errorf("wal_archive.mount_path: expected string, got %T", rv)
		}

		if s != "" {
			out.MountPath = s
		}
	}

	return out, nil
}

// validateWALArchiveSpec catches misconfigurations at apply time
// before any container spawns. Cheap, pure function — the cost
// of a wrong path here is "postgres won't start, all data
// inaccessible" so loud failure beats silent acceptance.
//
// Checks:
//
//  1. mount_path is non-empty when enabled.
//  2. mount_path is absolute (starts with /).
//  3. mount_path doesn't collide with PGDATA's mount root
//     (/var/lib/postgresql/data) — that would shadow the data
//     volume and corrupt the cluster.
func validateWALArchiveSpec(spec *walArchiveSpec) error {
	if spec == nil {
		return nil
	}

	if !spec.Enabled {
		return nil
	}

	if spec.MountPath == "" {
		return errors.New("wal_archive.mount_path is empty (must be an absolute path like /wal-archive)")
	}

	if !strings.HasPrefix(spec.MountPath, "/") {
		return fmt.Errorf("wal_archive.mount_path %q is not absolute (must start with /)", spec.MountPath)
	}

	// Forbid colliding with the data mount. PGDATA root is
	// /var/lib/postgresql/data; the wal_archive mounting
	// anywhere under that would shadow data files at runtime.
	const dataRoot = "/var/lib/postgresql/data"

	if spec.MountPath == dataRoot ||
		strings.HasPrefix(spec.MountPath, dataRoot+"/") {
		return fmt.Errorf("wal_archive.mount_path %q overlaps PGDATA root %s (would shadow data files)", spec.MountPath, dataRoot)
	}

	return nil
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
//   - archive_command = '<idempotent local cp>'
//   - archive_timeout = 60  (force a WAL switch every minute
//     even when traffic is quiet — bounds the recovery RPO at
//     ~1 minute on a low-write cluster)
//
// The string is single-quoted with the postgres-conf escape
// rule (doubled quote) to defend against operator-controlled
// MountPath containing a quote character (validation above
// doesn't reject that — pg-conf escape is enough).
func renderWALArchiveConf(spec *walArchiveSpec) string {
	if spec == nil || !spec.Enabled {
		// Return the header alone so the asset still emits a
		// file (the wrapper symlinks everything voodu-*.conf —
		// keeping the file present + empty avoids surprising
		// "file disappeared" semantics on a re-apply that
		// flipped enabled false).
		return "# wal_archive disabled — no plugin-side overrides emitted\n"
	}

	mount := spec.MountPath
	cmd := fmt.Sprintf("test ! -f %s/%%f && cp %%p %s/%%f", mount, mount)

	// Render parameters in a stable alphabetical order — same
	// reasoning as renderPgOverridesConf (asset digest
	// stability across map iteration noise).
	lines := map[string]string{
		"archive_command": quotePgString(cmd),
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
