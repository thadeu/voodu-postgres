// HCL spec parsing + apply-time validations for the
// `postgres "scope" "name" { ... }` block.
//
// This is the M-P0 surface — pure parse + validate, no runtime
// emission yet. cmdExpand uses parsePostgresSpec to extract
// fields from the operator's spec map and validatePostgresSpec
// to surface configuration errors at apply time (before any
// container is spawned).
//
// Field defaults match the production-ready posture: postgres:latest
// (evergreen — operator pins major in their HCL when stability
// matters), 1 replica (single primary), built-in "postgres" admin
// user/database. Operator override anything via HCL — alias contract.
//
// Replicas semantics:
//
//   - replicas = 1     single primary, no streaming replication
//   - replicas = N>1   1 primary + (N-1) standbys via WAL streaming
//
// Replicas <= 0 is rejected (postgres needs at least one pod to
// run). The HA-quorum semantics that voodu-redis sentinel had
// don't apply here — postgres failover is manual via
// `vd postgres:failover` and doesn't need a coordination quorum.

package main

import (
	"errors"
	"fmt"
	"strings"
)

// postgresSpec is the parsed view of the operator's HCL block
// fields the plugin owns. Statefulset-passthrough attrs (env,
// volumes, ports, command, etc.) flow through mergeSpec untouched
// — this struct only carries the postgres-specific knobs.
type postgresSpec struct {
	// Image is the postgres docker image. Default postgres:latest.
	// Operator typically pins a major (`image = "postgres:16"`) for
	// stability or brings a custom image with extensions baked
	// (pgvector, postgis) — same image must work as primary AND
	// standby.
	Image string

	// Replicas is the total pod count: 1 primary + (replicas-1)
	// standbys. Single primary when replicas=1; streaming
	// replication when replicas>1. Default 1.
	Replicas int

	// Database is the initial database created by initdb on first
	// primary boot. Default "postgres" (the postgres-installed
	// default). Operator overrides to something app-specific:
	// `database = "appdata"`. Validated as a postgres identifier.
	Database string

	// User is the SUPERUSER created by initdb. Default "postgres".
	// Distinct from PG_REPLICATION_USER (separate user with only
	// REPLICATION role, security best practice). Operator can
	// override but most apps use "postgres".
	User string

	// Password is the SUPERUSER password. Default "" (auto-generated
	// 32-char URL-safe string at first boot in M-P1). Operator
	// override is rare — typical flow is auto-gen + rotate via
	// `vd postgres:new-password`. Setting this in HCL means the
	// secret lives in plaintext in the repo; suitable for dev/test
	// only.
	Password string

	// Port is the postgres listen port. Default 5432. Statefulset
	// will bind 127.0.0.1:<port> by default; `vd postgres:expose`
	// (M-P4) flips to 0.0.0.0:<port>.
	Port int

	// InitdbLocale maps to initdb --locale. Default "C.UTF-8".
	// Honoured only on first primary boot (initdb is destructive).
	InitdbLocale string

	// InitdbEncoding maps to initdb --encoding. Default "UTF8".
	// Honoured only on first primary boot.
	InitdbEncoding string

	// PgConfig holds postgresql.conf overrides emitted into
	// /etc/postgresql/postgresql.conf.d/voodu-overrides.conf at
	// each boot. Keys validated as postgres parameter names
	// ([a-z][a-z0-9_]*) — anti-injection guard against
	// "max_connections = 200\nshared_preload_libraries = ..."
	// type tricks. Values stringified as-is.
	PgConfig map[string]any

	// Extensions is a list of extension names that the operator
	// declares the cluster needs. Validated as postgres identifiers
	// at apply time (anti-injection guard).
	//
	// IN M-P1: parsed and validated, NOT auto-installed at runtime.
	// Operators install via app migrations (Rails:
	// `enable_extension :pgvector`) or manually via psql.
	//
	// In M-P4: `vd postgres:exec "CREATE EXTENSION ..."` ships,
	// and this field becomes the source of truth for an
	// `extensions:install` subcommand the operator runs explicitly.
	// Auto-install on every boot was rejected because it conflicts
	// with the typical Rails/Django flow (the migration is the
	// source of truth) and timing the run inside the wrapper
	// requires a two-phase boot (postgres up → psql → restart) that
	// adds startup latency for no operator benefit.
	Extensions []string

	// WALArchive carries the operator's `wal_archive { ... }`
	// block. nil means the operator omitted the block entirely;
	// the caller substitutes defaultWALArchiveSpec() (enabled,
	// /wal-archive). M-P2 introduces this — see wal_archive.go
	// for the parser, validator, and conf renderer.
	WALArchive *walArchiveSpec

	// ReplicationUser is the postgres ROLE used for streaming
	// replication. Default "replicator". Distinct from the
	// superuser (User field) by security best practice — the
	// replicator role only carries the REPLICATION attribute,
	// not full DDL/DML powers, so a leaked replication password
	// can't drop your tables.
	//
	// The matching password is auto-generated and never
	// surfaced in HCL — it lives in the config bucket
	// (POSTGRES_REPLICATION_PASSWORD), generated on first
	// apply. See replication.go for the lifecycle.
	ReplicationUser string
}

// parsePostgresSpec extracts the plugin-owned fields from the
// operator's spec map. Unknown fields are NOT carried here —
// they flow through to the statefulset via mergeSpec. Returns
// the populated spec with defaults filled in for absent fields.
//
// Type errors (replicas as string, etc.) surface as parse errors
// so apply fails loudly instead of silently coercing to garbage.
func parsePostgresSpec(operatorSpec map[string]any) (*postgresSpec, error) {
	spec := &postgresSpec{
		Image:           defaultImage,
		Replicas:        1,
		Database:        "postgres",
		User:            "postgres",
		Password:        "",
		Port:            5432,
		InitdbLocale:    "C.UTF-8",
		InitdbEncoding:  "UTF8",
		ReplicationUser: "replicator",
	}

	if v, present := operatorSpec["image"]; present {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("image: expected string, got %T", v)
		}

		if s != "" {
			spec.Image = s
		}
	}

	if v, present := operatorSpec["replicas"]; present {
		n, ok := asInt(v)
		if !ok {
			return nil, fmt.Errorf("replicas: expected integer, got %T", v)
		}

		spec.Replicas = n
	}

	if v, present := operatorSpec["database"]; present {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("database: expected string, got %T", v)
		}

		if s != "" {
			spec.Database = s
		}
	}

	if v, present := operatorSpec["user"]; present {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("user: expected string, got %T", v)
		}

		if s != "" {
			spec.User = s
		}
	}

	if v, present := operatorSpec["password"]; present {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("password: expected string, got %T", v)
		}

		// Empty string === unset; auto-gen wins. Don't error — keeps
		// `password = ""` legal as a way to revert override.
		spec.Password = s
	}

	if v, present := operatorSpec["port"]; present {
		n, ok := asInt(v)
		if !ok {
			return nil, fmt.Errorf("port: expected integer, got %T", v)
		}

		spec.Port = n
	}

	if v, present := operatorSpec["initdb_locale"]; present {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("initdb_locale: expected string, got %T", v)
		}

		if s != "" {
			spec.InitdbLocale = s
		}
	}

	if v, present := operatorSpec["initdb_encoding"]; present {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("initdb_encoding: expected string, got %T", v)
		}

		if s != "" {
			spec.InitdbEncoding = s
		}
	}

	if v, present := operatorSpec["pg_config"]; present {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("pg_config: expected map, got %T", v)
		}

		spec.PgConfig = m
	}

	if v, present := operatorSpec["extensions"]; present {
		list, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("extensions: expected list, got %T", v)
		}

		out := make([]string, 0, len(list))

		for i, item := range list {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("extensions[%d]: expected string, got %T", i, item)
			}

			out = append(out, s)
		}

		spec.Extensions = out
	}

	if v, present := operatorSpec["replication_user"]; present {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("replication_user: expected string, got %T", v)
		}

		if s != "" {
			spec.ReplicationUser = s
		}
	}

	wal, err := parseWALArchiveSpec(operatorSpec)
	if err != nil {
		return nil, err
	}

	spec.WALArchive = wal

	return spec, nil
}

// validatePostgresSpec runs apply-time semantic checks. Pure
// function; no I/O. Errors surface to the operator before any
// container is created so misconfigurations don't crash a pod
// halfway into a deploy.
//
// Checks:
//
//  1. replicas >= 1 (at least one pod)
//  2. database is a valid postgres identifier
//  3. user is a valid postgres identifier
//  4. port is in 1..65535
//  5. each extension name is a valid postgres identifier
//  6. each pg_config key is a valid postgres parameter name
//     (anti-injection — values are passed through to .conf file)
func validatePostgresSpec(spec *postgresSpec) error {
	if spec == nil {
		return errors.New("nil spec")
	}

	if spec.Replicas < 1 {
		return fmt.Errorf("replicas = %d not allowed (must be >= 1; use 1 for single primary, >=2 for replicated cluster)", spec.Replicas)
	}

	if !isValidPostgresIdentifier(spec.Database) {
		return fmt.Errorf("database %q is not a valid postgres identifier (must be 1-63 chars, start with letter or underscore, contain only letters/digits/underscores)", spec.Database)
	}

	if !isValidPostgresIdentifier(spec.User) {
		return fmt.Errorf("user %q is not a valid postgres identifier (must be 1-63 chars, start with letter or underscore, contain only letters/digits/underscores)", spec.User)
	}

	if !isValidPostgresIdentifier(spec.ReplicationUser) {
		return fmt.Errorf("replication_user %q is not a valid postgres identifier (must be 1-63 chars, start with letter or underscore, contain only letters/digits/underscores)", spec.ReplicationUser)
	}

	if spec.ReplicationUser == spec.User {
		return fmt.Errorf("replication_user must differ from user (%q) — separate roles is a security best practice; the replicator role should only carry REPLICATION, not full superuser powers", spec.User)
	}

	if spec.Port < 1 || spec.Port > 65535 {
		return fmt.Errorf("port = %d not allowed (must be 1..65535)", spec.Port)
	}

	for i, ext := range spec.Extensions {
		if !isValidPostgresIdentifier(ext) {
			return fmt.Errorf("extensions[%d] = %q is not a valid postgres identifier (would fail CREATE EXTENSION at boot)", i, ext)
		}
	}

	for key := range spec.PgConfig {
		if !isValidPgConfigKey(key) {
			return fmt.Errorf("pg_config key %q is not a valid postgres parameter name (must match [a-z][a-z0-9_]*; anti-injection guard)", key)
		}
	}

	if err := validateWALArchiveSpec(spec.WALArchive); err != nil {
		return err
	}

	return nil
}

// isValidPgConfigKey gates pg_config map keys. Postgres parameter
// names are conventionally lowercase letters + digits + underscore,
// starting with a letter (max_connections, shared_buffers,
// log_min_messages, etc.). The strict regex doubles as an
// anti-injection guard: keys flow into a .conf file as `key = value`
// lines, so a key containing `\n` or `=` could smuggle additional
// directives. Values are stringified separately and don't pose the
// same risk because postgres.conf only honours `key = value` lines
// (a value with embedded newline gets ignored, not executed).
func isValidPgConfigKey(s string) bool {
	if len(s) == 0 {
		return false
	}

	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			// always allowed
		case r == '_':
			// always allowed
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}

	return true
}

// isValidPostgresIdentifier checks that a string is a valid
// unquoted postgres identifier per the SQL spec:
//
//   - Length 1..63 (NAMEDATALEN-1; longer names get truncated
//     silently which is a footgun)
//   - First char: letter (ASCII a-z, A-Z) or underscore
//   - Subsequent chars: letters, digits, or underscores
//   - No reserved words (we don't enforce — postgres lets you
//     use them with double-quoting, but our default user/db
//     aren't quoted at the SQL layer)
//
// We deliberately don't allow non-ASCII letters even though
// postgres does — keeps the surface predictable across locales
// and avoids encoding edge cases in shell scripts that use these
// names.
func isValidPostgresIdentifier(s string) bool {
	if len(s) < 1 || len(s) > 63 {
		return false
	}

	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			// always allowed
		case r >= 'A' && r <= 'Z':
			// always allowed
		case r == '_':
			// always allowed
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}

	return true
}

// asInt accepts the JSON-decoder's flavours of "integer-shaped
// number". encoding/json gives float64 for any number; some
// HCL→JSON paths preserve int. Returns false for non-numeric
// inputs so the caller can surface a typed error.
func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		if n == float64(int(n)) {
			return int(n), true
		}

		return 0, false
	}

	return 0, false
}

// refOrName formats a (scope, name) pair as the canonical voodu
// reference string. Same helper voodu-redis uses; duplicated here
// to keep the plugin self-contained (no shared SDK dep yet).
func refOrName(scope, name string) string {
	if scope == "" {
		return name
	}

	return scope + "/" + name
}

// stripPluginOwnedFields removes the keys parsePostgresSpec
// consumed from the operator's raw spec map, so they don't leak
// into the downstream statefulset wire shape. Statefulset doesn't
// know what `database`, `user`, `password`, `port`, `pg_config`,
// etc. mean — they're plugin concepts.
//
// `port` IS stripped because composeStatefulsetDefaults has
// already translated it into the statefulset's `ports` list (the
// real statefulset wire shape). Leaving `port` in the merged map
// would expose a plugin field on the statefulset spec that the
// controller doesn't recognise.
//
// `image` and `replicas` STAY in the merged spec — they're real
// statefulset fields and the controller needs them.
func stripPluginOwnedFields(merged map[string]any) {
	delete(merged, "database")
	delete(merged, "user")
	delete(merged, "password")
	delete(merged, "port")
	delete(merged, "initdb_locale")
	delete(merged, "initdb_encoding")
	delete(merged, "pg_config")
	delete(merged, "extensions")
	delete(merged, "wal_archive")
	delete(merged, "replication_user")
}

// looksLikePostgresIdentifier is a softer check used in error
// messages — returns a hint about WHY a candidate failed. Useful
// when the validation error needs to be actionable.
func describeIdentifierProblem(s string) string {
	if len(s) == 0 {
		return "empty"
	}

	if len(s) > 63 {
		return "too long (max 63 chars)"
	}

	if s[0] >= '0' && s[0] <= '9' {
		return "starts with a digit"
	}

	for _, r := range s {
		if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_", r) {
			return fmt.Sprintf("contains invalid char %q", r)
		}
	}

	return "unknown"
}
