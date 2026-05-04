// Tests for the M-P0 surface: parsePostgresSpec, validatePostgresSpec,
// isValidPostgresIdentifier, asInt and the plugin-owned-field stripper.
//
// These pin the contract every later milestone has to honour:
//   - defaults are stable (postgres:latest, replicas=1, database=postgres,
//     user=postgres) — operators have these baked into their HCL
//     expectations
//   - parse errors are typed (wrong-type fields don't silently coerce)
//   - validation rejects garbage identifiers and bad replica counts
//
// Apply-time errors are operator-facing — message wording is part of
// the contract, so the assertions deliberately match substrings of
// the human text. Changes to the messages should be deliberate.

package main

import (
	"strings"
	"testing"
)

func TestParsePostgresSpec_DefaultsWhenEmpty(t *testing.T) {
	spec, err := parsePostgresSpec(nil)
	if err != nil {
		t.Fatalf("parse nil spec: %v", err)
	}

	if spec.Image != "postgres:latest" {
		t.Errorf("default image: got %q, want postgres:latest", spec.Image)
	}

	if spec.Replicas != 1 {
		t.Errorf("default replicas: got %d, want 1", spec.Replicas)
	}

	if spec.Database != "postgres" {
		t.Errorf("default database: got %q, want postgres", spec.Database)
	}

	if spec.User != "postgres" {
		t.Errorf("default user: got %q, want postgres", spec.User)
	}

	if spec.Password != "" {
		t.Errorf("default password: got %q, want empty (auto-gen sentinel)", spec.Password)
	}

	if spec.Port != 5432 {
		t.Errorf("default port: got %d, want 5432", spec.Port)
	}

	if spec.InitdbLocale != "C.UTF-8" {
		t.Errorf("default initdb_locale: got %q, want C.UTF-8", spec.InitdbLocale)
	}

	if spec.InitdbEncoding != "UTF8" {
		t.Errorf("default initdb_encoding: got %q, want UTF8", spec.InitdbEncoding)
	}

	if spec.PgConfig != nil {
		t.Errorf("default pg_config: got %v, want nil", spec.PgConfig)
	}

	if spec.Extensions != nil {
		t.Errorf("default extensions: got %v, want nil", spec.Extensions)
	}
}

func TestParsePostgresSpec_DefaultsWhenEmptyMap(t *testing.T) {
	spec, err := parsePostgresSpec(map[string]any{})
	if err != nil {
		t.Fatalf("parse empty map: %v", err)
	}

	if spec.Image != "postgres:latest" {
		t.Errorf("default image: got %q", spec.Image)
	}
}

func TestParsePostgresSpec_OperatorOverridesAll(t *testing.T) {
	in := map[string]any{
		"image":           "postgres:16-alpine",
		"replicas":        3,
		"database":        "appdata",
		"user":            "appuser",
		"password":        "s3cret",
		"port":            5433,
		"initdb_locale":   "pt_BR.UTF-8",
		"initdb_encoding": "LATIN1",
		"pg_config": map[string]any{
			"max_connections": 200,
			"shared_buffers":  "256MB",
		},
		"extensions": []any{"pgvector", "pg_stat_statements"},
	}

	spec, err := parsePostgresSpec(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if spec.Image != "postgres:16-alpine" {
		t.Errorf("image: got %q", spec.Image)
	}

	if spec.Replicas != 3 {
		t.Errorf("replicas: got %d", spec.Replicas)
	}

	if spec.Database != "appdata" {
		t.Errorf("database: got %q", spec.Database)
	}

	if spec.User != "appuser" {
		t.Errorf("user: got %q", spec.User)
	}

	if spec.Password != "s3cret" {
		t.Errorf("password: got %q", spec.Password)
	}

	if spec.Port != 5433 {
		t.Errorf("port: got %d", spec.Port)
	}

	if spec.InitdbLocale != "pt_BR.UTF-8" {
		t.Errorf("initdb_locale: got %q", spec.InitdbLocale)
	}

	if spec.InitdbEncoding != "LATIN1" {
		t.Errorf("initdb_encoding: got %q", spec.InitdbEncoding)
	}

	if len(spec.PgConfig) != 2 {
		t.Errorf("pg_config: got %d keys, want 2", len(spec.PgConfig))
	}

	if len(spec.Extensions) != 2 || spec.Extensions[0] != "pgvector" || spec.Extensions[1] != "pg_stat_statements" {
		t.Errorf("extensions: got %v, want [pgvector pg_stat_statements]", spec.Extensions)
	}
}

func TestParsePostgresSpec_EmptyStringKeepsDefault(t *testing.T) {
	// Empty string image/database/user/initdb_* should NOT clobber
	// the default. Operators write `image = ""` accidentally; we
	// want that to surface as the default rather than booting
	// postgres with an unparseable image ref.
	//
	// Password is the deliberate exception — `password = ""` is
	// how an operator reverts a previous override back to auto-gen,
	// so empty stays empty (sentinel for "auto-gen at boot").
	in := map[string]any{
		"image":           "",
		"database":        "",
		"user":            "",
		"initdb_locale":   "",
		"initdb_encoding": "",
	}

	spec, err := parsePostgresSpec(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if spec.Image != "postgres:latest" {
		t.Errorf("empty image should keep default, got %q", spec.Image)
	}

	if spec.Database != "postgres" {
		t.Errorf("empty database should keep default, got %q", spec.Database)
	}

	if spec.User != "postgres" {
		t.Errorf("empty user should keep default, got %q", spec.User)
	}

	if spec.InitdbLocale != "C.UTF-8" {
		t.Errorf("empty initdb_locale should keep default, got %q", spec.InitdbLocale)
	}

	if spec.InitdbEncoding != "UTF8" {
		t.Errorf("empty initdb_encoding should keep default, got %q", spec.InitdbEncoding)
	}
}

func TestParsePostgresSpec_ReplicationUserDefault(t *testing.T) {
	spec, err := parsePostgresSpec(nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if spec.ReplicationUser != "replicator" {
		t.Errorf("default replication_user: got %q, want replicator", spec.ReplicationUser)
	}
}

func TestParsePostgresSpec_ReplicationUserOverride(t *testing.T) {
	spec, err := parsePostgresSpec(map[string]any{
		"replication_user": "repluser",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if spec.ReplicationUser != "repluser" {
		t.Errorf("override lost: got %q", spec.ReplicationUser)
	}
}

func TestParsePostgresSpec_ReplicationUserEmptyKeepsDefault(t *testing.T) {
	spec, err := parsePostgresSpec(map[string]any{
		"replication_user": "",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if spec.ReplicationUser != "replicator" {
		t.Errorf("empty replication_user should keep default, got %q", spec.ReplicationUser)
	}
}

func TestValidatePostgresSpec_ReplicationUserMustDifferFromUser(t *testing.T) {
	// Security best practice: separate roles. Same name on both
	// would mean the replicator role is the superuser, defeating
	// the principle of least privilege.
	spec, _ := parsePostgresSpec(map[string]any{
		"user":             "myapp",
		"replication_user": "myapp",
	})

	err := validatePostgresSpec(spec)
	if err == nil {
		t.Fatal("expected validation error when replication_user equals user")
	}

	if !strings.Contains(err.Error(), "replication_user must differ") {
		t.Errorf("error should explain conflict, got: %v", err)
	}
}

func TestValidatePostgresSpec_ReplicationUserBadIdentifier(t *testing.T) {
	spec, _ := parsePostgresSpec(map[string]any{
		"replication_user": "bad-name",
	})

	if err := validatePostgresSpec(spec); err == nil {
		t.Error("expected validation error for hyphenated replication_user")
	}
}

func TestParsePostgresSpec_PasswordEmptyMeansAutoGen(t *testing.T) {
	// `password = ""` is the documented way to revert override
	// and let the plugin auto-gen at boot. Spec field stays empty;
	// M-P1 will treat empty as the auto-gen sentinel.
	spec, err := parsePostgresSpec(map[string]any{"password": ""})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if spec.Password != "" {
		t.Errorf("password = \"\" should stay empty, got %q", spec.Password)
	}
}

func TestParsePostgresSpec_ReplicasAcceptsNumberFlavours(t *testing.T) {
	// JSON decoder produces float64 for any number, but some HCL→JSON
	// paths preserve int. asInt has to swallow both transparently.
	cases := []struct {
		name string
		val  any
		want int
	}{
		{"int", 5, 5},
		{"int64", int64(5), 5},
		{"float64-whole", float64(5), 5},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := parsePostgresSpec(map[string]any{"replicas": tc.val})
			if err != nil {
				t.Fatalf("parse: %v", err)
			}

			if spec.Replicas != tc.want {
				t.Errorf("replicas: got %d, want %d", spec.Replicas, tc.want)
			}
		})
	}
}

func TestParsePostgresSpec_ReplicasRejectsFractional(t *testing.T) {
	_, err := parsePostgresSpec(map[string]any{"replicas": 1.5})
	if err == nil {
		t.Fatalf("expected error for fractional replicas")
	}

	if !strings.Contains(err.Error(), "replicas") {
		t.Errorf("error should mention 'replicas', got: %v", err)
	}
}

func TestParsePostgresSpec_TypeErrors(t *testing.T) {
	cases := []struct {
		name string
		spec map[string]any
		want string
	}{
		{"image-not-string", map[string]any{"image": 42}, "image"},
		{"replicas-as-string", map[string]any{"replicas": "three"}, "replicas"},
		{"database-not-string", map[string]any{"database": []any{"a"}}, "database"},
		{"user-not-string", map[string]any{"user": map[string]any{}}, "user"},
		{"password-not-string", map[string]any{"password": 1234}, "password"},
		{"port-as-string", map[string]any{"port": "5432"}, "port"},
		{"initdb_locale-not-string", map[string]any{"initdb_locale": []any{}}, "initdb_locale"},
		{"initdb_encoding-not-string", map[string]any{"initdb_encoding": 99}, "initdb_encoding"},
		{"pg_config-not-map", map[string]any{"pg_config": []any{}}, "pg_config"},
		{"extensions-not-list", map[string]any{"extensions": "pgvector"}, "extensions"},
		{"extensions-item-not-string", map[string]any{"extensions": []any{42}}, "extensions[0]"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parsePostgresSpec(tc.spec)
			if err == nil {
				t.Fatalf("expected typed error")
			}

			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should mention %q, got: %v", tc.want, err)
			}
		})
	}
}

func TestValidatePostgresSpec_NilRejected(t *testing.T) {
	if err := validatePostgresSpec(nil); err == nil {
		t.Fatalf("expected error for nil spec")
	}
}

func TestValidatePostgresSpec_AcceptsDefaults(t *testing.T) {
	spec, err := parsePostgresSpec(nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if err := validatePostgresSpec(spec); err != nil {
		t.Fatalf("default spec should validate, got: %v", err)
	}
}

func TestValidatePostgresSpec_ReplicasMustBePositive(t *testing.T) {
	cases := []int{0, -1, -100}

	for _, n := range cases {
		spec, err := parsePostgresSpec(map[string]any{"replicas": n})
		if err != nil {
			t.Fatalf("parse replicas=%d: %v", n, err)
		}

		err = validatePostgresSpec(spec)
		if err == nil {
			t.Errorf("replicas=%d should fail validation", n)
			continue
		}

		if !strings.Contains(err.Error(), "replicas") {
			t.Errorf("error should mention replicas, got: %v", err)
		}
	}
}

func TestValidatePostgresSpec_ReplicasOneIsOK(t *testing.T) {
	spec, err := parsePostgresSpec(map[string]any{"replicas": 1})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if err := validatePostgresSpec(spec); err != nil {
		t.Errorf("replicas=1 (single primary) should be valid: %v", err)
	}
}

func TestValidatePostgresSpec_LargeReplicaCountOK(t *testing.T) {
	// Cap is implicit (replicas is int); reasonable cluster sizes
	// must validate. 7 = 1 primary + 6 standbys, plausible read-heavy
	// workload.
	spec, err := parsePostgresSpec(map[string]any{"replicas": 7})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if err := validatePostgresSpec(spec); err != nil {
		t.Errorf("replicas=7 should validate: %v", err)
	}
}

func TestValidatePostgresSpec_PortRange(t *testing.T) {
	bad := []int{0, -1, 65536, 100000}
	good := []int{1, 1024, 5432, 5433, 65535}

	for _, p := range bad {
		spec, err := parsePostgresSpec(map[string]any{"port": p})
		if err != nil {
			t.Fatalf("parse port=%d: %v", p, err)
		}

		if err := validatePostgresSpec(spec); err == nil {
			t.Errorf("port=%d should fail validation", p)
		}
	}

	for _, p := range good {
		spec, err := parsePostgresSpec(map[string]any{"port": p})
		if err != nil {
			t.Fatalf("parse port=%d: %v", p, err)
		}

		if err := validatePostgresSpec(spec); err != nil {
			t.Errorf("port=%d should validate, got: %v", p, err)
		}
	}
}

func TestValidatePostgresSpec_ExtensionIdentifiers(t *testing.T) {
	// Each extension name has to be a valid postgres identifier —
	// names get spliced into `CREATE EXTENSION IF NOT EXISTS <name>`
	// at boot, so a malformed name fails the SQL outright.
	badCases := [][]string{
		{"pg-vector"},                  // dash
		{"pgvector; DROP TABLE users"}, // injection attempt
		{""},                           // empty
		{"123vector"},                  // leading digit
		{"pgvector", "bad name"},       // second item bad
	}

	for _, exts := range badCases {
		spec, _ := parsePostgresSpec(map[string]any{
			"extensions": toAnyList(exts),
		})

		if err := validatePostgresSpec(spec); err == nil {
			t.Errorf("extensions %v should fail validation", exts)
		}
	}

	good := []string{"pgvector", "pg_stat_statements", "postgis", "uuid_ossp", "_internal"}

	spec, _ := parsePostgresSpec(map[string]any{
		"extensions": toAnyList(good),
	})

	if err := validatePostgresSpec(spec); err != nil {
		t.Errorf("extensions %v should validate, got: %v", good, err)
	}
}

func TestValidatePostgresSpec_PgConfigKeysAntiInjection(t *testing.T) {
	// Anti-injection: a key containing `\n` or `=` could smuggle
	// extra directives into postgresql.conf. Reject them at parse
	// time so an attacker who controls HCL can't shape the .conf.
	badKeys := []string{
		"max_connections\nshared_preload_libraries", // newline
		"max_connections=200; max_wal_size",          // = in key
		"Max_Connections",                            // uppercase (postgres params are lowercase by convention)
		"max-connections",                            // dash
		"123_param",                                  // leading digit
		"",                                           // empty
		"max connections",                            // space
	}

	for _, k := range badKeys {
		spec, _ := parsePostgresSpec(map[string]any{
			"pg_config": map[string]any{k: "anything"},
		})

		if err := validatePostgresSpec(spec); err == nil {
			t.Errorf("pg_config key %q should fail validation", k)
		}
	}

	goodKeys := []string{
		"max_connections",
		"shared_buffers",
		"work_mem",
		"effective_cache_size",
		"log_min_messages",
		"a",
		"a1",
	}

	for _, k := range goodKeys {
		spec, _ := parsePostgresSpec(map[string]any{
			"pg_config": map[string]any{k: "value"},
		})

		if err := validatePostgresSpec(spec); err != nil {
			t.Errorf("pg_config key %q should validate, got: %v", k, err)
		}
	}
}

func TestValidatePostgresSpec_PgConfigValuesPassThrough(t *testing.T) {
	// Values aren't validated — postgres handles type coercion when
	// reading the .conf. We only guard the key shape (anti-injection).
	spec, _ := parsePostgresSpec(map[string]any{
		"pg_config": map[string]any{
			"max_connections":      200,            // int
			"shared_buffers":       "256MB",        // string
			"log_connections":      true,           // bool
			"effective_cache_size": "1GB",          // string
		},
	})

	if err := validatePostgresSpec(spec); err != nil {
		t.Errorf("mixed-type values should pass: %v", err)
	}
}

func TestIsValidPgConfigKey(t *testing.T) {
	good := []string{"a", "max_connections", "_x", "a1", "log_min_messages"}
	bad := []string{"", "1abc", "Max_Connections", "max-connections", "max=val", "max connections", "max\nval"}

	for _, s := range good {
		if !isValidPgConfigKey(s) {
			t.Errorf("%q should be valid", s)
		}
	}

	for _, s := range bad {
		if isValidPgConfigKey(s) {
			t.Errorf("%q should be invalid", s)
		}
	}
}

// toAnyList converts a []string to []any for use in HCL spec maps
// (the parser sees []any after JSON decode).
func toAnyList(in []string) []any {
	out := make([]any, len(in))
	for i, s := range in {
		out[i] = s
	}

	return out
}

func TestValidatePostgresSpec_BadIdentifiers(t *testing.T) {
	cases := []struct {
		name  string
		field string
		val   string
	}{
		{"empty-database", "database", ""},
		{"empty-user", "user", ""},
		{"database-leading-digit", "database", "1app"},
		{"user-leading-digit", "user", "9user"},
		{"database-dash", "database", "my-app"},
		{"user-space", "user", "my user"},
		{"database-too-long", "database", strings.Repeat("a", 64)},
		{"user-too-long", "user", strings.Repeat("u", 64)},
		{"database-non-ascii", "database", "café"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := map[string]any{tc.field: tc.val}

			spec, err := parsePostgresSpec(in)
			if err != nil {
				// Empty string keeps default — that's fine, but then
				// validation has nothing to reject. Skip.
				t.Fatalf("unexpected parse error: %v", err)
			}

			// Empty values keep default (postgres) so we have to
			// re-poke the field to actually exercise the validator.
			if tc.val == "" {
				switch tc.field {
				case "database":
					spec.Database = ""
				case "user":
					spec.User = ""
				}
			}

			if err := validatePostgresSpec(spec); err == nil {
				t.Errorf("expected validation error for %s=%q", tc.field, tc.val)
			}
		})
	}
}

func TestValidatePostgresSpec_AcceptsValidIdentifiers(t *testing.T) {
	cases := []string{
		"a",
		"_underscore",
		"app_data",
		"App",
		"app123",
		"appdata",
		strings.Repeat("a", 63), // exact max
	}

	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			spec, _ := parsePostgresSpec(map[string]any{
				"database": name,
				"user":     name,
			})

			if err := validatePostgresSpec(spec); err != nil {
				t.Errorf("identifier %q should be valid: %v", name, err)
			}
		})
	}
}

func TestIsValidPostgresIdentifier(t *testing.T) {
	good := []string{"a", "Z", "_x", "abc123", "_123", "App_Data_2"}
	bad := []string{"", "1abc", "abc-def", "abc def", "abc.def", "café"}

	for _, s := range good {
		if !isValidPostgresIdentifier(s) {
			t.Errorf("%q should be valid", s)
		}
	}

	for _, s := range bad {
		if isValidPostgresIdentifier(s) {
			t.Errorf("%q should be invalid", s)
		}
	}
}

func TestIsValidPostgresIdentifier_LengthBoundary(t *testing.T) {
	if !isValidPostgresIdentifier(strings.Repeat("a", 63)) {
		t.Errorf("63 chars should be valid (NAMEDATALEN-1)")
	}

	if isValidPostgresIdentifier(strings.Repeat("a", 64)) {
		t.Errorf("64 chars should be invalid (would silently truncate)")
	}
}

func TestAsInt(t *testing.T) {
	cases := []struct {
		name   string
		val    any
		wantN  int
		wantOK bool
	}{
		{"int", 7, 7, true},
		{"int64", int64(7), 7, true},
		{"float64-whole", float64(7), 7, true},
		{"float64-zero", float64(0), 0, true},
		{"float64-fractional", 7.5, 0, false},
		{"string", "7", 0, false},
		{"bool", true, 0, false},
		{"nil", nil, 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, ok := asInt(tc.val)
			if ok != tc.wantOK {
				t.Errorf("ok: got %v, want %v", ok, tc.wantOK)
			}

			if n != tc.wantN {
				t.Errorf("n: got %d, want %d", n, tc.wantN)
			}
		})
	}
}

func TestRefOrName(t *testing.T) {
	cases := []struct {
		scope string
		name  string
		want  string
	}{
		{"", "db", "db"},
		{"clowk-lp", "db", "clowk-lp/db"},
		{"a", "b", "a/b"},
	}

	for _, tc := range cases {
		got := refOrName(tc.scope, tc.name)
		if got != tc.want {
			t.Errorf("refOrName(%q,%q): got %q, want %q", tc.scope, tc.name, got, tc.want)
		}
	}
}

func TestStripPluginOwnedFields(t *testing.T) {
	merged := map[string]any{
		// real statefulset fields (kept)
		"image":    "postgres:latest",
		"replicas": 3,
		"env":      map[string]any{"FOO": "bar"},

		// plugin-owned (stripped)
		"database":         "appdata",
		"user":             "appuser",
		"password":         "s3cret",
		"port":             5433,
		"initdb_locale":    "C.UTF-8",
		"initdb_encoding":  "UTF8",
		"pg_config":        map[string]any{"max_connections": 200},
		"extensions":       []any{"pgvector"},
		"wal_archive":      map[string]any{"enabled": true},
		"replication_user": "replicator",
	}

	stripPluginOwnedFields(merged)

	stripped := []string{"database", "user", "password", "port", "initdb_locale", "initdb_encoding", "pg_config", "extensions", "wal_archive", "replication_user"}

	for _, k := range stripped {
		if _, present := merged[k]; present {
			t.Errorf("%s should be stripped from statefulset spec", k)
		}
	}

	if merged["image"] != "postgres:latest" {
		t.Errorf("image should remain (real statefulset field)")
	}

	if merged["replicas"] != 3 {
		t.Errorf("replicas should remain (real statefulset field)")
	}

	if _, present := merged["env"]; !present {
		t.Errorf("env should remain (passthrough)")
	}
}

func TestDescribeIdentifierProblem(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", "empty"},
		{strings.Repeat("a", 64), "too long"},
		{"1abc", "starts with a digit"},
		{"abc-def", "invalid char"},
		{"abc def", "invalid char"},
	}

	for _, tc := range cases {
		got := describeIdentifierProblem(tc.in)
		if !strings.Contains(got, tc.want) {
			t.Errorf("describeIdentifierProblem(%q): got %q, want substring %q", tc.in, got, tc.want)
		}
	}
}
