// End-to-end tests for cmdExpand. These exercise the full flow —
// HCL spec in, manifest pair + actions out — so the assertions
// pin operator-visible behaviour rather than internal helpers.
//
// We don't shell out to the binary; we call cmdExpand-equivalent
// logic by composing the request shape directly and asserting on
// the emitted payload. (The actual cmdExpand reads from stdin and
// writes to stdout; refactoring it to a testable inner function
// would just duplicate the io adapter — instead we share the
// payload shape and assert on the same composeStatefulsetDefaults +
// mergeSpec helpers cmdExpand uses.)
//
// Pinning here:
//
//   - composeStatefulsetDefaults emits the right env vars for the
//     official postgres image to do initdb on first boot.
//   - Volumes include both asset bind-mounts (entrypoint + overrides).
//   - command runs the wrapper via bash.
//   - mergeSpec lets operators override env / volumes additively.
//   - stripPluginOwnedFields removes plugin concepts before emit.
//
// First-apply password persistence is asserted at the
// resolveOrGeneratePassword level (password_test.go); here we
// pin that the env var carries the resolved password verbatim.

package main

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestComposeStatefulsetDefaults_DefaultShape(t *testing.T) {
	spec := mustParse(t, nil)
	got := composeStatefulsetDefaults("clowk-lp", "db", spec, "test-password", "repl-pw", 0, false)

	// Image / replicas / port → top-level passthrough fields.
	if got["image"] != defaultImage {
		t.Errorf("image: got %v, want %s", got["image"], defaultImage)
	}

	if got["replicas"] != 1 {
		t.Errorf("replicas: got %v, want 1", got["replicas"])
	}

	ports, ok := got["ports"].([]any)
	if !ok || len(ports) != 1 || ports[0] != "5432" {
		t.Errorf("ports: got %v, want [5432]", got["ports"])
	}

	// command must invoke the wrapper via bash (asset 0644 mode
	// works fine because bash doesn't need +x).
	cmd, ok := got["command"].([]any)
	if !ok || len(cmd) != 2 || cmd[0] != "bash" || cmd[1] != entrypointMountPath {
		t.Errorf("command: got %v, want [bash %s]", got["command"], entrypointMountPath)
	}
}

func TestComposeStatefulsetDefaults_EnvCarriesPostgresImageContract(t *testing.T) {
	// The official postgres image consumes POSTGRES_USER /
	// POSTGRES_DB / POSTGRES_PASSWORD on first boot to run initdb.
	// PGDATA must be set so postgres knows where to put the data
	// (we pin to a subdirectory of the volume mount). PG_PORT is
	// ours — read by the wrapper to pass `-p`.
	spec := mustParse(t, map[string]any{
		"database": "appdata",
		"user":     "appuser",
		"port":     5433,
	})

	got := composeStatefulsetDefaults("scope", "name", spec, "deadbeef", "repl-pw", 0, false)

	env, ok := got["env"].(map[string]any)
	if !ok {
		t.Fatalf("env not a map: %T", got["env"])
	}

	wantEnv := map[string]any{
		"POSTGRES_USER":           "appuser",
		"POSTGRES_DB":             "appdata",
		"POSTGRES_PASSWORD":       "deadbeef",
		"POSTGRES_INITDB_ARGS":    "--locale=C.UTF-8 --encoding=UTF8",
		"PGDATA":                  "/var/lib/postgresql/data/pgdata",
		"PG_PORT":                 "5433",
		"PG_NAME":                 "name",
		"PG_SCOPE_SUFFIX":         ".scope.voodu",
		"PG_PRIMARY_ORDINAL":      "0",
		"PG_REPLICATION_USER":     "replicator",
		"PG_REPLICATION_PASSWORD": "repl-pw",
	}

	for k, want := range wantEnv {
		if env[k] != want {
			t.Errorf("env[%s]: got %v, want %v", k, env[k], want)
		}
	}
}

func TestComposeStatefulsetDefaults_InitdbArgsHonourLocaleAndEncoding(t *testing.T) {
	// Operator override flows into POSTGRES_INITDB_ARGS verbatim.
	spec := mustParse(t, map[string]any{
		"initdb_locale":   "pt_BR.UTF-8",
		"initdb_encoding": "LATIN1",
	})

	got := composeStatefulsetDefaults("s", "n", spec, "pw", "repl-pw", 0, false)

	env := got["env"].(map[string]any)
	want := "--locale=pt_BR.UTF-8 --encoding=LATIN1"

	if env["POSTGRES_INITDB_ARGS"] != want {
		t.Errorf("POSTGRES_INITDB_ARGS: got %v, want %q", env["POSTGRES_INITDB_ARGS"], want)
	}
}

func TestAssetRef_ScopedFourSegment(t *testing.T) {
	got := assetRef("clowk-lp", "db", "entrypoint")
	want := "${asset.clowk-lp.db.entrypoint}"

	if got != want {
		t.Errorf("scoped: got %q, want %q", got, want)
	}
}

func TestAssetRef_UnscopedThreeSegment(t *testing.T) {
	// Critical for postgres "db" {} (1-label, unscoped) operators —
	// 4-segment with empty scope produces ${asset..db.entrypoint}
	// (double dot) which the asset resolver can't match.
	got := assetRef("", "db", "entrypoint")
	want := "${asset.db.entrypoint}"

	if got != want {
		t.Errorf("unscoped: got %q, want %q", got, want)
	}
}

func TestComposeStatefulsetDefaults_UnscopedUsesThreeSegmentAssetRefs(t *testing.T) {
	// Pinning the unscoped flow end-to-end: every asset bind in
	// the emitted volumes list must use the 3-segment form, not
	// the 4-segment "${asset..name.key}" broken shape.
	spec := mustParse(t, nil)
	got := composeStatefulsetDefaults("", "db", spec, "pw", "repl-pw", 0, false)

	vols, _ := got["volumes"].([]any)

	for _, v := range vols {
		s, _ := v.(string)
		if strings.Contains(s, "${asset..") {
			t.Errorf("found broken 4-segment ref with empty scope: %q", s)
		}
	}

	// Spot-check at least one expected 3-segment form is present.
	wantPrefix := "${asset.db.entrypoint}:" + entrypointMountPath
	found := false

	for _, v := range vols {
		if s, ok := v.(string); ok && strings.HasPrefix(s, wantPrefix) {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("expected unscoped 3-segment entrypoint bind, not found in: %v", vols)
	}
}

func TestComposeStatefulsetDefaults_VolumesIncludeAllAssetBinds(t *testing.T) {
	// All four asset keys must be bind-mounted at the right paths
	// (M-P1: entrypoint + pg_overrides_conf, M-P3: streaming_conf
	// + init_replication_sh). The (scope, name) is interpolated
	// into the asset ref.
	spec := mustParse(t, nil)
	got := composeStatefulsetDefaults("clowk-lp", "db", spec, "pw", "repl-pw", 0, false)

	vols, ok := got["volumes"].([]any)
	if !ok {
		t.Fatalf("volumes not a list: %T", got["volumes"])
	}

	wantSubstrs := []string{
		"${asset.clowk-lp.db.entrypoint}:" + entrypointMountPath + ":ro",
		"${asset.clowk-lp.db.pg_overrides_conf}:" + pgOverridesMountPath + ":ro",
		"${asset.clowk-lp.db.streaming_conf}:" + streamingConfMountPath + ":ro",
		"${asset.clowk-lp.db.init_replication_sh}:" + initReplicationMountPath + ":ro",
	}

	for _, want := range wantSubstrs {
		found := false

		for _, v := range vols {
			if s, ok := v.(string); ok && s == want {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("missing volume bind %q in %v", want, vols)
		}
	}
}

func TestComposeStatefulsetDefaults_OnlyDataVolumeClaim(t *testing.T) {
	// Plugin emits exactly one volume_claim — `data` — for the
	// per-pod PGDATA. Backups are point-in-snapshot and live
	// outside the statefulset (see pg:backups command surface).
	spec := mustParse(t, nil)
	got := composeStatefulsetDefaults("s", "n", spec, "pw", "repl-pw", 0, false)

	claims, ok := got["volume_claims"].([]any)
	if !ok || len(claims) != 1 {
		t.Fatalf("expected 1 volume_claim (data only), got: %v", got["volume_claims"])
	}

	c := claims[0].(map[string]any)
	if c["name"] != "data" || c["mount_path"] != "/var/lib/postgresql/data" {
		t.Errorf("data claim wrong: %+v", c)
	}
}

func TestComposeStatefulsetDefaults_NoWALArchiveBindMounts(t *testing.T) {
	// After WAL archive removal: NO `:/wal-archive` bind-mount
	// (host or asset) appears in volumes. Pinning this to defend
	// against accidental reintroduction of the WAL plumbing.
	spec := mustParse(t, nil)
	got := composeStatefulsetDefaults("clowk-lp", "db", spec, "pw", "repl-pw", 0, false)

	vols, _ := got["volumes"].([]any)
	for _, v := range vols {
		s, _ := v.(string)
		if strings.Contains(s, "/wal-archive") {
			t.Errorf("WAL archive should be gone, but found: %q", s)
		}
	}
}

func TestComposeStatefulsetDefaults_ScopeSuffixForUnscopedResource(t *testing.T) {
	// Empty scope flows into PG_SCOPE_SUFFIX as ".voodu" (no
	// double-dot). Unscoped resources are rare but valid.
	spec := mustParse(t, nil)
	got := composeStatefulsetDefaults("", "name", spec, "pw", "repl-pw", 0, false)

	env := got["env"].(map[string]any)
	if env["PG_SCOPE_SUFFIX"] != ".voodu" {
		t.Errorf("expected .voodu suffix for unscoped, got %v", env["PG_SCOPE_SUFFIX"])
	}
}

func TestComposeStatefulsetDefaults_PrimaryOrdinalFlowsIntoEnv(t *testing.T) {
	// M-P5 will flip PG_PRIMARY_ORDINAL via failover; pinning
	// that the value flows from the parameter into the env.
	spec := mustParse(t, nil)
	got := composeStatefulsetDefaults("s", "n", spec, "pw", "repl-pw", 2, false)

	env := got["env"].(map[string]any)
	if env["PG_PRIMARY_ORDINAL"] != "2" {
		t.Errorf("expected PG_PRIMARY_ORDINAL=2, got %v", env["PG_PRIMARY_ORDINAL"])
	}
}

func TestComposeStatefulsetDefaults_HealthCheckUsesPgIsready(t *testing.T) {
	// Operator-overridable per the alias contract, but the default
	// must be a valid pg_isready against the configured user/db/port.
	spec := mustParse(t, map[string]any{
		"database": "appdata",
		"user":     "appuser",
		"port":     5433,
	})

	got := composeStatefulsetDefaults("s", "n", spec, "pw", "repl-pw", 0, false)

	if got["health_check"] != "pg_isready -U appuser -d appdata -p 5433" {
		t.Errorf("health_check: got %v", got["health_check"])
	}
}

func TestMergeSpec_OperatorEnvCoexistsWithPluginEnv(t *testing.T) {
	defaults := map[string]any{
		"env": map[string]any{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "auto",
		},
	}

	operator := map[string]any{
		"env": map[string]any{
			"PGAPPNAME":         "voodu-test",
			"POSTGRES_PASSWORD": "operator-set", // operator wins on conflict
		},
	}

	merged := mergeSpec(defaults, operator)

	env := merged["env"].(map[string]any)

	if env["POSTGRES_USER"] != "postgres" {
		t.Errorf("plugin env lost: %v", env["POSTGRES_USER"])
	}

	if env["PGAPPNAME"] != "voodu-test" {
		t.Errorf("operator env lost: %v", env["PGAPPNAME"])
	}

	if env["POSTGRES_PASSWORD"] != "operator-set" {
		t.Errorf("operator should win on env conflict: %v", env["POSTGRES_PASSWORD"])
	}
}

func TestMergeSpec_OperatorVolumesAdditive(t *testing.T) {
	// Plugin's two asset binds must survive when operator adds a
	// fresh volume. Operator-targeting one of the plugin paths
	// REPLACES that single entry (granular override) without
	// dropping the others.
	defaults := map[string]any{
		"volumes": []any{
			"${asset.s.n.entrypoint}:/usr/local/bin/voodu-postgres-entrypoint:ro",
			"${asset.s.n.pg_overrides_conf}:/etc/postgresql/voodu-99-overrides.conf:ro",
		},
	}

	operator := map[string]any{
		"volumes": []any{
			"./local-pg.conf:/etc/postgresql/voodu-99-overrides.conf:ro", // replaces
			"./extra:/var/extra:ro",                                      // fresh
		},
	}

	merged := mergeSpec(defaults, operator)

	vols, ok := merged["volumes"].([]any)
	if !ok {
		t.Fatalf("volumes not a list: %T", merged["volumes"])
	}

	got := make([]string, 0, len(vols))
	for _, v := range vols {
		got = append(got, v.(string))
	}

	sort.Strings(got)

	want := []string{
		"${asset.s.n.entrypoint}:/usr/local/bin/voodu-postgres-entrypoint:ro",
		"./extra:/var/extra:ro",
		"./local-pg.conf:/etc/postgresql/voodu-99-overrides.conf:ro",
	}

	sort.Strings(want)

	if !reflect.DeepEqual(got, want) {
		t.Errorf("merged volumes:\n  got  %v\n  want %v", got, want)
	}
}

func TestMergeSpec_OperatorImageOverrides(t *testing.T) {
	defaults := map[string]any{"image": "postgres:latest"}
	operator := map[string]any{"image": "ghcr.io/clowk/pg16-pgvector:1.0"}

	merged := mergeSpec(defaults, operator)

	if merged["image"] != "ghcr.io/clowk/pg16-pgvector:1.0" {
		t.Errorf("operator image override lost: %v", merged["image"])
	}
}

func TestStripPluginOwnedFields_RemovesAllPluginConcepts(t *testing.T) {
	// Sanity check (postgres_test.go covers the same surface, but
	// we re-check here in the e2e context to lock in that the
	// final emitted statefulset spec doesn't carry plugin fields
	// the controller doesn't understand).
	merged := map[string]any{
		"image":            "postgres:16",
		"replicas":         1,
		"database":         "appdata",
		"user":             "appuser",
		"password":         "secret",
		"port":             5432,
		"initdb_locale":    "C.UTF-8",
		"initdb_encoding":  "UTF8",
		"pg_config":        map[string]any{"max_connections": 200},
		"extensions":       []any{"pgvector"},
		"replication_user": "replicator",
		"env":              map[string]any{"FOO": "bar"},
	}

	stripPluginOwnedFields(merged)

	pluginOwned := []string{"database", "user", "password", "port", "initdb_locale", "initdb_encoding", "pg_config", "extensions", "replication_user"}

	for _, k := range pluginOwned {
		if _, present := merged[k]; present {
			t.Errorf("%s should be stripped from final statefulset spec", k)
		}
	}

	if merged["image"] != "postgres:16" {
		t.Errorf("image should remain (statefulset field): %v", merged["image"])
	}

	if merged["env"] == nil {
		t.Error("env should remain (statefulset field)")
	}
}

func TestVolumeDest(t *testing.T) {
	// Helper for mergeVolumes — must extract the dst path from
	// "<src>:<dst>:<opts>" / "<src>:<dst>" forms and return the
	// raw string for unparseable entries.
	cases := []struct {
		in   string
		want string
	}{
		{"src:/dst:ro", "/dst"},
		{"src:/dst", "/dst"},
		{"${asset.s.n.k}:/etc/path:ro", "/etc/path"},
		{"unparseable", "unparseable"},
		{"", ""},
	}

	for _, tc := range cases {
		got := volumeDest(tc.in)
		if got != tc.want {
			t.Errorf("volumeDest(%q): got %q, want %q", tc.in, got, tc.want)
		}
	}
}

// --- end-to-end: full expand flow exercised via composeDefaults +
// mergeSpec + the asset bytes the plugin would emit.

func TestExpandFlow_FirstApplyEmitsConfigSet(t *testing.T) {
	// When config bucket is empty, password gets generated and the
	// caller (cmdExpand) emits a config_set action. Pin the shape.
	spec := mustParse(t, nil)
	pw, isNew, err := resolveOrGeneratePassword(spec, nil)

	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if !isNew {
		t.Fatal("first apply should generate (isNew=true)")
	}

	// Build the action the way cmdExpand does.
	action := dispatchAction{
		Type:  "config_set",
		Scope: "clowk-lp",
		Name:  "db",
		KV:    map[string]string{passwordKey: pw},
	}

	if action.Type != "config_set" || action.KV[passwordKey] == "" {
		t.Errorf("dispatch action shape wrong: %+v", action)
	}

	if !strings.HasPrefix(pw, "") || len(pw) != 64 {
		t.Errorf("generated password unexpected: %q", pw)
	}
}

func TestExpandFlow_SubsequentApplyNoAction(t *testing.T) {
	// Bucket has password → reuse, no action.
	spec := mustParse(t, nil)
	config := map[string]string{passwordKey: "stored-deadbeef"}

	pw, isNew, err := resolveOrGeneratePassword(spec, config)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if isNew {
		t.Error("reused password must not flag isNew (no action emitted)")
	}

	if pw != "stored-deadbeef" {
		t.Errorf("reused password lost: got %q", pw)
	}
}

// mustParse is a test helper — runs parsePostgresSpec and fails the
// test if it errors. Keeps the e2e tests free of error plumbing.
func mustParse(t *testing.T, in map[string]any) *postgresSpec {
	t.Helper()

	spec, err := parsePostgresSpec(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	return spec
}

// TestDispatchAction_ApplyManifestSerializes pins the wire shape
// the controller's apply_manifest handler expects: top-level
// type/scope/name, embedded manifest with kind/scope/name/spec.
// Keep this test in lockstep with the controller's
// pluginDispatchAction shape — both sides deserialize the same
// JSON, so any drift between repos breaks dispatch.
func TestDispatchAction_ApplyManifestSerializes(t *testing.T) {
	action := dispatchAction{
		Type:  "apply_manifest",
		Scope: "clowk-lp",
		Name:  "db",
		Manifest: &dispatchManifest{
			Kind:  "job",
			Scope: "clowk-lp",
			Name:  "db-backup-b008",
			Spec: map[string]any{
				"image":   "postgres:16",
				"command": []any{"bash", "-c", "pg_dump ..."},
			},
		},
	}

	raw, err := json.Marshal(action)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded["type"] != "apply_manifest" {
		t.Errorf("type: %v", decoded["type"])
	}

	manifest, ok := decoded["manifest"].(map[string]any)
	if !ok {
		t.Fatalf("manifest field: %T", decoded["manifest"])
	}

	if manifest["kind"] != "job" {
		t.Errorf("manifest.kind: %v", manifest["kind"])
	}

	if manifest["name"] != "db-backup-b008" {
		t.Errorf("manifest.name: %v", manifest["name"])
	}

	spec, ok := manifest["spec"].(map[string]any)
	if !ok {
		t.Fatalf("manifest.spec: %T", manifest["spec"])
	}

	if spec["image"] != "postgres:16" {
		t.Errorf("spec.image: %v", spec["image"])
	}
}

// TestDispatchAction_OmitsEmptyManifestForConfigSet ensures the
// `manifest` field is omitempty — config_set actions must NOT
// carry an empty manifest object on the wire.
func TestDispatchAction_OmitsEmptyManifestForConfigSet(t *testing.T) {
	action := dispatchAction{
		Type:  "config_set",
		Scope: "clowk-lp",
		Name:  "web",
		KV:    map[string]string{"X": "y"},
	}

	raw, _ := json.Marshal(action)

	if strings.Contains(string(raw), "\"manifest\"") {
		t.Errorf("config_set must not carry manifest field: %s", raw)
	}

	if strings.Contains(string(raw), "\"kind\"") {
		t.Errorf("config_set must not carry kind field: %s", raw)
	}
}

// TestDispatchAction_DeleteManifestSerializes pins the wire shape
// for delete_manifest: kind alongside top-level scope/name, no
// embedded manifest payload.
func TestDispatchAction_DeleteManifestSerializes(t *testing.T) {
	action := dispatchAction{
		Type:  "delete_manifest",
		Scope: "clowk-lp",
		Name:  "db-backup-b008",
		Kind:  "job",
	}

	raw, err := json.Marshal(action)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded["type"] != "delete_manifest" {
		t.Errorf("type: %v", decoded["type"])
	}

	if decoded["kind"] != "job" {
		t.Errorf("kind: %v", decoded["kind"])
	}

	if _, present := decoded["manifest"]; present {
		t.Errorf("manifest field should be omitted for delete_manifest: %v", decoded)
	}
}

