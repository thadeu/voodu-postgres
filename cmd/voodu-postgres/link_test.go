// Tests for the M-P4 link/unlink surface. Pure-function helpers
// (URL building, consumer-list manipulation) get direct unit
// tests; the dispatch flow itself (cmdLink/cmdUnlink) reads
// stdin + calls the controller, which is integration territory
// — exercised separately via smoke E2E in build sweep.
//
// Pinning here:
//
//   - Single-replica → single DATABASE_URL, no read URL even
//     with --reads (echoes primary).
//   - Multi-replica + --reads → primary URL + multi-host libpq
//     read URL spanning standbys.
//   - Multi-replica without --reads → primary URL only.
//   - URL composition URL-encodes password (operator-supplied
//     special chars don't break parsing).
//   - Linked-consumers list maintains order, idempotent on
//     re-link, no-op on missing remove.

package main

import (
	"strings"
	"testing"
)

func TestBuildLinkURLs_SingleReplicaPrimaryOnly(t *testing.T) {
	spec := map[string]any{
		"env": map[string]any{
			"POSTGRES_USER": "postgres",
			"POSTGRES_DB":   "appdata",
			"PG_PORT":       "5432",
		},
		"replicas": 1,
	}

	config := map[string]string{passwordKey: "deadbeef"}

	urls, err := buildLinkURLs("scope", "name", spec, config, false)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if urls.WriteURL != "postgres://postgres:deadbeef@name-0.scope.voodu:5432/appdata" {
		t.Errorf("unexpected WriteURL: %q", urls.WriteURL)
	}

	if urls.ReadURL != "" {
		t.Errorf("expected no ReadURL for replicas=1, got %q", urls.ReadURL)
	}
}

func TestBuildLinkURLs_SingleReplicaReadsEchoesPrimary(t *testing.T) {
	// --reads on a single-replica cluster: no standbys. Echo
	// the primary URL so consumer code that always reads
	// DATABASE_READ_URL doesn't crash.
	spec := map[string]any{
		"env":      map[string]any{"POSTGRES_USER": "u", "POSTGRES_DB": "d", "PG_PORT": "5432"},
		"replicas": 1,
	}

	urls, err := buildLinkURLs("s", "n", spec, map[string]string{passwordKey: "p"}, true)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if urls.ReadURL != urls.WriteURL {
		t.Errorf("--reads on single-replica should echo primary: got read=%q write=%q", urls.ReadURL, urls.WriteURL)
	}
}

func TestBuildLinkURLs_MultiReplicaWithoutReadsOnlyPrimary(t *testing.T) {
	spec := map[string]any{
		"env":      map[string]any{"POSTGRES_USER": "u", "POSTGRES_DB": "d", "PG_PORT": "5432"},
		"replicas": 3,
	}

	urls, err := buildLinkURLs("s", "n", spec, map[string]string{passwordKey: "p"}, false)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if !strings.Contains(urls.WriteURL, "n-0.s.voodu") {
		t.Errorf("write URL should target ordinal 0: %q", urls.WriteURL)
	}

	if urls.ReadURL != "" {
		t.Errorf("no --reads → no ReadURL, got %q", urls.ReadURL)
	}
}

func TestBuildLinkURLs_MultiReplicaWithReadsMultiHost(t *testing.T) {
	// 3 replicas + --reads: WriteURL primary, ReadURL multi-host
	// libpq form spanning ordinals 1 and 2.
	spec := map[string]any{
		"env":      map[string]any{"POSTGRES_USER": "u", "POSTGRES_DB": "d", "PG_PORT": "5432"},
		"replicas": 3,
	}

	urls, err := buildLinkURLs("s", "n", spec, map[string]string{passwordKey: "p"}, true)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if !strings.Contains(urls.WriteURL, "n-0.s.voodu") {
		t.Errorf("WriteURL should hit primary: %q", urls.WriteURL)
	}

	wantReadHosts := []string{"n-1.s.voodu:5432", "n-2.s.voodu:5432"}
	for _, want := range wantReadHosts {
		if !strings.Contains(urls.ReadURL, want) {
			t.Errorf("ReadURL missing standby %q: %q", want, urls.ReadURL)
		}
	}

	if !strings.Contains(urls.ReadURL, "target_session_attrs=any") {
		t.Errorf("ReadURL should include target_session_attrs=any: %q", urls.ReadURL)
	}
}

func TestBuildLinkURLs_PasswordURLEncoded(t *testing.T) {
	// Defensive: operator-supplied password could include @ / : /
	// & — URL syntax delimiters. URL-encode userinfo so parsing
	// stays unambiguous.
	spec := map[string]any{
		"env":      map[string]any{"POSTGRES_USER": "u", "POSTGRES_DB": "d", "PG_PORT": "5432"},
		"replicas": 1,
	}

	urls, _ := buildLinkURLs("s", "n", spec, map[string]string{passwordKey: "p@ss/word"}, false)

	// "@" → "%40", "/" → "%2F"
	if !strings.Contains(urls.WriteURL, "p%40ss%2Fword") {
		t.Errorf("password not URL-encoded in: %q", urls.WriteURL)
	}
}

func TestBuildLinkURLs_MissingPasswordIsError(t *testing.T) {
	spec := map[string]any{
		"env":      map[string]any{"POSTGRES_USER": "u", "POSTGRES_DB": "d", "PG_PORT": "5432"},
		"replicas": 1,
	}

	_, err := buildLinkURLs("s", "n", spec, map[string]string{}, false)
	if err == nil {
		t.Fatal("expected error when bucket has no POSTGRES_PASSWORD")
	}

	if !strings.Contains(err.Error(), "POSTGRES_PASSWORD") {
		t.Errorf("error should mention POSTGRES_PASSWORD: %v", err)
	}
}

func TestReadUserDBPort_DefaultsWhenEnvAbsent(t *testing.T) {
	user, db, port := readUserDBPort(map[string]any{})

	if user != "postgres" || db != "postgres" || port != 5432 {
		t.Errorf("defaults wrong: user=%q db=%q port=%d", user, db, port)
	}
}

func TestReadUserDBPort_ReadsFromEnv(t *testing.T) {
	spec := map[string]any{
		"env": map[string]any{
			"POSTGRES_USER": "appuser",
			"POSTGRES_DB":   "appdata",
			"PG_PORT":       "5433",
		},
	}

	user, db, port := readUserDBPort(spec)
	if user != "appuser" || db != "appdata" || port != 5433 {
		t.Errorf("unexpected: user=%q db=%q port=%d", user, db, port)
	}
}

func TestReadReplicas_TolerantOfTypes(t *testing.T) {
	cases := []struct {
		val  any
		want int
	}{
		{nil, 1},                   // missing
		{1, 1},                     // int
		{3, 3},                     // int
		{float64(5), 5},            // JSON decode
		{"three", 1},               // garbage → default
	}

	for _, tc := range cases {
		spec := map[string]any{}
		if tc.val != nil {
			spec["replicas"] = tc.val
		}

		got := readReplicas(spec)
		if got != tc.want {
			t.Errorf("readReplicas(%v): got %d, want %d", tc.val, got, tc.want)
		}
	}
}

func TestAddLinkedConsumer_AppendsNew(t *testing.T) {
	config := map[string]string{linkedConsumersKey: "scope1/cons1"}
	got := addLinkedConsumer(config, "scope2", "cons2")

	if got != "scope1/cons1,scope2/cons2" {
		t.Errorf("got %q", got)
	}
}

func TestAddLinkedConsumer_IdempotentOnReLink(t *testing.T) {
	// Re-linking the same (scope, name) → no duplicate entry.
	config := map[string]string{linkedConsumersKey: "scope/cons"}
	got := addLinkedConsumer(config, "scope", "cons")

	if got != "scope/cons" {
		t.Errorf("re-link should be no-op, got %q", got)
	}
}

func TestAddLinkedConsumer_FromEmpty(t *testing.T) {
	got := addLinkedConsumer(map[string]string{}, "s", "n")
	if got != "s/n" {
		t.Errorf("got %q, want s/n", got)
	}
}

func TestRemoveLinkedConsumer_DropsEntry(t *testing.T) {
	config := map[string]string{linkedConsumersKey: "a/x,b/y,c/z"}
	got := removeLinkedConsumer(config, "b", "y")

	if got != "a/x,c/z" {
		t.Errorf("got %q, want a/x,c/z", got)
	}
}

func TestRemoveLinkedConsumer_MissingIsNoOp(t *testing.T) {
	config := map[string]string{linkedConsumersKey: "a/x"}
	got := removeLinkedConsumer(config, "z", "z")

	if got != "a/x" {
		t.Errorf("missing remove should be no-op, got %q", got)
	}
}

func TestParseLinkedConsumers_HandlesEmpty(t *testing.T) {
	if got := parseLinkedConsumers(map[string]string{}); got != nil {
		t.Errorf("empty bucket should return nil, got %v", got)
	}

	if got := parseLinkedConsumers(map[string]string{linkedConsumersKey: ""}); got != nil {
		t.Errorf("empty value should return nil, got %v", got)
	}
}

func TestParseLinkedConsumers_TrimsWhitespace(t *testing.T) {
	config := map[string]string{linkedConsumersKey: " a/x , b/y , "}
	got := parseLinkedConsumers(config)

	want := []string{"a/x", "b/y"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParseLinkFlags(t *testing.T) {
	cases := []struct {
		in       []string
		wantPos  []string
		wantRead bool
	}{
		{[]string{"a", "b"}, []string{"a", "b"}, false},
		{[]string{"a", "b", "--reads"}, []string{"a", "b"}, true},
		{[]string{"--reads", "a", "b"}, []string{"a", "b"}, true},
		{[]string{"a", "--reads", "b"}, []string{"a", "b"}, true},
	}

	for _, tc := range cases {
		gotPos, gotRead := parseLinkFlags(tc.in)
		if gotRead != tc.wantRead {
			t.Errorf("reads: got %v, want %v (in=%v)", gotRead, tc.wantRead, tc.in)
		}

		if len(gotPos) != len(tc.wantPos) {
			t.Errorf("positional length: got %v, want %v (in=%v)", gotPos, tc.wantPos, tc.in)
		}
	}
}

func TestComposePostgresURL_SingleHost(t *testing.T) {
	got := composePostgresURL("u", "p", "host.scope.voodu", 5432, "db", "")
	want := "postgres://u:p@host.scope.voodu:5432/db"

	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestComposePostgresURL_MultiHostNoPort(t *testing.T) {
	// Multi-host: hosts string already formatted, port=0 → no
	// extra :port suffix.
	got := composePostgresURL("u", "p", "host1:5432,host2:5432", 0, "db", "target_session_attrs=any")
	want := "postgres://u:p@host1:5432,host2:5432/db?target_session_attrs=any"

	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
