// Tests for the M-P3 streaming-replication surface:
// resolveOrGenerateReplicationPassword, composePrimaryFQDN,
// composeScopeSuffix, renderStreamingConf, and the init script
// renderer.
//
// Pinning here:
//
//   - Replication password lifecycle mirrors the superuser one
//     (read from bucket > generate). No HCL override codepath.
//   - Primary FQDN composition matches voodu0's DNS scheme
//     (<name>-<ord>.<scope>.voodu, with .voodu suffix when
//     scope is empty).
//   - Streaming conf renders alphabetically (asset digest
//     stability) and inlines password into primary_conninfo
//     with single-quote escape.
//   - Init script reads from PG_REPLICATION_USER + PG_REPLICATION_PASSWORD
//     env vars (set by composeStatefulsetDefaults) and is fail-fast
//     on missing required env via :? bash expansion.

package main

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestResolveOrGenerateReplicationPassword_FirstApplyGenerates(t *testing.T) {
	pw, isNew, err := resolveOrGenerateReplicationPassword(nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if !isNew {
		t.Error("first apply must flag isNew so caller emits config_set")
	}

	if len(pw) != 64 {
		t.Errorf("expected 64 hex chars (32-byte hex), got %d: %q", len(pw), pw)
	}

	if _, err := hex.DecodeString(pw); err != nil {
		t.Errorf("generated pw not valid hex: %v", err)
	}
}

func TestResolveOrGenerateReplicationPassword_BucketReused(t *testing.T) {
	config := map[string]string{replicationPasswordKey: "stable-deadbeef"}
	pw, isNew, err := resolveOrGenerateReplicationPassword(config)

	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if isNew {
		t.Error("bucket reuse must not flag isNew (no action emitted)")
	}

	if pw != "stable-deadbeef" {
		t.Errorf("bucket value lost: got %q", pw)
	}
}

func TestResolveOrGenerateReplicationPassword_EmptyBucketGenerates(t *testing.T) {
	// Empty string treated as absent — same posture as the
	// superuser password.
	config := map[string]string{replicationPasswordKey: ""}
	pw, isNew, err := resolveOrGenerateReplicationPassword(config)

	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if !isNew || pw == "" {
		t.Errorf("empty bucket should generate, got pw=%q isNew=%v", pw, isNew)
	}
}

func TestResolveOrGenerateReplicationPassword_DistinctValues(t *testing.T) {
	// Two consecutive generations should differ (256-bit entropy).
	a, _, _ := resolveOrGenerateReplicationPassword(nil)
	b, _, _ := resolveOrGenerateReplicationPassword(nil)

	if a == b {
		t.Errorf("two generations returned same value: %q", a)
	}
}

func TestComposePrimaryFQDN_WithScope(t *testing.T) {
	got := composePrimaryFQDN("clowk-lp", "db", 0)
	want := "db-0.clowk-lp.voodu"

	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestComposePrimaryFQDN_WithoutScope(t *testing.T) {
	// Unscoped: <name>-<ord>.voodu (no double-dot).
	got := composePrimaryFQDN("", "db", 0)
	want := "db-0.voodu"

	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestComposePrimaryFQDN_NonZeroOrdinal(t *testing.T) {
	// M-P5 will allow primary_ordinal != 0 via failover. Compose
	// must honour it for both wrapper-time runtime composition
	// and primary_conninfo bake-in.
	got := composePrimaryFQDN("scope", "name", 2)
	want := "name-2.scope.voodu"

	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestComposeScopeSuffix(t *testing.T) {
	cases := []struct {
		scope string
		want  string
	}{
		{"", ".voodu"},
		{"clowk-lp", ".clowk-lp.voodu"},
		{"data", ".data.voodu"},
	}

	for _, tc := range cases {
		got := composeScopeSuffix(tc.scope)
		if got != tc.want {
			t.Errorf("composeScopeSuffix(%q): got %q, want %q", tc.scope, got, tc.want)
		}
	}
}

// TestRenderStreamingConf_PinsStaticDirectives confirms the asset
// only carries the static replication tunables. primary_conninfo
// MOVED out of the asset (entrypoint wrapper writes it at boot
// from env vars to PGDATA/voodu-runtime.conf) so that promotes
// don't require re-rendering the asset to update the primary
// pointer. The whole point of this redesign is captured by the
// absence of primary_conninfo here — if a future refactor brings
// it back, this test fails and pulls the lever.
func TestRenderStreamingConf_PinsStaticDirectives(t *testing.T) {
	got := renderStreamingConf("clowk-lp", "db", 0, "replicator", "deadbeef")

	wantLines := []string{
		"hot_standby = on",
		"max_wal_senders = 10",
		"wal_keep_size = '1GB'",
	}

	for _, want := range wantLines {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}

	// The DIRECTIVE form (`primary_conninfo = ...`) MUST NOT appear
	// in the asset — that's the bug fix this redesign embodies.
	// Baking the FQDN into the asset meant post-promote standbys
	// streamed from the stale primary until `vd apply` re-rendered.
	// The asset can MENTION primary_conninfo in a comment (which it
	// does, explaining where the runtime version lives).
	if strings.Contains(got, "primary_conninfo = ") {
		t.Errorf("primary_conninfo directive leaked into static asset (must be wrapper-written at runtime):\n%s", got)
	}
}

// TestRenderStreamingConf_NoPrimaryOrdinalDependency proves the
// asset content is INDEPENDENT of primary ordinal — different
// ordinals must produce identical bytes. Together with the
// absence of primary_conninfo above, this guarantees post-
// promote behaviour: no asset re-render needed, the wrapper's
// runtime file picks up the new primary from PG_PRIMARY_ORDINAL
// env on the next boot.
func TestRenderStreamingConf_NoPrimaryOrdinalDependency(t *testing.T) {
	a := renderStreamingConf("scope", "name", 0, "replicator", "pw")
	b := renderStreamingConf("scope", "name", 1, "replicator", "pw")
	c := renderStreamingConf("scope", "name", 7, "replicator", "pw")

	if a != b || b != c {
		t.Errorf("asset content must be ordinal-independent:\nord=0: %s\nord=1: %s\nord=7: %s", a, b, c)
	}
}

// TestRenderStreamingConf_NoPasswordDependency: same as above but
// for replication credentials. Wrapper writes them at runtime
// from env vars (PG_REPLICATION_USER / PG_REPLICATION_PASSWORD),
// so passing different values to the asset renderer must produce
// identical bytes. Catches accidental regression where someone
// puts the credentials back into the asset.
func TestRenderStreamingConf_NoPasswordDependency(t *testing.T) {
	a := renderStreamingConf("s", "n", 0, "r1", "secret-1")
	b := renderStreamingConf("s", "n", 0, "r2", "secret-2")

	if a != b {
		t.Errorf("asset must not embed replication credentials:\na: %s\nb: %s", a, b)
	}
}

func TestRenderStreamingConf_Deterministic(t *testing.T) {
	a := renderStreamingConf("s", "n", 0, "r", "p")
	b := renderStreamingConf("s", "n", 0, "r", "p")

	if a != b {
		t.Error("renderStreamingConf is not deterministic")
	}
}

func TestRenderInitReplicationSH_PinsContract(t *testing.T) {
	got := renderInitReplicationSH()

	if !strings.HasPrefix(got, "#!/bin/bash\n") {
		t.Errorf("expected bash shebang, got first line: %q", firstLine(got))
	}

	if !strings.Contains(got, "set -euo pipefail") {
		t.Errorf("expected strict mode for safety:\n%s", got)
	}

	// Must read from env vars (set by composeStatefulsetDefaults).
	wantEnvReads := []string{
		`PG_REPLICATION_USER:-replicator`,
		`PG_REPLICATION_PASSWORD:?PG_REPLICATION_PASSWORD is required`,
	}

	for _, want := range wantEnvReads {
		if !strings.Contains(got, want) {
			t.Errorf("missing env contract %q in:\n%s", want, got)
		}
	}

	// Must use psql with ON_ERROR_STOP (so a failed CREATE USER
	// doesn't silently leave the cluster without a replicator).
	if !strings.Contains(got, "ON_ERROR_STOP=1") {
		t.Errorf("psql must use ON_ERROR_STOP=1 to fail loudly:\n%s", got)
	}

	// Must SQL-escape single quotes in password (defensive).
	if !strings.Contains(got, `s/'/''/g`) {
		t.Errorf("missing SQL single-quote escape on password:\n%s", got)
	}

	// Must wire pg_hba entry for replication.
	if !strings.Contains(got, "host replication") {
		t.Errorf("missing pg_hba entry for replication:\n%s", got)
	}
}

func TestRenderInitReplicationSH_Deterministic(t *testing.T) {
	a := renderInitReplicationSH()
	b := renderInitReplicationSH()

	if a != b {
		t.Error("renderInitReplicationSH is not deterministic")
	}
}
