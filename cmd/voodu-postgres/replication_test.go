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

func TestRenderStreamingConf_PinsExpectedDirectives(t *testing.T) {
	got := renderStreamingConf("clowk-lp", "db", 0, "replicator", "deadbeef")

	wantLines := []string{
		"hot_standby = on",
		"max_wal_senders = 10",
		"primary_conninfo = ",
		"wal_keep_size = '1GB'",
	}

	for _, want := range wantLines {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

func TestRenderStreamingConf_PrimaryConninfoIncludesFQDNAndPassword(t *testing.T) {
	got := renderStreamingConf("clowk-lp", "db", 0, "replicator", "secretpass")

	wantSubstrs := []string{
		"host=db-0.clowk-lp.voodu",
		"port=5432",
		"user=replicator",
		"password=secretpass",
	}

	for _, want := range wantSubstrs {
		if !strings.Contains(got, want) {
			t.Errorf("primary_conninfo missing %q in:\n%s", want, got)
		}
	}
}

func TestRenderStreamingConf_PasswordWithQuoteEscaped(t *testing.T) {
	// Defensive — operator-supplied passwords could include single
	// quotes; auto-gen is hex-only but the renderer must handle
	// quotes via the postgres-conf doubled-quote escape.
	got := renderStreamingConf("s", "n", 0, "replicator", "pa'ss")

	if !strings.Contains(got, "password=pa''ss") {
		t.Errorf("expected pa''ss escape in primary_conninfo:\n%s", got)
	}
}

func TestRenderStreamingConf_HonoursPrimaryOrdinal(t *testing.T) {
	// M-P5 failover flips PG_PRIMARY_ORDINAL via config_set; the
	// next expand re-renders streaming_conf with the new FQDN.
	got := renderStreamingConf("scope", "name", 2, "replicator", "pw")

	if !strings.Contains(got, "host=name-2.scope.voodu") {
		t.Errorf("primary_conninfo should target ordinal 2 FQDN:\n%s", got)
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
