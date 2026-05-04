// Tests for the failover surface. cmdFailover itself talks to the
// controller + emits dispatch actions; we test the pure helpers
// (parseFailoverFlags, readCurrentPrimaryOrdinal, buildLinkURLs
// WithPrimary) in isolation. End-to-end flow is exercised in the
// build sweep smoke test (post-shipping) where a real cluster
// gets failed over.

package main

import (
	"strings"
	"testing"
)

func TestParseFailoverFlags_ReplicaSpaceSeparated(t *testing.T) {
	args := []string{"clowk-lp/db", "--replica", "1"}

	pos, target, hasTarget, noRestart := parseFailoverFlags(args)

	if !hasTarget || target != 1 {
		t.Errorf("--replica 1 not parsed: target=%d hasTarget=%v", target, hasTarget)
	}

	if noRestart {
		t.Error("--no-restart should default to false")
	}

	if len(pos) != 1 || pos[0] != "clowk-lp/db" {
		t.Errorf("positional: got %v, want [clowk-lp/db]", pos)
	}
}

func TestParseFailoverFlags_ReplicaEqualsForm(t *testing.T) {
	args := []string{"clowk-lp/db", "--replica=2"}

	_, target, hasTarget, _ := parseFailoverFlags(args)

	if !hasTarget || target != 2 {
		t.Errorf("--replica=2 not parsed: target=%d hasTarget=%v", target, hasTarget)
	}
}

func TestParseFailoverFlags_ShortReplicaForm(t *testing.T) {
	args := []string{"-r", "3", "clowk-lp/db"}

	pos, target, hasTarget, _ := parseFailoverFlags(args)

	if !hasTarget || target != 3 {
		t.Errorf("-r 3 not parsed: target=%d", target)
	}

	if len(pos) != 1 || pos[0] != "clowk-lp/db" {
		t.Errorf("positional should survive flag interleaving: %v", pos)
	}
}

func TestParseFailoverFlags_NoRestartFlag(t *testing.T) {
	args := []string{"clowk-lp/db", "--replica", "1", "--no-restart"}

	_, _, _, noRestart := parseFailoverFlags(args)

	if !noRestart {
		t.Error("--no-restart not parsed")
	}
}

func TestParseFailoverFlags_NoReplicaIsAbsent(t *testing.T) {
	args := []string{"clowk-lp/db"}

	_, target, hasTarget, _ := parseFailoverFlags(args)

	if hasTarget || target != 0 {
		t.Errorf("missing --replica should yield hasTarget=false, got hasTarget=%v target=%d", hasTarget, target)
	}
}

func TestReadCurrentPrimaryOrdinal_Default(t *testing.T) {
	// Empty bucket → default to 0 (the convention for fresh clusters).
	got := readCurrentPrimaryOrdinal(map[string]string{})
	if got != 0 {
		t.Errorf("default should be 0, got %d", got)
	}
}

func TestReadCurrentPrimaryOrdinal_FromBucket(t *testing.T) {
	got := readCurrentPrimaryOrdinal(map[string]string{primaryOrdinalKey: "2"})
	if got != 2 {
		t.Errorf("got %d, want 2", got)
	}
}

func TestReadCurrentPrimaryOrdinal_GarbageFallsBackToZero(t *testing.T) {
	// Defensive: bucket should only ever contain integers, but if
	// someone manually `vd config set` a typo, default to 0 instead
	// of crashing the dispatch.
	got := readCurrentPrimaryOrdinal(map[string]string{primaryOrdinalKey: "garbage"})
	if got != 0 {
		t.Errorf("garbage should fall back to 0, got %d", got)
	}
}

func TestBuildLinkURLsWithPrimary_PointsAtNewPrimary(t *testing.T) {
	// After failover, primary moved from ordinal 0 to ordinal 1.
	// WriteURL must target db-1, not db-0.
	spec := map[string]any{
		"env":      map[string]any{"POSTGRES_USER": "u", "POSTGRES_DB": "d", "PG_PORT": "5432"},
		"replicas": 3,
	}

	urls, err := buildLinkURLsWithPrimary("scope", "db", spec, map[string]string{passwordKey: "pw"}, false, 1)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(urls.WriteURL, "db-1.scope.voodu") {
		t.Errorf("WriteURL should target ordinal 1: %q", urls.WriteURL)
	}

	if strings.Contains(urls.WriteURL, "db-0.scope.voodu") {
		t.Errorf("WriteURL still points at old primary db-0: %q", urls.WriteURL)
	}
}

func TestBuildLinkURLsWithPrimary_ReadURLExcludesNewPrimary(t *testing.T) {
	// 3 replicas, primary moved to ordinal 1 → standbys are
	// {0, 2}. ReadURL multi-host must include db-0 + db-2 but
	// NOT db-1 (the new primary).
	spec := map[string]any{
		"env":      map[string]any{"POSTGRES_USER": "u", "POSTGRES_DB": "d", "PG_PORT": "5432"},
		"replicas": 3,
	}

	urls, err := buildLinkURLsWithPrimary("s", "db", spec, map[string]string{passwordKey: "pw"}, true, 1)
	if err != nil {
		t.Fatal(err)
	}

	wantHosts := []string{"db-0.s.voodu:5432", "db-2.s.voodu:5432"}
	for _, want := range wantHosts {
		if !strings.Contains(urls.ReadURL, want) {
			t.Errorf("ReadURL missing standby %q: %q", want, urls.ReadURL)
		}
	}

	// New primary must NOT appear in the read pool.
	if strings.Contains(urls.ReadURL, "db-1.s.voodu:5432") {
		t.Errorf("ReadURL leaked the new primary: %q", urls.ReadURL)
	}
}

func TestBuildLinkURLsWithPrimary_SinglePrimaryReadsEcho(t *testing.T) {
	// Failover on a 1-replica cluster shouldn't even be allowed,
	// but if buildLinkURLsWithPrimary is called with replicas=1
	// + readsOnly, it falls back to echoing the primary URL
	// (mirror of buildLinkURLs single-replica behavior).
	spec := map[string]any{
		"env":      map[string]any{"POSTGRES_USER": "u", "POSTGRES_DB": "d", "PG_PORT": "5432"},
		"replicas": 1,
	}

	urls, err := buildLinkURLsWithPrimary("s", "n", spec, map[string]string{passwordKey: "pw"}, true, 0)
	if err != nil {
		t.Fatal(err)
	}

	if urls.ReadURL != urls.WriteURL {
		t.Errorf("single-replica reads should echo primary, got read=%q write=%q", urls.ReadURL, urls.WriteURL)
	}
}

func TestBuildLinkURLsWithPrimary_MissingPasswordIsError(t *testing.T) {
	spec := map[string]any{
		"env":      map[string]any{"POSTGRES_USER": "u", "POSTGRES_DB": "d", "PG_PORT": "5432"},
		"replicas": 3,
	}

	_, err := buildLinkURLsWithPrimary("s", "n", spec, map[string]string{}, false, 1)
	if err == nil {
		t.Fatal("expected error for missing POSTGRES_PASSWORD")
	}

	if !strings.Contains(err.Error(), "POSTGRES_PASSWORD") {
		t.Errorf("error should mention password: %v", err)
	}
}

func TestJoinComma(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{nil, ""},
		{[]string{}, ""},
		{[]string{"a"}, "a"},
		{[]string{"a", "b"}, "a,b"},
		{[]string{"a", "b", "c"}, "a,b,c"},
	}

	for _, tc := range cases {
		got := joinComma(tc.in)
		if got != tc.want {
			t.Errorf("joinComma(%v): got %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFailoverHelpMentionsPrereq(t *testing.T) {
	// Operator must run pg_promote BEFORE this command — the help
	// text must say so loud and clear, otherwise operators will
	// run failover first and wonder why nothing happened.
	wantPhrases := []string{
		"pg_promote",
		"PRE-FLIGHT",
		"vd postgres:psql",
		"rejoin",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(failoverHelp, want) {
			t.Errorf("failoverHelp missing %q: text needs to make the prereq explicit", want)
		}
	}
}
