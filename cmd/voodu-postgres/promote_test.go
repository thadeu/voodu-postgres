// Tests for the promote surface. cmdPromote itself talks to the
// controller + emits dispatch actions; we test the pure helpers
// (parsePromoteFlags, readCurrentPrimaryOrdinal, buildLinkURLs
// WithPrimary) in isolation. End-to-end flow is exercised in the
// build sweep smoke test (post-shipping) where a real cluster
// gets failed over.

package main

import (
	"strings"
	"testing"
)

func TestParsePromoteFlags_ReplicaSpaceSeparated(t *testing.T) {
	args := []string{"clowk-lp/db", "--replica", "1"}

	pos, target, hasTarget, _, noRestart := parsePromoteFlags(args)

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

func TestParsePromoteFlags_ReplicaEqualsForm(t *testing.T) {
	args := []string{"clowk-lp/db", "--replica=2"}

	_, target, hasTarget, _, _ := parsePromoteFlags(args)

	if !hasTarget || target != 2 {
		t.Errorf("--replica=2 not parsed: target=%d hasTarget=%v", target, hasTarget)
	}
}

func TestParsePromoteFlags_ShortReplicaForm(t *testing.T) {
	args := []string{"-r", "3", "clowk-lp/db"}

	pos, target, hasTarget, _, _ := parsePromoteFlags(args)

	if !hasTarget || target != 3 {
		t.Errorf("-r 3 not parsed: target=%d", target)
	}

	if len(pos) != 1 || pos[0] != "clowk-lp/db" {
		t.Errorf("positional should survive flag interleaving: %v", pos)
	}
}

func TestParsePromoteFlags_NoRestartFlag(t *testing.T) {
	args := []string{"clowk-lp/db", "--replica", "1", "--no-restart"}

	_, _, _, _, noRestart := parsePromoteFlags(args)

	if !noRestart {
		t.Error("--no-restart not parsed")
	}
}

func TestParsePromoteFlags_NoReplicaIsAbsent(t *testing.T) {
	args := []string{"clowk-lp/db"}

	_, target, hasTarget, _, _ := parsePromoteFlags(args)

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
	// After promote, primary moved from ordinal 0 to ordinal 1.
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
	// Promote on a 1-replica cluster shouldn't even be allowed,
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

func TestPromoteHelpMentionsCoreContract(t *testing.T) {
	// Help text must surface:
	//   - pg_promote: plugin owns the SQL; operator never types it
	//   - lag check + --force: data-loss safety belt is opt-out
	//   - rejoin: post-promote step required for the old primary
	wantPhrases := []string{
		"pg_promote",
		"--force",
		"lag",
		"rejoin",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(promoteHelp, want) {
			t.Errorf("promoteHelp missing %q: contract must be discoverable from --help", want)
		}
	}
}

func TestParseLagOutput_Valid(t *testing.T) {
	cases := []struct {
		raw           string
		wantStreaming int
		wantLag       int64
	}{
		{"2|0\n", 2, 0},
		{"2|0", 2, 0},
		{"3|4096", 3, 4096},
		{" 1 | 16384 ", 1, 16384},
		{"0|0", 0, 0},
	}

	for _, tc := range cases {
		got, err := parseLagOutput(tc.raw)
		if err != nil {
			t.Errorf("parseLagOutput(%q): unexpected err %v", tc.raw, err)
			continue
		}

		if got.streaming != tc.wantStreaming {
			t.Errorf("parseLagOutput(%q) streaming: got %d, want %d", tc.raw, got.streaming, tc.wantStreaming)
		}

		if got.maxLagBytes != tc.wantLag {
			t.Errorf("parseLagOutput(%q) maxLagBytes: got %d, want %d", tc.raw, got.maxLagBytes, tc.wantLag)
		}
	}
}

func TestParseLagOutput_Invalid(t *testing.T) {
	cases := []string{
		"",                  // empty
		"garbage",           // no separator
		"only one part",     // no |
		"a|b",               // non-numeric
		"1|notanumber",      // bad lag
		"notanumber|0",      // bad count
		"1|2|3",             // too many fields
	}

	for _, raw := range cases {
		_, err := parseLagOutput(raw)
		if err == nil {
			t.Errorf("parseLagOutput(%q): expected error, got none", raw)
		}
	}
}

func TestParsePromoteFlags_ForceFlag(t *testing.T) {
	args := []string{"clowk-lp/db", "--replica", "1", "--force"}
	_, _, _, force, _ := parsePromoteFlags(args)

	if !force {
		t.Error("--force not parsed")
	}
}

func TestParsePromoteFlags_ForceDefaultsFalse(t *testing.T) {
	args := []string{"clowk-lp/db", "--replica", "1"}
	_, _, _, force, _ := parsePromoteFlags(args)

	if force {
		t.Error("--force should default to false")
	}
}
