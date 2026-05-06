// Tests for rejoin's pure helpers. The dispatch flow itself
// shells out to docker (stop / run --volumes-from / start) and
// requires a real container to exercise — covered by the manual
// E2E smoke at ship time, not unit-testable cheaply.

package main

import (
	"strings"
	"testing"
)

func TestParseRejoinFlags_ReplicaForms(t *testing.T) {
	cases := []struct {
		args     []string
		wantTgt  int
		wantHas  bool
		wantPos1 string
	}{
		{[]string{"clowk-lp/db", "--replica", "0"}, 0, true, "clowk-lp/db"},
		{[]string{"--replica", "2", "clowk-lp/db"}, 2, true, "clowk-lp/db"},
		{[]string{"clowk-lp/db", "--replica=3"}, 3, true, "clowk-lp/db"},
		{[]string{"-r", "1", "clowk-lp/db"}, 1, true, "clowk-lp/db"},
		{[]string{"clowk-lp/db"}, 0, false, "clowk-lp/db"}, // no --replica
	}

	for _, tc := range cases {
		pos, target, hasTarget := parseRejoinFlags(tc.args)

		if hasTarget != tc.wantHas {
			t.Errorf("args=%v: hasTarget=%v, want %v", tc.args, hasTarget, tc.wantHas)
		}

		if target != tc.wantTgt {
			t.Errorf("args=%v: target=%d, want %d", tc.args, target, tc.wantTgt)
		}

		if len(pos) < 1 || pos[0] != tc.wantPos1 {
			t.Errorf("args=%v: positional[0]=%v, want %q", tc.args, pos, tc.wantPos1)
		}
	}
}

func TestReadReplicationUser_Default(t *testing.T) {
	// Empty spec → default "replicator".
	got := readReplicationUser(map[string]any{})
	if got != "replicator" {
		t.Errorf("default should be replicator, got %q", got)
	}

	// env present but no PG_REPLICATION_USER → still default.
	got = readReplicationUser(map[string]any{
		"env": map[string]any{"FOO": "bar"},
	})

	if got != "replicator" {
		t.Errorf("missing PG_REPLICATION_USER should default to replicator, got %q", got)
	}
}

func TestReadReplicationUser_FromEnv(t *testing.T) {
	spec := map[string]any{
		"env": map[string]any{"PG_REPLICATION_USER": "myrepl"},
	}

	got := readReplicationUser(spec)
	if got != "myrepl" {
		t.Errorf("got %q, want myrepl", got)
	}
}

func TestReadImage(t *testing.T) {
	if got := readImage(map[string]any{"image": "postgres:16"}); got != "postgres:16" {
		t.Errorf("got %q", got)
	}

	if got := readImage(map[string]any{}); got != "" {
		t.Errorf("missing image should yield empty, got %q", got)
	}
}

func TestReadPGDataPath(t *testing.T) {
	spec := map[string]any{
		"env": map[string]any{"PGDATA": "/custom/path"},
	}

	if got := readPGDataPath(spec); got != "/custom/path" {
		t.Errorf("got %q", got)
	}

	// Missing env → empty (caller substitutes default).
	if got := readPGDataPath(map[string]any{}); got != "" {
		t.Errorf("missing env should yield empty, got %q", got)
	}
}

func TestReadPortInt(t *testing.T) {
	spec := map[string]any{
		"env": map[string]any{"PG_PORT": "5433"},
	}

	if got := readPortInt(spec); got != 5433 {
		t.Errorf("got %d, want 5433", got)
	}

	// Missing → 0; caller defaults to 5432.
	if got := readPortInt(map[string]any{}); got != 0 {
		t.Errorf("missing should yield 0, got %d", got)
	}

	// Garbage → 0.
	bad := map[string]any{"env": map[string]any{"PG_PORT": "abc"}}
	if got := readPortInt(bad); got != 0 {
		t.Errorf("garbage should yield 0, got %d", got)
	}
}

// TestRejoinHelpMentionsAutoFallback pins the new auto-fallback
// guarantee: when pg_rewind fails, rejoin doesn't bail to the
// operator — it wipes the volume and re-clones via pg_basebackup
// automatically. The help text must explain that mechanism so
// operators understand what's about to happen (and don't think
// the failure is fatal).
func TestRejoinHelpMentionsAutoFallback(t *testing.T) {
	wantPhrases := []string{
		"pg_rewind",
		"FALLS BACK AUTOMATICALLY",
		"pg_basebackup",
		// Clarity that it's safe — standby is a clone, no data loss.
		"a clone",
		"no data is lost",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(rejoinHelp, want) {
			t.Errorf("rejoinHelp missing %q: auto-fallback must be discoverable", want)
		}
	}
}

// TestComposeStandbyVolumeName mirrors the controller's
// volumeName() so the plugin's wipe path actually targets the
// volume the controller created. Drift between the two breaks
// the auto-rebootstrap silently — `docker volume rm` succeeds on
// "no such volume", leaving the real one intact and the next pod
// boot reusing stale data.
func TestComposeStandbyVolumeName(t *testing.T) {
	cases := []struct {
		scope, name string
		ord         int
		want        string
	}{
		{"clowk-lp", "db", 0, "voodu-clowk-lp-db-data-0"},
		{"clowk-lp", "db", 1, "voodu-clowk-lp-db-data-1"},
		{"data", "main", 7, "voodu-data-main-data-7"},
		// Unscoped — no leading scope segment.
		{"", "pg", 2, "voodu-pg-data-2"},
	}

	for _, tc := range cases {
		got := composeStandbyVolumeName(tc.scope, tc.name, tc.ord)
		if got != tc.want {
			t.Errorf("composeStandbyVolumeName(%q,%q,%d): got %q, want %q",
				tc.scope, tc.name, tc.ord, got, tc.want)
		}
	}
}

func TestContainerNameFor(t *testing.T) {
	cases := []struct {
		scope, name string
		ord         int
		want        string
	}{
		{"clowk-lp", "db", 0, "clowk-lp-db.0"},
		{"clowk-lp", "db", 2, "clowk-lp-db.2"},
		{"", "redis", 1, "redis.1"},
	}

	for _, tc := range cases {
		got := containerNameFor(tc.scope, tc.name, tc.ord)
		if got != tc.want {
			t.Errorf("containerNameFor(%q,%q,%d): got %q, want %q",
				tc.scope, tc.name, tc.ord, got, tc.want)
		}
	}
}
