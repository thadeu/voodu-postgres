// Tests for psql parser. The interactive flow itself shells out
// via syscall.Exec — not unit-testable cheaply (would require a
// real container). The pure parser is the only safe surface.

package main

import (
	"strings"
	"testing"
)

func TestParsePsqlFlags_NoFlags(t *testing.T) {
	args := []string{"clowk-lp/db"}
	pos, _, hasReplica, psqlArgs := parsePsqlFlags(args)

	if len(pos) != 1 || pos[0] != "clowk-lp/db" {
		t.Errorf("positional: %v", pos)
	}

	if hasReplica {
		t.Error("no --replica should yield hasReplica=false")
	}

	if len(psqlArgs) != 0 {
		t.Errorf("psql passthrough should be empty, got %v", psqlArgs)
	}
}

func TestParsePsqlFlags_Replica(t *testing.T) {
	args := []string{"clowk-lp/db", "--replica", "2"}
	_, replica, hasReplica, _ := parsePsqlFlags(args)

	if !hasReplica || replica != 2 {
		t.Errorf("--replica 2 not parsed: hasReplica=%v replica=%d", hasReplica, replica)
	}
}

func TestParsePsqlFlags_DashCBecomesPassthrough(t *testing.T) {
	// -c "SELECT 1" should reach psql verbatim. Both -c and the
	// SQL string become positional in the docker exec argv.
	args := []string{"clowk-lp/db", "-c", "SELECT 1"}
	_, _, _, psqlArgs := parsePsqlFlags(args)

	if len(psqlArgs) != 2 || psqlArgs[0] != "-c" || psqlArgs[1] != "SELECT 1" {
		t.Errorf("expected [-c, SELECT 1], got %v", psqlArgs)
	}
}

func TestParsePsqlFlags_DoubleDashPassthrough(t *testing.T) {
	// Everything after `--` flows verbatim — useful for psql flags
	// voodu doesn't model (--csv, --tuples-only, etc).
	args := []string{"clowk-lp/db", "--", "--csv", "-c", "SELECT 1"}
	_, _, _, psqlArgs := parsePsqlFlags(args)

	want := []string{"--csv", "-c", "SELECT 1"}
	if len(psqlArgs) != len(want) {
		t.Fatalf("expected %d args, got %v", len(want), psqlArgs)
	}

	for i, w := range want {
		if psqlArgs[i] != w {
			t.Errorf("args[%d]: got %q, want %q", i, psqlArgs[i], w)
		}
	}
}

func TestParsePsqlFlags_ReplicaAndCommandTogether(t *testing.T) {
	// Common: connect to standby + run query.
	args := []string{"clowk-lp/db", "--replica", "1", "-c", "SELECT pg_promote();"}
	pos, replica, hasReplica, psqlArgs := parsePsqlFlags(args)

	if pos[0] != "clowk-lp/db" {
		t.Errorf("positional[0]: %v", pos)
	}

	if !hasReplica || replica != 1 {
		t.Errorf("replica: hasReplica=%v replica=%d", hasReplica, replica)
	}

	if len(psqlArgs) != 2 || psqlArgs[0] != "-c" || psqlArgs[1] != "SELECT pg_promote();" {
		t.Errorf("psql passthrough: %v", psqlArgs)
	}
}

func TestParsePsqlFlags_ReplicaEqualsForm(t *testing.T) {
	args := []string{"clowk-lp/db", "--replica=3"}
	_, replica, hasReplica, _ := parsePsqlFlags(args)

	if !hasReplica || replica != 3 {
		t.Errorf("--replica=3 not parsed: hasReplica=%v replica=%d", hasReplica, replica)
	}
}

func TestPsqlHelpMentionsTrustAuth(t *testing.T) {
	// Operator coming in cold needs to know psql works without a
	// password — the help must explain why.
	wantPhrases := []string{
		"NO PASSWORD",
		"unix socket",
		"trust",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(psqlHelp, want) {
			t.Errorf("psqlHelp missing %q: %s", want, psqlHelp)
		}
	}
}

func TestParseInt(t *testing.T) {
	cases := []struct {
		in    string
		want  int
		isErr bool
	}{
		{"0", 0, false},
		{"5", 5, false},
		{"123", 123, false},
		{"", 0, true},
		{"abc", 0, true},
		{"5x", 0, true},
		{"-1", 0, true}, // we don't accept negative
	}

	for _, tc := range cases {
		got, err := parseInt(tc.in)

		if tc.isErr {
			if err == nil {
				t.Errorf("parseInt(%q): expected error, got %d", tc.in, got)
			}

			continue
		}

		if err != nil {
			t.Errorf("parseInt(%q): unexpected err %v", tc.in, err)
			continue
		}

		if got != tc.want {
			t.Errorf("parseInt(%q): got %d, want %d", tc.in, got, tc.want)
		}
	}
}
