// Tests for backup command's pure helpers. The shell flow that
// runs pg_basebackup against a real container is exercised in
// the manual E2E smoke at ship time — too heavy for unit tests.

package main

import (
	"strings"
	"testing"
)

func TestParseBackupFlags_DestinationLong(t *testing.T) {
	args := []string{"clowk-lp/db", "--destination", "/tmp/db.tar"}

	pos, dest, _, _, err := parseBackupFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if dest != "/tmp/db.tar" {
		t.Errorf("dest: got %q", dest)
	}

	if len(pos) != 1 || pos[0] != "clowk-lp/db" {
		t.Errorf("positional: %v", pos)
	}
}

func TestParseBackupFlags_DestinationEqualsForm(t *testing.T) {
	args := []string{"clowk-lp/db", "--destination=/srv/db.tar"}

	_, dest, _, _, err := parseBackupFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if dest != "/srv/db.tar" {
		t.Errorf("dest: got %q", dest)
	}
}

func TestParseBackupFlags_DestinationShort(t *testing.T) {
	args := []string{"clowk-lp/db", "-d", "/tmp/db.tar"}

	_, dest, _, _, err := parseBackupFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if dest != "/tmp/db.tar" {
		t.Errorf("dest: got %q", dest)
	}
}

func TestParseBackupFlags_DestinationMissingValue(t *testing.T) {
	args := []string{"clowk-lp/db", "--destination"}

	_, _, _, _, err := parseBackupFlags(args)
	if err == nil {
		t.Fatal("expected error for missing value")
	}
}

func TestParseBackupFlags_FromReplicaForms(t *testing.T) {
	cases := []struct {
		args        []string
		wantReplica int
		wantHas     bool
	}{
		{[]string{"clowk-lp/db", "--from-replica", "1"}, 1, true},
		{[]string{"clowk-lp/db", "--from-replica=2"}, 2, true},
		{[]string{"clowk-lp/db"}, 0, false},
	}

	for _, tc := range cases {
		_, _, replica, hasReplica, err := parseBackupFlags(tc.args)
		if err != nil {
			t.Errorf("args %v: unexpected err %v", tc.args, err)
			continue
		}

		if hasReplica != tc.wantHas {
			t.Errorf("args %v: hasReplica=%v, want %v", tc.args, hasReplica, tc.wantHas)
		}

		if replica != tc.wantReplica {
			t.Errorf("args %v: replica=%d, want %d", tc.args, replica, tc.wantReplica)
		}
	}
}

func TestParseBackupFlags_FromReplicaInvalid(t *testing.T) {
	args := []string{"clowk-lp/db", "--from-replica", "garbage"}

	_, _, _, _, err := parseBackupFlags(args)
	if err == nil {
		t.Fatal("expected error for non-integer --from-replica")
	}
}

func TestBackupHelpMentionsPgBasebackup(t *testing.T) {
	wantPhrases := []string{
		"pg_basebackup",
		"docker run",
		"vd postgres:restore",
		"cronjob",
		"--from-replica",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(backupHelp, want) {
			t.Errorf("backupHelp missing %q", want)
		}
	}
}
