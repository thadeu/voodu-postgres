// Tests for restore command's pure helpers. The destructive
// shell flow (stop pods + wipe + extract) needs a real cluster
// to exercise — covered by manual E2E at ship time.

package main

import (
	"strings"
	"testing"
)

func TestParseRestoreFlags_FromLong(t *testing.T) {
	args := []string{"clowk-lp/db", "--from", "/srv/db.tar"}

	pos, src, _, _, err := parseRestoreFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if src != "/srv/db.tar" {
		t.Errorf("src: got %q", src)
	}

	if len(pos) != 1 || pos[0] != "clowk-lp/db" {
		t.Errorf("positional: %v", pos)
	}
}

func TestParseRestoreFlags_FromShort(t *testing.T) {
	args := []string{"clowk-lp/db", "-f", "/srv/db.tar"}

	_, src, _, _, err := parseRestoreFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if src != "/srv/db.tar" {
		t.Errorf("src: got %q", src)
	}
}

func TestParseRestoreFlags_FromEqualsForm(t *testing.T) {
	args := []string{"clowk-lp/db", "--from=/tmp/db.tar"}

	_, src, _, _, err := parseRestoreFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if src != "/tmp/db.tar" {
		t.Errorf("src: got %q", src)
	}
}

func TestParseRestoreFlags_TargetTime(t *testing.T) {
	args := []string{"clowk-lp/db", "--from", "/tmp/db.tar", "--target-time", "2026-05-04 14:30:00"}

	_, _, targetTime, _, err := parseRestoreFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if targetTime != "2026-05-04 14:30:00" {
		t.Errorf("target-time: got %q", targetTime)
	}
}

func TestParseRestoreFlags_TargetTimeEqualsForm(t *testing.T) {
	args := []string{"clowk-lp/db", "--from=/tmp/db.tar", "--target-time=2026-05-04 14:30:00 UTC"}

	_, _, targetTime, _, err := parseRestoreFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if targetTime != "2026-05-04 14:30:00 UTC" {
		t.Errorf("target-time: got %q", targetTime)
	}
}

func TestParseRestoreFlags_AutoYesForms(t *testing.T) {
	cases := []struct {
		args   []string
		wantOK bool
	}{
		{[]string{"x", "--from", "/t.tar", "--yes"}, true},
		{[]string{"x", "--from", "/t.tar", "-y"}, true},
		{[]string{"x", "--from", "/t.tar"}, false},
	}

	for _, tc := range cases {
		_, _, _, autoYes, _ := parseRestoreFlags(tc.args)
		if autoYes != tc.wantOK {
			t.Errorf("args %v: autoYes=%v, want %v", tc.args, autoYes, tc.wantOK)
		}
	}
}

func TestParseRestoreFlags_FromMissingValue(t *testing.T) {
	args := []string{"clowk-lp/db", "--from"}

	_, _, _, _, err := parseRestoreFlags(args)
	if err == nil {
		t.Fatal("expected error for missing value")
	}
}

func TestEscapeSingleQuote(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},
		{"abc", "abc"},
		{"O'Reilly", "O''Reilly"},
		{"'leading", "''leading"},
		{"trailing'", "trailing''"},
		{"two''already", "two''''already"},
	}

	for _, tc := range cases {
		got := escapeSingleQuote(tc.in)
		if got != tc.want {
			t.Errorf("escapeSingleQuote(%q): got %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestRestoreHelpMentionsDestructiveAndPITR(t *testing.T) {
	wantPhrases := []string{
		"DESTRUCTIVE",
		"NO rollback",
		"--yes",
		"Point-in-time",
		"--target-time",
		"pg_basebackup",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(restoreHelp, want) {
			t.Errorf("restoreHelp missing %q", want)
		}
	}
}
