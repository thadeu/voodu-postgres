// Tests for the local WAL archive strategy: default destination
// composition, dangerous-path rejection, and the bind-mount +
// archive_command produced by Apply.
//
// Cross-cutting integration (composeStatefulsetDefaults wiring the
// strategy's Volumes into the spec) lives in main_test.go. Here
// we pin the strategy's contract in isolation.

package main

import (
	"strings"
	"testing"
)

func TestLocalStrategy_Name(t *testing.T) {
	if (localStrategy{}).Name() != "local" {
		t.Errorf("Name(): want %q", "local")
	}
}

func TestLocalStrategy_DefaultDestination(t *testing.T) {
	cases := []struct {
		scope, name, want string
	}{
		{"clowk-lp", "db", "/opt/voodu/backups/clowk-lp/db"},
		{"data", "main", "/opt/voodu/backups/data/main"},
		{"", "db", "/opt/voodu/backups/_unscoped/db"},
	}

	for _, tc := range cases {
		got := (localStrategy{}).DefaultDestination(tc.scope, tc.name)
		if got != tc.want {
			t.Errorf("DefaultDestination(%q,%q): got %q, want %q",
				tc.scope, tc.name, got, tc.want)
		}
	}
}

func TestLocalStrategy_Validate_EmptyDestinationOK(t *testing.T) {
	// Empty Destination is valid — Apply substitutes the default.
	// Validator only kicks in for explicitly-set values.
	spec := &walArchiveSpec{Enabled: true, Strategy: "local", Destination: ""}

	if err := (localStrategy{}).Validate(spec); err != nil {
		t.Errorf("empty Destination should validate: %v", err)
	}
}

func TestLocalStrategy_Validate_RejectsRelativePath(t *testing.T) {
	cases := []string{"relative/path", "wal-archive", "./wal", "../wal"}

	for _, p := range cases {
		spec := &walArchiveSpec{Enabled: true, Strategy: "local", Destination: p}

		err := (localStrategy{}).Validate(spec)
		if err == nil {
			t.Errorf("expected error for relative destination %q", p)
			continue
		}

		if !strings.Contains(err.Error(), "absolute") {
			t.Errorf("error should mention 'absolute', got: %v", err)
		}
	}
}

func TestLocalStrategy_Validate_RejectsDangerousRoots(t *testing.T) {
	cases := []string{
		"/",
		"/etc",
		"/etc/postgres",
		"/usr",
		"/usr/local",
		"/proc/1/fd",
		"/sys/kernel",
		"/dev/null",
		"/root",
		"/root/wal",
		"/boot",
	}

	for _, p := range cases {
		spec := &walArchiveSpec{Enabled: true, Strategy: "local", Destination: p}

		if err := (localStrategy{}).Validate(spec); err == nil {
			t.Errorf("expected error for dangerous destination %q", p)
		}
	}
}

func TestLocalStrategy_Validate_AcceptsSafeRoots(t *testing.T) {
	cases := []string{
		"/opt/voodu/backups/scope/name",
		"/srv/postgres-wal",
		"/mnt/nfs/pg-archive",
		"/data/wal",
		"/var/lib/voodu/backups",
		"/storage/db/wal",
		"/home/storage/wal",
	}

	for _, p := range cases {
		spec := &walArchiveSpec{Enabled: true, Strategy: "local", Destination: p}

		if err := (localStrategy{}).Validate(spec); err != nil {
			t.Errorf("path %q should validate: %v", p, err)
		}
	}
}

func TestLocalStrategy_Apply_DefaultDestination(t *testing.T) {
	// Empty Destination → Apply substitutes scope/name default
	// in the resulting bind-mount.
	spec := &walArchiveSpec{Enabled: true, Strategy: "local"}
	plan := localStrategy{}.Apply(spec, "clowk-lp", "db")

	if len(plan.Volumes) != 1 {
		t.Fatalf("expected 1 bind-mount, got %d: %v", len(plan.Volumes), plan.Volumes)
	}

	want := "/opt/voodu/backups/clowk-lp/db:/wal-archive:rw"
	if plan.Volumes[0] != want {
		t.Errorf("bind-mount: got %q, want %q", plan.Volumes[0], want)
	}
}

func TestLocalStrategy_Apply_OperatorDestination(t *testing.T) {
	spec := &walArchiveSpec{
		Enabled:     true,
		Strategy:    "local",
		Destination: "/srv/custom/wal",
	}
	plan := localStrategy{}.Apply(spec, "s", "n")

	want := "/srv/custom/wal:/wal-archive:rw"
	if plan.Volumes[0] != want {
		t.Errorf("bind-mount: got %q, want %q", plan.Volumes[0], want)
	}
}

func TestLocalStrategy_Apply_ArchiveCommandIsIdempotent(t *testing.T) {
	// archive_command must use the `test ! -f` guard so postgres
	// retry on a partially-archived segment doesn't corrupt the
	// archive file. Pin the canonical postgres-docs form.
	spec := &walArchiveSpec{Enabled: true, Strategy: "local"}
	plan := localStrategy{}.Apply(spec, "s", "n")

	want := "test ! -f /wal-archive/%f && cp %p /wal-archive/%f"
	if plan.ArchiveCommand != want {
		t.Errorf("archive_command:\n  got:  %q\n  want: %q", plan.ArchiveCommand, want)
	}
}

func TestLocalStrategy_Apply_BindMountIsRWForFailoverContinuity(t *testing.T) {
	// Standbys mount the archive :rw too — when one gets promoted
	// via vd pg:promote, it picks up writing into the same host
	// directory without a re-mount. Pin :rw to defend against a
	// future "harden by making standbys :ro" change that would
	// silently break failover continuity.
	spec := &walArchiveSpec{Enabled: true, Strategy: "local"}
	plan := localStrategy{}.Apply(spec, "s", "n")

	if !strings.HasSuffix(plan.Volumes[0], ":rw") {
		t.Errorf("bind-mount should be :rw for failover continuity, got: %q", plan.Volumes[0])
	}
}

func TestRejectDangerousLocalDestination_AllowsVarLib(t *testing.T) {
	// /var/lib/* is OK (conventional container state location);
	// despite being under /var, this prefix is explicitly allowed.
	if err := rejectDangerousLocalDestination("/var/lib/voodu/backups"); err != nil {
		t.Errorf("/var/lib/* should be allowed: %v", err)
	}
}
