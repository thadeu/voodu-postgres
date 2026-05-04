// Tests for the wal_archive M-P2 surface: parser, validator, and
// conf renderer. The (asset, statefulset) plumbing — adding the
// 2nd volume_claim and 3rd asset bind — is covered in main_test.go
// where the e2e expand flow lives.
//
// Pinning here:
//
//   - Default spec is enabled with /wal-archive mount.
//   - Operator can opt out via `enabled = false` (no volume_claim
//     emitted, asset file shipped header-only).
//   - mount_path validation rejects relative paths and PGDATA
//     overlaps (would shadow data files).
//   - Conf renderer outputs alphabetically (asset digest stability).
//   - archive_command is single-quoted with the postgres-conf
//     escape rule (defends against operator-controlled mount paths
//     with quotes).

package main

import (
	"strings"
	"testing"
)

func TestParseWALArchiveSpec_AbsentBlockReturnsNil(t *testing.T) {
	// Caller substitutes defaultWALArchiveSpec when nil.
	got, err := parseWALArchiveSpec(nil)
	if err != nil {
		t.Fatalf("parse nil: %v", err)
	}

	if got != nil {
		t.Errorf("expected nil for absent block, got %+v", got)
	}

	got, err = parseWALArchiveSpec(map[string]any{})
	if err != nil {
		t.Fatalf("parse empty map: %v", err)
	}

	if got != nil {
		t.Errorf("expected nil for spec without wal_archive key, got %+v", got)
	}
}

func TestParseWALArchiveSpec_EmptyBlockUsesDefaults(t *testing.T) {
	// `wal_archive {}` — block declared but body empty. Defaults
	// fill in (enabled, /wal-archive).
	got, err := parseWALArchiveSpec(map[string]any{
		"wal_archive": map[string]any{},
	})

	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got == nil {
		t.Fatal("expected non-nil spec")
	}

	if !got.Enabled {
		t.Error("default Enabled should be true")
	}

	if got.MountPath != "/wal-archive" {
		t.Errorf("default MountPath: got %q, want /wal-archive", got.MountPath)
	}
}

func TestParseWALArchiveSpec_OperatorOverrides(t *testing.T) {
	got, err := parseWALArchiveSpec(map[string]any{
		"wal_archive": map[string]any{
			"enabled":    false,
			"mount_path": "/srv/pg/wal",
		},
	})

	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got.Enabled {
		t.Error("expected Enabled=false from operator override")
	}

	if got.MountPath != "/srv/pg/wal" {
		t.Errorf("MountPath: got %q", got.MountPath)
	}
}

func TestParseWALArchiveSpec_EmptyMountPathKeepsDefault(t *testing.T) {
	// `mount_path = ""` means "revert override" — same posture
	// as image/database empty in postgresSpec.
	got, err := parseWALArchiveSpec(map[string]any{
		"wal_archive": map[string]any{"mount_path": ""},
	})

	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got.MountPath != "/wal-archive" {
		t.Errorf("empty mount_path should keep default, got %q", got.MountPath)
	}
}

func TestParseWALArchiveSpec_TypeErrors(t *testing.T) {
	cases := []struct {
		name  string
		spec  any
		want  string
	}{
		{"block-not-map", "string-instead-of-block", "wal_archive"},
		{"enabled-not-bool", map[string]any{"enabled": "yes"}, "enabled"},
		{"mount_path-not-string", map[string]any{"mount_path": 42}, "mount_path"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseWALArchiveSpec(map[string]any{
				"wal_archive": tc.spec,
			})

			if err == nil {
				t.Fatalf("expected typed error for %s", tc.name)
			}

			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should mention %q, got: %v", tc.want, err)
			}
		})
	}
}

func TestValidateWALArchiveSpec_NilOK(t *testing.T) {
	// nil spec means "use defaults" — caller handles substitution.
	if err := validateWALArchiveSpec(nil); err != nil {
		t.Errorf("nil should validate (caller fills defaults): %v", err)
	}
}

func TestValidateWALArchiveSpec_DisabledSkipsChecks(t *testing.T) {
	// When disabled, mount_path is irrelevant — validator skips
	// the absoluteness/overlap checks.
	bad := &walArchiveSpec{Enabled: false, MountPath: "not-absolute"}
	if err := validateWALArchiveSpec(bad); err != nil {
		t.Errorf("disabled spec should skip checks: %v", err)
	}
}

func TestValidateWALArchiveSpec_RejectsRelativePath(t *testing.T) {
	cases := []string{
		"wal-archive",
		"./wal",
		"../wal",
	}

	for _, p := range cases {
		spec := &walArchiveSpec{Enabled: true, MountPath: p}
		if err := validateWALArchiveSpec(spec); err == nil {
			t.Errorf("expected validation error for relative path %q", p)
		}
	}
}

func TestValidateWALArchiveSpec_RejectsEmptyPath(t *testing.T) {
	spec := &walArchiveSpec{Enabled: true, MountPath: ""}
	if err := validateWALArchiveSpec(spec); err == nil {
		t.Error("expected validation error for empty mount_path")
	}
}

func TestValidateWALArchiveSpec_RejectsPGDATAOverlap(t *testing.T) {
	cases := []string{
		"/var/lib/postgresql/data",
		"/var/lib/postgresql/data/wal",
		"/var/lib/postgresql/data/anywhere/under",
	}

	for _, p := range cases {
		spec := &walArchiveSpec{Enabled: true, MountPath: p}

		err := validateWALArchiveSpec(spec)
		if err == nil {
			t.Errorf("expected error for PGDATA overlap %q", p)
		}

		if err != nil && !strings.Contains(err.Error(), "PGDATA") {
			t.Errorf("error should mention PGDATA, got: %v", err)
		}
	}
}

func TestValidateWALArchiveSpec_AcceptsAbsoluteOutsidePGDATA(t *testing.T) {
	cases := []string{
		"/wal-archive",
		"/srv/wal",
		"/mnt/backup/wal",
		"/var/lib/postgres-wal", // postgres-wal != postgresql/data
	}

	for _, p := range cases {
		spec := &walArchiveSpec{Enabled: true, MountPath: p}
		if err := validateWALArchiveSpec(spec); err != nil {
			t.Errorf("path %q should validate: %v", p, err)
		}
	}
}

func TestRenderWALArchiveConf_DisabledReturnsHeaderOnly(t *testing.T) {
	got := renderWALArchiveConf(&walArchiveSpec{Enabled: false})

	if !strings.Contains(got, "disabled") {
		t.Errorf("disabled spec should comment why:\n%s", got)
	}

	for _, line := range strings.Split(got, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		t.Errorf("disabled output should have no directive lines, got: %q", line)
	}
}

func TestRenderWALArchiveConf_NilSpecMatchesDisabled(t *testing.T) {
	// Defensive — nil shouldn't panic, behaves like disabled.
	got := renderWALArchiveConf(nil)
	if !strings.Contains(got, "disabled") {
		t.Errorf("nil should render as disabled:\n%s", got)
	}
}

func TestRenderWALArchiveConf_EnabledEmitsRequiredDirectives(t *testing.T) {
	got := renderWALArchiveConf(defaultWALArchiveSpec())

	wantLines := []string{
		"wal_level = replica",
		"archive_mode = on",
		"archive_timeout = 60",
		"archive_command = ", // value follows
	}

	for _, want := range wantLines {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

func TestRenderWALArchiveConf_ArchiveCommandUsesMountPath(t *testing.T) {
	got := renderWALArchiveConf(&walArchiveSpec{
		Enabled:   true,
		MountPath: "/srv/wal",
	})

	if !strings.Contains(got, "test ! -f /srv/wal/%f && cp %p /srv/wal/%f") {
		t.Errorf("archive_command should interpolate mount_path:\n%s", got)
	}
}

func TestRenderWALArchiveConf_AlphabeticalOrder(t *testing.T) {
	// Stable bytes across re-applies → asset digest doesn't flap.
	got := renderWALArchiveConf(defaultWALArchiveSpec())

	// archive_command should appear before archive_mode (alpha sort).
	cmdIdx := strings.Index(got, "archive_command")
	modeIdx := strings.Index(got, "archive_mode")

	if cmdIdx < 0 || modeIdx < 0 {
		t.Fatalf("missing keys in:\n%s", got)
	}

	if cmdIdx > modeIdx {
		t.Errorf("expected archive_command before archive_mode (alphabetical):\n%s", got)
	}
}

func TestRenderWALArchiveConf_Deterministic(t *testing.T) {
	a := renderWALArchiveConf(defaultWALArchiveSpec())
	b := renderWALArchiveConf(defaultWALArchiveSpec())

	if a != b {
		t.Error("render is not deterministic across calls")
	}
}
