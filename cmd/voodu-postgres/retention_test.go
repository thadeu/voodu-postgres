// Tests for retention policy parsing + application. The on-disk
// prune flow (pruneBackupsByPolicy) needs a real /opt/voodu/backups
// directory + docker; the pure logic — what gets selected for
// deletion under a given policy — is tested here exhaustively.

package main

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRetentionPolicy_IsZero(t *testing.T) {
	cases := []struct {
		policy retentionPolicy
		want   bool
	}{
		{retentionPolicy{}, true},
		{retentionPolicy{KeepLast: 0, MaxAge: 0}, true},
		{retentionPolicy{KeepLast: 1}, false},
		{retentionPolicy{MaxAge: time.Hour}, false},
		{retentionPolicy{KeepLast: 30, MaxAge: 7 * 24 * time.Hour}, false},
	}

	for _, tc := range cases {
		got := tc.policy.IsZero()
		if got != tc.want {
			t.Errorf("IsZero(%+v) = %v, want %v", tc.policy, got, tc.want)
		}
	}
}

func TestRetentionPolicy_String(t *testing.T) {
	cases := []struct {
		policy retentionPolicy
		want   string
	}{
		{retentionPolicy{}, "(none)"},
		{retentionPolicy{KeepLast: 30}, "keep 30"},
		// 7 days is exactly 1 week — formatDurationShort prefers
		// the larger unit when it divides cleanly. 14d → 2w,
		// 21d → 3w. Operators see "1w" instead of "7d" but the
		// shorthand parser accepts both, so policy round-trips.
		{retentionPolicy{MaxAge: 7 * 24 * time.Hour}, "max-age 1w"},
		{retentionPolicy{MaxAge: 14 * 24 * time.Hour}, "max-age 2w"},
		{retentionPolicy{MaxAge: 90 * time.Minute}, "max-age 1h30m0s"},
		{retentionPolicy{KeepLast: 10, MaxAge: 24 * time.Hour}, "keep 10, max-age 1d"},
	}

	for _, tc := range cases {
		got := tc.policy.String()
		if got != tc.want {
			t.Errorf("String(%+v): got %q, want %q", tc.policy, got, tc.want)
		}
	}
}

func TestApplyRetention_EmptyPolicy(t *testing.T) {
	// Empty policy must NEVER delete anything, even with a fat
	// candidate list — guards against an accidental "default
	// prune" wiping operator data on a typo'd flag.
	entries := []backupEntry{
		{ID: 1, Timestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)},
		{ID: 2, Timestamp: time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)},
	}

	got := applyRetention(entries, retentionPolicy{}, time.Now())
	if len(got) != 0 {
		t.Errorf("empty policy must not delete; got %d", len(got))
	}
}

func TestApplyRetention_KeepLastOnly(t *testing.T) {
	// 5 entries, keep 3. Newest 3 stay; oldest 2 get marked.
	now := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC)

	entries := []backupEntry{
		{ID: 1, Timestamp: now.Add(-5 * 24 * time.Hour)},
		{ID: 2, Timestamp: now.Add(-4 * 24 * time.Hour)},
		{ID: 3, Timestamp: now.Add(-3 * 24 * time.Hour)},
		{ID: 4, Timestamp: now.Add(-2 * 24 * time.Hour)},
		{ID: 5, Timestamp: now.Add(-1 * 24 * time.Hour)},
	}

	got := applyRetention(entries, retentionPolicy{KeepLast: 3}, now)
	if len(got) != 2 {
		t.Fatalf("expected 2 deletions, got %d: %+v", len(got), got)
	}

	// IDs 1 and 2 are oldest — they're the ones that should fall
	// past rank 3 in newest-first sort.
	wantIDs := map[int]bool{1: true, 2: true}
	for _, e := range got {
		if !wantIDs[e.ID] {
			t.Errorf("unexpected deletion id=%d (want IDs 1 or 2)", e.ID)
		}
	}
}

func TestApplyRetention_KeepLastBiggerThanInput(t *testing.T) {
	// KeepLast > total count → no deletions. Operator over-budgets;
	// keep everything.
	now := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC)

	entries := []backupEntry{
		{ID: 1, Timestamp: now.Add(-2 * 24 * time.Hour)},
		{ID: 2, Timestamp: now.Add(-1 * 24 * time.Hour)},
	}

	got := applyRetention(entries, retentionPolicy{KeepLast: 30}, now)
	if len(got) != 0 {
		t.Errorf("expected 0 deletions when KeepLast > total; got %d", len(got))
	}
}

func TestApplyRetention_MaxAgeOnly(t *testing.T) {
	// MaxAge = 7d. Anything older than that gets marked, regardless
	// of count.
	now := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC)

	entries := []backupEntry{
		{ID: 1, Timestamp: now.Add(-30 * 24 * time.Hour)}, // delete (30d old)
		{ID: 2, Timestamp: now.Add(-10 * 24 * time.Hour)}, // delete (10d old)
		{ID: 3, Timestamp: now.Add(-5 * 24 * time.Hour)},  // keep (5d old)
		{ID: 4, Timestamp: now.Add(-1 * 24 * time.Hour)},  // keep
	}

	got := applyRetention(entries, retentionPolicy{MaxAge: 7 * 24 * time.Hour}, now)

	wantIDs := map[int]bool{1: true, 2: true}

	if len(got) != 2 {
		t.Fatalf("expected 2 deletions, got %d: %+v", len(got), got)
	}

	for _, e := range got {
		if !wantIDs[e.ID] {
			t.Errorf("unexpected deletion id=%d", e.ID)
		}
	}
}

func TestApplyRetention_BothAxes(t *testing.T) {
	// KeepLast=3 AND MaxAge=10d. A backup is deleted if it fails
	// EITHER check.
	now := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC)

	entries := []backupEntry{
		{ID: 1, Timestamp: now.Add(-30 * 24 * time.Hour)}, // age fail (30d), rank fail (5)
		{ID: 2, Timestamp: now.Add(-15 * 24 * time.Hour)}, // age fail (15d), rank fail (4)
		{ID: 3, Timestamp: now.Add(-12 * 24 * time.Hour)}, // age fail (12d), rank ok (3)
		{ID: 4, Timestamp: now.Add(-8 * 24 * time.Hour)},  // age ok (8d),   rank ok (2)
		{ID: 5, Timestamp: now.Add(-1 * 24 * time.Hour)},  // age ok (1d),   rank ok (1)
	}

	got := applyRetention(entries, retentionPolicy{KeepLast: 3, MaxAge: 10 * 24 * time.Hour}, now)

	// Want: IDs 1, 2 (rank fail); ID 3 (age fail). Total 3 deletions.
	if len(got) != 3 {
		t.Fatalf("expected 3 deletions, got %d: %+v", len(got), got)
	}

	wantIDs := map[int]bool{1: true, 2: true, 3: true}
	for _, e := range got {
		if !wantIDs[e.ID] {
			t.Errorf("unexpected deletion id=%d", e.ID)
		}
	}
}

func TestApplyRetention_DoesNotMutateInput(t *testing.T) {
	// Input slice must be untouched after applyRetention — caller
	// uses it for rendering after pruning.
	now := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC)

	entries := []backupEntry{
		{ID: 1, Timestamp: now.Add(-3 * 24 * time.Hour)},
		{ID: 2, Timestamp: now.Add(-1 * 24 * time.Hour)},
		{ID: 3, Timestamp: now.Add(-2 * 24 * time.Hour)},
	}

	original := make([]backupEntry, len(entries))
	copy(original, entries)

	_ = applyRetention(entries, retentionPolicy{KeepLast: 1}, now)

	for i, want := range original {
		if entries[i] != want {
			t.Errorf("input mutated at [%d]: got %+v, want %+v", i, entries[i], want)
		}
	}
}

func TestApplyRetention_BoundaryAtMaxAge(t *testing.T) {
	// Backup exactly MaxAge old: kept (the predicate is strict
	// `>`, not `>=`). Sub-second drift in the operator's clock
	// shouldn't accidentally tip the balance.
	now := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC)

	entries := []backupEntry{
		{ID: 1, Timestamp: now.Add(-7 * 24 * time.Hour)}, // exactly 7d
	}

	got := applyRetention(entries, retentionPolicy{MaxAge: 7 * 24 * time.Hour}, now)
	if len(got) != 0 {
		t.Errorf("entry exactly at MaxAge should be kept; got %d deletions", len(got))
	}
}

func TestParseRetentionFlags_Empty(t *testing.T) {
	pos, policy, err := parseRetentionFlags([]string{"clowk-lp/db"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if len(pos) != 1 || pos[0] != "clowk-lp/db" {
		t.Errorf("residual: %v", pos)
	}

	if !policy.IsZero() {
		t.Errorf("policy should be empty, got %+v", policy)
	}
}

func TestParseRetentionFlags_KeepAndMaxAge(t *testing.T) {
	args := []string{"--keep", "30", "ref", "--max-age", "7d"}

	pos, policy, err := parseRetentionFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if len(pos) != 1 || pos[0] != "ref" {
		t.Errorf("residual should preserve positional 'ref': %v", pos)
	}

	if policy.KeepLast != 30 {
		t.Errorf("KeepLast: got %d, want 30", policy.KeepLast)
	}

	if policy.MaxAge != 7*24*time.Hour {
		t.Errorf("MaxAge: got %v, want 7d", policy.MaxAge)
	}
}

func TestParseRetentionFlags_EqualsForms(t *testing.T) {
	args := []string{"ref", "--keep=10", "--max-age=2w"}

	_, policy, err := parseRetentionFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if policy.KeepLast != 10 {
		t.Errorf("KeepLast: got %d, want 10", policy.KeepLast)
	}

	if policy.MaxAge != 14*24*time.Hour {
		t.Errorf("MaxAge: got %v, want 2w (336h)", policy.MaxAge)
	}
}

func TestParseRetentionFlags_RetentionAlias(t *testing.T) {
	// `--retention` is an alias for `--max-age`. Both spellings
	// land in policy.MaxAge.
	for _, args := range [][]string{
		{"ref", "--retention", "14d"},
		{"ref", "--retention=14d"},
	} {
		_, policy, err := parseRetentionFlags(args)
		if err != nil {
			t.Fatalf("parse %v: %v", args, err)
		}

		if policy.MaxAge != 14*24*time.Hour {
			t.Errorf("--retention 14d: got MaxAge %v, want 14d (args=%v)", policy.MaxAge, args)
		}
	}
}

func TestParseRetentionFlags_ErrorCases(t *testing.T) {
	cases := []struct {
		args     []string
		wantErr  string
	}{
		{[]string{"--keep"}, "--keep requires"},
		{[]string{"--keep", "abc"}, "--keep"},
		// parseInt rejects negative numbers entirely (the digit
		// loop sees '-' and bails). The message we surface is
		// the parseInt error, not our explicit ">= 0" guard —
		// the guard is still there as a defensive net for any
		// future parser that admits negatives.
		{[]string{"--keep", "-1"}, "not a number"},
		{[]string{"--max-age"}, "--max-age requires"},
		{[]string{"--max-age", "garbage"}, "not a duration"},
		{[]string{"--retention=neverr"}, "not a duration"},
	}

	for _, tc := range cases {
		_, _, err := parseRetentionFlags(tc.args)
		if err == nil {
			t.Errorf("expected error for %v", tc.args)
			continue
		}

		if !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("error for %v should contain %q, got %v", tc.args, tc.wantErr, err)
		}
	}
}

func TestParseRetentionDuration_Days(t *testing.T) {
	d, err := parseRetentionDuration("7d")
	if err != nil {
		t.Fatal(err)
	}

	if d != 7*24*time.Hour {
		t.Errorf("got %v, want 168h", d)
	}
}

func TestParseRetentionDuration_Weeks(t *testing.T) {
	d, err := parseRetentionDuration("2w")
	if err != nil {
		t.Fatal(err)
	}

	if d != 14*24*time.Hour {
		t.Errorf("got %v, want 336h", d)
	}
}

func TestParseRetentionDuration_GoFormat(t *testing.T) {
	// Standard time.ParseDuration formats (h, m, s, h+m+s
	// combinations) still work — falls through to ParseDuration.
	cases := map[string]time.Duration{
		"168h":  168 * time.Hour,
		"30m":   30 * time.Minute,
		"1h30m": time.Hour + 30*time.Minute,
	}

	for s, want := range cases {
		got, err := parseRetentionDuration(s)
		if err != nil {
			t.Errorf("parse(%q): %v", s, err)
			continue
		}

		if got != want {
			t.Errorf("parse(%q): got %v, want %v", s, got, want)
		}
	}
}

func TestFormatDurationShort(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{24 * time.Hour, "1d"},
		{7 * 24 * time.Hour, "1w"},
		{14 * 24 * time.Hour, "2w"},
		{3 * 24 * time.Hour, "3d"},
		{30 * time.Minute, "30m0s"},
		{0, "0"},
	}

	for _, tc := range cases {
		got := formatDurationShort(tc.d)
		if got != tc.want {
			t.Errorf("formatDurationShort(%v): got %q, want %q", tc.d, got, tc.want)
		}
	}
}

// TestBackupsPruneHelpMentionsSurfaces pins the operator-facing
// docstring for :prune so refactors don't accidentally drop the
// flags from the public surface.
func TestBackupsPruneHelpMentionsSurfaces(t *testing.T) {
	wantPhrases := []string{
		"--keep",
		"--max-age",
		"--dry-run",
		"--yes",
		"DESTRUCTIVE",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(backupsPruneHelp, want) {
			t.Errorf("backupsPruneHelp missing %q", want)
		}
	}
}

// TestLoadRetentionFromConfig pins the bucket-key contract: the
// keys plugin reads MUST be the same ones cmdExpand seeds, otherwise
// the default would silently never apply. Hard-coded to catch typos.
func TestLoadRetentionFromConfig(t *testing.T) {
	cfg := map[string]string{
		"BACKUP_KEEP":    "30",
		"BACKUP_MAX_AGE": "7d",
	}

	got := loadRetentionFromConfig(cfg)

	if got.KeepLast != 30 {
		t.Errorf("KeepLast: got %d, want 30", got.KeepLast)
	}

	if got.MaxAge != 7*24*time.Hour {
		t.Errorf("MaxAge: got %v, want 7d", got.MaxAge)
	}
}

func TestLoadRetentionFromConfig_EmptyBucket(t *testing.T) {
	got := loadRetentionFromConfig(nil)
	if !got.IsZero() {
		t.Errorf("nil config: got %+v, want zero", got)
	}

	got = loadRetentionFromConfig(map[string]string{})
	if !got.IsZero() {
		t.Errorf("empty config: got %+v, want zero", got)
	}
}

func TestLoadRetentionFromConfig_BlankValueIsZero(t *testing.T) {
	// Operator who did `vd config <ref> set BACKUP_KEEP=` (empty
	// value) is opting OUT — the axis stays zero. Pin the
	// trim-then-empty path.
	cfg := map[string]string{
		"BACKUP_KEEP":    "  ",
		"BACKUP_MAX_AGE": "",
	}

	got := loadRetentionFromConfig(cfg)
	if !got.IsZero() {
		t.Errorf("blank values should yield zero policy, got %+v", got)
	}
}

func TestLoadRetentionFromConfig_MalformedValuesAreIgnored(t *testing.T) {
	// Operator typo'd. Plugin must tolerate it (logs a warn) and
	// fall back to no-op for that axis. The OTHER axis still
	// applies, so a half-broken config doesn't disable retention
	// entirely.
	cfg := map[string]string{
		"BACKUP_KEEP":    "thirty",
		"BACKUP_MAX_AGE": "7d",
	}

	got := loadRetentionFromConfig(cfg)
	if got.KeepLast != 0 {
		t.Errorf("malformed KeepLast should be ignored; got %d", got.KeepLast)
	}

	if got.MaxAge != 7*24*time.Hour {
		t.Errorf("MaxAge should still parse; got %v", got.MaxAge)
	}
}

func TestMergeRetention_FlagOverridesPerAxis(t *testing.T) {
	// Flag wins per-axis when set (>0). A flag that's zero
	// leaves the config value alone — important for the
	// "BACKUP_MAX_AGE in bucket, only --keep on CLI" use case.
	flags := retentionPolicy{KeepLast: 5}                 // only count
	cfg := retentionPolicy{KeepLast: 30, MaxAge: 7 * 24 * time.Hour}

	got := mergeRetention(flags, cfg)

	if got.KeepLast != 5 {
		t.Errorf("flag KeepLast=5 should override config 30; got %d", got.KeepLast)
	}

	if got.MaxAge != 7*24*time.Hour {
		t.Errorf("config MaxAge should survive (flag didn't set it); got %v", got.MaxAge)
	}
}

func TestMergeRetention_BothEmptyStaysEmpty(t *testing.T) {
	got := mergeRetention(retentionPolicy{}, retentionPolicy{})
	if !got.IsZero() {
		t.Errorf("both empty should stay empty, got %+v", got)
	}
}

func TestMergeRetention_FlagOnlyWhenConfigEmpty(t *testing.T) {
	got := mergeRetention(retentionPolicy{KeepLast: 10}, retentionPolicy{})
	if got.KeepLast != 10 {
		t.Errorf("flag-only KeepLast: got %d, want 10", got.KeepLast)
	}
}

func TestDefaultBackupKeep_IsConservativeNonZeroInteger(t *testing.T) {
	// Pin the default value as a known constant, not just "any
	// integer". A typo in the default (e.g. "030" or "30 ") would
	// still parse via Atoi but wouldn't round-trip the way an
	// operator expects when they read `vd config get`.
	if defaultBackupKeep != "30" {
		t.Errorf("defaultBackupKeep changed from \"30\" to %q — verify README + tests are in sync",
			defaultBackupKeep)
	}

	n, err := strconv.Atoi(defaultBackupKeep)
	if err != nil || n <= 0 {
		t.Errorf("defaultBackupKeep should parse to a positive integer, got %q", defaultBackupKeep)
	}
}

// TestBackupsCaptureHelpMentionsRetention confirms :capture's help
// surfaces the new --keep / --max-age flags (and the --retention
// alias). Operators discover these through `vd pg:backups:capture
// -h` so their absence would silently regress UX.
func TestBackupsCaptureHelpMentionsRetention(t *testing.T) {
	wantPhrases := []string{
		"--keep",
		"--max-age",
		"--retention",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(backupsCaptureHelp, want) {
			t.Errorf("backupsCaptureHelp missing %q", want)
		}
	}
}
