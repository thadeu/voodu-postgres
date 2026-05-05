// Tests for the pg:backups pure helpers — filename parsing,
// ID generation, listing parser, host-path composition, size
// formatting. The destructive shell flow (docker exec pg_dump)
// needs a real cluster to exercise — covered by manual E2E at
// ship time.

package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestComposeBackupHostPath(t *testing.T) {
	cases := []struct {
		scope, name, want string
	}{
		{"clowk-lp", "db", "/opt/voodu/backups/clowk-lp/db"},
		{"data", "main", "/opt/voodu/backups/data/main"},
		{"", "db", "/opt/voodu/backups/_unscoped/db"},
	}

	for _, tc := range cases {
		got := composeBackupHostPath(tc.scope, tc.name)
		if got != tc.want {
			t.Errorf("composeBackupHostPath(%q,%q): got %q, want %q",
				tc.scope, tc.name, got, tc.want)
		}
	}
}

func TestFormatBackupFilename(t *testing.T) {
	ts := time.Date(2026, 5, 5, 2, 0, 0, 0, time.UTC)

	cases := []struct {
		id   int
		want string
	}{
		{1, "b001-20260505T020000Z.dump"},
		{42, "b042-20260505T020000Z.dump"},
		{999, "b999-20260505T020000Z.dump"},
		{1000, "b1000-20260505T020000Z.dump"},
		{12345, "b12345-20260505T020000Z.dump"},
	}

	for _, tc := range cases {
		got := formatBackupFilename(tc.id, ts)
		if got != tc.want {
			t.Errorf("formatBackupFilename(%d): got %q, want %q", tc.id, got, tc.want)
		}
	}
}

func TestFormatBackupFilename_NormalizesToUTC(t *testing.T) {
	// Caller might pass local time; format must convert to UTC
	// so filenames sort/compare consistently across machines.
	la, _ := time.LoadLocation("America/Los_Angeles")
	ts := time.Date(2026, 5, 4, 19, 0, 0, 0, la) // 19:00 LA = 02:00 next day UTC

	got := formatBackupFilename(1, ts)
	want := "b001-20260505T020000Z.dump"

	if got != want {
		t.Errorf("formatBackupFilename utc-normalize: got %q, want %q", got, want)
	}
}

func TestParseBackupFilename_Valid(t *testing.T) {
	cases := []struct {
		filename string
		wantID   int
		wantTS   time.Time
	}{
		{
			"b001-20260505T020000Z.dump", 1,
			time.Date(2026, 5, 5, 2, 0, 0, 0, time.UTC),
		},
		{
			"b042-20260101T120000Z.dump", 42,
			time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		{
			"b1000-20260505T020000Z.dump", 1000,
			time.Date(2026, 5, 5, 2, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range cases {
		id, ts, ok := parseBackupFilename(tc.filename)
		if !ok {
			t.Errorf("parseBackupFilename(%q): expected ok=true", tc.filename)
			continue
		}

		if id != tc.wantID {
			t.Errorf("parseBackupFilename(%q): id=%d, want %d", tc.filename, id, tc.wantID)
		}

		if !ts.Equal(tc.wantTS) {
			t.Errorf("parseBackupFilename(%q): ts=%v, want %v", tc.filename, ts, tc.wantTS)
		}
	}
}

func TestParseBackupFilename_Invalid(t *testing.T) {
	cases := []string{
		"",
		"random.txt",
		".DS_Store",
		"backup.dump",                       // missing bNNN- prefix
		"b001-bogus.dump",                   // invalid timestamp
		"b001-20260505T020000Z.dumpish",     // wrong extension
		"b001-20260505T020000Z.dump.bak",    // extra suffix
		"b001-20260505T020000Z.tar",         // wrong format
		"a001-20260505T020000Z.dump",        // wrong prefix letter
		"b001_20260505T020000Z.dump",        // underscore not hyphen
		"b001-2026-05-05T02-00-00Z.dump",    // extended ISO format (rejected)
		"b001-202605050200000Z.dump",        // missing T separator
	}

	for _, fn := range cases {
		_, _, ok := parseBackupFilename(fn)
		if ok {
			t.Errorf("parseBackupFilename(%q): expected ok=false", fn)
		}
	}
}

func TestRoundTrip_FormatThenParse(t *testing.T) {
	// Filenames the plugin generates must always parse cleanly
	// — defends against future format edits diverging.
	ts := time.Date(2026, 5, 5, 14, 37, 22, 0, time.UTC)

	for _, id := range []int{1, 7, 100, 999, 1000, 50000} {
		filename := formatBackupFilename(id, ts)

		gotID, gotTS, ok := parseBackupFilename(filename)
		if !ok {
			t.Errorf("round-trip parse failed for id=%d filename=%q", id, filename)
			continue
		}

		if gotID != id {
			t.Errorf("round-trip id: got %d, want %d (filename=%q)", gotID, id, filename)
		}

		if !gotTS.Equal(ts) {
			t.Errorf("round-trip ts: got %v, want %v", gotTS, ts)
		}
	}
}

func TestNextBackupID_Empty(t *testing.T) {
	got := nextBackupID(nil)
	if got != 1 {
		t.Errorf("empty input: got %d, want 1 (first backup)", got)
	}

	got = nextBackupID([]string{})
	if got != 1 {
		t.Errorf("empty slice: got %d, want 1", got)
	}
}

func TestNextBackupID_IncrementsHighest(t *testing.T) {
	cases := []struct {
		filenames []string
		want      int
	}{
		{[]string{"b001-20260505T020000Z.dump"}, 2},
		{[]string{"b001-20260505T020000Z.dump", "b002-20260506T020000Z.dump"}, 3},
		{[]string{"b007-20260505T020000Z.dump"}, 8},
		// Out-of-order input: caller may not pre-sort. Result
		// MUST still be max+1, not last+1.
		{[]string{"b005-20260505T020000Z.dump", "b001-20260501T020000Z.dump", "b003-20260503T020000Z.dump"}, 6},
	}

	for _, tc := range cases {
		got := nextBackupID(tc.filenames)
		if got != tc.want {
			t.Errorf("nextBackupID(%v): got %d, want %d", tc.filenames, got, tc.want)
		}
	}
}

func TestNextBackupID_IgnoresUnrelatedFiles(t *testing.T) {
	// Operator-dropped tarballs, .DS_Store, lock files in the
	// backups dir must not mess with sequence numbering.
	filenames := []string{
		"b001-20260505T020000Z.dump",
		".DS_Store",
		"random.txt",
		"backup.dump",
		"b002-20260506T020000Z.dump",
		"b007-20260507T020000Z.dump",
		"a001-20260508T020000Z.dump", // wrong prefix
	}

	got := nextBackupID(filenames)
	if got != 8 {
		t.Errorf("ignored unrelated: got %d, want 8 (max valid was b007)", got)
	}
}

func TestNextBackupID_GapsAreNotFilled(t *testing.T) {
	// After the operator deletes b002, the next capture should
	// be b004 (max+1), NOT b002 (gap-fill). Gap-fill would
	// cause filename collisions across history if the operator
	// keeps a registry of backup IDs externally.
	filenames := []string{
		"b001-20260505T020000Z.dump",
		"b003-20260507T020000Z.dump",
	}

	got := nextBackupID(filenames)
	if got != 4 {
		t.Errorf("gap-fill: got %d, want 4 (no fill)", got)
	}
}

func TestParseBackupListing_BasicFormat(t *testing.T) {
	// `find -printf '%f\t%s\n'` output: filename, tab, byte count.
	raw := "b001-20260505T020000Z.dump\t12345\nb002-20260506T020000Z.dump\t67890\n"

	entries := parseBackupListing(raw)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	if entries[0].ID != 1 || entries[0].SizeBytes != 12345 {
		t.Errorf("entry[0] wrong: %+v", entries[0])
	}

	if entries[1].ID != 2 || entries[1].SizeBytes != 67890 {
		t.Errorf("entry[1] wrong: %+v", entries[1])
	}
}

func TestParseBackupListing_SortedByID(t *testing.T) {
	// find output may be in any order (filesystem-defined).
	// Listing must sort by ID for stable display.
	raw := "b005-20260505T020000Z.dump\t100\nb001-20260501T020000Z.dump\t200\nb003-20260503T020000Z.dump\t150\n"

	entries := parseBackupListing(raw)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	wantIDs := []int{1, 3, 5}
	for i, want := range wantIDs {
		if entries[i].ID != want {
			t.Errorf("sort order wrong at %d: got %d, want %d", i, entries[i].ID, want)
		}
	}
}

func TestParseBackupListing_SkipsUnparseable(t *testing.T) {
	raw := strings.Join([]string{
		"b001-20260505T020000Z.dump\t100",
		".DS_Store\t6148",                       // not a backup
		"random.txt\t42",                        // not a backup
		"b002-20260506T020000Z.dump\tnot_a_num", // bad size
		"b003-20260507T020000Z.dump\t300",
		"",                                      // blank
		"malformed-line-no-tab",
		"b004-20260508T020000Z.dump\t",          // missing size
	}, "\n")

	entries := parseBackupListing(raw)

	if len(entries) != 2 {
		t.Fatalf("expected 2 valid entries (b001, b003), got %d: %+v",
			len(entries), entries)
	}

	if entries[0].ID != 1 || entries[1].ID != 3 {
		t.Errorf("unexpected ids: %+v", entries)
	}
}

func TestParseBackupListing_Empty(t *testing.T) {
	if entries := parseBackupListing(""); len(entries) != 0 {
		t.Errorf("empty input: got %d entries", len(entries))
	}
}

func TestBackupEntry_Ref(t *testing.T) {
	cases := []struct {
		id   int
		want string
	}{
		{1, "b001"},
		{42, "b042"},
		{100, "b100"},
		{999, "b999"},
		{1000, "b1000"},
		{12345, "b12345"},
	}

	for _, tc := range cases {
		e := backupEntry{ID: tc.id}
		if got := e.Ref(); got != tc.want {
			t.Errorf("Ref(id=%d): got %q, want %q", tc.id, got, tc.want)
		}
	}
}

func TestFormatSize(t *testing.T) {
	const (
		kb int64 = 1024
		mb int64 = 1024 * kb
		gb int64 = 1024 * mb
		tb int64 = 1024 * gb
	)

	cases := []struct {
		bytes int64
		want  string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{kb, "1.0 KB"},
		{kb + 512, "1.5 KB"},
		{mb, "1.0 MB"},
		{245*mb + 308*kb, "245.3 MB"}, // 245.3 MB exactly
		{gb + 512*mb, "1.5 GB"},       // 1.5 GB exactly
		{2 * tb, "2.0 TB"},
	}

	for _, tc := range cases {
		got := formatSize(tc.bytes)
		if got != tc.want {
			t.Errorf("formatSize(%d): got %q, want %q", tc.bytes, got, tc.want)
		}
	}
}

func TestParseCaptureFlags_FromReplica(t *testing.T) {
	cases := []struct {
		args         []string
		wantPos      []string
		wantReplica  int
		wantHasFlag  bool
		wantFollow   bool
	}{
		{[]string{"clowk-lp/db"}, []string{"clowk-lp/db"}, 0, false, false},
		{[]string{"clowk-lp/db", "--from-replica", "1"}, []string{"clowk-lp/db"}, 1, true, false},
		{[]string{"clowk-lp/db", "--from-replica=2"}, []string{"clowk-lp/db"}, 2, true, false},
		{[]string{"--from-replica", "3", "ref"}, []string{"ref"}, 3, true, false},
		{[]string{"clowk-lp/db", "--follow"}, []string{"clowk-lp/db"}, 0, false, true},
		{[]string{"clowk-lp/db", "-f"}, []string{"clowk-lp/db"}, 0, false, true},
		{[]string{"clowk-lp/db", "--from-replica", "1", "--follow"}, []string{"clowk-lp/db"}, 1, true, true},
	}

	for _, tc := range cases {
		pos, replica, hasFlag, follow, err := parseCaptureFlags(tc.args)
		if err != nil {
			t.Errorf("parseCaptureFlags(%v): unexpected err: %v", tc.args, err)
			continue
		}

		if hasFlag != tc.wantHasFlag {
			t.Errorf("hasFlag for %v: got %v, want %v", tc.args, hasFlag, tc.wantHasFlag)
		}

		if replica != tc.wantReplica {
			t.Errorf("replica for %v: got %d, want %d", tc.args, replica, tc.wantReplica)
		}

		if follow != tc.wantFollow {
			t.Errorf("follow for %v: got %v, want %v", tc.args, follow, tc.wantFollow)
		}

		if len(pos) != len(tc.wantPos) {
			t.Errorf("positional for %v: got %v, want %v", tc.args, pos, tc.wantPos)
		}
	}
}

func TestParseCaptureFlags_MissingValue(t *testing.T) {
	args := []string{"ref", "--from-replica"}

	_, _, _, _, err := parseCaptureFlags(args)
	if err == nil {
		t.Error("expected error for --from-replica without value")
	}
}

func TestParseCaptureFlags_InvalidInteger(t *testing.T) {
	args := []string{"ref", "--from-replica", "abc"}

	_, _, _, _, err := parseCaptureFlags(args)
	if err == nil {
		t.Error("expected error for non-integer --from-replica")
	}
}

func TestResolveBackupByID_ExactMatch(t *testing.T) {
	entries := []backupEntry{
		{ID: 1, Filename: "b001-20260501T020000Z.dump"},
		{ID: 7, Filename: "b007-20260507T020000Z.dump"},
		{ID: 42, Filename: "b042-20260601T020000Z.dump"},
	}

	got, err := resolveBackupByID(entries, "b007")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if got.ID != 7 {
		t.Errorf("got id=%d, want 7", got.ID)
	}
}

func TestResolveBackupByID_NormalisesPadding(t *testing.T) {
	// "b1", "b001", "b0001" all refer to the same backup —
	// operator can paste any decimal form back into the command.
	entries := []backupEntry{
		{ID: 1, Filename: "b001-20260501T020000Z.dump"},
	}

	for _, id := range []string{"b1", "b001", "b0001", "b00001"} {
		got, err := resolveBackupByID(entries, id)
		if err != nil {
			t.Errorf("resolve(%q): %v", id, err)
			continue
		}

		if got.ID != 1 {
			t.Errorf("resolve(%q): got id=%d, want 1", id, got.ID)
		}
	}
}

func TestResolveBackupByID_NotFound(t *testing.T) {
	entries := []backupEntry{
		{ID: 1, Filename: "b001-20260501T020000Z.dump"},
		{ID: 3, Filename: "b003-20260503T020000Z.dump"},
	}

	_, err := resolveBackupByID(entries, "b002")
	if err == nil {
		t.Fatal("expected not-found error for b002")
	}

	// Error message must list available backups so the operator
	// can pick one without re-running list.
	for _, want := range []string{"b001", "b003"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should list %q, got: %v", want, err)
		}
	}
}

func TestResolveBackupByID_EmptyEntries(t *testing.T) {
	_, err := resolveBackupByID(nil, "b001")
	if err == nil {
		t.Fatal("expected error when no backups exist")
	}

	// Steer operator to capture as the next action.
	if !strings.Contains(err.Error(), "capture") {
		t.Errorf("error should suggest capture, got: %v", err)
	}
}

func TestResolveBackupByID_RejectsBlankID(t *testing.T) {
	entries := []backupEntry{
		{ID: 1, Filename: "b001-20260501T020000Z.dump"},
	}

	_, err := resolveBackupByID(entries, "")
	if err == nil {
		t.Fatal("expected error for empty id")
	}
}

func TestResolveBackupByID_RejectsBadShape(t *testing.T) {
	entries := []backupEntry{{ID: 1}}

	cases := []string{
		"7",            // missing 'b' prefix
		"backup-007",   // non-shape
		"bxyz",         // non-numeric
		"b",            // no number
		"b001-extra",   // suffix
		"007",          // again no prefix
	}

	for _, id := range cases {
		_, err := resolveBackupByID(entries, id)
		if err == nil {
			t.Errorf("resolve(%q): expected error", id)
		}
	}
}

func TestParseYesFlag(t *testing.T) {
	cases := []struct {
		args     []string
		wantPos  []string
		wantYes  bool
	}{
		{[]string{"ref", "b001"}, []string{"ref", "b001"}, false},
		{[]string{"ref", "b001", "--yes"}, []string{"ref", "b001"}, true},
		{[]string{"ref", "b001", "-y"}, []string{"ref", "b001"}, true},
		{[]string{"--yes", "ref", "b001"}, []string{"ref", "b001"}, true},
		{[]string{"-y", "ref", "b001"}, []string{"ref", "b001"}, true},
	}

	for _, tc := range cases {
		pos, yes := parseYesFlag(tc.args)
		if yes != tc.wantYes {
			t.Errorf("parseYesFlag(%v): yes=%v, want %v", tc.args, yes, tc.wantYes)
		}

		if len(pos) != len(tc.wantPos) {
			t.Errorf("parseYesFlag(%v): pos=%v, want %v", tc.args, pos, tc.wantPos)
		}
	}
}

func TestParseToFlag(t *testing.T) {
	cases := []struct {
		args     []string
		wantPos  []string
		wantDest string
	}{
		{[]string{"ref", "b001"}, []string{"ref", "b001"}, ""},
		{[]string{"ref", "b001", "--to", "/srv/x.dump"}, []string{"ref", "b001"}, "/srv/x.dump"},
		{[]string{"ref", "b001", "--to=/srv/x.dump"}, []string{"ref", "b001"}, "/srv/x.dump"},
		{[]string{"--to", "/tmp/y", "ref", "b001"}, []string{"ref", "b001"}, "/tmp/y"},
	}

	for _, tc := range cases {
		pos, dest := parseToFlag(tc.args)
		if dest != tc.wantDest {
			t.Errorf("parseToFlag(%v): dest=%q, want %q", tc.args, dest, tc.wantDest)
		}

		if len(pos) != len(tc.wantPos) {
			t.Errorf("parseToFlag(%v): pos=%v, want %v", tc.args, pos, tc.wantPos)
		}
	}
}

func TestBackupsRestoreHelpMentionsDestructive(t *testing.T) {
	wantPhrases := []string{
		"DESTRUCTIVE",
		"--clean",
		"--yes",
		"pg_restore",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(backupsRestoreHelp, want) {
			t.Errorf("backupsRestoreHelp missing %q", want)
		}
	}
}

func TestBackupsDeleteHelpMentionsDestructive(t *testing.T) {
	wantPhrases := []string{
		"DESTRUCTIVE",
		"--yes",
		"os.Remove",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(backupsDeleteHelp, want) {
			t.Errorf("backupsDeleteHelp missing %q", want)
		}
	}
}

func TestIsHTTPURL(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"https://example.com/db.dump", true},
		{"http://example.com/db.dump", true},
		{"https://my-bucket.s3.amazonaws.com/db.dump?X-Amz-Signature=abc", true},
		{"b001", false},
		{"b001-20260505T020000Z.dump", false},
		{"/srv/backups/db.dump", false},
		{"./db.dump", false},
		{"", false},
		// Other URL schemes — not supported by the download path.
		{"ftp://example.com/db.dump", false},
		{"s3://bucket/key", false},
		{"file:///srv/db.dump", false},
	}

	for _, tc := range cases {
		got := isHTTPURL(tc.in)
		if got != tc.want {
			t.Errorf("isHTTPURL(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestDownloadURLToTempFile_StreamsBytes(t *testing.T) {
	// Spin up a local httptest server serving a known body. The
	// helper must stream it to a temp file and report the byte
	// count exactly.
	body := strings.Repeat("postgres-dump-content-", 1000) // ~22kB

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	path, n, err := downloadURLToTempFile(server.URL)
	if err != nil {
		t.Fatalf("download: %v", err)
	}

	defer os.Remove(path)

	if n != int64(len(body)) {
		t.Errorf("byte count: got %d, want %d", n, len(body))
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read tmp: %v", err)
	}

	if string(got) != body {
		t.Errorf("body mismatch (len got=%d, want=%d)", len(got), len(body))
	}
}

func TestDownloadURLToTempFile_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	_, _, err := downloadURLToTempFile(server.URL)
	if err == nil {
		t.Fatal("expected error for HTTP 404")
	}

	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error should mention status, got: %v", err)
	}
}

func TestParseScheduleFlags_Defaults(t *testing.T) {
	pos, schedule, _, hasFlag, err := parseScheduleFlags([]string{"clowk-lp/db"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if len(pos) != 1 || pos[0] != "clowk-lp/db" {
		t.Errorf("positional: %v", pos)
	}

	if schedule != "" {
		t.Errorf("schedule should be empty (caller fills default), got %q", schedule)
	}

	if hasFlag {
		t.Error("hasFromReplica should be false")
	}
}

func TestParseScheduleFlags_AtAndFromReplica(t *testing.T) {
	args := []string{"clowk-lp/db", "--at", "0 */6 * * *", "--from-replica", "1"}

	_, schedule, replica, hasFlag, err := parseScheduleFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if schedule != "0 */6 * * *" {
		t.Errorf("schedule: got %q", schedule)
	}

	if !hasFlag || replica != 1 {
		t.Errorf("from-replica: hasFlag=%v, n=%d", hasFlag, replica)
	}
}

func TestParseScheduleFlags_EqualsForms(t *testing.T) {
	args := []string{"clowk-lp/db", "--at=0 3 * * *", "--from-replica=2"}

	_, schedule, replica, hasFlag, err := parseScheduleFlags(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if schedule != "0 3 * * *" {
		t.Errorf("schedule: got %q", schedule)
	}

	if !hasFlag || replica != 2 {
		t.Errorf("from-replica: hasFlag=%v, n=%d", hasFlag, replica)
	}
}

func TestParseScheduleFlags_AtMissingValue(t *testing.T) {
	_, _, _, _, err := parseScheduleFlags([]string{"ref", "--at"})
	if err == nil {
		t.Error("expected error for --at without value")
	}
}

func TestBackupsScheduleHelpMentionsSurfaces(t *testing.T) {
	wantPhrases := []string{
		"vd apply",
		"--at",
		"--from-replica",
		"cronjob",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(backupsScheduleHelp, want) {
			t.Errorf("backupsScheduleHelp missing %q", want)
		}
	}
}

func TestFormatProgressLine_WithEstimate(t *testing.T) {
	const mb int64 = 1024 * 1024

	cases := []struct {
		elapsed time.Duration
		current int64
		est     int64
		want    string
	}{
		{
			3 * time.Second, 12 * mb, 245 * mb,
			"  [3s] 12.0 MB written (~5% of estimate)",
		},
		{
			10 * time.Second, 68 * mb, 245 * mb,
			"  [10s] 68.0 MB written (~28% of estimate)",
		},
		{
			45 * time.Second, 218 * mb, 245 * mb,
			"  [45s] 218.0 MB written (~89% of estimate)",
		},
		// Round to integer percent — no fractional %.
		{
			52 * time.Second, 251 * mb, 245 * mb,
			"  [52s] 251.0 MB written (~102% of estimate)",
		},
		// Boundary: exactly 200% — still shown.
		{
			60 * time.Second, 200 * mb, 100 * mb,
			"  [1m0s] 200.0 MB written (~200% of estimate)",
		},
	}

	for _, tc := range cases {
		got := formatProgressLine(tc.elapsed, tc.current, tc.est)
		if got != tc.want {
			t.Errorf("formatProgressLine(%v, %d, %d):\n  got:  %q\n  want: %q",
				tc.elapsed, tc.current, tc.est, got, tc.want)
		}
	}
}

func TestFormatProgressLine_NoEstimate(t *testing.T) {
	const mb int64 = 1024 * 1024

	// estDumpBytes = 0 → suppress "% of estimate" suffix
	// (db size query failed; we still report bytes + elapsed).
	got := formatProgressLine(15*time.Second, 87*mb, 0)
	want := "  [15s] 87.0 MB written"

	if got != want {
		t.Errorf("zero estimate:\n  got:  %q\n  want: %q", got, want)
	}

	// Negative estimate (defensive — shouldn't happen) → also
	// suppress.
	got = formatProgressLine(15*time.Second, 87*mb, -1)
	if got != want {
		t.Errorf("negative estimate:\n  got:  %q\n  want: %q", got, want)
	}
}

func TestFormatProgressLine_RunawayPercentSuppressed(t *testing.T) {
	const mb int64 = 1024 * 1024

	// Estimate clearly wrong (compression worse than we predicted).
	// >200% suppresses "of estimate" — showing "350% of estimate"
	// confuses operators more than it helps.
	got := formatProgressLine(60*time.Second, 350*mb, 100*mb)
	want := "  [1m0s] 350.0 MB written"

	if got != want {
		t.Errorf("runaway percent should be suppressed:\n  got:  %q\n  want: %q",
			got, want)
	}
}

func TestFormatProgressLine_ZeroBytes(t *testing.T) {
	const mb int64 = 1024 * 1024

	// pg_dump still in lock-acquisition phase — file empty.
	// Reporter normally skips this case (continue in the
	// goroutine), but the formatter must handle 0 cleanly.
	got := formatProgressLine(2*time.Second, 0, 100*mb)
	want := "  [2s] 0 B written"

	if got != want {
		t.Errorf("zero bytes:\n  got:  %q\n  want: %q", got, want)
	}
}

func TestBackupsCancelHelpMentionsDispatchAndSemantics(t *testing.T) {
	// Help text for the migrated cancel: emits delete_manifest
	// dispatch actions; voodu's JobHandler.remove sends SIGTERM.
	wantPhrases := []string{
		"delete_manifest",
		"pg_dump",
		"SIGTERM",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(backupsCancelHelp, want) {
			t.Errorf("backupsCancelHelp missing %q", want)
		}
	}
}

func TestComposeBackupContainerName(t *testing.T) {
	cases := []struct {
		scope, name string
		id          int
		want        string
	}{
		{"clowk-lp", "db", 1, "clowk-lp-db-backup-b001"},
		{"clowk-lp", "db", 42, "clowk-lp-db-backup-b042"},
		{"clowk-lp", "db", 1000, "clowk-lp-db-backup-b1000"},
		{"", "db", 7, "db-backup-b007"},
		{"data", "main", 99, "data-main-backup-b099"},
	}

	for _, tc := range cases {
		got := composeBackupContainerName(tc.scope, tc.name, tc.id)
		if got != tc.want {
			t.Errorf("composeBackupContainerName(%q, %q, %d): got %q, want %q",
				tc.scope, tc.name, tc.id, got, tc.want)
		}
	}
}

func TestComposeBackupContainerPrefix(t *testing.T) {
	cases := []struct {
		scope, name, want string
	}{
		{"clowk-lp", "db", "clowk-lp-db-backup-"},
		{"", "db", "db-backup-"},
		{"data", "main", "data-main-backup-"},
	}

	for _, tc := range cases {
		got := composeBackupContainerPrefix(tc.scope, tc.name)
		if got != tc.want {
			t.Errorf("composeBackupContainerPrefix(%q, %q): got %q, want %q",
				tc.scope, tc.name, got, tc.want)
		}
	}
}

func TestParseContainerNameForID(t *testing.T) {
	prefix := "clowk-lp-db-backup-"

	cases := []struct {
		name    string
		wantID  int
		wantOK  bool
	}{
		// Caminho A — exact match (--follow path, docker run -d direct)
		{"clowk-lp-db-backup-b001", 1, true},
		{"clowk-lp-db-backup-b042", 42, true},
		{"clowk-lp-db-backup-b1000", 1000, true},
		// Caminho B — voodu jobs use `.` between AppID and runID
		// (containers.ContainerName convention)
		{"clowk-lp-db-backup-b011.fccd", 11, true},
		{"clowk-lp-db-backup-b042.abcd1234", 42, true},
		{"clowk-lp-db-backup-b1000.xyz", 1000, true},
		// Legacy/hand-rolled `-` separator also accepted for safety
		{"clowk-lp-db-backup-b007-runidv2", 7, true},
		// wrong prefix
		{"other-scope-db-backup-b001", 0, false},
		// missing 'b' marker
		{"clowk-lp-db-backup-001", 0, false},
		// non-numeric tail
		{"clowk-lp-db-backup-bxyz", 0, false},
		// empty after prefix
		{"clowk-lp-db-backup-", 0, false},
		// digits followed by non-separator junk → reject (not a valid run name)
		{"clowk-lp-db-backup-b008xyz", 0, false},
	}

	for _, tc := range cases {
		gotID, gotOK := parseContainerNameForID(tc.name, prefix)

		if gotOK != tc.wantOK {
			t.Errorf("parseContainerNameForID(%q): ok=%v, want %v", tc.name, gotOK, tc.wantOK)
			continue
		}

		if gotOK && gotID != tc.wantID {
			t.Errorf("parseContainerNameForID(%q): id=%d, want %d", tc.name, gotID, tc.wantID)
		}
	}
}

func TestParseExitCodeFromStatus(t *testing.T) {
	cases := []struct {
		status string
		want   int
	}{
		{"Up 2 minutes", 0},
		{"Up About a minute (healthy)", 0},
		{"Exited (0) 3 minutes ago", 0},
		{"Exited (1) 3 minutes ago", 1},
		{"Exited (137) 1 hour ago", 137},
		// Created state — no exit code yet, treat as 0.
		{"Created", 0},
		// Defensive: malformed
		{"Exited (xx) blah", 0},
		{"Exited () blah", 0},
		{"", 0},
	}

	for _, tc := range cases {
		got := parseExitCodeFromStatus(tc.status)
		if got != tc.want {
			t.Errorf("parseExitCodeFromStatus(%q): got %d, want %d", tc.status, got, tc.want)
		}
	}
}

func TestMergeBackupListing_FilesOnly(t *testing.T) {
	files := []backupEntry{
		{ID: 1, Filename: "b001-...dump", SizeBytes: 100, Timestamp: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
		{ID: 2, Filename: "b002-...dump", SizeBytes: 200, Timestamp: time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)},
	}

	rows := mergeBackupListing(files, nil)

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	for _, r := range rows {
		if r.Status != "complete" {
			t.Errorf("ID %d: status=%q, want complete", r.ID, r.Status)
		}
	}
}

func TestMergeBackupListing_RunningContainerNoFile(t *testing.T) {
	containers := []backupContainer{
		{ID: 8, State: "running", ExitCode: 0},
	}

	rows := mergeBackupListing(nil, containers)

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if rows[0].Status != "running" || rows[0].ID != 8 {
		t.Errorf("got %+v, want {ID: 8, Status: running}", rows[0])
	}
}

func TestMergeBackupListing_RunningContainerWithGrowingFile(t *testing.T) {
	// Container running while file grows — status is "running"
	// even though the file is partially written.
	files := []backupEntry{
		{ID: 8, Filename: "b008-...dump", SizeBytes: 87 * 1024 * 1024},
	}
	containers := []backupContainer{
		{ID: 8, State: "running", ExitCode: 0},
	}

	rows := mergeBackupListing(files, containers)

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if rows[0].Status != "running" {
		t.Errorf("running container should override 'complete' status, got %q", rows[0].Status)
	}

	if rows[0].SizeBytes != 87*1024*1024 {
		t.Errorf("file metadata should still surface, got size=%d", rows[0].SizeBytes)
	}
}

func TestMergeBackupListing_FailedContainerNoFile(t *testing.T) {
	// pg_dump failed; wrapper removed the partial file. Container
	// is still around. List shows 'failed' status.
	containers := []backupContainer{
		{ID: 5, State: "exited", ExitCode: 1},
	}

	rows := mergeBackupListing(nil, containers)

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if rows[0].Status != "failed" {
		t.Errorf("exit-non-zero should be 'failed', got %q", rows[0].Status)
	}
}

func TestMergeBackupListing_ExitedZeroIsCompleteIfFile(t *testing.T) {
	// Cleanly-exited container + file present = complete.
	files := []backupEntry{
		{ID: 3, Filename: "b003-...dump", SizeBytes: 1234},
	}
	containers := []backupContainer{
		{ID: 3, State: "exited", ExitCode: 0},
	}

	rows := mergeBackupListing(files, containers)

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if rows[0].Status != "complete" {
		t.Errorf("exit-zero with file should be 'complete', got %q", rows[0].Status)
	}
}

func TestMergeBackupListing_SortedByID(t *testing.T) {
	files := []backupEntry{
		{ID: 5, Filename: "b005-...dump"},
		{ID: 1, Filename: "b001-...dump"},
	}
	containers := []backupContainer{
		{ID: 8, State: "running"},
	}

	rows := mergeBackupListing(files, containers)

	want := []int{1, 5, 8}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	for i, w := range want {
		if rows[i].ID != w {
			t.Errorf("rows[%d].ID = %d, want %d", i, rows[i].ID, w)
		}
	}
}

func TestParseFollowFlag(t *testing.T) {
	cases := []struct {
		args        []string
		wantPos     []string
		wantFollow  bool
	}{
		{[]string{"ref", "b001"}, []string{"ref", "b001"}, false},
		{[]string{"ref", "b001", "--follow"}, []string{"ref", "b001"}, true},
		{[]string{"ref", "b001", "-f"}, []string{"ref", "b001"}, true},
		{[]string{"--follow", "ref", "b001"}, []string{"ref", "b001"}, true},
	}

	for _, tc := range cases {
		pos, follow := parseFollowFlag(tc.args)

		if follow != tc.wantFollow {
			t.Errorf("parseFollowFlag(%v): follow=%v, want %v", tc.args, follow, tc.wantFollow)
		}

		if len(pos) != len(tc.wantPos) {
			t.Errorf("parseFollowFlag(%v): pos=%v, want %v", tc.args, pos, tc.wantPos)
		}
	}
}

// (strings is used in TestParseBackupListing_SkipsUnparseable above.)
