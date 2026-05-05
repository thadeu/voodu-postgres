// `vd pg:backups` — Heroku-style backup management.
//
// # Surface (Phase 2 MVP)
//
//   - `vd pg:backups <ref>`            — list (default verb omitted)
//   - `vd pg:backups:capture <ref>`    — pg_dump → /backups/bNNN-<ts>.dump
//
// # Surface (deferred to subsequent turns)
//
//   - `vd pg:backups:schedule <ref> --at "02:00"` (emit cronjob)
//   - `vd pg:backups:unschedule <ref>`
//   - `vd pg:backups:restore <ref> <id|url>`
//   - `vd pg:backups:download <ref> <id>`
//   - `vd pg:backups:delete <ref> <id>`
//
// # Mechanics
//
// pg_dump runs INSIDE the running postgres pod via `docker exec`,
// using the postgres user via Unix socket — no network round-trip,
// no password handling, no separate container image. Output goes
// to `/backups/` which is a host bind-mount of
// `/opt/voodu/backups/<scope>/<name>/`. Operator pre-flight is just
// `mkdir -p` the host dir; the entrypoint wrapper auto-chowns it
// to postgres uid 999 on every boot.
//
// # File format
//
// pg_dump custom format (-F c) — binary, restorable via `pg_restore`,
// portable across postgres versions ≥ source. Filename pattern:
//
//	bNNN-YYYYMMDDTHHMMSSZ.dump
//
// where NNN is a sequence number (zero-padded; embedded in the
// filename so IDs persist across deletes — not recomputed at list
// time). Timestamp is ISO 8601 basic format (no separators) so the
// filename is shell- and filesystem-safe across all platforms.
//
// # Source pod selection
//
// Default: current PRIMARY (PG_PRIMARY_ORDINAL). `--from-replica N`
// runs pg_dump against a standby instead — saves write-load on the
// primary; standby's data is replication-recent up to its replay
// LSN (typically <1s lag).

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// backupsContainerPath is the in-container path for the
	// host bind-mount of the backups directory. Internal
	// convention; not operator-facing.
	backupsContainerPath = "/backups"

	// backupsHostRoot is the prefix for backup directories on
	// the host. Final path = backupsHostRoot/<scope>/<name>.
	backupsHostRoot = "/opt/voodu/backups"

	// backupExtension is the suffix of every backup file.
	// pg_dump custom format binary; restored via pg_restore.
	backupExtension = ".dump"

	// backupTimeFormat is ISO 8601 basic format — compact and
	// filesystem-safe (no colons). Layout: YYYYMMDDTHHMMSSZ.
	backupTimeFormat = "20060102T150405Z"
)

// backupFilenameRegex matches `bNNN-YYYYMMDDTHHMMSSZ.dump`.
// Sub-groups: 1=ID digits, 2=timestamp.
var backupFilenameRegex = regexp.MustCompile(`^b(\d+)-(\d{8}T\d{6}Z)\.dump$`)

// composeBackupHostPath builds the host path for a postgres
// resource's backups directory. Resource (scope, name) embeds in
// the path so multiple postgres resources on the same host don't
// collide. Unscoped resources land at /opt/voodu/backups/_unscoped/<name>.
func composeBackupHostPath(scope, name string) string {
	if scope == "" {
		return backupsHostRoot + "/_unscoped/" + name
	}

	return backupsHostRoot + "/" + scope + "/" + name
}

// backupEntry is a parsed backup filename plus optional metadata
// (size). Pure data type — listing fills SizeBytes; capture leaves
// it zero.
type backupEntry struct {
	ID        int       // sequence number, e.g. 1 → display as "b001"
	Filename  string    // bNNN-YYYYMMDDTHHMMSSZ.dump
	Timestamp time.Time // parsed from filename, UTC
	SizeBytes int64     // 0 when unknown (not listed yet)
}

// Ref returns the operator-facing backup ID, e.g. "b001".
// Padded to 3 digits for sort-friendly display under typical
// retention sizes (≤999 backups). Wider IDs render unpadded.
func (b backupEntry) Ref() string {
	if b.ID < 1000 {
		return fmt.Sprintf("b%03d", b.ID)
	}

	return fmt.Sprintf("b%d", b.ID)
}

// formatBackupFilename composes the on-disk filename for a backup
// at the given ID + capture time. UTC always — operator local
// time is irrelevant; consistent ordering matters more.
func formatBackupFilename(id int, ts time.Time) string {
	if id < 1000 {
		return fmt.Sprintf("b%03d-%s%s", id, ts.UTC().Format(backupTimeFormat), backupExtension)
	}

	return fmt.Sprintf("b%d-%s%s", id, ts.UTC().Format(backupTimeFormat), backupExtension)
}

// parseBackupFilename extracts ID + timestamp from a filename
// matching the bNNN-YYYYMMDDTHHMMSSZ.dump pattern. Returns ok=false
// for unrelated files (e.g. operator-dropped tarballs, lock files,
// .DS_Store) so the listing skips them silently.
func parseBackupFilename(filename string) (id int, ts time.Time, ok bool) {
	m := backupFilenameRegex.FindStringSubmatch(filename)
	if m == nil {
		return 0, time.Time{}, false
	}

	parsedID, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, time.Time{}, false
	}

	parsedTS, err := time.Parse(backupTimeFormat, m[2])
	if err != nil {
		return 0, time.Time{}, false
	}

	return parsedID, parsedTS, true
}

// nextBackupID computes the ID for the next capture given the
// current set of backup filenames. Reads the highest existing ID
// and returns max+1. Empty input → 1 (first backup).
//
// Pure function — list source can be docker exec output, mocked
// data in tests, anything that decodes to filenames.
func nextBackupID(filenames []string) int {
	max := 0
	for _, f := range filenames {
		id, _, ok := parseBackupFilename(f)
		if !ok {
			continue
		}

		if id > max {
			max = id
		}
	}

	return max + 1
}

// parseBackupListing turns lines of "<filename>\t<size>" (the format
// emitted by `find -printf '%f\t%s\n'`) into a slice of backupEntry.
// Filenames not matching the backup pattern are skipped silently.
// Returns the entries sorted by ID ascending.
func parseBackupListing(raw string) []backupEntry {
	var entries []backupEntry

	scanner := bufio.NewScanner(strings.NewReader(raw))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}

		filename := parts[0]
		size, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}

		id, ts, ok := parseBackupFilename(filename)
		if !ok {
			continue
		}

		entries = append(entries, backupEntry{
			ID:        id,
			Filename:  filename,
			Timestamp: ts,
			SizeBytes: size,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ID < entries[j].ID
	})

	return entries
}

// resolveBackupByID finds the entry whose Ref() corresponds to the
// operator-supplied ID. Accepts any decimal form: "b1", "b001",
// "b0001" all match the same backup. Empty input is rejected so
// `vd pg:backups:restore <ref>` (missing ID arg) doesn't silently
// match the first backup.
//
// Returns a clear error message listing what's available — restore
// + download + delete all share this code path so the same user
// experience surfaces everywhere.
func resolveBackupByID(entries []backupEntry, id string) (backupEntry, error) {
	if id == "" {
		return backupEntry{}, fmt.Errorf("backup id required (e.g. b001)")
	}

	if !strings.HasPrefix(id, "b") {
		return backupEntry{}, fmt.Errorf("backup id %q must start with 'b' (e.g. b001)", id)
	}

	n, err := strconv.Atoi(id[1:])
	if err != nil {
		return backupEntry{}, fmt.Errorf("backup id %q: invalid number after 'b'", id)
	}

	for _, e := range entries {
		if e.ID == n {
			return e, nil
		}
	}

	if len(entries) == 0 {
		return backupEntry{}, fmt.Errorf("backup %s not found (no backups exist — run vd pg:backups:capture first)", id)
	}

	available := make([]string, 0, len(entries))
	for _, e := range entries {
		available = append(available, e.Ref())
	}

	return backupEntry{}, fmt.Errorf("backup %s not found (available: %s)", id, strings.Join(available, ", "))
}

// formatSize returns a human-readable byte count: "245 MB", "1.2 GB", etc.
// Plain integer for sub-KB sizes, 1 decimal place for everything else.
// Display-only — JSON output uses raw bytes.
func formatSize(bytes int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
		tb = 1024 * gb
	)

	switch {
	case bytes >= tb:
		return fmt.Sprintf("%.1f TB", float64(bytes)/float64(tb))
	case bytes >= gb:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(gb))
	case bytes >= mb:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(mb))
	case bytes >= kb:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(kb))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// findCmd is the shell command run inside the pod to list backup
// files with their byte sizes. Output: one line per file, "<name>\t<size>".
//
// `find -maxdepth 1 -name 'b*.dump' -printf '%f\t%s\n'` keeps the
// path absolute outside (no relative-path pitfalls) but emits only
// the basename — exactly what parseBackupListing needs.
const findBackupsCommand = `find /backups -maxdepth 1 -type f -name 'b*.dump' -printf '%f\t%s\n' 2>/dev/null || true`

// listBackupsInPod runs the find command inside the given container
// and returns parsed entries. Container must be running.
func listBackupsInPod(container string) ([]backupEntry, error) {
	cmd := exec.Command("docker", "exec", container, "bash", "-c", findBackupsCommand)

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker exec %s find: %w", container, err)
	}

	return parseBackupListing(string(out)), nil
}

// listBackupsOnHost stats the backup files directly via the host
// bind-mount path. Used when no postgres pod is running (rare —
// happens during teardown/restore) so list/inspection still works.
// Also faster: no docker round-trip.
func listBackupsOnHost(scope, name string) ([]backupEntry, error) {
	dir := composeBackupHostPath(scope, name)

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("read %s: %w", dir, err)
	}

	var raw strings.Builder

	for _, e := range dirEntries {
		if e.IsDir() {
			continue
		}

		info, err := e.Info()
		if err != nil {
			continue
		}

		raw.WriteString(e.Name())
		raw.WriteString("\t")
		raw.WriteString(strconv.FormatInt(info.Size(), 10))
		raw.WriteString("\n")
	}

	return parseBackupListing(raw.String()), nil
}

// composeBackupContainerName builds the deterministic name for a
// backup job container: `<scope>-<name>-backup-bNNN`. Sticking to
// scope-prefixed naming so backups across resources don't collide
// in the operator's `docker ps` output.
//
// Unscoped resources use `<name>-backup-bNNN` (no leading dash, no
// double-dash). Mirrors containerNameFor's unscoped handling.
func composeBackupContainerName(scope, name string, id int) string {
	prefix := composeBackupContainerPrefix(scope, name)

	if id < 1000 {
		return fmt.Sprintf("%sb%03d", prefix, id)
	}

	return fmt.Sprintf("%sb%d", prefix, id)
}

// composeBackupContainerPrefix returns the prefix shared by all
// backup containers for a (scope, name) pair: `<scope>-<name>-backup-`.
// Useful for `docker ps --filter name=<prefix>` queries.
func composeBackupContainerPrefix(scope, name string) string {
	if scope == "" {
		return name + "-backup-"
	}

	return scope + "-" + name + "-backup-"
}

// parseContainerNameForID extracts the backup ID from a container
// name produced for a backup capture. Two shapes accepted:
//
//   - "<prefix>bNNN"            (Caminho A: docker run -d direct)
//   - "<prefix>bNNN-<runID>"    (Caminho B: voodu jobs, runID
//                                appended by containers.ContainerName)
//
// Both end up keying the same logical backup; the runID suffix is
// voodu's audit-trail discriminator that we don't care about here.
//
// Returns ok=false for names that don't match either pattern
// (defensive — operator might've manually created an unrelated
// container with the same prefix).
func parseContainerNameForID(name, prefix string) (int, bool) {
	if !strings.HasPrefix(name, prefix) {
		return 0, false
	}

	tail := name[len(prefix):]
	if !strings.HasPrefix(tail, "b") {
		return 0, false
	}

	digits := tail[1:]

	end := 0
	for end < len(digits) && digits[end] >= '0' && digits[end] <= '9' {
		end++
	}

	if end == 0 {
		return 0, false
	}

	// Reject if there's a non-hyphen, non-end suffix after the
	// digits. "b008abc" isn't a valid voodu run name (would be
	// "b008-abc") so we don't want it parsing as id=8.
	if end < len(digits) && digits[end] != '-' {
		return 0, false
	}

	n, err := strconv.Atoi(digits[:end])
	if err != nil {
		return 0, false
	}

	return n, true
}

// backupContainer is a parsed entry from `docker ps --filter`. Tracks
// container lifecycle independently of the dump file in /backups —
// they may not match (e.g. failed dump deletes the file but leaves
// the exited container).
type backupContainer struct {
	ID       int    // sequence number from container name
	Name     string // full container name
	State    string // "running" / "exited" / "created" / etc.
	ExitCode int    // 0 for running or success, non-zero for failure
}

// listBackupContainers runs `docker ps -a` filtered by the backup
// prefix and returns one entry per matching container. State is the
// docker-reported lifecycle phase.
func listBackupContainers(scope, name string) ([]backupContainer, error) {
	prefix := composeBackupContainerPrefix(scope, name)

	cmd := exec.Command("docker", "ps", "-a",
		"--filter", "name="+prefix,
		"--format", "{{.Names}}\t{{.State}}\t{{.Status}}",
	)

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker ps: %w", err)
	}

	var entries []backupContainer

	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "\t", 3)
		if len(parts) < 3 {
			continue
		}

		id, ok := parseContainerNameForID(parts[0], prefix)
		if !ok {
			continue
		}

		entries = append(entries, backupContainer{
			ID:       id,
			Name:     parts[0],
			State:    parts[1],
			ExitCode: parseExitCodeFromStatus(parts[2]),
		})
	}

	return entries, nil
}

// parseExitCodeFromStatus pulls the exit code out of a `docker ps`
// Status string like "Exited (1) 3 minutes ago". Returns 0 for
// non-exited statuses ("Up 2 minutes") so callers can use a simple
// `code == 0` test for success.
func parseExitCodeFromStatus(status string) int {
	if !strings.HasPrefix(status, "Exited") {
		return 0
	}

	// "Exited (N) ..." → strip prefix, find the parenthesised int.
	open := strings.Index(status, "(")
	close := strings.Index(status, ")")

	if open < 0 || close < 0 || close <= open {
		return 0
	}

	n, err := strconv.Atoi(status[open+1 : close])
	if err != nil {
		return 0
	}

	return n
}

// pruneExitedBackupContainers removes any exited backup containers
// for the (scope, name) pair so a fresh capture can re-use a
// previously-occupied bNNN slot. Skips running containers (don't
// kill in-flight captures). Best-effort — errors are reported but
// not fatal.
func pruneExitedBackupContainers(scope, name string) error {
	containers, err := listBackupContainers(scope, name)
	if err != nil {
		return err
	}

	for _, c := range containers {
		if c.State == "running" || c.State == "created" {
			continue
		}

		if err := exec.Command("docker", "rm", c.Name).Run(); err != nil {
			fmt.Fprintf(os.Stderr, "warn: failed to remove exited container %s: %v\n", c.Name, err)
		}
	}

	return nil
}

// queryDBSize runs SELECT pg_database_size('<db>') inside the pod
// and returns the byte count. Used pre-capture to size up the
// progress reporter's estimate. Returns 0 + nil on parse failure
// — capture proceeds, just without an estimate.
func queryDBSize(container, user, db string) (int64, error) {
	cmd := exec.Command(
		"docker", "exec", "-u", "postgres", container,
		"psql", "-U", user, "-d", db,
		"-X", "-A", "-t",
		"-c", fmt.Sprintf("SELECT pg_database_size(%s);", quotePgString(db)),
	)

	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	n, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// statFileInPod returns the byte size of a file inside the pod.
// Returns 0 if the file doesn't exist yet (pg_dump may still be
// connecting; progress reporter handles that gracefully).
func statFileInPod(container, path string) int64 {
	cmd := exec.Command("docker", "exec", container, "stat", "-c", "%s", path)

	out, err := cmd.Output()
	if err != nil {
		return 0
	}

	n, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0
	}

	return n
}

// dumpCompressionFactor is the assumed ratio of dump-file size to
// raw database size (pg_database_size). pg_dump custom format with
// -Z 6 typically lands in the 0.4–0.6 range for production data
// (mixed text + indexes; indexes aren't dumped, only data, which
// drops the ratio below 1.0). The midpoint is a reasonable hint
// but capped to "of estimate" framing in formatProgressLine — we
// never claim the percent is real.
const dumpCompressionFactor = 0.5

// formatProgressLine renders one line of the rolling capture
// progress reporter. Pure function — testable without a postgres
// connection.
//
//	[3s]   12 MB written (~5% of estimate)
//	[10s]  68 MB written (~28% of estimate)
//	[45s] 218 MB written (~90% of estimate)
//	[60s] 245 MB written
//
// The "% of estimate" suffix is omitted when:
//   - estDumpBytes <= 0 (db size query failed)
//   - the percent would exceed 200% (estimate clearly wrong;
//     showing "240% of estimate" confuses more than helps)
func formatProgressLine(elapsed time.Duration, currentBytes, estDumpBytes int64) string {
	elapsedStr := elapsed.Round(time.Second).String()
	sizeStr := formatSize(currentBytes)

	if estDumpBytes <= 0 {
		return fmt.Sprintf("  [%s] %s written", elapsedStr, sizeStr)
	}

	pct := float64(currentBytes) * 100 / float64(estDumpBytes)

	if pct <= 0 || pct > 200 {
		return fmt.Sprintf("  [%s] %s written", elapsedStr, sizeStr)
	}

	return fmt.Sprintf("  [%s] %s written (~%.0f%% of estimate)", elapsedStr, sizeStr, pct)
}

// reportCaptureProgress polls the dump file size every ~3s and
// prints a rolling status line to stderr. Stops when done is
// closed (caller closes it after pg_dump returns).
//
// Polling overhead is one `docker exec stat` per tick — trivial.
// Stops cleanly even if pg_dump exits before the first tick.
func reportCaptureProgress(container, file string, estDumpBytes int64, done <-chan struct{}) {
	start := time.Now()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return

		case <-ticker.C:
			size := statFileInPod(container, file)
			if size == 0 {
				// File not created yet (pg_dump still in
				// SET search_path / lock acquisition phase).
				// Stay quiet for now — operator already saw
				// the "pg_dump in <pod>" line.
				continue
			}

			fmt.Fprintln(os.Stderr, formatProgressLine(time.Since(start), size, estDumpBytes))
		}
	}
}

// ─────────────────────────────────────────────────────────────────
// commands
// ─────────────────────────────────────────────────────────────────

const backupsListHelp = `vd pg:backups — list backups for a postgres resource.

USAGE
  vd pg:backups <postgres-scope/name> [-o text|json]

ARGUMENTS
  <postgres-scope/name>   The postgres resource.

FLAGS
  -o, --output            'text' (default, table) or 'json' (jq-friendly)

STATUS COLUMN
  running    A backup container is currently running. Size grows
             over time; file may not be complete yet.
  complete   pg_dump exited cleanly; the file is on disk.
  failed     The container exited non-zero; the wrapper removed the
             partial file. Container is still around for log
             inspection (auto-pruned by the next capture).

OUTPUT (text)
  ID     Status     Created at            Size
  b007   complete   2026-05-04T02:00:00Z  245.3 MB
  b008   running    2026-05-05T14:30:00Z  87.0 MB

EXAMPLES
  vd pg:backups clowk-lp/db
  vd pg:backups clowk-lp/db -o json | jq '.backups[] | .id'
`

// cmdBackups lists backups for a postgres resource. Combines two
// data sources:
//
//   - Files in /backups/ (read via host bind-mount — works without
//     a running pod) → completed captures.
//   - `docker ps -a` filtered by the backup container prefix →
//     in-progress + recently-failed captures.
//
// Status reflects which of those a given bNNN appears in.
func cmdBackups() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(backupsListHelp)
		return nil
	}

	positional, asJSON := parseInfoFlags(args)

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd pg:backups <postgres-scope/name> [-o json]")
	}

	scope, name := splitScopeName(positional[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", positional[0])
	}

	files, err := listBackupsOnHost(scope, name)
	if err != nil {
		return fmt.Errorf("list /backups dir: %w", err)
	}

	containers, err := listBackupContainers(scope, name)
	if err != nil {
		// Don't fail the listing if docker is unhappy — show
		// what we have on disk and warn.
		fmt.Fprintf(os.Stderr, "warn: docker ps for backup containers: %v\n", err)
		containers = nil
	}

	rows := mergeBackupListing(files, containers)

	if asJSON {
		return emitBackupsJSON(scope, name, rows)
	}

	emitBackupsText(scope, name, rows)

	return nil
}

// backupListEntry is the merged view of a backup — file metadata
// (when present) plus container state (when present). Either or
// both can be missing depending on lifecycle phase:
//
//	running      container present (state=running), file growing
//	complete     file present, container exited cleanly OR pruned
//	failed       container exited != 0, file removed by wrapper
//
// For sort stability + display, every row carries the ID extracted
// from whichever side surfaced it.
type backupListEntry struct {
	ID        int
	Status    string // "running" / "complete" / "failed"
	Filename  string
	Timestamp time.Time
	SizeBytes int64
}

// mergeBackupListing combines on-disk files and container state
// into a unified, sorted listing. Pure function — testable.
func mergeBackupListing(files []backupEntry, containers []backupContainer) []backupListEntry {
	byID := make(map[int]*backupListEntry)

	// Files: complete captures (any container has either exited
	// cleanly or been auto-pruned).
	for _, f := range files {
		byID[f.ID] = &backupListEntry{
			ID:        f.ID,
			Status:    "complete",
			Filename:  f.Filename,
			Timestamp: f.Timestamp,
			SizeBytes: f.SizeBytes,
		}
	}

	// Containers: running ones override "complete" (file may exist
	// at intermediate size); failed ones surface even without a
	// file (wrapper removes file on non-zero exit).
	for _, c := range containers {
		entry, hadFile := byID[c.ID]

		switch {
		case c.State == "running" || c.State == "created":
			if !hadFile {
				entry = &backupListEntry{ID: c.ID}
				byID[c.ID] = entry
			}

			entry.Status = "running"

		case c.ExitCode != 0:
			// Failed run. File usually absent (wrapper removed it).
			if !hadFile {
				entry = &backupListEntry{ID: c.ID}
				byID[c.ID] = entry
			}

			entry.Status = "failed"
			// Keep file metadata if any (rare edge case where the
			// wrapper failed before pg_dump's file existed); size
			// stays whatever was reported.
		}
	}

	rows := make([]backupListEntry, 0, len(byID))
	for _, r := range byID {
		rows = append(rows, *r)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ID < rows[j].ID
	})

	return rows
}

// emitBackupsJSON writes the merged list as a JSON document.
func emitBackupsJSON(scope, name string, rows []backupListEntry) error {
	type rowOut struct {
		ID        string `json:"id"`
		Status    string `json:"status"`
		Filename  string `json:"filename,omitempty"`
		CreatedAt string `json:"created_at,omitempty"`
		SizeBytes int64  `json:"size_bytes"`
	}

	out := make([]rowOut, 0, len(rows))

	for _, r := range rows {
		ref := fmt.Sprintf("b%03d", r.ID)
		if r.ID >= 1000 {
			ref = fmt.Sprintf("b%d", r.ID)
		}

		row := rowOut{
			ID:        ref,
			Status:    r.Status,
			Filename:  r.Filename,
			SizeBytes: r.SizeBytes,
		}

		if !r.Timestamp.IsZero() {
			row.CreatedAt = r.Timestamp.UTC().Format(time.RFC3339)
		}

		out = append(out, row)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	return enc.Encode(map[string]any{
		"ref":     refOrName(scope, name),
		"backups": out,
	})
}

// emitBackupsText prints the merged list as a human-readable table.
func emitBackupsText(scope, name string, rows []backupListEntry) {
	fmt.Printf("=== Backups for %s\n", refOrName(scope, name))

	if len(rows) == 0 {
		fmt.Println()
		fmt.Println("  (no backups yet)")
		fmt.Printf("  → vd pg:backups:capture %s\n", refOrName(scope, name))
		fmt.Println()

		return
	}

	fmt.Println()
	fmt.Println("  ID     Status     Created at            Size")

	for _, r := range rows {
		ref := fmt.Sprintf("b%03d", r.ID)
		if r.ID >= 1000 {
			ref = fmt.Sprintf("b%d", r.ID)
		}

		ts := "—"
		if !r.Timestamp.IsZero() {
			ts = r.Timestamp.UTC().Format(time.RFC3339)
		}

		size := "—"
		if r.SizeBytes > 0 {
			size = formatSize(r.SizeBytes)
		}

		fmt.Printf("  %-6s %-10s %-21s %s\n", ref, r.Status, ts, size)
	}

	fmt.Println()
}

const backupsCaptureHelp = `vd pg:backups:capture — take a pg_dump snapshot of the cluster.

USAGE
  vd pg:backups:capture <postgres-scope/name> [--from-replica <N>] [--follow]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.

FLAGS
  --from-replica <N>      Run pg_dump against ordinal N instead of
                          the current primary. Useful to offload the
                          dump load — standbys are read-only replicas,
                          no write contention. Defaults to the primary.
  --follow, -f            Stream the running container's logs and
                          progress in real time, blocking until pg_dump
                          finishes. Without --follow, capture detaches
                          immediately and returns the new ID; check
                          progress via vd pg:backups or vd pg:backups:logs.

DEFAULT: DETACHED (via voodu jobs)
  Capture emits a [apply_manifest, run_job] dispatch action pair.
  Voodu's job runner registers the job manifest, spawns the
  container, and returns immediately. The dump continues in the
  background even if your SSH session drops or you Ctrl-C the vd
  command. Track via:

    vd pg:backups <postgres-scope/name>           # status (running/complete/failed)
    vd pg:backups:logs <ref> <id> [--follow]      # tail container logs
    vd pg:backups:cancel <ref> [<id>]             # delete_manifest (stops + removes)

WHAT IT DOES (DETACHED)
  1. Resolves the source container (primary by default).
  2. Reads existing backup files in /backups/ to compute the next
     sequence ID (gap-free; deletes don't fill gaps).
  3. Composes a job manifest at <scope>/<name>-backup-bNNN:
       image:    same as source pod
       command:  pg_dump -h <FQDN>:port ... -f /backups/bNNN-...dump
       env:      PGPASSWORD from the resource's config bucket
       volumes:  /opt/voodu/backups/<scope>/<name>:/backups:rw
  4. Emits apply_manifest + run_job actions; controller persists
     the manifest, spawns the container via voodu's JobHandler,
     and returns "queued".

WHAT IT DOES (--follow)
  Falls back to docker run -d direct (Caminho A): plugin spawns
  the sibling container itself, streams docker logs, polls /backups/
  size for the progress reporter. This stays direct because dispatch
  actions are fire-and-forget — plugins can't follow logs across
  the controller boundary.

OUTPUT FORMAT
  pg_dump custom format (-F c, -Z 6). Restore via pg_restore on any
  postgres ≥ source major version; portable across hosts.

EXAMPLES
  # Default detached
  vd pg:backups:capture clowk-lp/db
  # → postgres clowk-lp/db: backup b008 capturing in background...

  # Foreground with progress
  vd pg:backups:capture clowk-lp/db --follow

  # From a standby
  vd pg:backups:capture clowk-lp/db --from-replica 1 --follow
`

// cmdBackupsCapture spawns a sibling Docker container that runs
// pg_dump against the source pod. Detached by default — returns
// the new backup ID immediately; --follow blocks + streams logs.
func cmdBackupsCapture() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(backupsCaptureHelp)
		return nil
	}

	positional, fromReplica, hasFromReplica, follow, err := parseCaptureFlags(args)
	if err != nil {
		return err
	}

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd pg:backups:capture <postgres-scope/name> [--from-replica <N>] [--follow]")
	}

	scope, name := splitScopeName(positional[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", positional[0])
	}

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	if ctx.ControllerURL == "" {
		return fmt.Errorf("capture requires controller_url (needs replicas count + primary ordinal from controller)")
	}

	client := newControllerClient(ctx.ControllerURL)

	spec, err := client.fetchSpec("statefulset", scope, name)
	if err != nil {
		return fmt.Errorf("describe %s: %w", refOrName(scope, name), err)
	}

	config, err := client.fetchConfig(scope, name)
	if err != nil {
		return fmt.Errorf("config get %s: %w", refOrName(scope, name), err)
	}

	replicas := readReplicas(spec)
	primaryOrdinal := readCurrentPrimaryOrdinal(config)

	source := primaryOrdinal
	if hasFromReplica {
		source = fromReplica
	}

	if source < 0 || source >= replicas {
		return fmt.Errorf("--from-replica %d out of range (valid: 0..%d)", source, replicas-1)
	}

	sourceContainer := containerNameFor(scope, name, source)

	if !containerExists(sourceContainer) {
		return fmt.Errorf("container %s not found on this host (capture must run on the same host as the source pod)", sourceContainer)
	}

	running, err := containerIsRunning(sourceContainer)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", sourceContainer, err)
	}

	if !running {
		return fmt.Errorf("container %s is not running (need a live pod to dump from)", sourceContainer)
	}

	user, db, port := readUserDBPort(spec)

	password := config[passwordKey]
	if password == "" {
		return fmt.Errorf("no POSTGRES_PASSWORD in config bucket — has %s been applied?", refOrName(scope, name))
	}

	// Auto-prune exited backup containers from prior captures so a
	// previously-occupied bNNN slot can be re-used after the file
	// was deleted. Best-effort — failures here don't block capture.
	// Only relevant for the follow path's docker-run-direct
	// containers; jobs migrated to the action path are reaped by
	// voodu's history limits when the manifest is re-applied.
	_ = pruneExitedBackupContainers(scope, name)

	// Compute next ID. Read files via the host bind-mount so this
	// works even if no postgres pod is running (rare — dev/test).
	existingHost, _ := listBackupsOnHost(scope, name)
	filenames := make([]string, 0, len(existingHost))

	for _, e := range existingHost {
		filenames = append(filenames, e.Filename)
	}

	id := nextBackupID(filenames)
	now := time.Now().UTC()
	filename := formatBackupFilename(id, now)
	containerFilePath := backupsContainerPath + "/" + filename
	hostFilePath := composeBackupHostPath(scope, name) + "/" + filename

	// Pre-flight: query raw db size for the progress reporter's
	// estimate. Best-effort — if psql fails, capture still runs;
	// progress lines just drop the "% of estimate" suffix.
	rawDBBytes, _ := queryDBSize(sourceContainer, user, db)
	estDumpBytes := int64(float64(rawDBBytes) * dumpCompressionFactor)

	image := readImage(spec)
	if image == "" {
		image = defaultImage
	}

	hostBackupsDir := composeBackupHostPath(scope, name)

	entry := backupEntry{ID: id, Filename: filename, Timestamp: now}

	// ------------------------------------------------------------
	// DETACHED path: emit [apply_manifest, run_job] dispatch
	// actions. Voodu's job runner spawns the container; we return
	// the new ID immediately. Plugin doesn't touch docker.
	//
	// Why this can only work for detached: dispatch actions are
	// fire-and-forget — the controller applies them AFTER the
	// plugin returns. The plugin therefore can't follow logs or
	// progress for the run it just queued. --follow takes the
	// docker-run-direct path below for that reason.
	// ------------------------------------------------------------
	if !follow {
		jobName := name + "-backup-" + entry.Ref()
		sourceFQDN := composePrimaryFQDN(scope, name, source)

		// pg_dump connects to the source pod via voodu0 DNS.
		// Bridge mode (default) gives the job container voodu0
		// membership which resolves <pod>.<scope>.voodu names.
		wrapper := fmt.Sprintf(
			`pg_dump -h %s -p %d -U %s -d %s -F c -Z 6 -f %s || { rc=$?; rm -f %s; exit $rc; }`,
			sourceFQDN, port, user, db, containerFilePath, containerFilePath,
		)

		jobSpec := map[string]any{
			"image":   image,
			"command": []any{"bash", "-c", wrapper},
			"env": map[string]any{
				"PGPASSWORD": password,
			},
			"volumes": []any{
				hostBackupsDir + ":" + backupsContainerPath + ":rw",
			},
			// Cap history so etcd doesn't accumulate run records
			// across many captures.
			"successful_history_limit": 3,
			"failed_history_limit":     3,
		}

		actions := []dispatchAction{
			{
				Type:  "apply_manifest",
				Scope: scope,
				Name:  name,
				Manifest: &dispatchManifest{
					Kind:  "job",
					Scope: scope,
					Name:  jobName,
					Spec:  jobSpec,
				},
			},
			{
				Type:  "run_job",
				Scope: scope,
				Name:  jobName,
			},
		}

		hint := fmt.Sprintf("track: vd pg:backups %s  |  vd pg:backups:logs %s %s --follow",
			refOrName(scope, name), refOrName(scope, name), entry.Ref())

		result := dispatchOutput{
			Message: fmt.Sprintf(
				"postgres %s: backup %s capturing in background (job %s, source: ordinal %d). %s",
				refOrName(scope, name), entry.Ref(), refOrName(scope, jobName), source, hint),
			Actions: actions,
		}

		return writeDispatchOutput(result)
	}

	// ------------------------------------------------------------
	// FOLLOW path: docker run -d direct + docker logs -f + wait.
	// Stays on Caminho A — dispatch actions can't carry follow
	// semantics across the controller boundary.
	// ------------------------------------------------------------
	jobContainer := composeBackupContainerName(scope, name, id)

	wrapper := fmt.Sprintf(
		`pg_dump -h 127.0.0.1 -p %d -U %s -d %s -F c -Z 6 -f %s || { rc=$?; rm -f %s; exit $rc; }`,
		port, user, db, containerFilePath, containerFilePath,
	)

	runArgs := []string{
		"run", "-d",
		"--name", jobContainer,
		"--network", "container:" + sourceContainer,
		"-e", "PGPASSWORD=" + password,
		"-v", hostBackupsDir + ":" + backupsContainerPath + ":rw",
		image,
		"bash", "-c", wrapper,
	}

	if err := exec.Command("docker", runArgs...).Run(); err != nil {
		return fmt.Errorf("docker run %s: %w", jobContainer, err)
	}

	// --follow path: rolling progress + docker logs streaming +
	// wait for the container to exit.
	if rawDBBytes > 0 {
		fmt.Fprintf(os.Stderr, "capture: pg_dump → %s (db=%s, user=%s; raw size %s, estimated dump ~%s, container %s)\n",
			containerFilePath, db, user,
			formatSize(rawDBBytes), formatSize(estDumpBytes), jobContainer)
	} else {
		fmt.Fprintf(os.Stderr, "capture: pg_dump → %s (db=%s, user=%s, container %s)\n",
			containerFilePath, db, user, jobContainer)
	}

	dumpStart := time.Now()

	// Progress reporter: stat the host file directly (no docker
	// round-trip — bind-mount means same inode).
	progressDone := make(chan struct{})
	go reportCaptureProgressOnHost(hostFilePath, estDumpBytes, progressDone)

	// Stream the container's logs in a goroutine so any stderr
	// from pg_dump (errors, warnings) reaches the operator.
	logsDone := make(chan struct{})
	go func() {
		defer close(logsDone)

		logsCmd := exec.Command("docker", "logs", "-f", jobContainer)
		logsCmd.Stdout = os.Stderr
		logsCmd.Stderr = os.Stderr
		_ = logsCmd.Run()
	}()

	// Block until the container exits.
	waitOut, waitErr := exec.Command("docker", "wait", jobContainer).Output()

	close(progressDone)
	<-logsDone

	if waitErr != nil {
		return fmt.Errorf("docker wait %s: %w", jobContainer, waitErr)
	}

	exitCode, _ := strconv.Atoi(strings.TrimSpace(string(waitOut)))

	if exitCode != 0 {
		return fmt.Errorf("pg_dump failed in %s (exit code %d) — see container logs: docker logs %s",
			jobContainer, exitCode, jobContainer)
	}

	// Success: stat the file (host-side) for the final size.
	info, err := os.Stat(hostFilePath)
	if err != nil {
		return fmt.Errorf("stat %s: %w", hostFilePath, err)
	}

	dumpElapsed := time.Since(dumpStart).Round(time.Second)
	size := info.Size()
	entry.SizeBytes = size

	fmt.Fprintf(os.Stderr, "done: %s in %s\n", formatSize(size), dumpElapsed)

	result := dispatchOutput{
		Message: fmt.Sprintf(
			"postgres %s: backup %s captured (%s, %s, source: ordinal %d, elapsed %s)",
			refOrName(scope, name), entry.Ref(), filename, formatSize(size), source, dumpElapsed),
	}

	return writeDispatchOutput(result)
}

// reportCaptureProgressOnHost is the host-side variant of the
// progress reporter. Stats the file directly via the host
// bind-mount path — no docker exec round-trips. Behaviour
// otherwise identical to reportCaptureProgress.
func reportCaptureProgressOnHost(hostFilePath string, estDumpBytes int64, done <-chan struct{}) {
	start := time.Now()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return

		case <-ticker.C:
			info, err := os.Stat(hostFilePath)
			if err != nil {
				continue
			}

			if info.Size() == 0 {
				continue
			}

			fmt.Fprintln(os.Stderr, formatProgressLine(time.Since(start), info.Size(), estDumpBytes))
		}
	}
}

// parseCaptureFlags extracts --from-replica + --follow.
func parseCaptureFlags(args []string) (positional []string, fromReplica int, hasFromReplica, follow bool, err error) {
	for i := 0; i < len(args); i++ {
		a := args[i]

		switch {
		case a == "--from-replica":
			if i+1 >= len(args) {
				return nil, 0, false, false, fmt.Errorf("--from-replica requires an integer argument")
			}

			n, perr := parseInt(args[i+1])
			if perr != nil {
				return nil, 0, false, false, fmt.Errorf("--from-replica %q: %w", args[i+1], perr)
			}

			fromReplica = n
			hasFromReplica = true
			i++

		case len(a) > 15 && a[:15] == "--from-replica=":
			n, perr := parseInt(a[15:])
			if perr != nil {
				return nil, 0, false, false, fmt.Errorf("--from-replica %q: %w", a[15:], perr)
			}

			fromReplica = n
			hasFromReplica = true

		case a == "--follow", a == "-f":
			follow = true

		case a == "-h" || a == "--help":
			// handled

		default:
			positional = append(positional, a)
		}
	}

	return positional, fromReplica, hasFromReplica, follow, nil
}

// ─────────────────────────────────────────────────────────────────
// pg:backups:restore
// ─────────────────────────────────────────────────────────────────

const backupsRestoreHelp = `vd pg:backups:restore — restore the database from a backup.

USAGE
  vd pg:backups:restore <postgres-scope/name> <backup-id|url> [--yes]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.
  <backup-id|url>         Either a local backup ID (e.g. b001 — run
                          vd pg:backups to list) or an http(s) URL
                          pointing at a pg_dump custom-format file
                          (e.g. an S3 presigned URL).

FLAGS
  --yes, -y               Skip the destructive-action confirmation
                          prompt. Required when stdin is non-interactive
                          (cronjobs etc.).

DESTRUCTIVE
  pg_restore --clean drops + recreates every table, function, sequence,
  etc. matching the dump. Existing rows in matching tables are wiped.
  No rollback. Use --yes (or -y) to confirm.

  Take a fresh capture before running restore if you need a fallback:
    vd pg:backups:capture <ref>

WHAT IT DOES (NOT DESTRUCTIVE OF THE CLUSTER)
  Unlike vd postgres:restore (which wipes PGDATA on every pod),
  pg:backups:restore keeps the cluster running. It runs pg_restore
  against the primary, replaying SQL into the live database. The
  cluster topology, replication, and standby pods are unaffected —
  only the database content (objects this dump touches) changes.

WORKFLOW (LOCAL ID)
  1. Resolve <backup-id> → /backups/bNNN-<ts>.dump on the primary pod.
  2. Run pg_restore --clean --if-exists --no-owner --no-privileges
     inside the primary pod via Unix socket (no password, peer auth).
  3. Postgres applies the dump SQL transactionally where possible.
     pg_restore exits with non-zero on the first error.

WORKFLOW (URL)
  1. Download the URL to a temp file on this host (host disk space
     must accommodate the dump).
  2. docker cp the file into /tmp/<random>.dump inside the primary pod.
  3. Run pg_restore (same flags as the local-ID path).
  4. Clean up: remove /tmp/<random>.dump from the pod and the host
     temp file.

EXAMPLES
  # Local backup
  vd pg:backups:restore clowk-lp/db b007 --yes

  # S3 presigned URL (or any http/https endpoint serving a dump file)
  vd pg:backups:restore clowk-lp/db https://my-bucket.s3.amazonaws.com/db.dump?... --yes
`

// cmdBackupsRestore restores the live database from a backup file
// already living in /backups/ on the primary pod.
//
// Compared to vd postgres:restore (legacy, pg_basebackup tar +
// PGDATA wipe), this is non-destructive of the cluster — it just
// runs pg_restore against the primary's database. Standby pods stay
// up the whole time and replicate the restored state via streaming
// WAL.
func cmdBackupsRestore() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(backupsRestoreHelp)
		return nil
	}

	positional, autoYes := parseYesFlag(args)

	if len(positional) < 2 {
		return fmt.Errorf("usage: vd pg:backups:restore <postgres-scope/name> <backup-id> [--yes]")
	}

	scope, name := splitScopeName(positional[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", positional[0])
	}

	target := positional[1]

	if !autoYes {
		fmt.Fprintf(os.Stderr,
			"\n⚠  RESTORE IS DESTRUCTIVE — pg_restore --clean drops + recreates objects in the database.\n"+
				"   No rollback. Use --yes (or -y) to confirm.\n\n")

		return fmt.Errorf("refusing to restore without --yes (destructive)")
	}

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	if ctx.ControllerURL == "" {
		return fmt.Errorf("restore requires controller_url (needs primary ordinal + db/user from spec)")
	}

	client := newControllerClient(ctx.ControllerURL)

	spec, err := client.fetchSpec("statefulset", scope, name)
	if err != nil {
		return fmt.Errorf("describe %s: %w", refOrName(scope, name), err)
	}

	config, err := client.fetchConfig(scope, name)
	if err != nil {
		return fmt.Errorf("config get %s: %w", refOrName(scope, name), err)
	}

	primaryOrdinal := readCurrentPrimaryOrdinal(config)
	primaryContainer := containerNameFor(scope, name, primaryOrdinal)

	if !containerExists(primaryContainer) {
		return fmt.Errorf("container %s not found on this host", primaryContainer)
	}

	running, err := containerIsRunning(primaryContainer)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", primaryContainer, err)
	}

	if !running {
		return fmt.Errorf("container %s is not running (need a live primary to restore into)", primaryContainer)
	}

	user, db, _ := readUserDBPort(spec)

	// Resolve target → containerFilePath. Two modes:
	//
	//   (a) URL: download to host temp file → docker cp into pod
	//       /tmp/<random>.dump → restore → cleanup both sides.
	//   (b) local ID: existing file in /backups/.
	var (
		containerFilePath string
		sourceLabel       string
		sourceSize        int64
		cleanup           func()
	)

	if isHTTPURL(target) {
		fmt.Fprintf(os.Stderr, "restore: downloading %s\n", target)

		hostTmp, n, err := downloadURLToTempFile(target)
		if err != nil {
			return fmt.Errorf("download URL: %w", err)
		}

		// docker cp accepts host:container paths, not :tmp directly,
		// so we copy to a deterministic name inside /tmp/.
		podPath := fmt.Sprintf("/tmp/vdrestore-%d.dump", time.Now().UnixNano())

		if err := dockerCpToContainer(hostTmp, primaryContainer, podPath); err != nil {
			_ = os.Remove(hostTmp)
			return fmt.Errorf("docker cp into pod: %w", err)
		}

		containerFilePath = podPath
		sourceLabel = target
		sourceSize = n
		cleanup = func() {
			_ = os.Remove(hostTmp)
			_ = exec.Command("docker", "exec", primaryContainer, "rm", "-f", podPath).Run()
		}
	} else {
		entries, err := listBackupsInPod(primaryContainer)
		if err != nil {
			return err
		}

		entry, err := resolveBackupByID(entries, target)
		if err != nil {
			return err
		}

		containerFilePath = backupsContainerPath + "/" + entry.Filename
		sourceLabel = entry.Ref() + " (" + entry.Filename + ")"
		sourceSize = entry.SizeBytes
	}

	fmt.Fprintf(os.Stderr,
		"restore: pg_restore in %s ← %s (db=%s, user=%s, %s)\n",
		primaryContainer, containerFilePath, db, user, formatSize(sourceSize))

	// pg_restore inside the primary pod via Unix socket (peer auth,
	// no password). Flags:
	//
	//   --clean          drop existing objects before recreating
	//   --if-exists      tolerate missing objects (idempotent re-run)
	//   --no-owner       skip ALTER ... OWNER TO (works across hosts
	//                    where the postgres role names differ)
	//   --no-privileges  skip GRANT/REVOKE (same portability concern;
	//                    operator re-applies app-specific perms via
	//                    init scripts or migrations)
	cmd := exec.Command(
		"docker", "exec", "-u", "postgres", primaryContainer,
		"pg_restore",
		"-U", user,
		"-d", db,
		"--clean", "--if-exists",
		"--no-owner", "--no-privileges",
		containerFilePath,
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if cleanup != nil {
			cleanup()
		}

		return fmt.Errorf("pg_restore failed: %w", err)
	}

	if cleanup != nil {
		cleanup()
	}

	result := dispatchOutput{
		Message: fmt.Sprintf(
			"postgres %s: restored from %s. Cluster stayed up; standbys replicate restored state via streaming WAL.",
			refOrName(scope, name), sourceLabel),
		Actions: nil,
	}

	return writeDispatchOutput(result)
}

// isHTTPURL reports whether s looks like an http or https URL.
// Used by cmdBackupsRestore to branch between local-ID resolution
// and the download+cp path.
func isHTTPURL(s string) bool {
	return strings.HasPrefix(s, "https://") || strings.HasPrefix(s, "http://")
}

// downloadURLToTempFile streams a URL to a host temp file. Returns
// the temp path + bytes copied. Caller is responsible for removing
// the temp file when done (cleanup happens via the closure inside
// cmdBackupsRestore).
//
// Why streaming + not in-memory: a postgres dump can be many GB.
// io.Copy keeps memory usage bounded.
func downloadURLToTempFile(url string) (string, int64, error) {
	httpClient := &http.Client{
		// Long downloads are normal — multi-GB on a slow link can
		// take a while. No request timeout; rely on operator
		// Ctrl-C if it stalls. Per-read deadlines could be added
		// later if hangs become a problem in practice.
		Timeout: 0,
	}

	resp, err := httpClient.Get(url)
	if err != nil {
		return "", 0, err
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return "", 0, fmt.Errorf("HTTP %d %s", resp.StatusCode, resp.Status)
	}

	tmpFile, err := os.CreateTemp("", "vdrestore-*.dump")
	if err != nil {
		return "", 0, err
	}

	defer tmpFile.Close()

	n, err := io.Copy(tmpFile, resp.Body)
	if err != nil {
		_ = os.Remove(tmpFile.Name())
		return "", 0, err
	}

	return tmpFile.Name(), n, nil
}


// ─────────────────────────────────────────────────────────────────
// pg:backups:download
// ─────────────────────────────────────────────────────────────────

const backupsDownloadHelp = `vd pg:backups:download — copy a backup file out of the pod onto the host.

USAGE
  vd pg:backups:download <postgres-scope/name> <backup-id> [--to <path>]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.
  <backup-id>             The backup ID (e.g. b001). Run vd pg:backups
                          to list available IDs.

FLAGS
  --to <path>             Destination on the host (where this plugin
                          runs). Default: <filename> in the current
                          working directory.

WHAT IT DOES
  docker cp <primary>:/backups/<filename> <host-path>

  Bytes are copied verbatim — pg_dump custom format file, restorable
  via pg_restore on any postgres ≥ source major version. Works fine
  to ship a backup to another voodu host or to a local laptop.

EXAMPLES
  vd pg:backups:download clowk-lp/db b007
  # → ./b007-20260505T020000Z.dump in the operator's CWD

  vd pg:backups:download clowk-lp/db b007 --to /srv/archive/db.dump
  # → /srv/archive/db.dump
`

// cmdBackupsDownload copies a backup file from the primary pod to a
// host path. Useful for off-host shipping (laptop, S3 sync via host
// awscli, etc.).
func cmdBackupsDownload() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(backupsDownloadHelp)
		return nil
	}

	positional, dest := parseToFlag(args)

	if len(positional) < 2 {
		return fmt.Errorf("usage: vd pg:backups:download <postgres-scope/name> <backup-id> [--to <path>]")
	}

	scope, name := splitScopeName(positional[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", positional[0])
	}

	id := positional[1]

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	if ctx.ControllerURL == "" {
		return fmt.Errorf("download requires controller_url (needs primary ordinal from config bucket)")
	}

	client := newControllerClient(ctx.ControllerURL)

	config, err := client.fetchConfig(scope, name)
	if err != nil {
		return fmt.Errorf("config get %s: %w", refOrName(scope, name), err)
	}

	primaryOrdinal := readCurrentPrimaryOrdinal(config)
	primaryContainer := containerNameFor(scope, name, primaryOrdinal)

	if !containerExists(primaryContainer) {
		return fmt.Errorf("container %s not found on this host", primaryContainer)
	}

	running, err := containerIsRunning(primaryContainer)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", primaryContainer, err)
	}

	if !running {
		return fmt.Errorf("container %s is not running (need a live pod to copy from)", primaryContainer)
	}

	entries, err := listBackupsInPod(primaryContainer)
	if err != nil {
		return err
	}

	entry, err := resolveBackupByID(entries, id)
	if err != nil {
		return err
	}

	if dest == "" {
		dest = entry.Filename
	}

	srcPath := backupsContainerPath + "/" + entry.Filename

	fmt.Fprintf(os.Stderr, "download: docker cp %s:%s → %s (%s)\n",
		primaryContainer, srcPath, dest, formatSize(entry.SizeBytes))

	cmd := exec.Command("docker", "cp", primaryContainer+":"+srcPath, dest)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker cp failed: %w", err)
	}

	result := dispatchOutput{
		Message: fmt.Sprintf("postgres %s: backup %s copied to %s (%s)",
			refOrName(scope, name), entry.Ref(), dest, formatSize(entry.SizeBytes)),
		Actions: nil,
	}

	return writeDispatchOutput(result)
}

// ─────────────────────────────────────────────────────────────────
// pg:backups:delete
// ─────────────────────────────────────────────────────────────────

const backupsDeleteHelp = `vd pg:backups:delete — remove a backup file from the pod.

USAGE
  vd pg:backups:delete <postgres-scope/name> <backup-id> [--yes]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.
  <backup-id>             The backup ID (e.g. b001).

FLAGS
  --yes, -y               Skip the destructive-action confirmation
                          prompt. Required when stdin is non-interactive
                          (cronjobs etc.).

DESTRUCTIVE
  Removes /opt/voodu/backups/<scope>/<name>/<filename> from the
  host disk via os.Remove (no docker round-trip — backups dir is
  a host bind-mount). Plus emits delete_manifest to clean up the
  job manifest if it's still in etcd from the original capture
  (idempotent — no-op if already gone).

  Operator's responsibility to ensure no off-host sync is
  mid-flight (see Backup automation).

  This DOES NOT renumber subsequent backups; sequence IDs persist
  in their filenames. So deleting b005 leaves a gap and the next
  capture is b008 (max+1 of remaining).

EXAMPLES
  vd pg:backups:delete clowk-lp/db b007 --yes
`

// cmdBackupsDelete removes a backup file from /backups/ inside the
// primary pod. The host bind-mount means the file disappears from
// the host disk too.
func cmdBackupsDelete() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(backupsDeleteHelp)
		return nil
	}

	positional, autoYes := parseYesFlag(args)

	if len(positional) < 2 {
		return fmt.Errorf("usage: vd pg:backups:delete <postgres-scope/name> <backup-id> [--yes]")
	}

	scope, name := splitScopeName(positional[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", positional[0])
	}

	id := positional[1]

	if !autoYes {
		fmt.Fprintf(os.Stderr,
			"\n⚠  This deletes a backup file from disk. No undelete. Use --yes (or -y) to confirm.\n\n")

		return fmt.Errorf("refusing to delete without --yes")
	}

	// Read the file list directly from the host bind-mount —
	// no controller round-trip, no docker exec. Files are the
	// source of truth for completed backups; the (possibly
	// outdated) job manifest is best-effort cleaned up below.
	entries, err := listBackupsOnHost(scope, name)
	if err != nil {
		return fmt.Errorf("list backups: %w", err)
	}

	entry, err := resolveBackupByID(entries, id)
	if err != nil {
		return err
	}

	hostFilePath := composeBackupHostPath(scope, name) + "/" + entry.Filename

	fmt.Fprintf(os.Stderr, "delete: removing %s\n", hostFilePath)

	if err := os.Remove(hostFilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("rm %s: %w", hostFilePath, err)
	}

	// Auto-cleanup: emit delete_manifest for the job manifest
	// (if it still exists in etcd from the original capture).
	// Idempotent — controller's delete_manifest is no-op on
	// missing.
	jobName := name + "-backup-" + entry.Ref()

	actions := []dispatchAction{{
		Type:  "delete_manifest",
		Scope: scope,
		Name:  jobName,
		Kind:  "job",
	}}

	result := dispatchOutput{
		Message: fmt.Sprintf("postgres %s: backup %s (%s) deleted",
			refOrName(scope, name), entry.Ref(), entry.Filename),
		Actions: actions,
	}

	return writeDispatchOutput(result)
}

// ─────────────────────────────────────────────────────────────────
// pg:backups:logs
// ─────────────────────────────────────────────────────────────────

const backupsLogsHelp = `vd pg:backups:logs — show docker logs for a backup container.

USAGE
  vd pg:backups:logs <postgres-scope/name> <backup-id> [--follow]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.
  <backup-id>             The backup ID (e.g. b008). Run vd pg:backups
                          to find available IDs (running + completed).

FLAGS
  --follow, -f            Stream logs in real time, blocking until
                          the container exits. Without --follow,
                          prints accumulated logs and returns.

WHAT IT DOES
  Runs docker logs against the backup job container
  <scope>-<name>-backup-bNNN. Works for running, completed, and
  failed captures — until the container is auto-pruned by the next
  vd pg:backups:capture (which removes exited siblings to free up
  the bNNN slot).

EXAMPLES
  vd pg:backups:logs clowk-lp/db b008
  vd pg:backups:logs clowk-lp/db b008 --follow
`

// cmdBackupsLogs runs `docker logs` against a backup container.
func cmdBackupsLogs() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(backupsLogsHelp)
		return nil
	}

	positional, follow := parseFollowFlag(args)

	if len(positional) < 2 {
		return fmt.Errorf("usage: vd pg:backups:logs <postgres-scope/name> <backup-id> [--follow]")
	}

	scope, name := splitScopeName(positional[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", positional[0])
	}

	id := positional[1]

	if !strings.HasPrefix(id, "b") {
		return fmt.Errorf("backup id %q must start with 'b' (e.g. b008)", id)
	}

	n, err := strconv.Atoi(id[1:])
	if err != nil {
		return fmt.Errorf("backup id %q: invalid number after 'b'", id)
	}

	jobContainer := composeBackupContainerName(scope, name, n)

	if !containerExists(jobContainer) {
		return fmt.Errorf("backup container %s not found (auto-pruned, or capture never started)", jobContainer)
	}

	logsArgs := []string{"logs"}
	if follow {
		logsArgs = append(logsArgs, "-f")
	}

	logsArgs = append(logsArgs, jobContainer)

	cmd := exec.Command("docker", logsArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// parseFollowFlag splits args into positional + the follow bool.
// Used by :logs and a couple of other places that need streaming
// vs one-shot output.
func parseFollowFlag(args []string) (positional []string, follow bool) {
	for _, a := range args {
		switch a {
		case "--follow", "-f":
			follow = true
		case "-h", "--help":
			// handled
		default:
			positional = append(positional, a)
		}
	}

	return positional, follow
}

// ─────────────────────────────────────────────────────────────────
// pg:backups:schedule
// ─────────────────────────────────────────────────────────────────

const backupsScheduleHelp = `vd pg:backups:schedule — print a cronjob HCL block that captures backups on a schedule.

USAGE
  vd pg:backups:schedule <postgres-scope/name> [--at <cron-expr>] [--from-replica <N>]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster the schedule will back up.

FLAGS
  --at <cron-expr>        Cron schedule (5-field syntax). Default: "0 3 * * *"
                          (daily at 03:00 UTC). Examples:
                            "*/30 * * * *"   every 30 minutes
                            "0 3 * * *"      daily at 03:00
                            "0 3 * * 0"      Sundays at 03:00
  --from-replica <N>      Capture from ordinal N instead of the
                          current primary. Useful to offload backup
                          load — standbys are read-only replicas.

WHAT IT DOES
  Prints an HCL cronjob block to stdout. Operator pastes it into
  voodu.hcl and runs vd apply. Voodu manages the cron container
  going forward — pg:backups:schedule itself emits no manifests
  and no actions; it's a template helper.

  Why a helper instead of a managed schedule: voodu cronjobs are
  fully declarative — the source of truth is the HCL, not plugin-
  side state. Keeping the schedule visible in HCL lets vd diff and
  vd apply --prune work without surprises.

EXAMPLES
  # Print the default daily snapshot
  vd pg:backups:schedule clowk-lp/db

  # Custom cadence + offload from a standby
  vd pg:backups:schedule clowk-lp/db --at "0 */6 * * *" --from-replica 1
`

// cmdBackupsSchedule prints an HCL cronjob template for the
// operator to paste. Pure stdout helper — emits no manifests, no
// dispatch actions, no controller calls.
//
// Why no managed-schedule storage: voodu cronjobs are declarative
// (lives in HCL). Plugin-side schedule state would diverge from
// the HCL source-of-truth on the next vd apply --prune. Better to
// keep the operator in the apply/diff loop.
func cmdBackupsSchedule() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(backupsScheduleHelp)
		return nil
	}

	positional, schedule, fromReplica, hasFromReplica, err := parseScheduleFlags(args)
	if err != nil {
		return err
	}

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd pg:backups:schedule <postgres-scope/name> [--at <cron-expr>] [--from-replica <N>]")
	}

	scope, name := splitScopeName(positional[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", positional[0])
	}

	if schedule == "" {
		schedule = "0 3 * * *"
	}

	captureCmd := fmt.Sprintf("vd pg:backups:capture %s", refOrName(scope, name))
	if hasFromReplica {
		captureCmd += fmt.Sprintf(" --from-replica %d", fromReplica)
	}

	// Cronjob name suffix — keeps multiple postgres resources on
	// the same scope from colliding.
	cronName := name + "-backup"

	fmt.Println()
	fmt.Println("# Add this to your voodu.hcl, then run `vd apply`:")
	fmt.Println()
	fmt.Printf("cronjob %q %q {\n", scope, cronName)
	fmt.Printf("  schedule = %q\n", schedule)
	fmt.Printf("  image    = \"ghcr.io/clowk/voodu-cli:latest\"\n")
	fmt.Println()
	fmt.Printf("  command = [\"bash\", \"-c\", %q]\n", captureCmd)
	fmt.Println("}")
	fmt.Println()
	fmt.Println("# To remove the schedule later: delete the block + run `vd apply --prune`.")
	fmt.Println()

	return nil
}

func parseScheduleFlags(args []string) (positional []string, schedule string, fromReplica int, hasFromReplica bool, err error) {
	for i := 0; i < len(args); i++ {
		a := args[i]

		switch {
		case a == "--at":
			if i+1 >= len(args) {
				return nil, "", 0, false, fmt.Errorf("--at requires a cron expression")
			}

			schedule = args[i+1]
			i++

		case len(a) > 5 && a[:5] == "--at=":
			schedule = a[5:]

		case a == "--from-replica":
			if i+1 >= len(args) {
				return nil, "", 0, false, fmt.Errorf("--from-replica requires an integer argument")
			}

			n, perr := parseInt(args[i+1])
			if perr != nil {
				return nil, "", 0, false, fmt.Errorf("--from-replica %q: %w", args[i+1], perr)
			}

			fromReplica = n
			hasFromReplica = true
			i++

		case len(a) > 15 && a[:15] == "--from-replica=":
			n, perr := parseInt(a[15:])
			if perr != nil {
				return nil, "", 0, false, fmt.Errorf("--from-replica %q: %w", a[15:], perr)
			}

			fromReplica = n
			hasFromReplica = true

		case a == "-h" || a == "--help":
			// handled

		default:
			positional = append(positional, a)
		}
	}

	return positional, schedule, fromReplica, hasFromReplica, nil
}

// ─────────────────────────────────────────────────────────────────
// pg:backups:cancel
// ─────────────────────────────────────────────────────────────────

const backupsCancelHelp = `vd pg:backups:cancel — stop a running backup capture.

USAGE
  vd pg:backups:cancel <postgres-scope/name> [<backup-id>]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.
  <backup-id>             Optional. Specific backup to cancel
                          (e.g. b008). Without an ID, every running
                          capture for this cluster is stopped.

WHAT IT DOES
  Emits delete_manifest dispatch actions for the running backup
  jobs (<scope>/<name>-backup-bNNN). Voodu's JobHandler.remove
  fires on each delete: SIGTERM the container (10s grace) then
  SIGKILL, plus clear the manifest from the store. Postgres
  detects the dropped connection and rolls back the transaction —
  non-destructive of the cluster.

  Partial dump files in /backups/ are removed automatically by the
  container's wrapper script (it does rm -f on the file when
  pg_dump exits non-zero).

EXAMPLES
  # Cancel everything in flight for clowk-lp/db
  vd pg:backups:cancel clowk-lp/db

  # Cancel just b008
  vd pg:backups:cancel clowk-lp/db b008
`

// cmdBackupsCancel cancels running backup captures by emitting
// `delete_manifest` actions for the matching job manifests. Voodu's
// JobHandler.remove tears down the running container as part of the
// store delete (ContainersByIdentity → Remove) — same effect as
// docker stop, but routed through the controller so the manifest
// state is consistent.
//
// Falls back to docker stop for legacy --follow captures (which
// stay docker-run-direct since dispatch can't carry follow
// semantics): we scan local containers for the prefix and stop
// any running ones that DON'T have a matching job manifest.
func cmdBackupsCancel() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(backupsCancelHelp)
		return nil
	}

	if len(args) < 1 {
		return fmt.Errorf("usage: vd pg:backups:cancel <postgres-scope/name> [<backup-id>]")
	}

	scope, name := splitScopeName(args[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", args[0])
	}

	var targetID int
	hasTarget := false

	if len(args) >= 2 && !strings.HasPrefix(args[1], "-") {
		id := args[1]

		if !strings.HasPrefix(id, "b") {
			return fmt.Errorf("backup id %q must start with 'b' (e.g. b008)", id)
		}

		n, err := strconv.Atoi(id[1:])
		if err != nil {
			return fmt.Errorf("backup id %q: invalid number after 'b'", id)
		}

		targetID = n
		hasTarget = true
	}

	containers, err := listBackupContainers(scope, name)
	if err != nil {
		return err
	}

	var actions []dispatchAction

	stopped := 0

	for _, c := range containers {
		if c.State != "running" && c.State != "created" {
			continue
		}

		if hasTarget && c.ID != targetID {
			continue
		}

		// Migration path: each running capture corresponds to a
		// job manifest at <scope>/<name>-backup-bNNN. Emit
		// delete_manifest; voodu's JobHandler.remove will SIGTERM
		// the container and clear the manifest in one shot.
		jobName := name + "-backup-" + fmt.Sprintf("b%03d", c.ID)
		if c.ID >= 1000 {
			jobName = name + "-backup-" + fmt.Sprintf("b%d", c.ID)
		}

		actions = append(actions, dispatchAction{
			Type:  "delete_manifest",
			Scope: scope,
			Name:  jobName,
			Kind:  "job",
		})

		fmt.Fprintf(os.Stderr, "cancelling backup b%03d (job %s/%s)\n", c.ID, scope, jobName)

		stopped++
	}

	var msg string

	switch {
	case stopped == 0 && hasTarget:
		msg = fmt.Sprintf("postgres %s: backup b%03d is not running (nothing to cancel)",
			refOrName(scope, name), targetID)

	case stopped == 0:
		msg = fmt.Sprintf("postgres %s: no captures in progress",
			refOrName(scope, name))

	case stopped == 1:
		msg = fmt.Sprintf("postgres %s: stopped 1 capture",
			refOrName(scope, name))

	default:
		msg = fmt.Sprintf("postgres %s: stopped %d captures",
			refOrName(scope, name), stopped)
	}

	result := dispatchOutput{Message: msg, Actions: actions}

	return writeDispatchOutput(result)
}

// ─────────────────────────────────────────────────────────────────
// shared flag parsers
// ─────────────────────────────────────────────────────────────────

// parseYesFlag splits args into positional + the autoYes bool.
// Used by every destructive backup command (restore, delete).
// `--yes` and `-y` both work.
func parseYesFlag(args []string) (positional []string, autoYes bool) {
	for _, a := range args {
		switch a {
		case "--yes", "-y":
			autoYes = true
		case "-h", "--help":
			// handled by hasHelpFlag at the entrypoint
		default:
			positional = append(positional, a)
		}
	}

	return positional, autoYes
}

// parseToFlag splits args into positional + the --to <path> value.
// Used by pg:backups:download (where it picks the host destination
// for docker cp).
func parseToFlag(args []string) (positional []string, dest string) {
	for i := 0; i < len(args); i++ {
		a := args[i]

		switch {
		case a == "--to":
			if i+1 < len(args) {
				dest = args[i+1]
				i++
			}

		case len(a) > 5 && a[:5] == "--to=":
			dest = a[5:]

		case a == "-h" || a == "--help":
			// handled

		default:
			positional = append(positional, a)
		}
	}

	return positional, dest
}
