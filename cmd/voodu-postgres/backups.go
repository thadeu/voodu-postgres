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
// backup job container: `<scope>-<name>.bk.bNNN`. Mirrors voodu's
// container naming (dot-separated AppID.replica) — putting the
// `.bk` category in the JOB MANIFEST NAME so voodu's
// containers.ContainerName auto-produces the dotted form when it
// appends the runID:
//
//	job manifest:  scope=clowk-lp, name=db.bk.b011
//	container:     clowk-lp-db.bk.b011.<runID>
//
// Unscoped resources drop the leading scope segment (just
// `<name>.bk.bNNN`). For Caminho A (docker run -d direct), no
// runID is appended, so the container name IS the
// composeBackupContainerName output.
func composeBackupContainerName(scope, name string, id int) string {
	prefix := composeBackupContainerPrefix(scope, name)

	if id < 1000 {
		return fmt.Sprintf("%sb%03d", prefix, id)
	}

	return fmt.Sprintf("%sb%d", prefix, id)
}

// composeBackupContainerPrefix returns the prefix shared by all
// backup containers for a (scope, name) pair: `<scope>-<name>.bk.`
// (note the trailing dot). Used for `docker ps --filter name=<prefix>`
// scans and as the strip-prefix for parseContainerNameForID.
//
// The `.bk.` infix differentiates backup-related containers from
// any other voodu container under the same scope/name (e.g.
// statefulset replicas `<scope>-<name>.0`). Future categories
// (e.g. `.rs.` for restore jobs, `.cl.` for cleanup) can plug in
// without colliding.
func composeBackupContainerPrefix(scope, name string) string {
	if scope == "" {
		return name + ".bk."
	}

	return scope + "-" + name + ".bk."
}

// composeBackupJobName returns the JOB MANIFEST name (kind=job)
// for a given backup. Voodu's containers.ContainerName takes
// (scope, name, runID) and produces `<scope>-<name>.<runID>`, so
// embedding `.bk.bNNN` in the name yields the desired
// `<scope>-<name>.bk.bNNN.<runID>` container name automatically.
func composeBackupJobName(name string, id int) string {
	if id < 1000 {
		return fmt.Sprintf("%s.bk.b%03d", name, id)
	}

	return fmt.Sprintf("%s.bk.b%d", name, id)
}

// parseContainerNameForID extracts the backup ID from a container
// name produced for a backup capture. Two shapes accepted:
//
//   - "<prefix>bNNN"            (Caminho A: docker run -d direct)
//   - "<prefix>bNNN.<runID>"    (Caminho B: voodu jobs, runID
//                                appended by containers.ContainerName
//                                with `.` as separator)
//
// Voodu's containers.ContainerName uses `.` between the AppID and
// the replica/run id (e.g. `clowk-lp-db.0`,
// `clowk-lp-db-backup-b011.fccd`). The Caminho A path used `-`
// historically; we accept both for safety so legacy containers
// from --follow captures still resolve.
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

	// After the digits, we expect either:
	//   - end of string (Caminho A exact match: "<prefix>bNNN")
	//   - "." (Caminho B voodu jobs: "<prefix>bNNN.<runID>")
	//   - "-" (legacy / hand-rolled docker run -d: "<prefix>bNNN-<runID>")
	// Anything else (e.g. "b008abc") is not a valid run name; reject
	// to avoid spurious matches.
	if end < len(digits) {
		sep := digits[end]
		if sep != '.' && sep != '-' {
			return 0, false
		}
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

// backupContainerPrefixes returns every prefix shape under which
// a backup container may have been created. The current shape is
// canonical (composeBackupContainerPrefix); the legacy shape is
// kept so containers from before the .bk. rename are still
// discoverable by `vd pg:backups`, `:logs`, `:cancel`, etc. — until
// the operator deletes the underlying backups (or the controllers
// pruning catches up).
//
// New format: `<scope>-<name>.bk.` (e.g. clowk-lp-db.bk.b012.ed52)
// Legacy:     `<scope>-<name>-backup-` (e.g. clowk-lp-db-backup-b012.ed52)
//
// Drop the legacy entry once no operator has containers in the old
// shape — likely safe a few weeks/months after rollout.
func backupContainerPrefixes(scope, name string) []string {
	current := composeBackupContainerPrefix(scope, name)

	var legacy string
	if scope == "" {
		legacy = name + "-backup-"
	} else {
		legacy = scope + "-" + name + "-backup-"
	}

	return []string{current, legacy}
}

// listBackupContainers runs `docker ps -a` once per known backup
// container prefix shape and returns the merged list. Multiple
// prefix scans cover the rename: containers spawned before the
// `.bk.` rename used `-backup-` and are otherwise invisible to
// the new parser; widening the scan keeps `:logs`, `:cancel`, and
// the auto-prune path working across the migration.
func listBackupContainers(scope, name string) ([]backupContainer, error) {
	var entries []backupContainer

	seen := map[string]bool{}

	for _, prefix := range backupContainerPrefixes(scope, name) {
		batch, err := listBackupContainersForPrefix(prefix)
		if err != nil {
			return nil, err
		}

		for _, c := range batch {
			if seen[c.Name] {
				// Defensive: a container could match both prefixes
				// only if a future rename uses both as substrings.
				// Today they're disjoint so this branch is dead
				// weight, but cheap to keep against drift.
				continue
			}

			seen[c.Name] = true

			entries = append(entries, c)
		}
	}

	return entries, nil
}

// listBackupContainersForPrefix is the single-prefix worker that
// listBackupContainers fans out to. Pulled out so each prefix
// scan has its own parser instance — necessary because
// parseContainerNameForID strips a SPECIFIC prefix from each
// candidate container name.
func listBackupContainersForPrefix(prefix string) ([]backupContainer, error) {
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
                                              [--keep <N>] [--max-age <duration>]

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
  --keep <N>              After capture, keep at most N backups (newest
                          first). Older ones are deleted from disk and
                          their containers reaped. 0 / unset = no limit.
  --max-age <duration>    After capture, delete any backup older than
                          the duration. Accepts 7d, 2w, 168h, etc.
                          0 / unset = no age cap. Alias: --retention.

  Retention is applied AFTER successful capture. With --follow the
  just-completed dump counts toward the limit; in detached mode the
  prune runs against existing files (the new one will land into the
  freed slot). Both axes are enforced together — a backup is deleted
  if it fails EITHER check (rank > N OR age > duration).

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

  # With retention: keep last 30 backups, drop anything older than 7d
  vd pg:backups:capture clowk-lp/db --keep 30 --max-age 7d

  # Just count cap (single-host dev/test)
  vd pg:backups:capture clowk-lp/db --keep 10
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

	positional, fromReplica, hasFromReplica, follow, retention, err := parseCaptureFlags(args)
	if err != nil {
		return err
	}

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd pg:backups:capture <postgres-scope/name> [--from-replica <N>] [--follow] [--keep <N>] [--max-age <duration>]")
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

	// Layer config-bucket retention defaults UNDER the flag-derived
	// policy so an operator can persist BACKUP_KEEP=30 once via
	// `vd config <ref> set` and have every subsequent capture
	// auto-prune without typing flags. Per-axis: a flag overrides
	// only that axis (--keep 5 ad-hoc doesn't wipe BACKUP_MAX_AGE).
	retention = mergeRetention(retention, loadRetentionFromConfig(config))

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
		jobName := composeBackupJobName(name, id)
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
			// voodu.role label categorises the container so future
			// `vd jobs:list --role=backup` (or generic queries on
			// label) work without depending on name parsing.
			// Operators can also `docker ps --filter
			// label=voodu.role=backup` to grep any in-flight backup
			// across resources/scopes.
			"labels": map[string]any{
				"voodu.role": "backup",
			},
			// Cap history so etcd doesn't accumulate run records
			// across many captures.
			"successful_history_limit": 3,
			"failed_history_limit":     3,
		}

		// Friendly action labels — the controller's `applied`
		// checklist shows these instead of the verbose
		// `apply_manifest <kind>/<scope>/<name>` defaults. Operators
		// scanning `vd pg:backups:capture` output don't care which
		// dispatch primitive is which; they want a quick "yes, the
		// backup got queued."
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
				Summary: fmt.Sprintf("job manifest applied: %s", refOrName(scope, jobName)),
			},
			{
				Type:    "run_job",
				Scope:   scope,
				Name:    jobName,
				Summary: fmt.Sprintf("backup %s dispatched", entry.Ref()),
			},
		}

		// Apply retention BEFORE dispatch. The new capture is still
		// queued at this point — pruning now frees disk for it
		// instead of after the fact (matters on tight HDs). Existing
		// files are the only inputs since the new dump hasn't started
		// writing yet. Caller-controlled via --keep / --max-age; no-op
		// when the policy is empty.
		prunedRefs := []string{}
		if !retention.IsZero() {
			deleted, perr := pruneBackupsByPolicy(scope, name, retention, time.Now())
			if perr != nil {
				fmt.Fprintf(os.Stderr, "warn: retention prune failed: %v\n", perr)
			}

			for _, e := range deleted {
				prunedRefs = append(prunedRefs, e.Ref())
			}
		}

		// Compose a multi-line message that reads top-down:
		//
		//   1. Headline — what's happening, where, from which source
		//   2. Side effects — retention if any
		//   3. Track block — copy-paste-able commands the operator
		//      uses to follow the running capture
		//
		// The controller appends the ✓ checklist below this; that
		// gives operators (a) the human summary first, (b) the
		// programmatic confirmations second. Reading order matches
		// what they'd ask: "what happened?" → "what should I run
		// next?" → "did it apply?".
		var b strings.Builder

		fmt.Fprintf(&b, "postgres %s: capturing backup %s in background (source: %s)",
			refOrName(scope, name), entry.Ref(), formatSourceLabel(source, primaryOrdinal))

		if len(prunedRefs) > 0 {
			fmt.Fprintf(&b, "\nretention: %s, pruned %s",
				retention, strings.Join(prunedRefs, ", "))
		}

		fmt.Fprintf(&b, "\n\nTrack:\n  vd pg:backups %s\n  vd pg:backups:logs %s %s --follow",
			refOrName(scope, name), refOrName(scope, name), entry.Ref())

		result := dispatchOutput{
			Message: b.String(),
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
		// Same role label the Caminho B job spec carries — keeps
		// `docker ps --filter label=voodu.role=backup` consistent
		// across both code paths.
		"--label", "voodu.role=backup",
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

	// Apply retention AFTER capture. Follow path waits for the
	// new dump file to land — running prune after that lets the
	// policy include the just-completed capture in its rank
	// computation, so KeepLast=N really means "no more than N
	// files exist at any time after this command returns".
	prunedRefs := []string{}
	if !retention.IsZero() {
		deleted, perr := pruneBackupsByPolicy(scope, name, retention, time.Now())
		if perr != nil {
			fmt.Fprintf(os.Stderr, "warn: retention prune failed: %v\n", perr)
		}

		for _, e := range deleted {
			prunedRefs = append(prunedRefs, e.Ref())
		}

		if len(prunedRefs) > 0 {
			fmt.Fprintf(os.Stderr, "retention (%s): pruned %d backup(s): %s\n",
				retention, len(prunedRefs), strings.Join(prunedRefs, ", "))
		}
	}

	// Compose the success message in the same multi-line shape as
	// the detached path so operators see consistent output between
	// `--follow` and detached captures. Summary fields aren't used
	// here (no actions to label — the work already happened
	// foreground), so all the side effects ride the message body.
	var b strings.Builder

	fmt.Fprintf(&b, "postgres %s: backup %s captured (%s, %s, source: %s, elapsed %s)",
		refOrName(scope, name), entry.Ref(), filename, formatSize(size),
		formatSourceLabel(source, primaryOrdinal), dumpElapsed)

	if len(prunedRefs) > 0 {
		fmt.Fprintf(&b, "\nretention: %s, pruned %s",
			retention, strings.Join(prunedRefs, ", "))
	}

	result := dispatchOutput{
		Message: b.String(),
	}

	return writeDispatchOutput(result)
}

// formatSourceLabel renders the dump source as "primary" or
// "replica N" instead of the raw ordinal. Operators read it as
// role, not coordinate — the ordinal number doesn't say anything
// about whether they're hammering the writer or offloading to a
// standby. The plain "ordinal N" form was a debug crutch leaking
// into operator-facing output.
func formatSourceLabel(source, primaryOrdinal int) string {
	if source == primaryOrdinal {
		return "primary"
	}

	return fmt.Sprintf("replica %d", source)
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

// parseCaptureFlags extracts --from-replica, --follow, and the
// retention flags (--keep / --max-age / --retention). Retention
// parsing is delegated to parseRetentionFlags so the same flag set
// works on `:capture`, `:prune`, and the cronjob HCL emitted by
// `:schedule`.
func parseCaptureFlags(args []string) (positional []string, fromReplica int, hasFromReplica, follow bool, policy retentionPolicy, err error) {
	// Retention flags first — strips them out so the remaining
	// switch only sees the capture-specific ones.
	residual, parsedPolicy, perr := parseRetentionFlags(args)
	if perr != nil {
		return nil, 0, false, false, retentionPolicy{}, perr
	}

	policy = parsedPolicy

	for i := 0; i < len(residual); i++ {
		a := residual[i]

		switch {
		case a == "--from-replica":
			if i+1 >= len(residual) {
				return nil, 0, false, false, retentionPolicy{}, fmt.Errorf("--from-replica requires an integer argument")
			}

			n, perr := parseInt(residual[i+1])
			if perr != nil {
				return nil, 0, false, false, retentionPolicy{}, fmt.Errorf("--from-replica %q: %w", residual[i+1], perr)
			}

			fromReplica = n
			hasFromReplica = true
			i++

		case len(a) > 15 && a[:15] == "--from-replica=":
			n, perr := parseInt(a[15:])
			if perr != nil {
				return nil, 0, false, false, retentionPolicy{}, fmt.Errorf("--from-replica %q: %w", a[15:], perr)
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

	return positional, fromReplica, hasFromReplica, follow, policy, nil
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
  Resolves the file path on the controller host (the same
  /opt/voodu/backups/<scope>/<name>/<filename> the capture writes via
  the host bind-mount) and emits a fetch_file dispatch action. The
  operator-side CLI then transfers it:

    - SSH auto-forward in play   → scp from controller to operator
    - running on the controller  → cp local

  scp shows native byte-rate progress and has no size limit, so this
  works for multi-GB dumps the same as for a 906-byte test dump. SSH
  credentials reuse the same pair the auto-forward already trusts.

  Output is the verbatim pg_dump custom format, restorable via
  pg_restore on any postgres ≥ source major version. Works fine to
  ship a backup to another voodu host or to a local laptop.

EXAMPLES
  vd pg:backups:download clowk-lp/db b007
  # → ./b007-20260505T020000Z.dump in the operator's CWD

  vd pg:backups:download clowk-lp/db b007 --to /srv/archive/db.dump
  # → /srv/archive/db.dump
`

// cmdBackupsDownload looks up the backup file on the controller
// host's bind-mount and emits a fetch_file dispatch action telling
// the operator-side CLI where to find it. The CLI handles the
// actual transfer — scp when auto-forwarded over SSH (with native
// progress + no size limit), cp when running locally on the
// controller. The plugin itself never reads bytes.
//
// Why metadata-only: with auto-forward (Mac CLI → SSH → VM
// controller), a server-side `docker cp` writes to the VM, not
// the operator's machine. Earlier we tried base64-in-envelope
// streaming, but that capped at ~256 MiB and burned 3x RAM.
// Delegating to scp gives unlimited size + native progress and
// reuses the SSH credentials the auto-forward already has.
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

	// Read the backup catalog from the HOST bind-mount, not the
	// pod. listBackupsOnHost works whether or not a pod is
	// running — operators sometimes need to download backups
	// during teardown (the pod is gone but the volume remains).
	entries, err := listBackupsOnHost(scope, name)
	if err != nil {
		return fmt.Errorf("list backups: %w", err)
	}

	entry, err := resolveBackupByID(entries, id)
	if err != nil {
		return err
	}

	if dest == "" {
		dest = entry.Filename
	}

	hostPath := composeBackupHostPath(scope, name) + "/" + entry.Filename

	info, err := os.Stat(hostPath)
	if err != nil {
		return fmt.Errorf("stat %s: %w", hostPath, err)
	}

	result := dispatchOutput{
		Message: fmt.Sprintf("postgres %s: backup %s located (%s)",
			refOrName(scope, name), entry.Ref(), formatSize(info.Size())),
		Actions: []dispatchAction{
			{
				Type:       "fetch_file",
				Scope:      scope,
				Name:       name,
				RemotePath: hostPath,
				DestPath:   dest,
				SizeBytes:  info.Size(),
				// Operator-friendly checklist line — `b013 →
				// bkp/db2.dump` instead of the verbose default
				// (full host path + "deferred to CLI"). Same
				// info, scannable in one glance.
				Summary: fmt.Sprintf("fetch_file %s → %s", entry.Ref(), dest),
			},
		},
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

	// Look up the container by ID via the docker labels scan.
	// This handles BOTH naming conventions:
	//
	//   - Caminho A (--follow detached): container exact name
	//     `<scope>-<name>-backup-bNNN` (no runID suffix).
	//   - Caminho B (default detached, voodu jobs): container
	//     name `<scope>-<name>-backup-bNNN-<runID>` (voodu's
	//     containers.ContainerName appends runID for audit).
	//
	// composeBackupContainerName + containerExists would only
	// match Caminho A — Caminho B containers slip through.
	// listBackupContainers parses both forms via parseContainerNameForID.
	containers, err := listBackupContainers(scope, name)
	if err != nil {
		return err
	}

	var jobContainer string
	for _, c := range containers {
		if c.ID == n {
			jobContainer = c.Name
			break
		}
	}

	if jobContainer == "" {
		return fmt.Errorf("backup b%03d not found (auto-pruned, or capture never started)", n)
	}

	logsArgs := []string{"logs"}
	if follow {
		logsArgs = append(logsArgs, "-f")
	}

	logsArgs = append(logsArgs, jobContainer)

	cmd := exec.Command("docker", logsArgs...)

	// Merge docker's stderr into stdout. Failed pg_dump runs write
	// the error message (e.g. "FATAL: password authentication
	// failed") to the container's stderr — `docker logs` keeps that
	// stream split, and the plugin's stderr is captured separately
	// by the controller and only surfaced on plugin non-zero exit.
	// We exit 0 here (we DID find the container and ran logs),
	// so without merging the operator sees an empty response. Pin
	// this with TestBackupsLogsMergesStderrIntoStdout.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stdout

	if err := cmd.Run(); err != nil {
		// docker logs itself failing is rare — usually means the
		// container vanished between listBackupContainers and the
		// logs call. Surface the error rather than silently
		// returning empty.
		return fmt.Errorf("docker logs %s: %w", jobContainer, err)
	}

	return nil
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
// pg:backups:prune
// ─────────────────────────────────────────────────────────────────

const backupsPruneHelp = `vd pg:backups:prune — apply retention to a cluster's backups.

USAGE
  vd pg:backups:prune <postgres-scope/name> [--keep <N>] [--max-age <duration>]
                                            [--dry-run] [--yes]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.

FLAGS
  --keep <N>              Keep at most N most-recent backups. Older
                          ones get deleted. 0 / unset = no count cap.
  --max-age <duration>    Delete any backup older than the duration.
                          Accepts 7d, 2w, 168h, etc. 0 / unset = no
                          age cap. Alias: --retention.
  --dry-run               Print what would be pruned, then exit
                          without touching disk or docker.
  --yes, -y               Skip the destructive-action confirmation.
                          Required for non-interactive runs (cronjobs).

DESTRUCTIVE
  This deletes .dump files from disk AND removes the corresponding
  exited backup containers. There is no undelete.

WHAT IT DOES
  1. Reads the existing backup files from /opt/voodu/backups/<scope>/<name>.
  2. Sorts them newest-first by timestamp.
  3. Marks for deletion: anything past rank --keep, OR older than
     --max-age. Both gates apply — failing either one is enough to
     delete.
  4. Removes each .dump file and (best-effort) the matching exited
     backup container so 'vd get pd' stays clean.

EXAMPLES
  # Drop everything except the 30 newest
  vd pg:backups:prune clowk-lp/db --keep 30 --yes

  # Drop anything older than a week, no count cap
  vd pg:backups:prune clowk-lp/db --max-age 7d --yes

  # Both: trim to 30, then drop any of those past 7d
  vd pg:backups:prune clowk-lp/db --keep 30 --max-age 7d --yes

  # Preview without touching disk
  vd pg:backups:prune clowk-lp/db --keep 5 --dry-run

NOTES
  Auto-prune also runs after :capture when --keep / --max-age are
  passed there. Use this command for one-shots and for the cronjob
  you'd typically pair with :schedule.
`

// cmdBackupsPrune implements the manual retention command. Mirrors
// the auto-prune logic that runs after a capture, but exposed as
// its own subcommand so operators can clean up out of band (or
// schedule a periodic prune cronjob without an attached capture).
func cmdBackupsPrune() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(backupsPruneHelp)
		return nil
	}

	// Pull retention flags first; the residual carries positional
	// args + non-retention flags.
	residual, policy, err := parseRetentionFlags(args)
	if err != nil {
		return err
	}

	positional, autoYes := parseYesFlag(residual)

	// Strip --dry-run after yes-parsing so it lands in residual
	// regardless of position.
	dryRun := false

	cleaned := make([]string, 0, len(positional))

	for _, a := range positional {
		if a == "--dry-run" {
			dryRun = true
			continue
		}

		cleaned = append(cleaned, a)
	}

	positional = cleaned

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd pg:backups:prune <postgres-scope/name> [--keep <N>] [--max-age <duration>] [--dry-run] [--yes]")
	}

	scope, name := splitScopeName(positional[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", positional[0])
	}

	// Layer config-bucket defaults under the flag-derived policy
	// (same precedence as :capture). Lets the operator run `vd
	// pg:backups:prune <ref>` with no flags and have it pick up
	// BACKUP_KEEP / BACKUP_MAX_AGE from the bucket — fits the
	// "set once, run forever" use case for cron-driven cleanup.
	//
	// Best-effort fetch: if the controller is unreachable (e.g.
	// running locally on a node without controller_url), we keep
	// going with just the flag-policy. Empty policy after merge
	// is still rejected below with a helpful error.
	ctx, ctxErr := readInvocationContext()
	if ctxErr == nil && ctx.ControllerURL != "" {
		client := newControllerClient(ctx.ControllerURL)
		if config, cerr := client.fetchConfig(scope, name); cerr == nil {
			policy = mergeRetention(policy, loadRetentionFromConfig(config))
		}
	}

	if policy.IsZero() {
		return fmt.Errorf("--keep or --max-age required, or set BACKUP_KEEP / BACKUP_MAX_AGE in the config bucket (nothing to prune by)")
	}

	files, err := listBackupsOnHost(scope, name)
	if err != nil {
		return fmt.Errorf("list backups: %w", err)
	}

	now := time.Now()
	doomed := applyRetention(files, policy, now)

	if len(doomed) == 0 {
		fmt.Fprintf(os.Stderr, "postgres %s: nothing to prune (%d backups, policy: %s)\n",
			refOrName(scope, name), len(files), policy)

		return writeDispatchOutput(dispatchOutput{
			Message: fmt.Sprintf("postgres %s: nothing to prune (%d backups, policy: %s)",
				refOrName(scope, name), len(files), policy),
		})
	}

	// Print the list before destructive execution so dry-run AND
	// the live confirmation prompt see the same preview.
	refs := make([]string, 0, len(doomed))
	for _, e := range doomed {
		refs = append(refs, e.Ref())
	}

	fmt.Fprintf(os.Stderr, "postgres %s: %d backup(s) match policy %s: %s\n",
		refOrName(scope, name), len(doomed), policy, strings.Join(refs, ", "))

	if dryRun {
		return writeDispatchOutput(dispatchOutput{
			Message: fmt.Sprintf("postgres %s: dry-run — would prune %d backup(s) under policy %s: %s",
				refOrName(scope, name), len(doomed), policy, strings.Join(refs, ", ")),
		})
	}

	if !autoYes {
		fmt.Fprintf(os.Stderr,
			"\n⚠  This deletes %d backup file(s) from disk + their containers. No undelete. Use --yes (or -y) to confirm.\n\n",
			len(doomed))

		return fmt.Errorf("refusing to prune without --yes")
	}

	deleted, err := pruneBackupsByPolicy(scope, name, policy, now)
	if err != nil {
		return err
	}

	deletedRefs := make([]string, 0, len(deleted))
	for _, e := range deleted {
		deletedRefs = append(deletedRefs, e.Ref())
	}

	return writeDispatchOutput(dispatchOutput{
		Message: fmt.Sprintf("postgres %s: pruned %d backup(s) under policy %s: %s",
			refOrName(scope, name), len(deleted), policy, strings.Join(deletedRefs, ", ")),
	})
}

// ─────────────────────────────────────────────────────────────────
// pg:backups:schedule
// ─────────────────────────────────────────────────────────────────

const backupsScheduleHelp = `vd pg:backups:schedule — print a cronjob HCL block that captures backups on a schedule.

USAGE
  vd pg:backups:schedule <postgres-scope/name> [--at <cron-expr>] [--from-replica <N>]
                                               [--keep <N>] [--max-age <duration>]

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
  --keep <N>              Cap on count. Inlined into the emitted
                          capture command so the cronjob auto-prunes
                          on every run.
  --max-age <duration>    Cap on age (7d, 2w, 168h). Inlined the same
                          way. Alias: --retention.

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

  # Daily with retention (keep 30, drop anything > 14 days)
  vd pg:backups:schedule clowk-lp/db --keep 30 --max-age 14d
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

	positional, schedule, fromReplica, hasFromReplica, retention, err := parseScheduleFlags(args)
	if err != nil {
		return err
	}

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd pg:backups:schedule <postgres-scope/name> [--at <cron-expr>] [--from-replica <N>] [--keep <N>] [--max-age <duration>]")
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

	// Inline retention so the cronjob self-trims on every run. The
	// cronjob runs in voodu-cli's environment, so the same flags
	// the operator passed here are valid in the captured command.
	if retention.KeepLast > 0 {
		captureCmd += fmt.Sprintf(" --keep %d", retention.KeepLast)
	}

	if retention.MaxAge > 0 {
		captureCmd += fmt.Sprintf(" --max-age %s", formatDurationShort(retention.MaxAge))
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

func parseScheduleFlags(args []string) (positional []string, schedule string, fromReplica int, hasFromReplica bool, retention retentionPolicy, err error) {
	residual, retention, perr := parseRetentionFlags(args)
	if perr != nil {
		return nil, "", 0, false, retentionPolicy{}, perr
	}

	for i := 0; i < len(residual); i++ {
		a := residual[i]

		switch {
		case a == "--at":
			if i+1 >= len(residual) {
				return nil, "", 0, false, retentionPolicy{}, fmt.Errorf("--at requires a cron expression")
			}

			schedule = residual[i+1]
			i++

		case len(a) > 5 && a[:5] == "--at=":
			schedule = a[5:]

		case a == "--from-replica":
			if i+1 >= len(residual) {
				return nil, "", 0, false, retentionPolicy{}, fmt.Errorf("--from-replica requires an integer argument")
			}

			n, perr := parseInt(residual[i+1])
			if perr != nil {
				return nil, "", 0, false, retentionPolicy{}, fmt.Errorf("--from-replica %q: %w", residual[i+1], perr)
			}

			fromReplica = n
			hasFromReplica = true
			i++

		case len(a) > 15 && a[:15] == "--from-replica=":
			n, perr := parseInt(a[15:])
			if perr != nil {
				return nil, "", 0, false, retentionPolicy{}, fmt.Errorf("--from-replica %q: %w", a[15:], perr)
			}

			fromReplica = n
			hasFromReplica = true

		case a == "-h" || a == "--help":
			// handled

		default:
			positional = append(positional, a)
		}
	}

	return positional, schedule, fromReplica, hasFromReplica, retention, nil
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
