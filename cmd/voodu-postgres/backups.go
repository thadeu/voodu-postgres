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

OUTPUT (text)
  ID    Created at            Size
  b001  2026-05-04T02:00:00Z  245.3 MB
  b002  2026-05-05T02:00:00Z  248.1 MB

EXAMPLES
  vd pg:backups clowk-lp/db
  vd pg:backups clowk-lp/db -o json | jq '.[] | .id'
`

// cmdBackups lists existing backups for a postgres resource.
// Default verb when operator runs `vd pg:backups <ref>` (no
// sub-action specified).
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

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	if ctx.ControllerURL == "" {
		return fmt.Errorf("listing backups requires controller_url (needs primary ordinal from config bucket)")
	}

	client := newControllerClient(ctx.ControllerURL)

	config, err := client.fetchConfig(scope, name)
	if err != nil {
		return fmt.Errorf("config get %s: %w", refOrName(scope, name), err)
	}

	primaryOrdinal := readCurrentPrimaryOrdinal(config)
	primaryContainer := containerNameFor(scope, name, primaryOrdinal)

	if !containerExists(primaryContainer) {
		return fmt.Errorf("container %s not found on this host (listing must run on the same host)", primaryContainer)
	}

	running, err := containerIsRunning(primaryContainer)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", primaryContainer, err)
	}

	if !running {
		return fmt.Errorf("container %s is not running (need a live pod to list /backups)", primaryContainer)
	}

	entries, err := listBackupsInPod(primaryContainer)
	if err != nil {
		return err
	}

	if asJSON {
		return emitBackupsJSON(scope, name, entries)
	}

	emitBackupsText(scope, name, entries)

	return nil
}

// emitBackupsJSON writes the backup list as a JSON array of objects.
// Fields are stable (operator-facing) — id, created_at, size_bytes.
func emitBackupsJSON(scope, name string, entries []backupEntry) error {
	type row struct {
		ID        string `json:"id"`
		Filename  string `json:"filename"`
		CreatedAt string `json:"created_at"`
		SizeBytes int64  `json:"size_bytes"`
	}

	rows := make([]row, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, row{
			ID:        e.Ref(),
			Filename:  e.Filename,
			CreatedAt: e.Timestamp.UTC().Format(time.RFC3339),
			SizeBytes: e.SizeBytes,
		})
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	return enc.Encode(map[string]any{
		"ref":     refOrName(scope, name),
		"backups": rows,
	})
}

// emitBackupsText prints the backup list as a human-readable table.
// Empty list shows a "no backups yet" hint pointing at the capture
// command.
func emitBackupsText(scope, name string, entries []backupEntry) {
	fmt.Printf("=== Backups for %s\n", refOrName(scope, name))

	if len(entries) == 0 {
		fmt.Println()
		fmt.Println("  (no backups yet)")
		fmt.Printf("  → vd pg:backups:capture %s\n", refOrName(scope, name))
		fmt.Println()
		return
	}

	fmt.Println()
	fmt.Println("  ID     Created at            Size")

	for _, e := range entries {
		fmt.Printf("  %-6s %s  %s\n",
			e.Ref(),
			e.Timestamp.UTC().Format(time.RFC3339),
			formatSize(e.SizeBytes),
		)
	}

	fmt.Println()
}

const backupsCaptureHelp = `vd pg:backups:capture — take a pg_dump snapshot of the cluster.

USAGE
  vd pg:backups:capture <postgres-scope/name> [--from-replica <N>]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.

FLAGS
  --from-replica <N>      Run pg_dump against ordinal N instead of
                          the current primary. Useful to offload the
                          dump load — standbys are read-only replicas,
                          no write contention. Optional; defaults to
                          the primary (PG_PRIMARY_ORDINAL).

WHAT IT DOES
  1. Resolves the source container (primary by default).
  2. Reads /backups/ to find the next sequence ID.
  3. Runs pg_dump custom format (-F c) inside the pod, writing to
     /backups/bNNN-YYYYMMDDTHHMMSSZ.dump.
  4. Reports the new backup's ID, filename, and size.

OUTPUT FORMAT
  pg_dump custom format. Restore via pg_restore (compatible with
  any postgres ≥ source major version, portable across hosts).

EXAMPLES
  # Capture from primary
  vd pg:backups:capture clowk-lp/db

  # Capture from a standby (offloads work)
  vd pg:backups:capture clowk-lp/db --from-replica 1
`

// cmdBackupsCapture takes a fresh pg_dump snapshot of the target
// cluster and writes it to /backups/.
func cmdBackupsCapture() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(backupsCaptureHelp)
		return nil
	}

	positional, fromReplica, hasFromReplica, err := parseCaptureFlags(args)
	if err != nil {
		return err
	}

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd pg:backups:capture <postgres-scope/name> [--from-replica <N>]")
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

	user, db, _ := readUserDBPort(spec)

	// Compute next ID by inspecting existing files in /backups/.
	existing, err := listBackupsInPod(sourceContainer)
	if err != nil {
		return fmt.Errorf("read existing backups: %w", err)
	}

	filenames := make([]string, 0, len(existing))
	for _, e := range existing {
		filenames = append(filenames, e.Filename)
	}

	id := nextBackupID(filenames)
	now := time.Now().UTC()
	filename := formatBackupFilename(id, now)
	containerFilePath := backupsContainerPath + "/" + filename

	// Pre-flight: query raw db size to size up the progress
	// reporter's estimate. Best-effort — if psql/parsing fails
	// we proceed without an estimate (progress lines drop the
	// "% of estimate" suffix; bytes-written + elapsed still show).
	rawDBBytes, _ := queryDBSize(sourceContainer, user, db)
	estDumpBytes := int64(float64(rawDBBytes) * dumpCompressionFactor)

	if rawDBBytes > 0 {
		fmt.Fprintf(os.Stderr, "capture: pg_dump in %s → %s (db=%s, user=%s; raw size %s, estimated dump ~%s)\n",
			sourceContainer, containerFilePath, db, user,
			formatSize(rawDBBytes), formatSize(estDumpBytes))
	} else {
		fmt.Fprintf(os.Stderr, "capture: pg_dump in %s → %s (db=%s, user=%s)\n",
			sourceContainer, containerFilePath, db, user)
	}

	// Spawn rolling progress reporter. Goroutine ticks every ~3s
	// while pg_dump runs; closes cleanly via the done channel.
	done := make(chan struct{})
	go reportCaptureProgress(sourceContainer, containerFilePath, estDumpBytes, done)

	// Run pg_dump as the postgres user via Unix socket — no
	// password handling needed (peer auth on the local socket).
	// -F c = custom binary format (restorable via pg_restore).
	// -Z 6 = compression level 6 (default 1 in custom format;
	// bumping for storage savings — CPU cost is negligible on
	// idle hardware and we run on a standby anyway when -- from -replica
	// is used).
	dumpStart := time.Now()
	dumpCmd := exec.Command(
		"docker", "exec", "-u", "postgres", sourceContainer,
		"pg_dump",
		"-U", user,
		"-d", db,
		"-F", "c",
		"-Z", "6",
		"-f", containerFilePath,
	)
	dumpCmd.Stdout = os.Stderr
	dumpCmd.Stderr = os.Stderr

	dumpErr := dumpCmd.Run()

	// Stop the progress reporter regardless of success/failure.
	close(done)

	if dumpErr != nil {
		// Best-effort cleanup of any partial file so a retry
		// gets a fresh sequence number on the next call.
		_ = exec.Command("docker", "exec", sourceContainer, "rm", "-f", containerFilePath).Run()

		return fmt.Errorf("pg_dump failed: %w (partial file removed)", dumpErr)
	}

	dumpElapsed := time.Since(dumpStart).Round(time.Second)

	// Stat the result so we can report size.
	statOut, err := exec.Command("docker", "exec", sourceContainer,
		"stat", "-c", "%s", containerFilePath).Output()
	if err != nil {
		return fmt.Errorf("stat %s: %w", containerFilePath, err)
	}

	size, _ := strconv.ParseInt(strings.TrimSpace(string(statOut)), 10, 64)

	entry := backupEntry{ID: id, Filename: filename, Timestamp: now, SizeBytes: size}

	fmt.Fprintf(os.Stderr, "done: %s in %s\n", formatSize(size), dumpElapsed)

	result := dispatchOutput{
		Message: fmt.Sprintf(
			"postgres %s: backup %s captured (%s, %s, source: ordinal %d, elapsed %s)",
			refOrName(scope, name), entry.Ref(), filename, formatSize(size), source, dumpElapsed),
		Actions: nil,
	}

	return writeDispatchOutput(result)
}

// parseCaptureFlags extracts --from-replica.
func parseCaptureFlags(args []string) (positional []string, fromReplica int, hasFromReplica bool, err error) {
	for i := 0; i < len(args); i++ {
		a := args[i]

		switch {
		case a == "--from-replica":
			if i+1 >= len(args) {
				return nil, 0, false, fmt.Errorf("--from-replica requires an integer argument")
			}

			n, perr := parseInt(args[i+1])
			if perr != nil {
				return nil, 0, false, fmt.Errorf("--from-replica %q: %w", args[i+1], perr)
			}

			fromReplica = n
			hasFromReplica = true
			i++

		case len(a) > 15 && a[:15] == "--from-replica=":
			n, perr := parseInt(a[15:])
			if perr != nil {
				return nil, 0, false, fmt.Errorf("--from-replica %q: %w", a[15:], perr)
			}

			fromReplica = n
			hasFromReplica = true

		case a == "-h" || a == "--help":
			// handled

		default:
			positional = append(positional, a)
		}
	}

	return positional, fromReplica, hasFromReplica, nil
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
  rm /backups/<filename> inside the pod. Bind-mounted to the host —
  the backup file disappears from disk. Operator's responsibility to
  ensure no off-host sync is mid-flight (see Backup automation).

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

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	if ctx.ControllerURL == "" {
		return fmt.Errorf("delete requires controller_url (needs primary ordinal from config bucket)")
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
		return fmt.Errorf("container %s is not running (need a live pod to delete from)", primaryContainer)
	}

	entries, err := listBackupsInPod(primaryContainer)
	if err != nil {
		return err
	}

	entry, err := resolveBackupByID(entries, id)
	if err != nil {
		return err
	}

	containerFilePath := backupsContainerPath + "/" + entry.Filename

	fmt.Fprintf(os.Stderr, "delete: rm %s on %s\n", containerFilePath, primaryContainer)

	cmd := exec.Command("docker", "exec", primaryContainer, "rm", "-f", containerFilePath)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("rm %s: %w", containerFilePath, err)
	}

	result := dispatchOutput{
		Message: fmt.Sprintf("postgres %s: backup %s (%s) deleted",
			refOrName(scope, name), entry.Ref(), entry.Filename),
		Actions: nil,
	}

	return writeDispatchOutput(result)
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

const backupsCancelHelp = `vd pg:backups:cancel — cancel any pg_dump or pg_restore in progress.

USAGE
  vd pg:backups:cancel <postgres-scope/name>

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.

WHAT IT DOES
  Queries pg_stat_activity for connections whose application_name is
  'pg_dump' or 'pg_restore', and runs pg_terminate_backend(pid) on
  each. The terminated process exits cleanly; partial backup files
  in /backups/ are left in place (a retry is the operator's call —
  see vd pg:backups:delete to clean them up).

  pg_terminate_backend is non-destructive of the cluster — it just
  closes the TCP connection of the named process. No PGDATA touch,
  no replication impact.

WHEN TO USE
  - A capture is taking too long and you want to free up the source pod.
  - A restore needs to be aborted before it finishes drop+recreating
    objects (NOTE: partial restore leaves the database in an
    inconsistent state — operator must follow up with a fresh restore
    from a different backup).

EXAMPLES
  vd pg:backups:cancel clowk-lp/db
`

// cmdBackupsCancel terminates any in-progress pg_dump or
// pg_restore against the cluster. Uses pg_terminate_backend() so
// postgres handles the cleanup; no PID files, no signal plumbing.
func cmdBackupsCancel() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(backupsCancelHelp)
		return nil
	}

	if len(args) < 1 || args[0] == "-h" || args[0] == "--help" {
		return fmt.Errorf("usage: vd pg:backups:cancel <postgres-scope/name>")
	}

	scope, name := splitScopeName(args[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", args[0])
	}

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	if ctx.ControllerURL == "" {
		return fmt.Errorf("cancel requires controller_url (needs primary ordinal + db/user from spec)")
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
		return fmt.Errorf("container %s is not running", primaryContainer)
	}

	user, db, _ := readUserDBPort(spec)

	// Query + terminate in one round trip. RETURNING surfaces what
	// we killed so the operator gets a clear report — vs running
	// blind and saying "0 or more processes terminated."
	//
	// The IN clause covers both directions: pg_dump (capture) and
	// pg_restore (restore). pg_dump uses pid != pg_backend_pid()
	// guard implicitly because it runs as a separate connection;
	// we add it explicitly to avoid self-termination if the
	// operator ever runs this from inside an active session.
	sql := `SELECT pid, application_name, state, query_start
            FROM pg_stat_activity
            WHERE application_name IN ('pg_dump', 'pg_restore')
              AND pid != pg_backend_pid();
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE application_name IN ('pg_dump', 'pg_restore')
              AND pid != pg_backend_pid();`

	cmd := exec.Command(
		"docker", "exec", "-u", "postgres", primaryContainer,
		"psql", "-U", user, "-d", db,
		"-X", "-A", "-t",
		"-c", sql,
	)

	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("psql cancel: %w", err)
	}

	// Output is two result sets concatenated. Count non-empty
	// lines that look like termination results (`t` for true) to
	// report a number to the operator.
	terminated := 0
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == "t" {
			terminated++
		}
	}

	var msg string
	if terminated == 0 {
		msg = fmt.Sprintf("postgres %s: no pg_dump or pg_restore in progress",
			refOrName(scope, name))
	} else {
		msg = fmt.Sprintf("postgres %s: terminated %d backup-related connection(s)",
			refOrName(scope, name), terminated)
	}

	result := dispatchOutput{Message: msg}

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
