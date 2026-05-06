package main

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config bucket keys for retention defaults. Set on first apply
// via cmdExpand and overridable by `vd config <ref> set` afterwards.
// Operators who want NO auto-prune can `vd config <ref> set
// BACKUP_KEEP=0` (or leave the key empty) — IsZero short-circuits.
//
// Values mirror the CLI flag accepted shapes:
//
//	BACKUP_KEEP     decimal integer (e.g. "30"). 0 / "" = no count cap.
//	BACKUP_MAX_AGE  duration (e.g. "7d", "2w", "168h"). "" = no age cap.
const (
	backupKeepKey   = "BACKUP_KEEP"
	backupMaxAgeKey = "BACKUP_MAX_AGE"
)

// defaultBackupKeep is the value seeded into the config bucket on
// first apply. 30 backups is conservative for single-host dev/test
// HDs (the user's stated cap is 80GB) — even at 100MB/dump that's
// 3GB. Operators with larger DBs should `vd config <ref> set
// BACKUP_KEEP=...` to a smaller value.
//
// Only seeded on isNew (first apply); subsequent applies leave the
// existing value untouched so an operator who sets BACKUP_KEEP=0
// (disable) doesn't get the default re-applied behind their back.
const defaultBackupKeep = "30"

// retentionPolicy controls how many backups to keep on disk for a
// given (scope, name). Both axes default to "no limit" (zero) — a
// fully-empty policy is a no-op so capture doesn't accidentally start
// trimming when the operator forgot to opt in.
//
//   - KeepLast: cap on count. Newest N backups are always kept;
//     older ones get pruned regardless of age. 0 = no count cap.
//   - MaxAge:   cap on age. Anything older than now-MaxAge gets
//     pruned regardless of count. 0 = no age cap.
//
// The two gates are AND-merged in applyRetention: a backup is
// deleted if it fails EITHER check (rank > KeepLast OR age > MaxAge).
// "Fails either" is the strict interpretation operators expect when
// they configure a budget — if I say "keep 30 OR keep 7 days",
// either alone is enough to justify a delete. The conservative AND
// "delete only when BOTH fail" leaves orphans the operator was
// explicitly trying to drop.
//
// Empty policy (KeepLast=0, MaxAge=0) returns no deletions —
// applyRetention is a pure no-op so callers can pass it
// unconditionally without an "if policy.IsZero()" guard.
type retentionPolicy struct {
	KeepLast int
	MaxAge   time.Duration
}

// IsZero reports whether the policy is the empty / no-op state.
// Callers can use it to skip the prune step entirely (e.g. don't
// even print "0 pruned" lines when nothing was configured).
func (p retentionPolicy) IsZero() bool {
	return p.KeepLast <= 0 && p.MaxAge <= 0
}

// String renders the policy for log lines / dispatch summaries —
// e.g. "keep 30, max-age 7d" or "max-age 14d". Only non-zero axes
// appear; an empty policy renders as "(none)".
func (p retentionPolicy) String() string {
	parts := []string{}

	if p.KeepLast > 0 {
		parts = append(parts, fmt.Sprintf("keep %d", p.KeepLast))
	}

	if p.MaxAge > 0 {
		parts = append(parts, fmt.Sprintf("max-age %s", formatDurationShort(p.MaxAge)))
	}

	if len(parts) == 0 {
		return "(none)"
	}

	return strings.Join(parts, ", ")
}

// applyRetention computes which backups would be deleted under the
// given policy, holding `now` injectable for testability. The input
// slice is NOT mutated — caller can keep using it afterwards.
//
// Algorithm:
//  1. Sort entries newest-first by Timestamp.
//  2. For each entry at rank i (0 = newest):
//     - If KeepLast > 0 AND i >= KeepLast → mark for deletion.
//     - If MaxAge > 0 AND now-Timestamp > MaxAge → mark for deletion.
//     - Otherwise keep.
//  3. Return the list of entries marked for deletion (in newest-first
//     order, matching the iteration). Caller is free to re-sort.
//
// Timestamps come from parseBackupFilename's UTC parsing — applying
// retention with `now` in any other timezone still works because
// `now.Sub(e.Timestamp)` is timezone-agnostic.
func applyRetention(entries []backupEntry, policy retentionPolicy, now time.Time) []backupEntry {
	if policy.IsZero() || len(entries) == 0 {
		return nil
	}

	// Sort newest-first. Don't mutate the input slice.
	sorted := make([]backupEntry, len(entries))
	copy(sorted, entries)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.After(sorted[j].Timestamp)
	})

	var toDelete []backupEntry

	for i, e := range sorted {
		drop := false

		if policy.KeepLast > 0 && i >= policy.KeepLast {
			drop = true
		}

		if !drop && policy.MaxAge > 0 && now.Sub(e.Timestamp) > policy.MaxAge {
			drop = true
		}

		if drop {
			toDelete = append(toDelete, e)
		}
	}

	return toDelete
}

// parseRetentionFlags pulls --keep, --max-age, and --retention out
// of args and returns the residual positional args plus the parsed
// policy. Flags are accepted in `--flag value` and `--flag=value`
// shapes consistent with the rest of the CLI.
//
// `--retention` is an alias for `--max-age`. Operators tend to
// use one or the other; both spellings work.
//
// MaxAge accepts Go duration strings (1h, 24h, 168h) AND a couple
// of human shortcuts:
//
//	30d → 30 * 24h
//	2w  → 2  *  7 * 24h
//
// More elaborate parsing (months, years) is intentionally absent —
// for backups the unit is days/weeks; longer than that you should
// be using offsite cold storage anyway.
func parseRetentionFlags(args []string) (positional []string, policy retentionPolicy, err error) {
	for i := 0; i < len(args); i++ {
		a := args[i]

		switch {
		case a == "--keep":
			if i+1 >= len(args) {
				return nil, retentionPolicy{}, fmt.Errorf("--keep requires an integer")
			}

			n, perr := parseInt(args[i+1])
			if perr != nil {
				return nil, retentionPolicy{}, fmt.Errorf("--keep %q: %w", args[i+1], perr)
			}

			if n < 0 {
				return nil, retentionPolicy{}, fmt.Errorf("--keep %d: must be >= 0", n)
			}

			policy.KeepLast = n
			i++

		case strings.HasPrefix(a, "--keep="):
			n, perr := parseInt(a[len("--keep="):])
			if perr != nil {
				return nil, retentionPolicy{}, fmt.Errorf("--keep %q: %w", a[len("--keep="):], perr)
			}

			if n < 0 {
				return nil, retentionPolicy{}, fmt.Errorf("--keep %d: must be >= 0", n)
			}

			policy.KeepLast = n

		case a == "--max-age", a == "--retention":
			if i+1 >= len(args) {
				return nil, retentionPolicy{}, fmt.Errorf("%s requires a duration (e.g. 7d, 168h)", a)
			}

			d, perr := parseRetentionDuration(args[i+1])
			if perr != nil {
				return nil, retentionPolicy{}, fmt.Errorf("%s %q: %w", a, args[i+1], perr)
			}

			policy.MaxAge = d
			i++

		case strings.HasPrefix(a, "--max-age="):
			d, perr := parseRetentionDuration(a[len("--max-age="):])
			if perr != nil {
				return nil, retentionPolicy{}, fmt.Errorf("--max-age %q: %w", a[len("--max-age="):], perr)
			}

			policy.MaxAge = d

		case strings.HasPrefix(a, "--retention="):
			d, perr := parseRetentionDuration(a[len("--retention="):])
			if perr != nil {
				return nil, retentionPolicy{}, fmt.Errorf("--retention %q: %w", a[len("--retention="):], perr)
			}

			policy.MaxAge = d

		default:
			positional = append(positional, a)
		}
	}

	return positional, policy, nil
}

// parseRetentionDuration extends time.ParseDuration with `Nd` (days)
// and `Nw` (weeks) since neither is in the standard library and
// both are by far the most natural units for "how long to keep
// backups around".
func parseRetentionDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	// Try the human shortcuts first. Single suffix only — `7d12h`
	// would need a real parser, which is overkill here.
	if strings.HasSuffix(s, "d") {
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil {
			return 0, fmt.Errorf("invalid days value %q", s)
		}

		if n < 0 {
			return 0, fmt.Errorf("duration must be >= 0")
		}

		return time.Duration(n) * 24 * time.Hour, nil
	}

	if strings.HasSuffix(s, "w") {
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil {
			return 0, fmt.Errorf("invalid weeks value %q", s)
		}

		if n < 0 {
			return 0, fmt.Errorf("duration must be >= 0")
		}

		return time.Duration(n) * 7 * 24 * time.Hour, nil
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("not a duration (try 7d, 2w, or Go duration like 168h): %w", err)
	}

	if d < 0 {
		return 0, fmt.Errorf("duration must be >= 0")
	}

	return d, nil
}

// formatDurationShort renders a duration back in the same shorthand
// parseRetentionDuration accepts when it's a clean multiple of a
// day or hour. Falls back to time.Duration.String() otherwise.
//
// Used for log lines (`pruned 3 backups (max-age 7d)`) so the
// operator sees the same shape they typed.
func formatDurationShort(d time.Duration) string {
	if d <= 0 {
		return "0"
	}

	day := 24 * time.Hour
	week := 7 * day

	if d%week == 0 {
		return fmt.Sprintf("%dw", d/week)
	}

	if d%day == 0 {
		return fmt.Sprintf("%dd", d/day)
	}

	return d.String()
}

// pruneBackupsByPolicy applies the retention policy to a (scope,
// name) pair: deletes the dump files on disk AND the corresponding
// exited backup containers (via docker rm). Running containers are
// always skipped — never reap an in-flight capture.
//
// Returns the deleted backup entries (caller decides how to render).
//
// Best-effort on the container side: an error removing a container
// is logged to stderr but doesn't fail the prune. The .dump file
// is the canonical artefact; the container is metadata.
func pruneBackupsByPolicy(scope, name string, policy retentionPolicy, now time.Time) ([]backupEntry, error) {
	if policy.IsZero() {
		return nil, nil
	}

	files, err := listBackupsOnHost(scope, name)
	if err != nil {
		return nil, fmt.Errorf("list backups: %w", err)
	}

	doomed := applyRetention(files, policy, now)
	if len(doomed) == 0 {
		return nil, nil
	}

	hostDir := composeBackupHostPath(scope, name)

	// Index running/exited containers once so we can wipe the
	// container alongside the file in a single docker round-trip
	// per pruned ID.
	containers, err := listBackupContainers(scope, name)
	if err != nil {
		// Continue with file-only prune — container cleanup is
		// best-effort. Surface the warn so operator knows.
		fmt.Fprintf(os.Stderr, "warn: list backup containers: %v\n", err)

		containers = nil
	}

	containersByID := map[int]string{}

	for _, c := range containers {
		// Don't reap running. If a backup we want to delete is
		// somehow still capturing, leave it alone — the file
		// is still being written.
		if c.State == "running" || c.State == "created" {
			continue
		}

		containersByID[c.ID] = c.Name
	}

	var deleted []backupEntry

	for _, e := range doomed {
		// File first. If we delete the container but the file
		// remains, retention silently fails to free space.
		path := hostDir + "/" + e.Filename
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "warn: rm %s: %v\n", path, err)
			continue
		}

		if cName, ok := containersByID[e.ID]; ok {
			if err := dockerRm(cName); err != nil {
				fmt.Fprintf(os.Stderr, "warn: docker rm %s: %v\n", cName, err)
			}
		}

		deleted = append(deleted, e)
	}

	return deleted, nil
}

// loadRetentionFromConfig reads BACKUP_KEEP / BACKUP_MAX_AGE out of
// the postgres config bucket. Missing or malformed keys are tolerated
// — the corresponding axis stays zero (no-op) and a single warn line
// goes to stderr so operators with a typo'd value see why their
// retention isn't taking effect.
//
// Used as the FALLBACK behind the CLI flags: capture/prune mergeWith
// the flag-derived policy via mergeRetention, where the flag wins
// per-axis. So an operator can set BACKUP_KEEP=30 in config and pass
// `--keep 5` for a one-off tighter run without disturbing the
// persistent default.
func loadRetentionFromConfig(config map[string]string) retentionPolicy {
	var p retentionPolicy

	if raw := strings.TrimSpace(config[backupKeepKey]); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			fmt.Fprintf(os.Stderr, "warn: ignoring %s=%q in config bucket: not a non-negative integer\n",
				backupKeepKey, raw)
		} else {
			p.KeepLast = n
		}
	}

	if raw := strings.TrimSpace(config[backupMaxAgeKey]); raw != "" {
		d, err := parseRetentionDuration(raw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: ignoring %s=%q in config bucket: %v\n",
				backupMaxAgeKey, raw, err)
		} else {
			p.MaxAge = d
		}
	}

	return p
}

// mergeRetention layers flags on top of config — per-axis, flag
// wins when set (>0). This is the precedence the operator expects
// from the docs:
//
//	1. CLI flag (--keep N / --max-age D)   (highest)
//	2. Config bucket (BACKUP_KEEP / BACKUP_MAX_AGE)
//	3. Plugin built-in (currently nothing — empty policy)
//
// Per-axis merge means `--keep 5` alone (no --max-age) doesn't
// blow away a BACKUP_MAX_AGE that's still configured in the bucket;
// only the keep axis gets overridden.
func mergeRetention(flagsPolicy, configPolicy retentionPolicy) retentionPolicy {
	merged := configPolicy

	if flagsPolicy.KeepLast > 0 {
		merged.KeepLast = flagsPolicy.KeepLast
	}

	if flagsPolicy.MaxAge > 0 {
		merged.MaxAge = flagsPolicy.MaxAge
	}

	return merged
}

// dockerRm runs `docker rm <name>`. Pulled into a helper so the
// retention path doesn't reach into exec.Command directly; keeps
// the prune flow easy to read.
func dockerRm(name string) error {
	cmd := exec.Command("docker", "rm", name)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s (output: %s)", err, strings.TrimSpace(string(out)))
	}

	return nil
}
