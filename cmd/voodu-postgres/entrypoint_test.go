// Tests for the bash entrypoint wrapper. Pure-function output —
// we assert on substrings rather than full bytes because the
// script is multi-line and small wording changes shouldn't break
// pinning. The contracts these tests pin:
//
//   - Wrapper is bash (not sh) so set -euo pipefail and other
//     bashisms are safe.
//   - PGDATA defaults to the subdirectory of the volume mount
//     (avoids the lost+found gotcha some volume drivers carry).
//   - include_dir wiring is idempotent (grep-then-append).
//   - Symlink target points at /etc/postgresql/voodu-overrides.conf
//     (the asset bind-mount path).
//   - Final exec hands off to docker-entrypoint.sh — we delegate
//     initdb / password setup / pg_hba bootstrap rather than
//     reimplementing.

package main

import (
	"strings"
	"testing"
)

func TestRenderEntrypointScript_StartsWithBashShebang(t *testing.T) {
	got := renderEntrypointScript()

	if !strings.HasPrefix(got, "#!/bin/bash\n") {
		t.Errorf("expected #!/bin/bash shebang, got first line: %q", firstLine(got))
	}
}

func TestRenderEntrypointScript_EnablesStrictMode(t *testing.T) {
	got := renderEntrypointScript()

	if !strings.Contains(got, "set -euo pipefail") {
		t.Errorf("expected `set -euo pipefail` for safety, got:\n%s", got)
	}
}

func TestRenderEntrypointScript_PGDATADefault(t *testing.T) {
	// PGDATA must default to the subdirectory of the mount
	// (/var/lib/postgresql/data/pgdata). The reasoning lives in
	// composeStatefulsetDefaults' env: postgres rejects starting
	// in a non-empty mountpoint that wasn't initdb'd, so the
	// subdir keeps lost+found etc. out of postgres's view.
	got := renderEntrypointScript()

	if !strings.Contains(got, `PGDATA="${PGDATA:-/var/lib/postgresql/data/pgdata}"`) {
		t.Errorf("PGDATA default lost or wrong path:\n%s", got)
	}
}

func TestRenderEntrypointScript_IdempotentIncludeDir(t *testing.T) {
	// grep -q before append so re-runs don't accumulate include_dir
	// lines. Matches voodu-redis's "always-rewrite-bootstrap +
	// preserve discovered state" pattern.
	got := renderEntrypointScript()

	if !strings.Contains(got, "grep -q \"^include_dir = 'conf.d'\"") {
		t.Errorf("missing idempotency guard around include_dir append:\n%s", got)
	}

	if !strings.Contains(got, "include_dir = 'conf.d'") {
		t.Errorf("missing the include_dir directive itself:\n%s", got)
	}
}

func TestRenderEntrypointScript_SymlinksAllVooduConfsViaGlob(t *testing.T) {
	// M-P2 generalised the symlink step: instead of one fixed
	// OVERRIDES_SRC, the wrapper loops over voodu-*.conf so any
	// number of plugin-emitted .conf files (M-P2 wal_archive,
	// future M-P3 streaming-replication, etc.) flow into
	// PGDATA/conf.d/ without touching the wrapper.
	//
	// nullglob is essential so an empty match doesn't iterate
	// over the literal pattern string and try to symlink a
	// non-existent file.
	got := renderEntrypointScript()

	if !strings.Contains(got, "for src in /etc/postgresql/voodu-*.conf") {
		t.Errorf("missing glob loop over voodu-*.conf:\n%s", got)
	}

	if !strings.Contains(got, "shopt -s nullglob") {
		t.Errorf("expected `shopt -s nullglob` so empty match is a no-op:\n%s", got)
	}

	if !strings.Contains(got, "ln -sf") {
		t.Errorf("expected `ln -sf` to keep symlinks fresh each boot:\n%s", got)
	}
}

func TestRenderEntrypointScript_RoleBranchOnOrdinal(t *testing.T) {
	// M-P3 added the primary/standby branch up top. Pinning the
	// shape so future edits don't accidentally drop the role
	// detection.
	got := renderEntrypointScript()

	wantSubstrs := []string{
		`ORDINAL="${VOODU_REPLICA_ORDINAL:-0}"`,
		`PRIMARY_ORDINAL="${PG_PRIMARY_ORDINAL:-0}"`,
		`if [ "$ORDINAL" != "$PRIMARY_ORDINAL" ]; then`,
		"role=STANDBY",
		"role=PRIMARY",
	}

	for _, want := range wantSubstrs {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in role branch:\n%s", want, got)
		}
	}
}

func TestRenderEntrypointScript_StandbyRunsBasebackupOnFirstBoot(t *testing.T) {
	// Standby first-boot path: wait for primary, run pg_basebackup,
	// touch standby.signal. Idempotent — skipped when PGDATA already
	// has PG_VERSION (subsequent boots).
	got := renderEntrypointScript()

	wantSubstrs := []string{
		`if [ ! -f "$PGDATA/PG_VERSION" ]; then`,
		"pg_isready -h",
		"pg_basebackup -h",
		"-X stream",
		"-D \"$PGDATA\"",
		`touch "$PGDATA/standby.signal"`,
	}

	for _, want := range wantSubstrs {
		if !strings.Contains(got, want) {
			t.Errorf("missing standby first-boot %q in:\n%s", want, got)
		}
	}
}

func TestRenderEntrypointScript_StandbyComposesPrimaryFQDNFromEnv(t *testing.T) {
	// Primary FQDN is composed at runtime from PG_NAME +
	// PG_SCOPE_SUFFIX env vars (set by composeStatefulsetDefaults)
	// rather than baked into the script — keeps the wrapper a pure
	// function (no scope/name in the bytes, asset digest stable
	// across resources).
	got := renderEntrypointScript()

	if !strings.Contains(got, `${PG_NAME:?PG_NAME is required}-${PRIMARY_ORDINAL}${PG_SCOPE_SUFFIX:-.voodu}`) {
		t.Errorf("primary FQDN composition wrong or missing:\n%s", got)
	}

	// Required env reads should fail loudly via :? expansion.
	wantFailFast := []string{
		"PG_REPLICATION_USER:?",
		"PG_REPLICATION_PASSWORD:?",
	}

	for _, want := range wantFailFast {
		if !strings.Contains(got, want) {
			t.Errorf("expected %q fail-fast env guard:\n%s", want, got)
		}
	}
}

func TestRenderEntrypointScript_HandsOffToDockerEntrypoint(t *testing.T) {
	// We delegate initdb / password setup / pg_hba bootstrap to
	// the official image's docker-entrypoint.sh — the wrapper's
	// last line must be `exec docker-entrypoint.sh postgres -p`.
	got := renderEntrypointScript()

	if !strings.Contains(got, `exec docker-entrypoint.sh postgres -p "$PG_PORT"`) {
		t.Errorf("missing final exec to docker-entrypoint.sh:\n%s", got)
	}
}

func TestRenderEntrypointScript_StandbyGuardAgainstSplitBrain(t *testing.T) {
	// M-P5 added a guard for the post-failover scenario: pod is
	// configured as standby (ORDINAL != PRIMARY_ORDINAL) but PGDATA
	// has no standby.signal — meaning it was last booted as primary.
	// Wrapper must refuse to start so postgres doesn't boot a second
	// primary in parallel with the new one (split-brain).
	got := renderEntrypointScript()

	// Guard structure: elif checks for missing standby.signal AFTER
	// the first-boot branch (so first-boot doesn't trip the guard).
	if !strings.Contains(got, `elif [ ! -f "$PGDATA/standby.signal" ]; then`) {
		t.Errorf("missing split-brain guard structure:\n%s", got)
	}

	// Must point operator at the recovery command.
	if !strings.Contains(got, "vd postgres:rejoin") {
		t.Errorf("guard error must mention vd postgres:rejoin recovery command:\n%s", got)
	}

	// Must exit non-zero so docker doesn't keep restarting the
	// pod into the same broken state.
	if !strings.Contains(got, "exit 1") {
		t.Errorf("guard must exit 1 to avoid an endless restart loop:\n%s", got)
	}
}

func TestRenderEntrypointScript_Deterministic(t *testing.T) {
	// Pure function: same call must yield byte-identical output.
	// Stability matters for asset digests — if the bytes flap
	// between expand calls, voodu apply triggers a spurious
	// rolling restart on every replay.
	a := renderEntrypointScript()
	b := renderEntrypointScript()

	if a != b {
		t.Error("renderEntrypointScript is not deterministic")
	}
}

// firstLine returns the first line of s, sans newline. Helper for
// shebang assertions.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}

	return s
}
