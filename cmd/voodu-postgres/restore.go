// `vd postgres:restore` — restore a cluster from a pg_basebackup
// snapshot, with optional point-in-time-recovery via WAL replay.
//
// # Workflow
//
//   1. Stop EVERY pod in the cluster (primary + standbys). Restore
//      is destructive of PGDATA; can't have postgres running.
//   2. Wipe primary's PGDATA volume (ordinal-0 by default; or
//      whichever ordinal currently holds the primary role).
//   3. Extract the backup tar into PGDATA via a one-shot container
//      sharing the volume:
//
//        docker run --rm --volumes-from <primary> postgres \
//          tar -xf /backup/db.tar -C $PGDATA
//
//   4. (PITR) Drop a recovery.signal file + write recovery_target_time
//      into postgresql.auto.conf. Postgres replays archived WAL until
//      the target time, then promotes itself to primary.
//   5. Start primary container. It boots, replays WAL (post-basebackup
//      crash recovery + PITR if applicable), accepts connections.
//   6. Wipe + bootstrap each standby's PGDATA from scratch — the
//      wrapper's first-boot path runs pg_basebackup against the new
//      primary and gets a fresh, correctly-aligned copy.
//
// # Why we wipe + recreate standbys instead of restoring them too
//
// Standbys must be EXACT byte-level copies of the primary (same
// timeline, same LSN). Extracting the same tar on every pod
// gets close, but the post-restore primary will have run WAL
// replay (PITR or even just startup recovery) — its new state
// diverges from the tar slightly. Cleaner to let the standbys
// pg_basebackup fresh from the just-restored primary.
//
// # Why this is destructive
//
// We `docker stop` + `wipe PGDATA` BEFORE extracting. There's no
// rollback if the operator pointed at the wrong backup file: the
// cluster's data is already gone. Hence the explicit `--yes` /
// `-y` requirement (we refuse to run destructively without it
// unless stdin is non-interactive — for cronjob automation).

package main

import (
	"fmt"
	"os"
	"os/exec"
)

const restoreHelp = `vd postgres:restore — restore a cluster from a pg_basebackup snapshot.

USAGE
  vd postgres:restore <postgres-scope/name> --from <path> [--target-time "<timestamp>"] [--yes]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.

FLAGS
  --from <path>           Backup tar file (the output of
                          vd postgres:backup). Required. Path is on
                          the docker host.
  --target-time "<ts>"    Point-in-time recovery target. Postgres
                          replays archived WAL until this timestamp.
                          Format: "YYYY-MM-DD HH:MM:SS[+TZ]". Optional;
                          omit for "restore latest from this backup +
                          replay all available WAL".
  --yes, -y               Skip the destructive-action confirmation
                          prompt. Required when stdin is non-interactive
                          (cronjobs etc.).

DESTRUCTIVE
  Wipes the cluster's PGDATA + every standby's data dir. There is
  NO rollback: get the wrong backup file and your data is gone.

  Take a fresh backup of the current state BEFORE running restore
  if you need a fallback:
    vd postgres:backup <ref> --destination /tmp/before-restore-$(date +%Y%m%d).tar

WORKFLOW
  1. Stop all pods (primary + standbys).
  2. Wipe primary's PGDATA.
  3. Extract <backup-path> into primary's PGDATA via one-shot
     docker run --volumes-from.
  4. (--target-time) Write recovery_target_time into
     postgresql.auto.conf and drop recovery.signal.
  5. Start primary; postgres replays WAL, promotes when caught up.
  6. Wipe each standby's PGDATA. The wrapper's first-boot path
     bootstraps each via pg_basebackup against the new primary on
     next start.

EXAMPLES
  # Latest restore (no PITR)
  vd postgres:restore clowk-lp/db --from /srv/backups/db-20260504.tar --yes

  # Point-in-time recovery to just before a bad migration
  vd postgres:restore clowk-lp/db \
    --from /srv/backups/db-20260504.tar \
    --target-time "2026-05-04 14:30:00 UTC" \
    --yes

PRE-FLIGHT
  - The backup file must exist + be readable on this host.
  - The cluster must be applied (manifest in the controller). If
    starting from scratch, vd apply -f voodu.hcl FIRST, let it
    run for a few seconds, then restore.
`

// cmdRestore restores a cluster from a backup tar.
func cmdRestore() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		// os.Stdout.WriteString avoids vet's "% directive in
		// fmt.Print" false-positive on the help text (which
		// embeds `date +%Y%m%d` in a shell example).
		_, _ = os.Stdout.WriteString(restoreHelp)
		return nil
	}

	positional, src, targetTime, autoYes, err := parseRestoreFlags(args)
	if err != nil {
		return err
	}

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd postgres:restore <postgres-scope/name> --from <path> [--target-time \"<ts>\"] [--yes]")
	}

	if src == "" {
		return fmt.Errorf("--from <path> is required")
	}

	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("backup file %q: %w", src, err)
	}

	scope, name := splitScopeName(positional[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", positional[0])
	}

	if !autoYes {
		fmt.Fprintf(os.Stderr,
			"\n⚠  RESTORE IS DESTRUCTIVE — wipes postgres %s's data + every standby's data.\n"+
				"   No rollback. Use --yes (or -y) to confirm.\n\n",
			refOrName(scope, name))

		return fmt.Errorf("refusing to restore without --yes (destructive)")
	}

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	if ctx.ControllerURL == "" {
		return fmt.Errorf("restore requires controller_url (needs replicas count + image + primary ordinal)")
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

	pgdataPath := readPGDataPath(spec)
	if pgdataPath == "" {
		pgdataPath = "/var/lib/postgresql/data/pgdata"
	}

	image := readImage(spec)
	if image == "" {
		image = defaultImage
	}

	primaryContainer := containerNameFor(scope, name, primaryOrdinal)

	if !containerExists(primaryContainer) {
		return fmt.Errorf("container %s not found on this host (restore must run on the same host)", primaryContainer)
	}

	// Step 1: stop every pod. Restore needs ALL pods offline so
	// nothing keeps a stale state running while we wipe.
	fmt.Fprintf(os.Stderr, "restore: stopping all pods in cluster (primary + %d standbys)\n", replicas-1)

	for i := 0; i < replicas; i++ {
		c := containerNameFor(scope, name, i)

		if !containerExists(c) {
			continue
		}

		if err := dockerStop(c); err != nil {
			return fmt.Errorf("stop %s: %w", c, err)
		}
	}

	// Step 2: wipe primary's PGDATA. We use a one-shot container
	// sharing the volume; rm -rf inside fresh postgres image avoids
	// host-side root requirements.
	fmt.Fprintf(os.Stderr, "restore: wiping primary's PGDATA (%s)\n", pgdataPath)

	if err := wipePGDataViaDocker(primaryContainer, image, pgdataPath); err != nil {
		return fmt.Errorf("wipe primary PGDATA: %w", err)
	}

	// Step 3: extract backup tar into primary's PGDATA. We pipe
	// stdin → docker run tar -x via --volumes-from. The path
	// "$PGDATA" is the directory tar wrote during pg_basebackup;
	// extracting the same tar there reverses the operation.
	fmt.Fprintf(os.Stderr, "restore: extracting %s → %s on %s\n", src, pgdataPath, primaryContainer)

	if err := extractBackupViaDocker(primaryContainer, image, pgdataPath, src); err != nil {
		return fmt.Errorf("extract backup: %w", err)
	}

	// Step 4: PITR setup (if --target-time given).
	if targetTime != "" {
		fmt.Fprintf(os.Stderr, "restore: configuring PITR — recovery_target_time=%s\n", targetTime)

		if err := writePITRConfig(primaryContainer, image, pgdataPath, targetTime); err != nil {
			return fmt.Errorf("write PITR config: %w", err)
		}
	}

	// Step 5: start primary. Postgres replays WAL on startup
	// (basebackup brought consistent state; PITR replays archived
	// WAL until target_time then exits recovery via pg_promote).
	fmt.Fprintf(os.Stderr, "restore: starting primary %s\n", primaryContainer)

	if err := dockerStart(primaryContainer); err != nil {
		return fmt.Errorf("start primary: %w", err)
	}

	// Step 6: wipe each standby's PGDATA. The wrapper's first-boot
	// path will pg_basebackup from the just-restored primary on
	// next start, getting a fresh, timeline-aligned copy.
	for i := 0; i < replicas; i++ {
		if i == primaryOrdinal {
			continue
		}

		c := containerNameFor(scope, name, i)

		if !containerExists(c) {
			continue
		}

		fmt.Fprintf(os.Stderr, "restore: wiping standby %s PGDATA (will rebootstrap via pg_basebackup)\n", c)

		if err := wipePGDataViaDocker(c, image, pgdataPath); err != nil {
			return fmt.Errorf("wipe standby %s PGDATA: %w", c, err)
		}

		if err := dockerStart(c); err != nil {
			return fmt.Errorf("start standby %s: %w", c, err)
		}
	}

	pitrSuffix := ""
	if targetTime != "" {
		pitrSuffix = fmt.Sprintf(" (PITR target: %s)", targetTime)
	}

	result := dispatchOutput{
		Message: fmt.Sprintf(
			"postgres %s: restored from %s%s. Primary up; standbys rebootstrapping via pg_basebackup. "+
				"Watch `vd logs %s` to confirm WAL replay completed.",
			refOrName(scope, name), src, pitrSuffix, refOrName(scope, name)),
		Actions: nil,
	}

	return writeDispatchOutput(result)
}

// parseRestoreFlags extracts --from, --target-time, --yes/-y.
func parseRestoreFlags(args []string) (positional []string, src, targetTime string, autoYes bool, err error) {
	for i := 0; i < len(args); i++ {
		a := args[i]

		switch {
		case a == "--from" || a == "-f":
			if i+1 >= len(args) {
				return nil, "", "", false, fmt.Errorf("--from requires a path")
			}

			src = args[i+1]
			i++

		case len(a) > 7 && a[:7] == "--from=":
			src = a[7:]

		case a == "--target-time":
			if i+1 >= len(args) {
				return nil, "", "", false, fmt.Errorf("--target-time requires a timestamp")
			}

			targetTime = args[i+1]
			i++

		case len(a) > 14 && a[:14] == "--target-time=":
			targetTime = a[14:]

		case a == "--yes" || a == "-y":
			autoYes = true

		case a == "-h" || a == "--help":
			// handled

		default:
			positional = append(positional, a)
		}
	}

	return positional, src, targetTime, autoYes, nil
}

// wipePGDataViaDocker deletes everything inside PGDATA from a
// one-shot container sharing the target's volume. Uses bash glob
// `${PGDATA:?}/* ${PGDATA:?}/.* 2>/dev/null` to remove visible AND
// dotfiles (PG_VERSION et al) without erroring on the . / .. entries.
func wipePGDataViaDocker(targetContainer, image, pgdataPath string) error {
	cmd := exec.Command(
		"docker", "run", "--rm",
		"--volumes-from", targetContainer,
		image,
		"bash", "-c",
		fmt.Sprintf("rm -rf %s/* %s/.[!.]* 2>/dev/null; mkdir -p %s; chmod 0700 %s; chown postgres:postgres %s",
			pgdataPath, pgdataPath, pgdataPath, pgdataPath, pgdataPath),
	)
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// extractBackupViaDocker pipes the host's backup tar into a docker
// run that runs tar -x against PGDATA. The plugin's stdin is
// repurposed to pipe the file content; the docker container reads
// from its stdin.
func extractBackupViaDocker(targetContainer, image, pgdataPath, srcTar string) error {
	src, err := os.Open(srcTar)
	if err != nil {
		return fmt.Errorf("open %s: %w", srcTar, err)
	}

	defer src.Close()

	cmd := exec.Command(
		"docker", "run", "--rm", "-i",
		"--volumes-from", targetContainer,
		"-u", "postgres",
		image,
		"bash", "-c",
		fmt.Sprintf("tar -xf - -C %s && chmod 0700 %s", pgdataPath, pgdataPath),
	)

	cmd.Stdin = src
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// writePITRConfig drops postgresql.auto.conf with
// `recovery_target_time = '<targetTime>'` and `restore_command =
// 'cp /wal-archive/%f %p'` plus a recovery.signal file. Postgres
// reads these on startup and enters PITR replay mode; once the
// target time is reached, postgres promotes itself out of recovery.
func writePITRConfig(targetContainer, image, pgdataPath, targetTime string) error {
	// auto.conf: postgres-managed file; we splice our settings.
	// restore_command points at the WAL archive volume the plugin
	// emits at /wal-archive (M-P2 default mount path).
	autoConf := fmt.Sprintf(`
# voodu-postgres restore — PITR setup
restore_command = 'cp /wal-archive/%%f %%p'
recovery_target_time = '%s'
recovery_target_action = 'promote'
`, escapeSingleQuote(targetTime))

	// Append to postgresql.auto.conf + create recovery.signal
	// in a one-shot container sharing the volume.
	script := fmt.Sprintf(
		"cat >> %s/postgresql.auto.conf <<'EOF'\n%sEOF\ntouch %s/recovery.signal\n",
		pgdataPath, autoConf, pgdataPath,
	)

	cmd := exec.Command(
		"docker", "run", "--rm",
		"--volumes-from", targetContainer,
		"-u", "postgres",
		image,
		"bash", "-c", script,
	)

	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// escapeSingleQuote escapes single quotes for postgres conf
// string literals (doubled quote convention).
func escapeSingleQuote(s string) string {
	out := make([]rune, 0, len(s)+2)

	for _, r := range s {
		if r == '\'' {
			out = append(out, '\'', '\'')
			continue
		}

		out = append(out, r)
	}

	return string(out)
}
