// `vd postgres:backup` — pg_basebackup snapshot of the cluster.
//
// # Mechanics
//
// pg_basebackup runs in a one-shot container that shares the
// network namespace of the target pod (so `db-N.scope.voodu`
// resolves via voodu0 DNS). It streams the backup to stdout in
// tar format; the plugin pipes that into the operator-supplied
// destination file on the host.
//
//	docker run --rm \
//	  --network container:<pod> \
//	  -e PGPASSWORD=<repl_pw> \
//	  postgres:<ver> \
//	  pg_basebackup -h <primary-fqdn> -p <port> -U <repl_user> \
//	    -D - -F tar -X stream -P
//	→ stdout pipe → <destination>
//
// # Why pg_basebackup, not pg_dump?
//
//   - pg_basebackup is physical (data files + WAL) — restore is a
//     direct PGDATA replacement, exact bit-level copy. Compatible
//     with WAL archive for PITR.
//   - pg_dump is logical (SQL or custom format) — useful for
//     cross-version migration but slow on large databases and
//     incompatible with PITR.
//
// Voodu's backup story leans on PITR (M-P2 ships WAL archive on by
// default), so pg_basebackup is the right primitive. Operators who
// want logical dumps run `vd postgres:psql <ref> -- pg_dump > out.sql`
// directly via the psql command's passthrough.
//
// # Source pod selection
//
// Default: the current PRIMARY (PG_PRIMARY_ORDINAL). Operator can
// override via `--from-replica N` to back up from a standby
// (saves write-load on the primary; standby's data is replication-
// recent up to its replay LSN).
//
// # Why use a one-shot pg_basebackup container instead of `docker exec`?
//
// pg_basebackup as `docker exec` works inside the running pod, BUT:
//
//   - The pod might not have spare disk for the tarball if writing
//     locally. Streaming to stdout via `docker run` avoids that.
//   - The one-shot pattern matches restore (which DEFINITELY needs
//     a separate container, see restore.go for why).
//
// Symmetry > minor optimisation.

package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
)

const backupHelp = `vd postgres:backup — pg_basebackup snapshot to a file.

USAGE
  vd postgres:backup <postgres-scope/name> --destination <path> [--from-replica <N>]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.

FLAGS
  --destination <path>    Output file (tar format). Required.
                          Path is on the docker host (where this
                          plugin runs). Examples:
                            /srv/backups/db-20260504.tar
                            ./db.tar  (relative to plugin's CWD)

  --from-replica <N>      Back up from ordinal N instead of the
                          current primary. Useful to offload the
                          backup load — standbys are read-only
                          replicas, no write contention. Optional;
                          defaults to the primary (PG_PRIMARY_ORDINAL).

WHAT IT DOES
  1. docker run --rm --network container:<pod> -e PGPASSWORD=...
     postgres pg_basebackup -h <pod-fqdn> -D - -F tar -X stream
  2. Pipe stdout → <destination> file

OUTPUT
  Single tar file. Contents:
    base.tar   - PGDATA cluster (data files)
    pg_wal.tar - WAL needed to make the backup self-consistent
                 (because of -X stream)

  Restore via vd postgres:restore <ref> --from <path>.

AUTOMATION (operator)
  Plugin doesn't ship a scheduled-backup feature — operator wraps
  this in a voodu cronjob:

    cronjob "clowk-lp" "db-backup" {
      schedule = "0 3 * * *"
      image    = "ghcr.io/clowk/voodu-cli:latest"   # has vd binary

      command = ["bash", "-c", "vd postgres:backup clowk-lp/db --destination /backup/db-$(date +%Y%m%d).tar"]

      volumes = ["/srv/pg-backups:/backup:rw"]
    }

  S3/R2 upload: pipe through awscli or rclone in the cronjob command.

EXAMPLES
  # Local backup
  vd postgres:backup clowk-lp/db --destination /srv/backups/db.tar

  # Backup from a standby (avoid primary load)
  vd postgres:backup clowk-lp/db --destination /tmp/db.tar --from-replica 1
`

// cmdBackup runs pg_basebackup against the target pod and pipes
// the tar stream into the operator-supplied destination file.
func cmdBackup() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		// os.Stdout.WriteString avoids vet's "% directive in
		// fmt.Print" false-positive on the help text (which
		// embeds `date +%Y%m%d` in a shell example).
		_, _ = os.Stdout.WriteString(backupHelp)
		return nil
	}

	positional, dest, fromReplica, hasFromReplica, err := parseBackupFlags(args)
	if err != nil {
		return err
	}

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd postgres:backup <postgres-scope/name> --destination <path> [--from-replica <N>]")
	}

	if dest == "" {
		return fmt.Errorf("--destination <path> is required (the output file)")
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
		return fmt.Errorf("backup requires controller_url (needs replicas count + replication password from controller)")
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

	replPassword := config[replicationPasswordKey]
	if replPassword == "" {
		return fmt.Errorf("postgres %s: %s missing from bucket — has the cluster ever booted?",
			refOrName(scope, name), replicationPasswordKey)
	}

	replUser := readReplicationUser(spec)

	containerName := containerNameFor(scope, name, source)

	if !containerExists(containerName) {
		return fmt.Errorf("container %s not found on this host (backup must run on the same host as the source pod)", containerName)
	}

	running, err := containerIsRunning(containerName)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", containerName, err)
	}

	if !running {
		return fmt.Errorf("container %s is not running (need a live pod to stream from; start it with `vd start %s`)",
			containerName, refOrName(scope, name))
	}

	sourceFQDN := composePrimaryFQDN(scope, name, source)
	port := readPortInt(spec)

	if port == 0 {
		port = 5432
	}

	image := readImage(spec)
	if image == "" {
		image = defaultImage
	}

	// Open destination file BEFORE starting the docker pipe, so a
	// permission error surfaces immediately instead of after we've
	// already started the container (which would be a wasted
	// round-trip).
	out, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("create %s: %w", dest, err)
	}

	defer out.Close()

	fmt.Fprintf(os.Stderr, "backup: streaming pg_basebackup from %s → %s\n", sourceFQDN, dest)

	cmd := exec.Command(
		"docker", "run", "--rm",
		"--network", "container:"+containerName,
		"-e", "PGPASSWORD="+replPassword,
		image,
		"pg_basebackup",
		"-h", sourceFQDN,
		"-p", fmt.Sprintf("%d", port),
		"-U", replUser,
		"-D", "-",
		"-F", "tar",
		"-X", "stream",
		"-P",
		"-v",
	)

	cmd.Stdout = out
	cmd.Stderr = os.Stderr // pg_basebackup -P + -v progress to stderr

	if err := cmd.Run(); err != nil {
		// Remove the partial file so a retry doesn't pick it up
		// thinking it's complete. exit-code surfaces upstream.
		_ = out.Close()
		_ = os.Remove(dest)

		return fmt.Errorf("pg_basebackup failed: %w (partial file removed)", err)
	}

	stat, _ := os.Stat(dest)
	size := int64(0)
	if stat != nil {
		size = stat.Size()
	}

	fmt.Fprintf(os.Stderr, "backup: complete, %s (%d bytes)\n", dest, size)

	result := dispatchOutput{
		Message: fmt.Sprintf("postgres %s: backup written to %s (%d bytes, source: ordinal %d)",
			refOrName(scope, name), dest, size, source),
		Actions: nil,
	}

	return writeDispatchOutput(result)
}

// parseBackupFlags extracts --destination + --from-replica.
func parseBackupFlags(args []string) (positional []string, dest string, fromReplica int, hasFromReplica bool, err error) {
	for i := 0; i < len(args); i++ {
		a := args[i]

		switch {
		case a == "--destination" || a == "-d":
			if i+1 >= len(args) {
				return nil, "", 0, false, fmt.Errorf("--destination requires a path argument")
			}

			dest = args[i+1]
			i++

		case len(a) > 14 && a[:14] == "--destination=":
			dest = a[14:]

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

	return positional, dest, fromReplica, hasFromReplica, nil
}

// dockerCpFromContainer copies a file out of a stopped or running
// container to a host path. Used by restore (cp dump in) symmetry;
// kept here so backup.go and restore.go share the helper.
func dockerCpFromContainer(container, srcPath, hostPath string) error {
	cmd := exec.Command("docker", "cp", container+":"+srcPath, hostPath)
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// dockerCpToContainer copies a host file into a container at the
// given path. Inverse of dockerCpFromContainer.
func dockerCpToContainer(hostPath, container, dstPath string) error {
	cmd := exec.Command("docker", "cp", hostPath, container+":"+dstPath)
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// pipeStreamToFile is a tiny helper that copies a reader to an
// open file. Wraps io.Copy so callers don't repeat the boilerplate.
// Nothing M-P6-specific — just kept here next to its only caller.
func pipeStreamToFile(r io.Reader, w *os.File) (int64, error) {
	return io.Copy(w, r)
}
