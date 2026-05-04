// `vd postgres:rejoin` — re-attach a divergent pod to the current
// primary as a standby. The typical trigger is post-failover:
// the OLD primary still has its data dir but no standby.signal,
// the wrapper's split-brain guard refused to start the pod, and
// now the operator wants it back as a follower of the new primary.
//
// # Mechanics
//
//   1. docker stop the target pod (pg_rewind requires offline target)
//   2. docker run --volumes-from <target> --rm <image> pg_rewind \
//        --target-pgdata=$PGDATA \
//        --source-server="host=<primary-fqdn> port=<port> user=replicator
//                         password=<pw> dbname=postgres"
//   3. docker run --volumes-from <target> --rm <image> touch standby.signal
//   4. docker start <target> — pod boots as standby, picks up
//      streaming from the current primary via primary_conninfo
//
// We use `docker run --volumes-from` (not `docker exec`) because:
//
//   - pg_rewind requires postgres NOT running on the target
//   - exec inside a stopped container fails
//   - --volumes-from gives us the target's PGDATA volume in a
//     fresh, postgres-image-equipped container that has pg_rewind
//
// # When pg_rewind fails
//
// pg_rewind needs the diverging WAL records on the source primary.
// If the divergence is too large (WAL recycled past the divergence
// point), pg_rewind exits non-zero with a message like:
//
//     "could not find previous WAL record at ..."
//
// Recovery in that case: wipe the pod's data volume entirely and
// re-apply, the wrapper's first-boot path will pg_basebackup a
// fresh copy from the current primary. Operator-driven:
//
//     vd delete <postgres-scope/name> --replica <N> --prune
//     vd apply -f voodu.hcl
//
// # Why not auto-rejoin in the wrapper?
//
// pg_rewind is destructive (overwrites the target's data with
// the source's). Voodu deliberately requires operator confirmation
// because:
//
//   - The "old primary" might have writes that didn't replicate
//     to the new primary before failover. pg_rewind silently
//     drops those writes.
//   - In some recoveries the operator wants to dump those
//     orphaned writes via pg_dump BEFORE rewinding.
//
// Manual rejoin gives the operator a chance to inspect.

package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
)

const rejoinHelp = `vd postgres:rejoin — re-attach a divergent pod as standby of the current primary.

USAGE
  vd postgres:rejoin <postgres-scope/name> --replica <N>

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.

FLAGS
  --replica <N>           Ordinal of the pod to rejoin (required).

WHAT IT DOES
  1. docker stop <container>          # pg_rewind requires offline target
  2. docker run --volumes-from <container> --rm postgres:<ver> pg_rewind ...
  3. touch standby.signal in PGDATA   # via second --volumes-from run
  4. docker start <container>         # pod boots as standby of new primary

PRE-FLIGHT
  - The CURRENT primary (PG_PRIMARY_ORDINAL in the bucket) must be
    running and reachable from the rejoining pod's network.
  - The replication user/password (POSTGRES_REPLICATION_PASSWORD in
    the bucket) must be valid on the current primary.

WHEN pg_rewind FAILS
  pg_rewind needs the diverging WAL on the source primary. If the
  divergence is too large (WAL recycled past the divergence point),
  it exits with "could not find previous WAL record at...". Recovery
  fallback:

    vd delete <postgres-scope/name> --replica <N> --prune
    vd apply -f voodu.hcl

  That wipes the pod's data volume; the wrapper's first-boot path
  bootstraps a fresh copy via pg_basebackup.

EXAMPLES
  # Standard post-failover recovery (old primary was pod-0)
  vd postgres:rejoin clowk-lp/db --replica 0

  # If pg_rewind fails, fallback:
  vd delete clowk-lp/db --replica 0 --prune
  vd apply -f voodu.hcl

NOTES
  - pg_rewind silently drops writes the old primary had that didn't
    replicate before failover. If you suspect orphaned writes, take
    a pg_dump of the old primary's data BEFORE running rejoin.
  - This command runs inside the docker daemon — it requires the
    plugin to be running on the same host as the target container.
`

// cmdRejoin handles the rejoin dispatch.
func cmdRejoin() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(rejoinHelp)
		return nil
	}

	positional, target, hasTarget := parseRejoinFlags(args)

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd postgres:rejoin <postgres-scope/name> --replica <N>")
	}

	if !hasTarget {
		return fmt.Errorf("--replica <N> is required (the ordinal to rejoin)")
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
		return fmt.Errorf("rejoin requires controller_url (no offline mode — needs primary ordinal + replication password from controller)")
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

	if target < 0 || target >= replicas {
		return fmt.Errorf("--replica %d out of range (valid: 0..%d)", target, replicas-1)
	}

	primaryOrdinal := readCurrentPrimaryOrdinal(config)

	if target == primaryOrdinal {
		return fmt.Errorf("postgres %s: cannot rejoin ordinal %d — it IS the current primary",
			refOrName(scope, name), target)
	}

	replPassword := config[replicationPasswordKey]
	if replPassword == "" {
		return fmt.Errorf("postgres %s: %s missing from bucket — has the cluster ever booted? cmdExpand auto-gens it on first apply",
			refOrName(scope, name), replicationPasswordKey)
	}

	replUser := readReplicationUser(spec)

	containerName := containerNameFor(scope, name, target)

	if !containerExists(containerName) {
		return fmt.Errorf("container %s not found on this host (rejoin must run on the same host as the target pod)", containerName)
	}

	primaryFQDN := composePrimaryFQDN(scope, name, primaryOrdinal)

	image := readImage(spec)
	if image == "" {
		image = defaultImage
	}

	pgdataPath := readPGDataPath(spec)
	if pgdataPath == "" {
		pgdataPath = "/var/lib/postgresql/data/pgdata"
	}

	port := readPortInt(spec)
	if port == 0 {
		port = 5432
	}

	fmt.Fprintf(os.Stderr, "rejoin: stopping container %s\n", containerName)

	if err := dockerStop(containerName); err != nil {
		return fmt.Errorf("stop %s: %w", containerName, err)
	}

	fmt.Fprintf(os.Stderr, "rejoin: running pg_rewind --target=%s --source=host=%s port=%d user=%s\n",
		pgdataPath, primaryFQDN, port, replUser)

	if err := dockerRunPgRewind(containerName, image, pgdataPath, primaryFQDN, port, replUser, replPassword); err != nil {
		// pg_rewind failed. Container stays stopped; operator
		// inspects + decides whether to retry, fallback to
		// wipe-and-basebackup, or restore from a dump.
		return fmt.Errorf("pg_rewind on %s: %w (container left stopped — inspect + retry, or wipe with `vd delete --replica %d --prune` then re-apply for fresh basebackup)",
			containerName, err, target)
	}

	fmt.Fprintf(os.Stderr, "rejoin: arming standby.signal in %s\n", pgdataPath)

	if err := dockerRunTouchStandbySignal(containerName, image, pgdataPath); err != nil {
		return fmt.Errorf("touch standby.signal on %s: %w (container stopped — operator must arm signal manually + start)",
			containerName, err)
	}

	fmt.Fprintf(os.Stderr, "rejoin: starting container %s\n", containerName)

	if err := dockerStart(containerName); err != nil {
		return fmt.Errorf("start %s: %w (rejoin succeeded but container failed to start — check `docker logs %s`)",
			containerName, err, containerName)
	}

	out := dispatchOutput{
		Message: fmt.Sprintf(
			"postgres %s: rejoined ordinal %d as standby of pod-%d. The pod is starting; check `vd logs %s` to confirm streaming resumed.",
			refOrName(scope, name), target, primaryOrdinal, refOrName(scope, name)),
		Actions: nil,
	}

	return writeDispatchOutput(out)
}

// parseRejoinFlags pulls --replica <N> from args.
func parseRejoinFlags(args []string) (positional []string, target int, hasTarget bool) {
	for i := 0; i < len(args); i++ {
		a := args[i]

		switch {
		case a == "--replica" || a == "-r":
			if i+1 >= len(args) {
				continue
			}

			n, err := strconv.Atoi(args[i+1])
			if err == nil {
				target = n
				hasTarget = true
				i++
			}

		case len(a) > 10 && a[:10] == "--replica=":
			n, err := strconv.Atoi(a[10:])
			if err == nil {
				target = n
				hasTarget = true
			}

		case a == "-h" || a == "--help":
			// handled by hasHelpFlag in caller

		default:
			positional = append(positional, a)
		}
	}

	return positional, target, hasTarget
}

// readReplicationUser pulls PG_REPLICATION_USER from the
// statefulset env. Defaults to "replicator" when missing.
func readReplicationUser(spec map[string]any) string {
	env, ok := spec["env"].(map[string]any)
	if !ok {
		return "replicator"
	}

	if v, ok := env["PG_REPLICATION_USER"].(string); ok && v != "" {
		return v
	}

	return "replicator"
}

// readImage pulls the docker image string from the spec.
func readImage(spec map[string]any) string {
	if v, ok := spec["image"].(string); ok {
		return v
	}

	return ""
}

// readPGDataPath pulls PGDATA from the env. Defaults to the
// standard subdir-of-mount path the plugin emits.
func readPGDataPath(spec map[string]any) string {
	env, ok := spec["env"].(map[string]any)
	if !ok {
		return ""
	}

	if v, ok := env["PGDATA"].(string); ok && v != "" {
		return v
	}

	return ""
}

// readPortInt pulls PG_PORT from env (string), parses to int.
func readPortInt(spec map[string]any) int {
	env, ok := spec["env"].(map[string]any)
	if !ok {
		return 0
	}

	v, ok := env["PG_PORT"].(string)
	if !ok || v == "" {
		return 0
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}

	return n
}

// dockerStop stops a container, sending SIGTERM with 30s grace.
// Postgres flushes WAL + writes shutdown checkpoint within that
// window even on a busy primary.
func dockerStop(name string) error {
	cmd := exec.Command("docker", "stop", "-t", "30", name)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// dockerStart starts a stopped container.
func dockerStart(name string) error {
	cmd := exec.Command("docker", "start", name)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// dockerRunPgRewind runs pg_rewind in a one-shot container that
// shares the target's PGDATA volume via --volumes-from. PGPASSWORD
// flows in as an env var so the secret doesn't ride on argv (where
// `docker inspect` and `ps -ef` would surface it).
func dockerRunPgRewind(targetContainer, image, pgdataPath, primaryFQDN string, port int, user, password string) error {
	sourceConn := fmt.Sprintf("host=%s port=%d user=%s dbname=postgres",
		primaryFQDN, port, user)

	cmd := exec.Command(
		"docker", "run", "--rm",
		"--volumes-from", targetContainer,
		"-e", "PGPASSWORD="+password,
		// pg_rewind also dials the primary to fetch WAL — needs
		// the same network the target was on, so it can resolve
		// db-N.scope.voodu via voodu0's embedded DNS.
		"--network", "container:"+targetContainer,
		image,
		"pg_rewind",
		"--target-pgdata="+pgdataPath,
		"--source-server="+sourceConn,
		"--progress",
	)

	// pg_rewind writes to stderr; mirror to ours so the operator
	// sees progress in real time.
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// dockerRunTouchStandbySignal arms standby.signal in PGDATA via
// a one-shot container. Same --volumes-from trick as the
// pg_rewind step. Without standby.signal postgres would boot as
// primary on next start, defeating the rejoin.
func dockerRunTouchStandbySignal(targetContainer, image, pgdataPath string) error {
	cmd := exec.Command(
		"docker", "run", "--rm",
		"--volumes-from", targetContainer,
		image,
		"touch", pgdataPath+"/standby.signal",
	)

	cmd.Stdout = io.Discard // success is silent
	cmd.Stderr = os.Stderr

	return cmd.Run()
}
