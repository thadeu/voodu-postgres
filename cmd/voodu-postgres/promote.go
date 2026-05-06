// `vd postgres:promote` — manual failover. Promotes the target
// standby to primary and updates every linked consumer's
// DATABASE_URL to point at the new primary.
//
// # Why "promote" and not "failover"
//
// Plugin owns ALL the postgres-side SQL. The operator types
// `vd pg:promote --replica N` and the plugin handles:
//
//   - lag check via pg_stat_replication on the current primary
//   - SELECT pg_promote(true, 60) on the target standby
//   - poll pg_is_in_recovery() until promotion completes
//   - flip PG_PRIMARY_ORDINAL bucket flag
//   - refresh consumer URLs
//
// "Failover" was the original name when the workflow required the
// operator to run pg_promote() manually via psql. That worked but
// invited typos — operator could fat-finger the SQL or hit the
// wrong replica. Renaming to "promote" + folding the SQL into the
// plugin removes the footgun.
//
// # Lag check
//
// Promote is destructive of the source primary's writability:
// writes still on the old primary that haven't replicated to the
// new primary are LOST. Plugin queries pg_stat_replication on the
// current primary and refuses to proceed when any standby is
// behind (max_lag_bytes > 0). Operator opts into data-loss with
// `--force` after acknowledging.
//
// The lag check is coarse — it doesn't distinguish which standby
// is lagged. Conservative posture: any non-zero lag in the
// cluster blocks promote without --force. Operators who know
// their target standby is caught up but a different one is
// behind use `--force` plus a manual pg_stat_replication check
// (via vd pg:psql) to confirm.
//
// # Post-promote: rejoin still manual
//
// After promote, the OLD primary's pod boots without
// standby.signal and the wrapper's split-brain guard refuses to
// start. Operator runs `vd pg:rejoin --replica <old>` which
// pg_rewinds the old primary against the new one.
//
// We deliberately do NOT auto-rejoin: pg_rewind silently
// DISCARDS writes the old primary had that didn't replicate
// before promote. Operators who suspect orphaned writes take a
// pg_dump first. Auto-rejoin would hide that opportunity.

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// primaryOrdinalKey is the bucket key the wrapper script reads
// at boot to pick the primary ordinal. Promote flips it.
const primaryOrdinalKey = "PG_PRIMARY_ORDINAL"

const promoteHelp = `vd postgres:promote — promote a standby to primary.

USAGE
  vd postgres:promote <postgres-scope/name> --replica <N> [--force] [--no-restart]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.

FLAGS
  --replica <N>           Ordinal of the standby to promote (required).
  --force                 Promote even when standbys are behind on
                          replication. Use only when you understand
                          you're acknowledging potential data loss.
  --no-restart            Skip the rolling restart on the postgres pods
                          after the bucket flip. Useful for staged
                          promotes (verify, then restart on demand).

WHAT IT DOES
  1. Lag check — query pg_stat_replication on the current primary.
     Refuses if any standby is behind (unless --force).
  2. SELECT pg_promote(true, 60) on the target standby (waits up
     to 60s for promotion to complete inside postgres).
  3. Polls pg_is_in_recovery() on the target until it returns
     false (target is now primary).
  4. Flips PG_PRIMARY_ORDINAL=<N> on the bucket.
  5. Refreshes DATABASE_URL[/_READ_URL] on every linked consumer
     to point at the new primary FQDN.
  6. Rolling restart of postgres pods so they re-read
     streaming.conf with the new primary_conninfo (unless
     --no-restart).
  7. AUTO-REJOIN — chains a rejoin call on the OLD primary so it
     comes back as a fresh standby of the new primary. Single-
     command failover; no manual rejoin step required.

WHY AUTO-REJOIN
  Without it, the old primary's wrapper detects "PGDATA in primary
  state but spec says STANDBY" (split-brain guard) and exits in a
  loop. The auto-rejoin runs pg_rewind first (preserves the small
  divergence between the two primaries' WAL); on failure (typical
  when WAL has diverged enough), it auto-falls back to wiping the
  old primary's volume and re-cloning via pg_basebackup. Either
  way the cluster ends with both pods serving.

  Skipped under --no-restart so staged failovers can manually
  decide when to rejoin the old primary.

  If auto-rejoin fails for any reason (rare), the rest of the
  promote still succeeded; operator can run rejoin manually:

    vd postgres:rejoin <postgres-scope/name> --replica <old-ordinal>

EXAMPLES
  # Standard promote (refuses if any standby is behind).
  # Auto-rejoins the old primary at the end — single command.
  vd postgres:promote clowk-lp/db --replica 1

  # Force promote despite replication lag (data loss accepted).
  vd postgres:promote clowk-lp/db --replica 1 --force

  # Staged: flip + refresh URLs but don't roll the pods yet.
  # Skips auto-rejoin too — operator runs rejoin manually later.
  vd postgres:promote clowk-lp/db --replica 1 --no-restart
  vd restart clowk-lp/db                    # later, when ready
  vd postgres:rejoin clowk-lp/db --replica 0

NOTES
  - replicas must be > 1; can't promote on a single-replica cluster.
  - --replica must differ from the current primary ordinal — running
    promote on the current primary is a no-op error.
  - The current primary must be reachable for the lag check. If it's
    already down (real failover), pass --force to skip the check.
`

// cmdPromote handles the promote dispatch.
func cmdPromote() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(promoteHelp)
		return nil
	}

	positional, target, hasTarget, force, noRestart := parsePromoteFlags(args)

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd postgres:promote <postgres-scope/name> --replica <N> [--force] [--no-restart]")
	}

	if !hasTarget {
		return fmt.Errorf("--replica <N> is required (the ordinal to promote to primary)")
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
		return fmt.Errorf("promote requires controller_url (no offline mode — needs replicas count + linked-consumers list from the controller)")
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

	if replicas <= 1 {
		return fmt.Errorf("postgres %s has replicas=%d; promote requires replicas > 1 (raise the count and re-apply first)",
			refOrName(scope, name), replicas)
	}

	if target < 0 || target >= replicas {
		return fmt.Errorf("--replica %d out of range (valid: 0..%d)", target, replicas-1)
	}

	current := readCurrentPrimaryOrdinal(config)

	if target == current {
		return fmt.Errorf("postgres %s already has primary at ordinal %d — no-op (use --replica different from %d)",
			refOrName(scope, name), current, current)
	}

	primaryContainer := containerNameFor(scope, name, current)
	targetContainer := containerNameFor(scope, name, target)

	if !containerExists(targetContainer) {
		return fmt.Errorf("container %s not found on this host (promote must run on the same host as the target pod)", targetContainer)
	}

	// Step 1: lag check on current primary. Skipped under --force
	// (operator either accepts data loss or the primary is down
	// and the check would itself error).
	if !force {
		fmt.Fprintf(os.Stderr, "promote: checking replication lag on current primary (%s)…\n", primaryContainer)

		lag, err := queryReplicationLag(primaryContainer)
		if err != nil {
			return fmt.Errorf("lag check on %s: %w (pass --force to skip if the primary is down)", primaryContainer, err)
		}

		if lag.maxLagBytes > 0 {
			return fmt.Errorf(
				"replication lag detected: %d standby(s) streaming, max lag %d bytes. "+
					"Promote would risk data loss. "+
					"Inspect with `vd pg:psql %s -c \"SELECT * FROM pg_stat_replication;\"` "+
					"and pass --force if you want to proceed anyway.",
				lag.streaming, lag.maxLagBytes, refOrName(scope, name),
			)
		}

		fmt.Fprintf(os.Stderr, "promote: %d standby(s) caught up (lag=0)\n", lag.streaming)
	} else {
		fmt.Fprintln(os.Stderr, "promote: --force passed; skipping lag check")
	}

	// Step 2: pg_promote(wait=true, wait_seconds=60) on the
	// target. Postgres returns true when promotion completes
	// successfully within the timeout.
	fmt.Fprintf(os.Stderr, "promote: running SELECT pg_promote(true, 60) on %s…\n", targetContainer)

	if err := runPgPromote(targetContainer); err != nil {
		return fmt.Errorf("pg_promote on %s: %w", targetContainer, err)
	}

	// Step 3: poll pg_is_in_recovery() on the target. pg_promote
	// returned but postgres takes a moment to fully exit recovery.
	// We poll up to 30s with 2s gaps before bailing.
	fmt.Fprintf(os.Stderr, "promote: waiting for %s to exit recovery mode…\n", targetContainer)

	if err := waitForPromotion(targetContainer, 30*time.Second); err != nil {
		return fmt.Errorf("target %s never exited recovery: %w (manual recovery: vd pg:psql %s --replica %d to inspect)",
			targetContainer, err, refOrName(scope, name), target)
	}

	fmt.Fprintf(os.Stderr, "promote: %s is now primary; flipping bucket + refreshing consumers\n", targetContainer)

	// Synthetic config carrying the post-promote ordinal so the
	// URL builder pins the new primary host. Same trick failover
	// originally used: actions in this envelope must reflect the
	// post-promote state, not the pre-promote one.
	newConfig := cloneConfig(config)
	newConfig[primaryOrdinalKey] = strconv.Itoa(target)

	// Action 1: flip the primary ordinal on the postgres bucket.
	// ALWAYS SkipRestart=true — the auto-rejoin below already
	// rebootstraps the OLD primary (and pod-1 was promoted in
	// place, no restart needed). Without skip, the controller's
	// rolling restart fires AFTER cmdPromote returns and races
	// the auto-rejoin: it kills pod-0 mid-pg_basebackup → PGDATA
	// left partial → split-brain guard → restart loop.
	//
	// We still emit the config_set for state-tracking (so the
	// bucket value is recorded by the dispatch's audit trail and
	// the controller logs reflect what happened); the value
	// itself was already flushed synchronously via flushPrimaryOrdinal
	// above so subsequent fetchConfig calls (e.g. by runRejoinCore
	// inside this same plugin invocation) saw the post-promote
	// ordinal.
	//
	// `noRestart` (operator's --no-restart flag) is checked further
	// down for the auto-rejoin gate; here it's redundant because
	// we always skip the bucket-driven restart fan-out.

	actions := []dispatchAction{{
		Type:        "config_set",
		Scope:       scope,
		Name:        name,
		KV:          map[string]string{primaryOrdinalKey: strconv.Itoa(target)},
		SkipRestart: true,
	}}

	// Action 2..N: refresh every linked consumer's DATABASE_URL[/_READ_URL]
	// to point at the new primary FQDN. We re-emit using the same
	// shape the consumer originally got at link time: presence of
	// DATABASE_READ_URL on the consumer's bucket signals the
	// dual-URL link mode; absence signals single-URL.
	refreshed := 0
	consumers := parseLinkedConsumers(config)

	for _, ref := range consumers {
		cScope, cName := splitScopeName(ref)
		if cName == "" {
			continue
		}

		consumerCfg, _ := client.fetchConfig(cScope, cName)
		_, hadRead := consumerCfg[consumerReadEnvVar]

		urls, err := buildLinkURLsWithPrimary(scope, name, spec, newConfig, hadRead, target)
		if err != nil {
			return fmt.Errorf("build URLs for consumer %s: %w", ref, err)
		}

		kv := map[string]string{consumerWriteEnvVar: urls.WriteURL}
		if urls.ReadURL != "" {
			kv[consumerReadEnvVar] = urls.ReadURL
		}

		actions = append(actions, dispatchAction{
			Type:  "config_set",
			Scope: cScope,
			Name:  cName,
			KV:    kv,
		})

		refreshed++
	}

	msg := fmt.Sprintf(
		"postgres %s: promoted ordinal %d → primary (was %d). New primary live (pg_promote completed in place — no restart needed).",
		refOrName(scope, name), target, current,
	)

	if noRestart {
		msg = fmt.Sprintf(
			"postgres %s: promoted ordinal %d → primary (was %d) (--no-restart: bucket + URLs updated, auto-rejoin skipped).",
			refOrName(scope, name), target, current,
		)
	}

	if refreshed > 0 {
		msg = fmt.Sprintf("%s Refreshed %d linked consumer URL(s).", msg, refreshed)
	}

	// Auto-rejoin the OLD primary as a fresh standby of the new one
	// — single-command failover. Without this, the old primary
	// hits the wrapper's split-brain guard (PGDATA in primary state
	// + spec says STANDBY) and exits in a loop until the operator
	// runs rejoin manually.
	//
	// runRejoin is idempotent: if pg_rewind succeeds, data on old
	// primary is preserved across the rewind; if it fails (most
	// common after promote because WAL has diverged), the new
	// auto-fallback wipes the old primary's volume and re-clones
	// via pg_basebackup. Either way the cluster ends with both
	// pods serving — no further operator steps required.
	//
	// Skipped under --no-restart: that flag is for staged failovers
	// where the operator wants to flip the bucket but defer pod
	// rolling; auto-rejoin would cut against that intent.
	autoRejoinErr := error(nil)

	if !noRestart {
		fmt.Fprintf(os.Stderr, "promote: auto-rejoining old primary (ordinal %d) as standby of new primary (ordinal %d)\n",
			current, target)

		// Emit the bucket + URL actions FIRST so the controller
		// sees the new PG_PRIMARY_ORDINAL before runRejoin queries
		// for it. We achieve that by pre-writing actions via the
		// dispatch round-trip would normally do at the end — but
		// the rejoin needs to read fresh state. Easiest path: emit
		// the actions in two passes. Today we accumulate them and
		// emit once at the end; the rejoin runs against state that
		// MAY still show the old primary ordinal. To avoid that
		// race we synthesise a controller-side flush by issuing a
		// direct config_set RPC for PG_PRIMARY_ORDINAL right now,
		// then let the dispatch envelope re-emit it idempotently.
		if err := flushPrimaryOrdinal(client, scope, name, target); err != nil {
			fmt.Fprintf(os.Stderr,
				"promote: warn — flushing PG_PRIMARY_ORDINAL pre-rejoin failed (%v); proceeding anyway, runRejoin will read whatever the bucket has\n",
				err)
		}

		// Use runRejoinCore (not runRejoin) so the inner step
		// returns its result struct without emitting a separate
		// dispatch envelope. cmdPromote folds the rejoin summary
		// into ITS envelope below — operator sees one JSON
		// payload, not two interleaved on stdout.
		rejoinRes, err := runRejoinCore(scope, name, current)
		autoRejoinErr = err
		if autoRejoinErr != nil {
			// Old primary failed to rejoin. The promote ITSELF
			// succeeded (new primary is live, bucket flipped, URLs
			// refreshed). Surface the failure as a warning in the
			// success message so the operator can run rejoin
			// manually with diagnostics.
			fmt.Fprintf(os.Stderr,
				"promote: warn — auto-rejoin of ordinal %d failed: %v\n",
				current, autoRejoinErr)

			msg = fmt.Sprintf("%s\n⚠  Auto-rejoin of OLD primary (ordinal %d) FAILED: %v\n   Run manually: vd pg:rejoin %s --replica %d",
				msg, current, autoRejoinErr, refOrName(scope, name), current)
		} else {
			suffix := ""
			if rejoinRes.Rebootstrapped {
				suffix = " (rebootstrapped via pg_basebackup — slower path; pg_rewind couldn't reconcile the divergence)"
			} else {
				suffix = " (pg_rewind preserved local data)"
			}

			msg = fmt.Sprintf("%s\nOLD primary (ordinal %d) auto-rejoined as standby%s.",
				msg, current, suffix)
		}
	} else {
		// --no-restart: keep the breadcrumb so the operator knows
		// they need to run rejoin themselves once they're ready.
		msg = fmt.Sprintf("%s\nOLD primary (ordinal %d) needs `vd pg:rejoin %s --replica %d` to recover as standby (skipped under --no-restart).",
			msg, current, refOrName(scope, name), current)
	}

	return writeDispatchOutput(dispatchOutput{
		Message: msg,
		Actions: actions,
	})
}

// flushPrimaryOrdinal writes PG_PRIMARY_ORDINAL=<target> to the
// postgres config bucket via a direct controller RPC so subsequent
// reads (e.g. runRejoin's fetchConfig) see the post-promote
// primary ordinal. Without this flush, runRejoin's
// readCurrentPrimaryOrdinal would return the OLD primary, point
// pg_rewind at itself, and the auto-rejoin would loop or fail.
//
// CRITICAL: passes skipRestart=true. The /config POST endpoint
// triggers a rolling restart fan-out by default, which would race
// the auto-rejoin's rebootstrap below and kill pod-0 mid-
// pg_basebackup → partial PGDATA → split-brain guard → restart
// loop. The auto-rejoin handles its own targeted pod recreation;
// no rolling restart is needed for the bucket flip alone.
//
// The dispatch envelope at the end of cmdPromote ALSO emits the
// same config_set action with SkipRestart=true — that's for
// state-tracking/audit, also restart-suppressing for the same
// reason.
func flushPrimaryOrdinal(client *controllerClient, scope, name string, ordinal int) error {
	if client == nil {
		return fmt.Errorf("controller client unavailable")
	}

	return client.patchConfig(scope, name, map[string]string{
		primaryOrdinalKey: strconv.Itoa(ordinal),
	}, true /* skipRestart — auto-rejoin handles its own teardown */)
}

// readCurrentPrimaryOrdinal pulls PG_PRIMARY_ORDINAL from the
// config bucket. Defaults to 0 (the convention for fresh
// clusters where pod-0 is primary). Tolerant of garbage —
// non-integer values fall back to 0 (defensive; bucket should
// only ever be set by the plugin or operator).
func readCurrentPrimaryOrdinal(config map[string]string) int {
	raw := config[primaryOrdinalKey]
	if raw == "" {
		return 0
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}

	return n
}

// parsePromoteFlags pulls --replica <N>, --force, --no-restart from
// args, returning the rest as positional. Order-agnostic.
func parsePromoteFlags(args []string) (positional []string, target int, hasTarget, force, noRestart bool) {
	for i := 0; i < len(args); i++ {
		a := args[i]

		switch {
		case a == "--no-restart":
			noRestart = true

		case a == "--force":
			force = true

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
			// already handled by hasHelpFlag in caller

		default:
			positional = append(positional, a)
		}
	}

	return positional, target, hasTarget, force, noRestart
}

// buildLinkURLsWithPrimary is a thin wrapper around buildLinkURLs
// that overrides the assumed primary ordinal (which buildLinkURLs
// hardcodes to 0). After promote the primary lives at a different
// ordinal — the consumer URLs must reflect that.
//
// readsOnly mirrors the link-time --reads flag: when true, also
// emit DATABASE_READ_URL spanning standbys (excluding the new
// primary). When false, only the primary URL.
func buildLinkURLsWithPrimary(scope, name string, spec map[string]any, config map[string]string, readsOnly bool, primaryOrdinal int) (linkURLs, error) {
	user, db, port := readUserDBPort(spec)
	password := config[passwordKey]

	if password == "" {
		return linkURLs{}, fmt.Errorf("provider has no POSTGRES_PASSWORD in config bucket — has the resource been applied?")
	}

	primaryFQDN := composePrimaryFQDN(scope, name, primaryOrdinal)
	writeURL := composePostgresURL(user, password, primaryFQDN, port, db, "")

	out := linkURLs{WriteURL: writeURL}

	if !readsOnly {
		return out, nil
	}

	replicas := readReplicas(spec)
	if replicas <= 1 {
		out.ReadURL = writeURL
		return out, nil
	}

	// Standbys are every ordinal EXCEPT the new primary.
	hosts := make([]string, 0, replicas-1)

	for i := 0; i < replicas; i++ {
		if i == primaryOrdinal {
			continue
		}

		standbyFQDN := composePrimaryFQDN(scope, name, i)
		hosts = append(hosts, fmt.Sprintf("%s:%d", standbyFQDN, port))
	}

	out.ReadURL = composePostgresURL(user, password, joinComma(hosts), 0, db, "target_session_attrs=any")

	return out, nil
}

// joinComma joins strings with comma. Tiny helper to avoid
// pulling in strings.Join just for this; matches buildLinkURLs's
// existing use pattern.
func joinComma(parts []string) string {
	out := ""

	for i, p := range parts {
		if i > 0 {
			out += ","
		}

		out += p
	}

	return out
}

// lagInfo summarises pg_stat_replication output: how many standbys
// are streaming and the max replay lag in bytes. Coarse but
// sufficient for the promote safety check — any non-zero lag
// blocks promote without --force.
type lagInfo struct {
	streaming   int
	maxLagBytes int64
}

// queryReplicationLag runs a single psql query inside the
// primary container and parses the |-separated output. The query:
//
//	SELECT count(*) FILTER (WHERE state = 'streaming'),
//	       COALESCE(MAX(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)), 0)
//	FROM pg_stat_replication
//
// is hardcoded so the operator never types SQL. Output format
// (with -A -t flags): "<count>|<max_lag>".
func queryReplicationLag(primaryContainer string) (lagInfo, error) {
	const query = `SELECT count(*) FILTER (WHERE state = 'streaming'), COALESCE(MAX(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)), 0) FROM pg_stat_replication`

	cmd := exec.Command(
		"docker", "exec",
		primaryContainer,
		"psql", "-U", "postgres", "-d", "postgres",
		"-A", "-t", "-c", query,
	)

	out, err := cmd.Output()
	if err != nil {
		return lagInfo{}, fmt.Errorf("psql query failed: %w", err)
	}

	return parseLagOutput(string(out))
}

// parseLagOutput extracts the (streaming_count, max_lag_bytes)
// tuple from psql's `-A -t` output. Split out for unit testing
// without needing a live postgres.
func parseLagOutput(raw string) (lagInfo, error) {
	line := strings.TrimSpace(raw)

	parts := strings.Split(line, "|")
	if len(parts) != 2 {
		return lagInfo{}, fmt.Errorf("unexpected psql output: %q (expected '<count>|<bytes>')", line)
	}

	streaming, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return lagInfo{}, fmt.Errorf("parse streaming count %q: %w", parts[0], err)
	}

	maxLag, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		return lagInfo{}, fmt.Errorf("parse max lag bytes %q: %w", parts[1], err)
	}

	return lagInfo{streaming: streaming, maxLagBytes: maxLag}, nil
}

// runPgPromote runs the hardcoded `SELECT pg_promote(true, 60)`
// query inside the target container. The two-arg form asks
// postgres to wait up to 60 seconds for promotion to complete
// before returning. Plugin owns the SQL — operator never types
// it.
func runPgPromote(targetContainer string) error {
	cmd := exec.Command(
		"docker", "exec",
		targetContainer,
		"psql", "-U", "postgres", "-d", "postgres",
		"-c", "SELECT pg_promote(true, 60);",
	)

	cmd.Stdout = os.Stderr // mirror psql output to operator's terminal
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// waitForPromotion polls pg_is_in_recovery() on the target
// container until it returns "f" (false) — meaning postgres has
// fully exited recovery and is now the primary. pg_promote
// returns true before the recovery exit completes, so a brief
// poll loop after pg_promote() avoids races where the next
// step (bucket flip → reconnect from standbys) hits a target
// still in transition.
func waitForPromotion(targetContainer string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		cmd := exec.Command(
			"docker", "exec",
			targetContainer,
			"psql", "-U", "postgres", "-d", "postgres",
			"-A", "-t", "-c", "SELECT pg_is_in_recovery();",
		)

		out, err := cmd.Output()
		if err == nil {
			if strings.TrimSpace(string(out)) == "f" {
				return nil
			}
		}

		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("timed out after %s waiting for pg_is_in_recovery() = false", timeout)
}
