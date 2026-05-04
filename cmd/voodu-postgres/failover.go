// `vd postgres:failover` — promote a standby to primary by
// flipping the cluster's primary ordinal in the config bucket.
//
// # Workflow (operator-driven, NOT auto-failover)
//
// Manual failover is a 3-step process:
//
//   1. PROMOTE THE NEW PRIMARY — operator runs pg_promote() on
//      the standby pod they want as the new primary. The plugin
//      does NOT do this for you (intentional — promotion is
//      destructive of the old primary's writability and we want
//      operator confirmation to be explicit):
//
//          vd postgres:psql <scope>/<name> --replica <N> \
//            -c "SELECT pg_promote();"
//
//   2. RUN FAILOVER — flips PG_PRIMARY_ORDINAL=<N> in the bucket
//      and refreshes every linked consumer's DATABASE_URL to point
//      at the new primary FQDN. Triggers a rolling restart of the
//      postgres pods so they re-read voodu-50-streaming.conf with
//      the new primary_conninfo.
//
//          vd postgres:failover <scope>/<name> --replica <N>
//
//   3. REJOIN THE OLD PRIMARY — the rolling restart hits the OLD
//      primary (pod-0) which now has PGDATA but no standby.signal.
//      The wrapper script's split-brain guard refuses to start
//      until the operator runs:
//
//          vd postgres:rejoin <scope>/<name> --replica 0
//
//      That command runs pg_rewind against the new primary,
//      touches standby.signal, and unfreezes the pod.
//
// # Why not auto-promote?
//
// pg_promote is destructive: it ends recovery on the standby
// pod, and the WAL it had streaming is now disconnected. If the
// "new primary" pod was lagging, you may have just lost data.
// Voodu deliberately stops short of running this for you; the
// operator confirms the standby is caught up (via pg_stat_replication
// on the old primary) BEFORE running step 2.
//
// # Why does the rolling restart need to happen?
//
// The streaming.conf we baked at apply time pinned db-0 as the
// primary FQDN. After flip, every pod needs the new conf so
// standbys reconnect to the new primary. The bucket flip alone
// isn't enough — postgres re-reads conf only on (re)start.

package main

import (
	"fmt"
	"os"
	"strconv"
)

// primaryOrdinalKey is the bucket key the wrapper script reads
// at boot to pick the primary ordinal. Failover flips it.
const primaryOrdinalKey = "PG_PRIMARY_ORDINAL"

const failoverHelp = `vd postgres:failover — promote a standby to primary.

USAGE
  vd postgres:failover <postgres-scope/name> --replica <N> [--no-restart]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster to fail over.

FLAGS
  --replica <N>           Ordinal of the standby to promote (required).
  --no-restart            Update the bucket flag + consumer URLs but skip
                          the rolling restart on the postgres pods.
                          Operator must restart manually afterward.

PRE-FLIGHT (operator)
  Run pg_promote() on the target standby BEFORE this command:
    vd postgres:psql <postgres-scope/name> --replica <N> \
      -c "SELECT pg_promote();"

  Verify the standby is caught up FIRST (avoids data loss):
    vd postgres:psql <postgres-scope/name> --replica 0 \
      -c "SELECT pid, application_name, state, sync_state, replay_lsn
          FROM pg_stat_replication;"

WHAT THIS COMMAND DOES
  1. Flip PG_PRIMARY_ORDINAL=<N> on the bucket.
  2. Refresh DATABASE_URL[/_READ_URL] on every linked consumer.
  3. Trigger rolling restart so all pods re-read streaming.conf
     with the new primary FQDN — standbys reconnect via the new
     primary_conninfo.

POST-FLIGHT (operator)
  The OLD primary's pod (e.g. ordinal-0) won't restart cleanly —
  the wrapper's split-brain guard catches "no standby.signal but
  designated as standby" and exits 1. Recover with:

    vd postgres:rejoin <postgres-scope/name> --replica <old-ordinal>

  That runs pg_rewind, arms standby.signal, and brings the pod back
  as a standby of the new primary.

EXAMPLES
  # Standard 3-step failover from pod-0 to pod-1
  vd postgres:psql clowk-lp/db --replica 1 -c "SELECT pg_promote();"
  vd postgres:failover clowk-lp/db --replica 1
  vd postgres:rejoin   clowk-lp/db --replica 0

NOTES
  - --no-restart skips the rolling restart on the postgres pods.
    Useful when staging a failover (verify, then restart on demand).
    Consumer URLs still refresh.
  - replicas must be > 1; can't fail over a single-replica cluster.
  - The new primary ordinal must differ from the current — running
    failover with --replica equal to the current primary is a no-op
    error.
`

// cmdFailover handles the failover dispatch.
func cmdFailover() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(failoverHelp)
		return nil
	}

	positional, target, hasTarget, noRestart := parseFailoverFlags(args)

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd postgres:failover <postgres-scope/name> --replica <N> [--no-restart]")
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
		return fmt.Errorf("failover requires controller_url (no offline mode — needs replicas count + linked-consumers list from the controller)")
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
		return fmt.Errorf("postgres %s has replicas=%d; failover requires replicas > 1 (raise the count and re-apply first)",
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

	// Synthetic config carrying the post-failover ordinal so the
	// URL builder pins the new primary host. We haven't written
	// the new value to the controller yet (it lands as one of
	// THIS command's config_set actions), but the linked-consumer
	// URLs in the same envelope must reflect the post-failover
	// state, not the pre-failover one. Same trick voodu-redis
	// failover uses.
	newConfig := cloneConfig(config)
	newConfig[primaryOrdinalKey] = strconv.Itoa(target)

	// Action 1: flip the primary ordinal on the postgres bucket.
	// SkipRestart honoured per --no-restart; default false →
	// triggers the rolling restart that re-renders streaming.conf
	// with the new primary FQDN on every pod.
	actions := []dispatchAction{{
		Type:        "config_set",
		Scope:       scope,
		Name:        name,
		KV:          map[string]string{primaryOrdinalKey: strconv.Itoa(target)},
		SkipRestart: noRestart,
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
		"postgres %s: primary ordinal %d → %d. Pods rolling top-down; new primary live once ordinal-%d finishes restarting. "+
			"OLD primary (ordinal-%d) needs `vd postgres:rejoin %s --replica %d` to recover as standby.",
		refOrName(scope, name), current, target, target,
		current, refOrName(scope, name), current,
	)

	if noRestart {
		msg = fmt.Sprintf(
			"postgres %s: primary ordinal %d → %d (--no-restart: bucket + URLs updated, postgres pods NOT rolled).",
			refOrName(scope, name), current, target,
		)
	}

	if refreshed > 0 {
		msg = fmt.Sprintf("%s Refreshed %d linked consumer URL(s).", msg, refreshed)
	}

	return writeDispatchOutput(dispatchOutput{
		Message: msg,
		Actions: actions,
	})
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

// parseFailoverFlags pulls --replica <N> (space- or =-separated)
// and --no-restart from args, returning the rest as positional.
// Order-agnostic.
func parseFailoverFlags(args []string) (positional []string, target int, hasTarget bool, noRestart bool) {
	for i := 0; i < len(args); i++ {
		a := args[i]

		switch {
		case a == "--no-restart":
			noRestart = true

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

	return positional, target, hasTarget, noRestart
}

// buildLinkURLsWithPrimary is a thin wrapper around buildLinkURLs
// that overrides the assumed primary ordinal (which buildLinkURLs
// hardcodes to 0). After failover the primary lives at a different
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
