// `vd postgres:expose` and `vd postgres:unexpose` — flip the
// statefulset's port binding between loopback (default, safe)
// and 0.0.0.0 (internet-facing, opt-in).
//
// # The mechanism
//
// Voodu binds container ports to 127.0.0.1 by default — a
// statefulset declaring `ports = ["5432"]` lands at
// 127.0.0.1:5432 on the host, NOT exposed beyond the VM.
// Operator who wants public access has to write
// `ports = ["0.0.0.0:5432"]` explicitly in HCL.
//
// expose flips a config bucket flag (PG_EXPOSE_PUBLIC=true)
// that cmdExpand reads at expand time. When set, ports are
// composed as ["0.0.0.0:<port>"] instead of ["<port>"]. The
// next reconcile (triggered by config_set's restart fan-out)
// recreates the container with the new bind address.
//
// # Security posture
//
// Postgres 14+ official image ships with scram-sha-256 auth
// enabled by default for all hosts (`host all all all
// scram-sha-256` in pg_hba.conf). Plus the wrapper's password
// is 256-bit hex (auto-gen) — strong enough to face the open
// internet. Still, exposure shifts attack surface from
// "anyone with VM access" to "anyone in the world", so
// operator opts in explicitly.
//
// The expose message reminds the operator to rotate the
// password if it had been previously committed somewhere
// untrusted (CI logs, screenshots, etc.).

package main

import (
	"fmt"
	"os"
	"strconv"
)

const exposeFlagKey = "PG_EXPOSE_PUBLIC"

const exposeHelp = `vd postgres:expose — publish postgres on 0.0.0.0:<port> (Internet-facing).

USAGE
  vd postgres:expose <postgres-scope/name>

ARGUMENTS
  <postgres-scope/name>   The postgres resource to publish.

EFFECT
  - Sets PG_EXPOSE_PUBLIC=true on the resource's config bucket.
  - Triggers reconcile — pods restart with their port bound to
    0.0.0.0 (was 127.0.0.1).
  - postgres now reachable from outside the host VM, subject to
    your firewall / security group rules. Verify before exposing.

WHEN YOU NEED THIS
  - External tools (DBeaver, TablePlus, datagrip) connecting from
    your laptop.
  - SaaS integrations (Heroku Connect, Hightouch, Fivetran)
    reading from the database.
  - Read replicas in a different VM that aren't on the voodu0
    bridge yet.

SECURITY POSTURE
  - Auto-gen password is 256-bit hex (1.16e77 keyspace) — strong
    against brute force from the open Internet.
  - postgres image enables scram-sha-256 by default for all hosts
    (encrypted password exchange, replay-safe).
  - Even so: if the password EVER leaked (CI logs, screenshots,
    pasted in a chat), rotate first:
        vd postgres:new-password <postgres-scope/name>
    then expose.

EXAMPLES
  vd postgres:expose clowk-lp/db

  # Connect from outside the VM
  PW=$(vd config clowk-lp/db get POSTGRES_PASSWORD -o json | jq -r .POSTGRES_PASSWORD)
  psql "postgres://postgres:$PW@vm-public-ip:5432/postgres"

REVERSE
  vd postgres:unexpose <postgres-scope/name>

NOTES
  - Custom port (port = 5433 in HCL) is honoured: bind becomes
    0.0.0.0:5433.
  - Operator firewall rules (ufw, security groups, iptables) still
    apply — exposing inside the container ≠ open from the Internet
    if the host blocks the port.

HA / REPLICAS
  Refused when replicas > 1. Voodu's statefulset reconciler applies
  spec.Ports uniformly to every pod (no per-ordinal port maps), so
  binding 0.0.0.0:<port> on multiple pods produces a host-port
  conflict — only the first pod boots, the others fail.

  For HA clusters, expose via:
    1. pgbouncer (or HAProxy / nginx-stream) fronting the cluster
       on a single dedicated host port. Pods stay loopback-only;
       the proxy is the single external listener.
    2. SSH tunnel from your machine to the primary FQDN
       (db-0.<scope>.voodu:5432) — fine for ad-hoc access.
    3. Temporarily scale to replicas=1, expose, do the work,
       scale back. Lossy if you depend on standby uptime.
`

const unexposeHelp = `vd postgres:unexpose — un-publish postgres back to 127.0.0.1 (loopback only).

USAGE
  vd postgres:unexpose <postgres-scope/name>

ARGUMENTS
  <postgres-scope/name>   The postgres resource to un-publish.

EFFECT
  - Unsets PG_EXPOSE_PUBLIC on the bucket.
  - Triggers reconcile — pods restart with their port bound to
    127.0.0.1 (loopback only, host-internal access).
  - Apps inside voodu (linked via vd postgres:link) keep working —
    they reach postgres via the voodu0 bridge, not via the host's
    public IP.

EXAMPLES
  vd postgres:unexpose clowk-lp/db

NOTES
  - Idempotent: running on a non-exposed postgres is safe (still
    fires the config_unset, controller no-ops).
  - This does NOT close existing TCP connections — clients
    currently connected stay connected. Use vd restart <ref> to
    force them to reconnect via the new bind.
`

// cmdExpose flips ports to 0.0.0.0 binding via apply_manifest +
// persists the toggle in the config bucket so subsequent
// vd apply runs preserve the exposed state.
func cmdExpose() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(exposeHelp)
		return nil
	}

	if len(args) < 1 {
		return fmt.Errorf("usage: vd postgres:expose <postgres-scope/name>")
	}

	scope, name := splitScopeName(args[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", args[0])
	}

	actions, err := composeExposeActions(scope, name, true)
	if err != nil {
		return err
	}

	out := dispatchOutput{
		Message: fmt.Sprintf("postgres %s now exposed on 0.0.0.0:<port> — container will be recreated. "+
			"Reminder: rotate password (vd postgres:new-password %s) if it ever leaked anywhere untrusted.",
			refOrName(scope, name), refOrName(scope, name)),
		Actions: actions,
	}

	return writeDispatchOutput(out)
}

// cmdUnexpose flips ports back to loopback binding via
// apply_manifest + clears PG_EXPOSE_PUBLIC from the bucket.
func cmdUnexpose() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(unexposeHelp)
		return nil
	}

	if len(args) < 1 {
		return fmt.Errorf("usage: vd postgres:unexpose <postgres-scope/name>")
	}

	scope, name := splitScopeName(args[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", args[0])
	}

	actions, err := composeExposeActions(scope, name, false)
	if err != nil {
		return err
	}

	out := dispatchOutput{
		Message: fmt.Sprintf("postgres %s back to loopback (127.0.0.1:<port>) — container will be recreated",
			refOrName(scope, name)),
		Actions: actions,
	}

	return writeDispatchOutput(out)
}

// composeExposeActions assembles the action pair both cmdExpose
// and cmdUnexpose emit:
//
//  1. config_set / config_unset on PG_EXPOSE_PUBLIC — persistent
//     state so the next `vd apply` (operator HCL re-expand) keeps
//     the exposed/loopback choice. SkipRestart=true to avoid the
//     legacy fanout (which only re-Puts the manifest unchanged
//     and doesn't actually flip ports — the historical bug this
//     refactor fixes).
//
//  2. apply_manifest with the current statefulset spec, ports
//     mutated to "0.0.0.0:<port>" (expose) or "<port>" (unexpose).
//     Reconciler sees the spec change and recreates the container
//     with the new bind address. THIS is what actually flips the
//     port mapping.
//
// Why both: config bucket is the source of truth for cmdExpand
// (preserves state across applies); apply_manifest is the
// imperative path that takes effect immediately without waiting
// for a re-apply.
//
// HA constraint: voodu's statefulset reconciler applies
// spec.Ports uniformly to every pod (no per-ordinal port maps
// today). With replicas > 1, binding 0.0.0.0:<port> on every pod
// causes the second pod to fail at startup with "port already in
// use". We refuse expose in that case and point the operator at
// safer alternatives — pgbouncer fronting the cluster, an SSH
// tunnel from the operator's box, or a separate read-replica
// service. unexpose stays unconditional because rolling back to
// loopback is always safe regardless of replica count.
func composeExposeActions(scope, name string, exposed bool) ([]dispatchAction, error) {
	ctx, err := readInvocationContext()
	if err != nil {
		return nil, err
	}

	if ctx.ControllerURL == "" {
		return nil, fmt.Errorf("expose/unexpose requires controller_url (needs to read current statefulset spec)")
	}

	client := newControllerClient(ctx.ControllerURL)

	spec, err := client.fetchSpec("statefulset", scope, name)
	if err != nil {
		return nil, fmt.Errorf("describe %s: %w", refOrName(scope, name), err)
	}

	if exposed {
		replicas := readReplicas(spec)
		if replicas > 1 {
			return nil, fmt.Errorf(
				"expose refused: postgres %s has replicas=%d; binding 0.0.0.0:<port> on every pod produces a port-conflict at boot.\n\n"+
					"Workarounds:\n"+
					"  1. Front the cluster with pgbouncer (or another TCP proxy) on a dedicated host port.\n"+
					"     The proxy keeps a single external listener; pods stay loopback-only.\n"+
					"  2. Open an SSH tunnel from your machine to db-0.%s.voodu:5432 for ad-hoc access.\n"+
					"  3. Scale to replicas=1 temporarily if you only need a writer endpoint.\n",
				refOrName(scope, name), replicas, scope)
		}
	}

	port := readPortInt(spec)
	if port == 0 {
		port = 5432
	}

	portStr := strconv.Itoa(port)
	if exposed {
		portStr = "0.0.0.0:" + portStr
	}

	spec["ports"] = []any{portStr}

	bucketAction := dispatchAction{
		Type:        "config_set",
		Scope:       scope,
		Name:        name,
		KV:          map[string]string{exposeFlagKey: "true"},
		SkipRestart: true,
	}

	if !exposed {
		bucketAction = dispatchAction{
			Type:        "config_unset",
			Scope:       scope,
			Name:        name,
			Keys:        []string{exposeFlagKey},
			SkipRestart: true,
		}
	}

	return []dispatchAction{
		bucketAction,
		{
			Type:  "apply_manifest",
			Scope: scope,
			Name:  name,
			Manifest: &dispatchManifest{
				Kind:  "statefulset",
				Scope: scope,
				Name:  name,
				Spec:  spec,
			},
		},
	}, nil
}
