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

// cmdExpose flips PG_EXPOSE_PUBLIC=true on the bucket.
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

	out := dispatchOutput{
		Message: fmt.Sprintf("postgres %s now exposed on 0.0.0.0:<port> — pod restart triggered. "+
			"Reminder: rotate password (vd postgres:new-password %s) if it ever leaked anywhere untrusted.",
			refOrName(scope, name), refOrName(scope, name)),
		Actions: []dispatchAction{{
			Type:  "config_set",
			Scope: scope,
			Name:  name,
			KV:    map[string]string{exposeFlagKey: "true"},
		}},
	}

	return writeDispatchOutput(out)
}

// cmdUnexpose unsets PG_EXPOSE_PUBLIC on the bucket.
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

	out := dispatchOutput{
		Message: fmt.Sprintf("postgres %s back to loopback (127.0.0.1:<port>) — pod restart triggered",
			refOrName(scope, name)),
		Actions: []dispatchAction{{
			Type:  "config_unset",
			Scope: scope,
			Name:  name,
			Keys:  []string{exposeFlagKey},
		}},
	}

	return writeDispatchOutput(out)
}
