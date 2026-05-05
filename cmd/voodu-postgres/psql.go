// `vd postgres:psql` — Heroku-style "drop me into psql against
// this cluster" shortcut. Operator doesn't need to know password,
// host, or user — the plugin shells out:
//
//	docker exec -it <container> psql -U <user> -d <db>
//
// Postgres's stock pg_hba.conf has `local all all trust` for unix
// socket connections; psql -U postgres without -h goes through
// the socket → no password needed.
//
// # Modes
//
//   - Interactive: vd postgres:psql <ref> [--replica N]
//     Drops you into a psql REPL on the primary (or pod-N with
//     --replica). Ctrl-D / \q exits.
//
//   - One-shot: vd postgres:psql <ref> -c "SELECT 1"
//     Runs the SQL, prints output, exits. -c works the same as
//     psql's native flag.
//
//   - Passthrough: vd postgres:psql <ref> -- --csv -A -c "SELECT *
//     FROM pg_stat_replication"
//     Everything after `--` flows verbatim into psql's argv.
//     Useful for psql-specific flags (--csv, --tuples-only,
//     --html, etc.).
//
// # Why not stream stdin via the dispatch envelope?
//
// Interactive psql needs a real TTY for line editing, history,
// and Ctrl-C handling. The dispatch envelope (`writeDispatchOutput`)
// emits one JSON object on stdout — that's a one-shot pattern
// incompatible with an interactive shell.
//
// Instead, when the user invokes psql interactively the plugin
// `os.Exec`s docker directly, replacing its own process. The
// controller dispatch wrapper (which captures stdout for env
// envelope decoding) sees the plugin terminate with the docker
// exit code; if it asked for a JSON envelope and got nothing,
// the wrapper degrades gracefully (silent — psql output already
// went to the operator's terminal).

package main

import (
	"fmt"
	"os"
)

const psqlHelp = `vd postgres:psql — drop into psql against the cluster.

USAGE
  vd postgres:psql <postgres-scope/name> [--replica <N>] [-c "<sql>"] [-- <psql-args>]

ARGUMENTS
  <postgres-scope/name>   The postgres cluster.

FLAGS
  --replica <N>           Connect to ordinal N instead of the primary.
                          Use for read-only queries against a standby
                          (e.g. lag inspection without primary load).
  -c "<sql>"              Run a single SQL statement and exit (psql's
                          native -c). Output streams to stdout.
  --                      Everything after this passes through to psql
                          unchanged. Use for psql flags voodu doesn't
                          model directly: --csv, --tuples-only, etc.

NO PASSWORD NEEDED
  Connection goes through the unix socket inside the container; the
  postgres image's stock pg_hba.conf trusts socket connections from
  the local user. The plugin runs psql as the OS user "postgres"
  inside the container.

EXAMPLES
  # Interactive REPL on the primary
  vd postgres:psql clowk-lp/db

  # Interactive on a standby (read-only)
  vd postgres:psql clowk-lp/db --replica 1

  # One-shot query
  vd postgres:psql clowk-lp/db -c "SELECT version();"

  # Replication status (post-failover sanity check)
  vd postgres:psql clowk-lp/db -c "SELECT * FROM pg_stat_replication;"

  # CSV output via passthrough flag
  vd postgres:psql clowk-lp/db -- --csv -c "SELECT * FROM pg_stat_database"

  # Trigger pg_promote (manual failover step 1)
  vd postgres:psql clowk-lp/db --replica 1 -c "SELECT pg_promote();"

NOTES
  - This command runs on the same docker host as the target pod —
    it shells out 'docker exec -it' against the container. If voodu
    is multi-host (future), this command will require the controller
    to forward the exec to the right host.
  - Interactive mode (-it) requires a real TTY. If invoked from a
    non-TTY context (CI pipe, no terminal), pass -c "<sql>" instead.
`

// cmdPsql shells into psql.
func cmdPsql() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(psqlHelp)
		return nil
	}

	positional, replica, hasReplica, psqlArgs := parsePsqlFlags(args)

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd postgres:psql <postgres-scope/name> [--replica <N>] [-c \"<sql>\"]")
	}

	scope, name := splitScopeName(positional[0])
	if name == "" {
		return fmt.Errorf("invalid ref %q (expected scope/name)", positional[0])
	}

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	// Read spec to discover user + database. We use the env vars
	// the plugin emitted at expand time as the source of truth —
	// they reflect the operator's HCL settings (and our defaults
	// when omitted).
	user := "postgres"
	db := "postgres"
	primaryOrdinal := 0

	if ctx.ControllerURL != "" {
		client := newControllerClient(ctx.ControllerURL)

		spec, specErr := client.fetchSpec("statefulset", scope, name)
		if specErr == nil {
			u, d, _ := readUserDBPort(spec)
			user, db = u, d

			config, _ := client.fetchConfig(scope, name)
			primaryOrdinal = readCurrentPrimaryOrdinal(config)
		}
	}

	target := primaryOrdinal
	if hasReplica {
		target = replica
	}

	containerName := containerNameFor(scope, name, target)

	if !containerExists(containerName) {
		return fmt.Errorf("container %s not found on this host (psql must run on the same host as the target pod)", containerName)
	}

	running, err := containerIsRunning(containerName)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", containerName, err)
	}

	if !running {
		return fmt.Errorf("container %s is not running (start it with `vd start %s` or check pod health)",
			containerName, refOrName(scope, name))
	}

	// Compose docker exec argv. -i + -t for interactive REPL.
	// We hand this to the controller via an `exec_local`
	// dispatch action — the controller passes it through to the
	// CLI, which runs it locally with the operator's TTY
	// attached. (syscall.Exec'ing docker from inside the plugin
	// process doesn't work because plugin's stdio is the HTTP
	// dispatch envelope, not the operator's terminal.)
	command := []string{
		"docker", "exec", "-it",
		containerName,
		"psql", "-U", user, "-d", db,
	}

	command = append(command, psqlArgs...)

	out := dispatchOutput{
		Message: fmt.Sprintf("postgres %s: opening psql in %s (user=%s, db=%s)",
			refOrName(scope, name), containerName, user, db),
		Actions: []dispatchAction{
			{
				Type:    "exec_local",
				Scope:   scope,
				Name:    name,
				Command: command,
			},
		},
	}

	return writeDispatchOutput(out)
}

// parsePsqlFlags pulls --replica and the `--` passthrough boundary
// out of args. Everything before `--` (minus --replica) becomes
// positional or psql-flag args; everything after `--` is verbatim
// psql passthrough.
//
// We let -c "<sql>" through verbatim to psql — no special parsing.
// Operators write `vd postgres:psql ref -c "SELECT 1"` and the -c
// + value end up as positional[1:] which we forward to psql.
func parsePsqlFlags(args []string) (positional []string, replica int, hasReplica bool, psqlArgs []string) {
	seenDoubleDash := false

	for i := 0; i < len(args); i++ {
		a := args[i]

		if seenDoubleDash {
			psqlArgs = append(psqlArgs, a)
			continue
		}

		switch {
		case a == "--":
			seenDoubleDash = true

		case a == "--replica" || a == "-r":
			if i+1 >= len(args) {
				continue
			}

			n, err := parseInt(args[i+1])
			if err == nil {
				replica = n
				hasReplica = true
				i++
			}

		case len(a) > 10 && a[:10] == "--replica=":
			n, err := parseInt(a[10:])
			if err == nil {
				replica = n
				hasReplica = true
			}

		case a == "-h" || a == "--help":
			// handled by hasHelpFlag in caller

		case a == "-c", a == "--command":
			// psql native flag; capture it AND the next arg as
			// passthrough so they reach docker exec verbatim.
			psqlArgs = append(psqlArgs, a)
			if i+1 < len(args) {
				psqlArgs = append(psqlArgs, args[i+1])
				i++
			}

		default:
			positional = append(positional, a)
		}
	}

	return positional, replica, hasReplica, psqlArgs
}

// parseInt is a thin wrapper around strconv.Atoi to avoid
// importing strconv in this file's surface (the only other
// numeric parsing happens via psql's native -c which doesn't
// touch ints). Returns the int + a non-nil err on parse fail
// without surfacing strconv's specific error type.
func parseInt(s string) (int, error) {
	n := 0

	if s == "" {
		return 0, fmt.Errorf("empty number")
	}

	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("not a number: %q", s)
		}

		n = n*10 + int(r-'0')
	}

	return n, nil
}
