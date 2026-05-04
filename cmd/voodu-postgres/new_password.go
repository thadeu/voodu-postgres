// `vd postgres:new-password` — rotate the superuser password.
//
// Generates a fresh 32-byte hex string and emits config_set
// actions to:
//
//   1. Persist the new POSTGRES_PASSWORD on the postgres bucket.
//      The next reconcile sees it via Config and re-renders the
//      asset (entrypoint env carries the new value, streaming.conf
//      gets a new primary_conninfo password). Rolling restart
//      cycles every pod onto the new password.
//
//   2. Refresh DATABASE_URL on every linked consumer. The new
//      password is baked into each consumer's connection string
//      so the app's DB pool reconnects with the new password
//      after its next restart (which the config_set itself
//      triggers via the controller's standard restart fan-out).
//
// Flag: --no-restart suppresses the restart fan-out on the
// provider's bucket. Useful when the operator wants to stage the
// rotation (run new-password, manually verify the new value,
// then restart on demand). Consumer URL refreshes still fire
// (their config_set triggers their own restart) so apps don't
// keep a stale password.

package main

import (
	"fmt"
	"os"
)

const newPasswordHelp = `vd postgres:new-password — rotate the superuser password.

USAGE
  vd postgres:new-password <postgres-scope/name> [--no-restart]

ARGUMENTS
  <postgres-scope/name>   The postgres resource to rotate.

FLAGS
  --no-restart            Skip the rolling restart on the postgres
                          pods. Useful for staged rotation: persist
                          the new password, validate, then restart
                          on demand. Consumer URLs still refresh
                          (their own restart triggers).

EFFECT
  1. Generate a fresh 32-byte hex password
  2. config_set POSTGRES_PASSWORD on the provider bucket
     → rolling restart of postgres pods (unless --no-restart)
     → next expand re-renders streaming.conf with new
       primary_conninfo password (standbys reconnect)
  3. config_set DATABASE_URL[/_READ_URL] on every linked consumer
     → consumer restart picks up new URL → app reconnects

EXAMPLES
  # Standard rotation
  vd postgres:new-password clowk-lp/db

  # Staged rotation (verify before restart)
  vd postgres:new-password clowk-lp/db --no-restart
  vd config clowk-lp/db get POSTGRES_PASSWORD   # confirm value
  vd restart clowk-lp/db                        # apply when ready

NOTES
  - Auto-refreshes EVERY consumer in POSTGRES_LINKED_CONSUMERS.
    Operator doesn't have to re-run vd postgres:link by hand.
  - Replication user password (POSTGRES_REPLICATION_PASSWORD) is
    separate and NOT rotated by this command. It rarely needs
    rotation; if you must, use vd config <ref> unset POSTGRES_REPLICATION_PASSWORD
    + vd apply (next expand auto-gens fresh).
  - Old password is overwritten — no rollback path. Capture
    POSTGRES_PASSWORD beforehand if you need to revert.
`

// cmdNewPassword rotates the superuser password.
func cmdNewPassword() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(newPasswordHelp)
		return nil
	}

	positional, noRestart := parseNewPasswordFlags(args)

	if len(positional) < 1 {
		return fmt.Errorf("usage: vd postgres:new-password <postgres-scope/name> [--no-restart]")
	}

	provScope, provName := splitScopeName(positional[0])
	if provName == "" {
		return fmt.Errorf("invalid provider ref %q (expected scope/name)", positional[0])
	}

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	client := newControllerClient(ctx.ControllerURL)

	provSpec, err := client.fetchSpec("statefulset", provScope, provName)
	if err != nil {
		return fmt.Errorf("describe %s: %w", positional[0], err)
	}

	provConfig, err := client.fetchConfig(provScope, provName)
	if err != nil {
		return fmt.Errorf("config get %s: %w", positional[0], err)
	}

	freshPw, err := generatePassword()
	if err != nil {
		return fmt.Errorf("generate password: %w", err)
	}

	// Provider-side action: persist new password. SkipRestart
	// honoured per --no-restart.
	actions := []dispatchAction{{
		Type:        "config_set",
		Scope:       provScope,
		Name:        provName,
		KV:          map[string]string{passwordKey: freshPw},
		SkipRestart: noRestart,
	}}

	// Consumer-side fan-out: re-emit DATABASE_URL[/_READ_URL]
	// for every linked consumer with the fresh password baked
	// in. We synthesise a new bucket map (provConfig with the
	// fresh password) so buildLinkURLs picks up the new value
	// instead of the stale one. URL renderer is the same one
	// cmdLink uses, so consumer get the same shape they got at
	// link time (single primary or primary + read pool).
	updatedConfig := cloneConfig(provConfig)
	updatedConfig[passwordKey] = freshPw

	for _, ref := range parseLinkedConsumers(provConfig) {
		consScope, consName := splitScopeName(ref)
		if consName == "" {
			// Malformed entry — skip but don't fail the rotation
			// (an old bucket might have stale garbage; we don't
			// want one bad entry blocking the password change).
			continue
		}

		// Use the same readsOnly setting we'd want for this
		// consumer. We don't track per-consumer flags in the
		// linked list (M-P4 minimal); always emit the dual-URL
		// shape if replicas > 1, single URL otherwise. Conservative
		// — covers the common case; --reads consumers already
		// got DATABASE_READ_URL at link time and we re-emit it
		// here, ones that linked without --reads only need
		// DATABASE_URL but we send DATABASE_READ_URL too, which
		// they harmlessly ignore.
		urls, err := buildLinkURLs(provScope, provName, provSpec, updatedConfig, true)
		if err != nil {
			return fmt.Errorf("build URLs for consumer %s: %w", ref, err)
		}

		consumerKV := map[string]string{consumerWriteEnvVar: urls.WriteURL}
		if urls.ReadURL != "" {
			consumerKV[consumerReadEnvVar] = urls.ReadURL
		}

		actions = append(actions, dispatchAction{
			Type:  "config_set",
			Scope: consScope,
			Name:  consName,
			KV:    consumerKV,
		})
	}

	consumerCount := len(actions) - 1

	out := dispatchOutput{
		Message: fmt.Sprintf("rotated password for %s — refreshed %d linked consumer(s)%s",
			refOrName(provScope, provName),
			consumerCount,
			map[bool]string{true: " (provider restart suppressed via --no-restart)", false: ""}[noRestart],
		),
		Actions: actions,
	}

	return writeDispatchOutput(out)
}

// parseNewPasswordFlags pulls the --no-restart flag out of args.
func parseNewPasswordFlags(args []string) (positional []string, noRestart bool) {
	for _, a := range args {
		switch a {
		case "--no-restart":
			noRestart = true
		case "-h", "--help":
			// already handled
		default:
			positional = append(positional, a)
		}
	}

	return positional, noRestart
}

// cloneConfig returns a shallow copy of the bucket so we can
// mutate without affecting the caller's reference.
func cloneConfig(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))

	for k, v := range in {
		out[k] = v
	}

	return out
}

