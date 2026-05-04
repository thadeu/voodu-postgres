// `vd postgres:link` and `vd postgres:unlink` — wire a postgres
// provider's connection URL into a consumer's config bucket.
//
// # URL emission rules (mirror voodu-redis with postgres URL syntax)
//
//   - replicas == 1: ONE DATABASE_URL pointing at the primary
//     (db-0.scope.voodu). No DATABASE_READ_URL — there's no
//     standby to read from.
//
//   - replicas > 1, no --reads: ONE DATABASE_URL pointing at the
//     primary (db-0). Apps that don't care about read scaling
//     get the simple shape.
//
//   - replicas > 1, --reads: TWO URLs:
//        DATABASE_URL       → primary (db-0)
//        DATABASE_READ_URL  → multi-host libpq URL spanning every
//                             standby (db-1, db-2, ...) with
//                             target_session_attrs=any. Postgres
//                             libpq tries hosts left-to-right;
//                             clients can shuffle on their side
//                             for round-robin distribution.
//
// # Linked-consumers tracking
//
// Every link writes the consumer ref into the provider's
// POSTGRES_LINKED_CONSUMERS bucket key. cmdNewPassword reads
// this list to auto-refresh every consumer's URL when the
// password rotates — operator doesn't have to re-run link by
// hand after rotation.

package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const (
	// consumerWriteEnvVar is the env-var name postgres:link sets
	// on the consumer for the primary URL. Standard across the
	// postgres ecosystem (Rails, Django, Node pg, sqlalchemy
	// all check DATABASE_URL).
	consumerWriteEnvVar = "DATABASE_URL"

	// consumerReadEnvVar is the read-pool URL emitted alongside
	// DATABASE_URL when the provider has replicas > 1 and the
	// consumer passed --reads. Convention follows the dual-URL
	// pattern AWS RDS exposes (writer endpoint + reader endpoint).
	consumerReadEnvVar = "DATABASE_READ_URL"

	// linkedConsumersKey is the config-bucket key on the provider
	// that lists every consumer currently linked. Maintained by
	// cmdLink (add) and cmdUnlink (remove); used by cmdNewPassword
	// to auto-refresh every linked consumer's URL after rotation.
	//
	// Format: comma-separated `scope/name` refs. Unscoped
	// consumers ride as `/<name>` (leading slash) so the parser
	// can recover both halves from the comma-split.
	linkedConsumersKey = "POSTGRES_LINKED_CONSUMERS"
)

const linkHelp = `vd postgres:link — wire a postgres provider's URL into a consumer's config bucket.

USAGE
  vd postgres:link <provider-scope/name> <consumer-scope/name> [--reads]

ARGUMENTS
  <provider-scope/name>   The postgres resource declared in HCL.
  <consumer-scope/name>   The app/job/cronjob receiving the URL.

FLAGS
  --reads                 Also emit DATABASE_READ_URL spanning
                          standbys (libpq multi-host with
                          target_session_attrs=any). Requires the
                          provider to have replicas > 1; on a single
                          replica DATABASE_READ_URL echoes the primary.

EMITS (CONSUMER)
  DATABASE_URL            postgres://user:pw@db-0.scope.voodu:5432/db
                          → primary (write endpoint, ordinal 0)
  DATABASE_READ_URL       postgres://user:pw@db-1.scope.voodu:5432,
                          db-2.scope.voodu:5432/db?target_session_attrs=any
                          → multi-host pool of standbys (only with --reads)

EMITS (PROVIDER, internal)
  POSTGRES_LINKED_CONSUMERS  comma-separated consumer refs, used by
                             vd postgres:new-password to auto-refresh
                             every linked consumer after rotation.

EXAMPLES
  # Single primary → consumer gets DATABASE_URL only
  vd postgres:link clowk-lp/db clowk-lp/web

  # Cluster (replicas=3) → consumer gets primary + read-pool
  vd postgres:link clowk-lp/db clowk-lp/web --reads

  # Multiple consumers can share one provider
  vd postgres:link clowk-lp/db clowk-lp/web
  vd postgres:link clowk-lp/db clowk-lp/worker
  vd postgres:link clowk-lp/db clowk-lp/migrations-job

NOTES
  - Re-linking the same (provider, consumer) is idempotent — same URL
    emitted, no spurious config_set.
  - Triggers a restart on the consumer so the app reconnects with
    the new env vars.
  - The provider isn't restarted on link (tracking write skips
    restart fan-out).
`

// cmdLink wires a postgres provider to a consumer.
func cmdLink() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(linkHelp)
		return nil
	}

	positional, readsOnly := parseLinkFlags(args)

	if len(positional) < 2 {
		return fmt.Errorf("usage: vd postgres:link <provider-scope/name> <consumer-scope/name> [--reads]")
	}

	providerRef := positional[0]
	consumerRef := positional[1]

	provScope, provName := splitScopeName(providerRef)
	consScope, consName := splitScopeName(consumerRef)

	if provName == "" {
		return fmt.Errorf("invalid provider ref %q (expected scope/name)", providerRef)
	}

	if consName == "" {
		return fmt.Errorf("invalid consumer ref %q (expected scope/name)", consumerRef)
	}

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	client := newControllerClient(ctx.ControllerURL)

	provSpec, err := client.fetchSpec("statefulset", provScope, provName)
	if err != nil {
		return fmt.Errorf("describe %s: %w", providerRef, err)
	}

	provConfig, err := client.fetchConfig(provScope, provName)
	if err != nil {
		return fmt.Errorf("config get %s: %w", providerRef, err)
	}

	urls, err := buildLinkURLs(provScope, provName, provSpec, provConfig, readsOnly)
	if err != nil {
		return fmt.Errorf("build URLs: %w", err)
	}

	consumerKV := map[string]string{consumerWriteEnvVar: urls.WriteURL}

	if urls.ReadURL != "" {
		consumerKV[consumerReadEnvVar] = urls.ReadURL
	}

	actions := []dispatchAction{{
		Type:  "config_set",
		Scope: consScope,
		Name:  consName,
		KV:    consumerKV,
	}}

	// Track the consumer on the provider's bucket — cmdNewPassword
	// reads this to auto-refresh URLs after rotation. Idempotent
	// on re-link (consumer already in the list → same string).
	updatedList := addLinkedConsumer(provConfig, consScope, consName)

	actions = append(actions, dispatchAction{
		Type:  "config_set",
		Scope: provScope,
		Name:  provName,
		KV:    map[string]string{linkedConsumersKey: updatedList},
		// Provider doesn't need a restart for tracking changes —
		// the value isn't consumed by postgres itself, only by
		// the plugin's password rotation flow.
		SkipRestart: true,
	})

	out := dispatchOutput{
		Message: linkedMessage(provScope, provName, consScope, consName, urls, readsOnly),
		Actions: actions,
	}

	return writeDispatchOutput(out)
}

const unlinkHelp = `vd postgres:unlink — remove a postgres link from a consumer.

USAGE
  vd postgres:unlink <provider-scope/name> <consumer-scope/name>

EMITS (CONSUMER)
  Unsets DATABASE_URL + DATABASE_READ_URL — consumer can no longer
  reach the database. Triggers a restart so the app picks up the
  removal.

EMITS (PROVIDER, internal)
  Removes consumer from POSTGRES_LINKED_CONSUMERS — future password
  rotations skip this consumer.

EXAMPLES
  vd postgres:unlink clowk-lp/db clowk-lp/web

NOTES
  - No-op when the consumer wasn't linked (removes nothing, returns
    success).
  - Doesn't drop the postgres database / user — purely about env vars
    on the consumer side.
`

// cmdUnlink removes a link.
func cmdUnlink() error {
	args := os.Args[2:]

	if hasHelpFlag(args) {
		fmt.Print(unlinkHelp)
		return nil
	}

	positional, _ := parseLinkFlags(args)

	if len(positional) < 2 {
		return fmt.Errorf("usage: vd postgres:unlink <provider-scope/name> <consumer-scope/name>")
	}

	provScope, provName := splitScopeName(positional[0])
	consScope, consName := splitScopeName(positional[1])

	if provName == "" || consName == "" {
		return fmt.Errorf("invalid refs (expected scope/name): %v", positional)
	}

	ctx, err := readInvocationContext()
	if err != nil {
		return err
	}

	client := newControllerClient(ctx.ControllerURL)

	provConfig, err := client.fetchConfig(provScope, provName)
	if err != nil {
		return fmt.Errorf("config get %s: %w", positional[0], err)
	}

	actions := []dispatchAction{{
		Type:  "config_unset",
		Scope: consScope,
		Name:  consName,
		Keys:  []string{consumerWriteEnvVar, consumerReadEnvVar},
	}}

	updatedList := removeLinkedConsumer(provConfig, consScope, consName)

	actions = append(actions, dispatchAction{
		Type:        "config_set",
		Scope:       provScope,
		Name:        provName,
		KV:          map[string]string{linkedConsumersKey: updatedList},
		SkipRestart: true,
	})

	out := dispatchOutput{
		Message: fmt.Sprintf("unlinked %s/%s from %s/%s — DATABASE_URL[%s] removed", provScope, provName, consScope, consName, joinNonEmpty(", ", consumerWriteEnvVar, consumerReadEnvVar)),
		Actions: actions,
	}

	return writeDispatchOutput(out)
}

// parseLinkFlags walks args and pulls out the optional --reads
// flag. Positional args (provider + consumer refs) come back in
// order. Flag stays out of positional slice.
func parseLinkFlags(args []string) (positional []string, reads bool) {
	for _, a := range args {
		switch a {
		case "--reads":
			reads = true
		case "-h", "--help":
			// already handled by hasHelpFlag in caller
		default:
			positional = append(positional, a)
		}
	}

	return positional, reads
}

type linkURLs struct {
	WriteURL string
	ReadURL  string
}

// buildLinkURLs composes the URL(s) the consumer needs based on
// the provider's replicas count and the --reads flag.
//
// Spec inputs (from the statefulset's env, which the plugin
// emitted at expand time):
//
//   - POSTGRES_USER / POSTGRES_DB → URL userinfo + path
//   - PG_PORT → URL port
//
// Config inputs (from the bucket):
//
//   - POSTGRES_PASSWORD → URL userinfo password (URL-encoded
//     so special chars don't break parsing — auto-gen is
//     hex-only so this is defensive only)
func buildLinkURLs(scope, name string, spec map[string]any, config map[string]string, readsOnly bool) (linkURLs, error) {
	user, db, port := readUserDBPort(spec)
	password := config[passwordKey]

	if password == "" {
		return linkURLs{}, fmt.Errorf("provider has no POSTGRES_PASSWORD in config bucket — has the resource been applied?")
	}

	replicas := readReplicas(spec)

	primaryFQDN := composePrimaryFQDN(scope, name, 0)
	writeURL := composePostgresURL(user, password, primaryFQDN, port, db, "")

	out := linkURLs{WriteURL: writeURL}

	if !readsOnly {
		return out, nil
	}

	if replicas <= 1 {
		// --reads on a single-replica resource: no standbys to
		// read from. Echo the primary URL so consumer code
		// assuming dual-URL doesn't crash.
		out.ReadURL = writeURL
		return out, nil
	}

	// Multi-host libpq URL spanning every standby (ordinals
	// 1..replicas-1, primary at 0 excluded). target_session_attrs=any
	// lets libpq accept whichever host responds first; clients
	// who want strict standby-only can use prefer-standby (postgres
	// 14+) by overriding via env var.
	hosts := make([]string, 0, replicas-1)
	for i := 1; i < replicas; i++ {
		standbyFQDN := composePrimaryFQDN(scope, name, i)
		hosts = append(hosts, fmt.Sprintf("%s:%d", standbyFQDN, port))
	}

	out.ReadURL = composePostgresURL(user, password, strings.Join(hosts, ","), 0, db, "target_session_attrs=any")

	return out, nil
}

// composePostgresURL builds a postgres:// URL. Password is
// URL-encoded for safety (handles non-hex passwords from
// operator-supplied vd config set).
//
// When port is 0, hosts is treated as already-formatted
// "host:port[,host:port]..." (the multi-host case) — no extra
// :port suffix. When port > 0, hosts is a single hostname and
// the URL wears :port directly.
func composePostgresURL(user, password, hosts string, port int, db, query string) string {
	pwEnc := url.QueryEscape(password)

	var hostPart string
	if port > 0 {
		hostPart = fmt.Sprintf("%s:%d", hosts, port)
	} else {
		hostPart = hosts
	}

	out := fmt.Sprintf("postgres://%s:%s@%s/%s", user, pwEnc, hostPart, db)

	if query != "" {
		out += "?" + query
	}

	return out
}

// readUserDBPort pulls POSTGRES_USER / POSTGRES_DB / PG_PORT
// from the statefulset's env. Defaults match composeStatefulset
// Defaults so a manifest without explicit overrides still
// resolves cleanly.
func readUserDBPort(spec map[string]any) (user, db string, port int) {
	user, db, port = "postgres", "postgres", 5432

	env, ok := spec["env"].(map[string]any)
	if !ok {
		return
	}

	if v, ok := env["POSTGRES_USER"].(string); ok && v != "" {
		user = v
	}

	if v, ok := env["POSTGRES_DB"].(string); ok && v != "" {
		db = v
	}

	if v, ok := env["PG_PORT"].(string); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			port = n
		}
	}

	return
}

// readReplicas pulls the replicas count from the statefulset
// spec. Defaults to 1 when missing. Float64 (JSON decode) is
// tolerated alongside int.
func readReplicas(spec map[string]any) int {
	v, ok := spec["replicas"]
	if !ok {
		return 1
	}

	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	}

	return 1
}

// addLinkedConsumer returns a comma-separated list with the
// (scope, name) added if not already present. Order-preserving —
// new entry appended at end, existing entries unchanged.
func addLinkedConsumer(config map[string]string, scope, name string) string {
	current := parseLinkedConsumers(config)
	ref := refOrName(scope, name)

	for _, existing := range current {
		if existing == ref {
			// Already linked — return unchanged. Idempotent
			// re-link is a no-op write of the same value.
			return strings.Join(current, ",")
		}
	}

	current = append(current, ref)

	return strings.Join(current, ",")
}

// removeLinkedConsumer returns a comma-separated list with
// (scope, name) removed if present. No-op when consumer wasn't
// in the list.
func removeLinkedConsumer(config map[string]string, scope, name string) string {
	current := parseLinkedConsumers(config)
	ref := refOrName(scope, name)

	out := make([]string, 0, len(current))

	for _, existing := range current {
		if existing != ref {
			out = append(out, existing)
		}
	}

	return strings.Join(out, ",")
}

// parseLinkedConsumers turns the comma-separated bucket value
// into a slice of refs. Empty bucket / empty string returns nil.
func parseLinkedConsumers(config map[string]string) []string {
	raw := config[linkedConsumersKey]
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")

	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}

	return out
}

// linkedMessage formats the operator-facing one-liner.
func linkedMessage(provScope, provName, consScope, consName string, urls linkURLs, readsOnly bool) string {
	prov := refOrName(provScope, provName)
	cons := refOrName(consScope, consName)

	if urls.ReadURL != "" {
		return fmt.Sprintf("linked %s → %s — set DATABASE_URL (primary) + DATABASE_READ_URL (%d standby pool)",
			prov, cons, strings.Count(urls.ReadURL, ",")+1)
	}

	if readsOnly {
		return fmt.Sprintf("linked %s → %s — set DATABASE_URL (single replica; --reads echoed primary)", prov, cons)
	}

	return fmt.Sprintf("linked %s → %s — set DATABASE_URL", prov, cons)
}

// joinNonEmpty joins non-empty strings with sep. Tiny helper for
// the unlinked-message formatter.
func joinNonEmpty(sep string, parts ...string) string {
	out := make([]string, 0, len(parts))

	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}

	return strings.Join(out, sep)
}
