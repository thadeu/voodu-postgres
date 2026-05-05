// M-P3: streaming replication. Adds the second + third asset
// files (streaming_conf + init_replication_sh) and the
// replication user/password lifecycle. The wrapper script in
// entrypoint.go gains a role branch — pod-0 (default primary
// ordinal) runs as primary, pod-N+ as standby.
//
// # The pattern
//
//   - Primary: pod whose VOODU_REPLICA_ORDINAL == PG_PRIMARY_ORDINAL.
//     Runs the official docker-entrypoint.sh which fires initdb on
//     first boot. Init script creates the replication user and
//     appends a pg_hba entry allowing replication conns.
//
//   - Standby: pod whose VOODU_REPLICA_ORDINAL != PG_PRIMARY_ORDINAL.
//     On first boot (PGDATA empty), waits for primary to be ready
//     then runs `pg_basebackup` to clone the primary's data dir.
//     Touches standby.signal so postgres comes up in recovery mode
//     reading WAL from primary via primary_conninfo.
//
// # Why no replication slots
//
// Slots prevent the primary from purging WAL a standby still
// needs — but an abandoned slot (standby died) keeps the WAL
// growing forever, eating primary disk. With manual failover (no
// HA orchestrator), an abandoned slot is a real risk.
//
// We use `wal_keep_size = 1GB` instead — bounded retention with
// a simple recovery path: a standby that falls behind beyond the
// keep window gets recreated via pg_basebackup (the wrapper's
// "PGDATA empty → basebackup" branch handles it on a fresh
// volume). Operator can override via pg_config.
//
// # Replication user lifecycle
//
// Same pattern as superuser POSTGRES_PASSWORD: read from config
// bucket on every expand, generate-and-persist on first apply.
// HCL doesn't accept replication_password override (auto-gen
// only) — operator who needs a deterministic value pre-sets via
// `vd config <ref> set POSTGRES_REPLICATION_PASSWORD=...` before
// the first apply.

package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	// replicationPasswordKey is the config bucket key for the
	// auto-generated replication-role password. Distinct from
	// passwordKey (POSTGRES_PASSWORD = superuser) because the
	// two roles serve different purposes and rotating one
	// shouldn't roll the other.
	replicationPasswordKey = "POSTGRES_REPLICATION_PASSWORD"

	// streamingConfAssetKey + mount path. Slot 50- in the
	// alphabetical sort: between wal_archive (00-) and operator
	// pg_config (99-). Operator overrides still win on conflict
	// because their conf comes later in the include_dir scan.
	streamingConfAssetKey  = "streaming_conf"
	streamingConfMountPath = "/etc/postgresql/conf.d/voodu-50-streaming.conf"

	// initReplicationAssetKey + mount path. Lands in
	// /docker-entrypoint-initdb.d/ which the official postgres
	// image runs ONCE during initdb (first-boot only). The
	// 00_ prefix puts it before any operator-supplied init
	// scripts they might add via the volumes block.
	initReplicationAssetKey  = "init_replication_sh"
	initReplicationMountPath = "/docker-entrypoint-initdb.d/00_create_replication.sh"
)

// resolveOrGenerateReplicationPassword mirrors
// resolveOrGeneratePassword but for the replication role. No HCL
// override path — only bucket-reuse or generate. See the package
// header for the rationale.
//
// Returns (password, isNew, err). isNew=true means the caller
// must emit a config_set action persisting the value, otherwise
// the next expand sees an empty bucket and re-generates,
// rotating the password without the operator asking.
func resolveOrGenerateReplicationPassword(config map[string]string) (password string, isNew bool, err error) {
	if existing, ok := config[replicationPasswordKey]; ok && existing != "" {
		return existing, false, nil
	}

	buf := make([]byte, passwordEntropyBytes)

	if _, err := rand.Read(buf); err != nil {
		return "", false, fmt.Errorf("read random for replication password: %w", err)
	}

	return hex.EncodeToString(buf), true, nil
}

// composePrimaryFQDN returns the DNS name standbys use to reach
// the primary. Voodu0 resolves <name>-<ordinal>.<scope>.voodu (or
// <name>-<ordinal>.voodu when scope is empty) — same convention
// voodu-redis uses for replicaof targeting.
//
// Pure function, called both at expand time (to bake the FQDN
// into primary_conninfo) and conceptually mirrored at runtime
// inside the wrapper script (which composes the same FQDN from
// PG_NAME + PG_SCOPE_SUFFIX env vars).
func composePrimaryFQDN(scope, name string, primaryOrdinal int) string {
	suffix := ".voodu"
	if scope != "" {
		suffix = "." + scope + ".voodu"
	}

	return fmt.Sprintf("%s-%d%s", name, primaryOrdinal, suffix)
}

// composeScopeSuffix returns the DNS suffix the wrapper uses to
// build per-pod FQDNs. ".scope.voodu" with scope, ".voodu"
// without. Mirrors composePrimaryFQDN's suffix logic — used in
// the env so wrapper bash doesn't need scope-injection at render
// time.
func composeScopeSuffix(scope string) string {
	if scope == "" {
		return ".voodu"
	}

	return "." + scope + ".voodu"
}

// renderStreamingConf produces the bytes the plugin emits as
// the streaming_conf asset file. Same on every pod — postgres
// honours `primary_conninfo` only when standby.signal exists in
// PGDATA, so the primary ignores it cleanly.
//
// Settings:
//
//   - max_wal_senders = 10  (postgres default in 13+; explicit
//     here so the operator pg_config doesn't have to remember)
//   - wal_keep_size = 1GB   (bounded retention for slow standbys
//     without slot footgun; see header)
//   - hot_standby = on      (no-op on primary; lets standbys
//     accept read queries)
//   - primary_conninfo      (host=<primary-fqdn> user=replicator
//     password=<inline> — file is 0644 inside the container,
//     readable only by the postgres user)
//
// The password is inlined into primary_conninfo. Single-quoted
// per the postgres-conf escape rule (doubled quote on embedded
// quote — same defense as renderPgOverridesConf).
func renderStreamingConf(scope, name string, primaryOrdinal int, replicationUser, replicationPassword string) string {
	primaryFQDN := composePrimaryFQDN(scope, name, primaryOrdinal)

	conninfo := fmt.Sprintf(
		"host=%s port=5432 user=%s password=%s application_name=voodu-postgres-standby",
		primaryFQDN, replicationUser, replicationPassword,
	)

	var b strings.Builder

	b.Grow(512)
	b.WriteString("# generated by voodu-postgres — streaming replication defaults\n")
	b.WriteString("# (operator overrides via pg_config = { ... } win)\n")

	// Alphabetical order — same digest-stability reasoning as
	// renderPgOverridesConf.
	b.WriteString("hot_standby = on\n")
	b.WriteString("max_wal_senders = 10\n")
	b.WriteString("primary_conninfo = ")
	b.WriteString(quotePgString(conninfo))
	b.WriteByte('\n')
	b.WriteString("wal_keep_size = '1GB'\n")

	return b.String()
}

// renderInitReplicationSH produces the shell script the asset
// drops into /docker-entrypoint-initdb.d/. The official postgres
// image runs every *.sh in that directory once after initdb on
// first boot — perfect home for the replication-role bootstrap
// (creating the user requires postgres to be initialised but
// before it accepts external connections).
//
// What the script does:
//
//   1. psql CREATE USER <replicator> WITH REPLICATION ENCRYPTED
//      PASSWORD '<pw>'.
//   2. Append `host replication <user> all scram-sha-256` to
//      pg_hba.conf so standbys can authenticate.
//
// The replication user is read from PG_REPLICATION_USER and
// PG_REPLICATION_PASSWORD env vars (set on the statefulset by
// composeStatefulsetDefaults). Single-quote SQL escape applied
// to the password defensively even though auto-gen is hex-only;
// future operator-supplied passwords could include any chars.
//
// Standby pods never run this script — they pg_basebackup from
// the primary and inherit pg_authid + pg_hba state.
func renderInitReplicationSH() string {
	return `#!/bin/bash
# voodu-postgres init script — creates the replication role and
# wires the pg_hba entry that lets standbys authenticate.
# Generated by voodu-postgres plugin. Runs ONCE during initdb
# on the primary's first boot; standbys never see this (they
# clone the primary's authoritative state via pg_basebackup).
set -euo pipefail

REPL_USER="${PG_REPLICATION_USER:-replicator}"
REPL_PASS="${PG_REPLICATION_PASSWORD:?PG_REPLICATION_PASSWORD is required}"

# Defensive: SQL-escape any embedded single quote (auto-gen pw
# is hex-only, but operator-supplied passwords could vary).
ESCAPED_PASS=$(printf '%s' "$REPL_PASS" | sed "s/'/''/g")

echo "voodu-postgres: creating replication user $REPL_USER" >&2
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
    CREATE USER "$REPL_USER" WITH REPLICATION ENCRYPTED PASSWORD '$ESCAPED_PASS';
SQL

echo "voodu-postgres: wiring pg_hba.conf entry for replication" >&2
{
    echo ""
    echo "# Added by voodu-postgres — allow streaming replication conns"
    echo "host replication $REPL_USER all scram-sha-256"
} >> "$PGDATA/pg_hba.conf"
`
}
