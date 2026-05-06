// Wrapper script the plugin emits as an asset key. The statefulset
// is configured to exec this as its command instead of the official
// docker-entrypoint.sh directly — we wire `include_dir = 'conf.d'`
// into the postgres-managed postgresql.conf, symlink our
// bind-mounted overrides into PGDATA/conf.d/, then hand off to the
// official entrypoint which honours POSTGRES_USER/PASSWORD/DB env
// vars to do initdb on first boot.
//
// Why a wrapper instead of just `command = ["postgres", "-c",
// "..."]`:
//
//   - Initdb is destructive + one-shot. Re-implementing it would
//     duplicate every edge case the official image already handles
//     (locale, encoding, host-based auth bootstrap, password setup).
//     The wrapper delegates to docker-entrypoint.sh which is the
//     officially supported entry.
//
//   - postgresql.conf includes are how operators tune postgres in
//     production (postgresql.conf.d/ is the postgres convention).
//     The default postgres image's postgresql.conf doesn't have an
//     `include_dir` directive enabled, so we add it idempotently on
//     first boot. Subsequent boots see the line is already there.
//
//   - Asset bind-mounts come 0644 read-only — we can't drop files
//     into PGDATA/conf.d/ directly because PGDATA is a volume_claim.
//     Instead we mount the override file in /etc/postgresql/ and
//     symlink it into PGDATA/conf.d/ at boot. Same source-of-truth,
//     postgres reads through the symlink.
//
// The wrapper is dependency-light (bash, no extras): the postgres
// official image is debian-slim and ships bash but not wget/curl.
// We don't need those — no HTTP calls in the entrypoint path
// (failover hooks come later in M-P5).

package main

const (
	// entrypointAssetKey + pgOverridesAssetKey are the keys the
	// asset manifest uses for the wrapper script and the
	// postgres.conf overrides file. Both files live in the same
	// asset emission so a single asset reference resolves both —
	// the statefulset's volumes list mounts each via its own
	// `${asset.<scope>.<name>.<key>}` reference.
	entrypointAssetKey  = "entrypoint"
	pgOverridesAssetKey = "pg_overrides_conf"

	// Mount paths inside the container.
	//
	//   - /usr/local/bin is on PATH in the official postgres
	//     image, the conventional spot for site-installed scripts.
	//
	//   - /etc/postgresql is the canonical location for
	//     postgres-related configuration files on Debian.
	//
	// Plugin-side conf files (M-P2 wal_archive, future M-P3
	// streaming-replication, etc.) use the prefix `voodu-NN-`
	// where NN is a 2-digit ordering hint:
	//
	//	voodu-00-* — earliest (plugin defaults)
	//	voodu-99-overrides.conf — last (operator pg_config wins)
	//
	// The wrapper symlinks every voodu-*.conf into PGDATA/conf.d/
	// in alphabetical order so postgres's "last config wins"
	// rule lets operator overrides trump plugin defaults
	// cleanly. Adding a new plugin-side conf needs zero changes
	// to the wrapper.
	entrypointMountPath  = "/usr/local/bin/voodu-postgres-entrypoint"
	pgOverridesMountPath = "/etc/postgresql/conf.d/voodu-99-overrides.conf"
)

// renderEntrypointScript produces the bash wrapper bytes the asset
// carries. Pure function — same inputs always yield the same bytes,
// so the asset digest stays stable across replays unless something
// changes in the script itself.
//
// Behaviour summary:
//
//  1. Always-rewrite-bootstrap: ensure `include_dir = 'conf.d'`
//     exists in PGDATA/postgresql.conf. Idempotent — `grep -q` then
//     append-once. On first boot postgresql.conf doesn't exist yet
//     (initdb hasn't run), so this is skipped; the wrapper restarts
//     itself implicitly via exec docker-entrypoint.sh and the next
//     boot picks up.
//
//  2. Symlink the bind-mounted overrides file into PGDATA/conf.d/
//     so the relative `include_dir` resolves. Asset-mounted files
//     live at /etc/postgresql/voodu-overrides.conf (read-only); the
//     symlink is rewritten each boot.
//
//  3. Exec docker-entrypoint.sh — the official image entry. It does
//     initdb on first boot (honouring POSTGRES_USER/PASSWORD/DB env
//     vars), wires pg_hba defaults, then execs postgres in
//     foreground with the right flags.
//
// First-boot flow (postgresql.conf doesn't exist yet):
//
//	→ wrapper skips include_dir wiring (no file to edit)
//	→ wrapper symlinks overrides into conf.d/ (path doesn't exist
//	  yet, but ln -sf creates the symlink target — the dir itself
//	  is created via mkdir -p)
//	→ exec docker-entrypoint.sh postgres -p $PG_PORT
//	  → initdb fires, creates PGDATA/postgresql.conf
//	  → postgres starts WITHOUT include_dir (it didn't exist when
//	    docker-entrypoint.sh forked)
//	→ on the NEXT boot (operator restart, or rolling apply), the
//	  wrapper sees PGDATA/postgresql.conf, appends include_dir,
//	  symlink already in place, postgres now honours overrides.
//
// The first-boot postgres process therefore runs without the
// pg_config overrides applied. That's deliberate: applying them
// during initdb (which is itself parameterised by env vars) risks
// fighting the official entrypoint. The overrides take effect on
// the SECOND boot, which is what `vd apply` produces immediately
// after the first when an operator declares pg_config.
//
// If this behaviour proves surprising in practice, M-P2 can swap
// to a "two-phase first-boot" approach: wrapper waits for postgres
// to be ready, edits postgresql.conf, sends SIGHUP. For M-P1 the
// simpler always-append model is enough.
func renderEntrypointScript() string {
	return `#!/bin/bash
# voodu-postgres entrypoint — role-aware wrapper. pod-0 (default
# primary) runs as primary; pod-N (N != PG_PRIMARY_ORDINAL) runs
# pg_basebackup on first boot then enters recovery mode as a
# streaming standby. Generated by voodu-postgres plugin. Do not
# edit in place; the plugin re-emits this asset on every apply.
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data/pgdata}"
PG_PORT="${PG_PORT:-5432}"
ORDINAL="${VOODU_REPLICA_ORDINAL:-0}"
PRIMARY_ORDINAL="${PG_PRIMARY_ORDINAL:-0}"

log() { echo "voodu-postgres: $*" >&2; }

# Role branch: are we primary or standby?
#
# Primary: pod ordinal == PG_PRIMARY_ORDINAL. Runs the official
# docker-entrypoint.sh which fires initdb on first boot. Init
# scripts under /docker-entrypoint-initdb.d/ create the
# replication user and add the pg_hba entry.
#
# Standby: pod ordinal != PG_PRIMARY_ORDINAL. On first boot
# (PGDATA empty), waits for primary then pg_basebackup clones
# its data dir; touches standby.signal so postgres comes up in
# recovery mode using primary_conninfo from voodu-50-streaming.conf.
if [ "$ORDINAL" != "$PRIMARY_ORDINAL" ]; then
    log "role=STANDBY (ordinal=$ORDINAL, primary=$PRIMARY_ORDINAL)"

    if [ ! -f "$PGDATA/PG_VERSION" ]; then
        # First-boot bootstrap: clone primary via pg_basebackup.
        # PGDATA must be empty for pg_basebackup to write into it
        # (the volume_claim is fresh on first apply; the docker
        # entrypoint hasn't created subdirs yet).
        primary_fqdn="${PG_NAME:?PG_NAME is required}-${PRIMARY_ORDINAL}${PG_SCOPE_SUFFIX:-.voodu}"
        repl_user="${PG_REPLICATION_USER:?PG_REPLICATION_USER is required}"
        repl_pass="${PG_REPLICATION_PASSWORD:?PG_REPLICATION_PASSWORD is required}"

        log "first boot — waiting for primary $primary_fqdn:$PG_PORT to accept replication"

        export PGPASSWORD="$repl_pass"

        # Wait for primary AND replication user (init script
        # might still be running CREATE USER). pg_isready alone
        # would race; we explicitly try pg_basebackup with a
        # short timeout and retry on failure.
        attempts=0
        until pg_isready -h "$primary_fqdn" -p "$PG_PORT" -t 5 >/dev/null 2>&1; do
            attempts=$((attempts + 1))
            if [ "$attempts" -gt 60 ]; then
                log "primary $primary_fqdn:$PG_PORT not ready after 60 attempts (~5min) — giving up"
                exit 1
            fi
            log "primary not ready yet (attempt $attempts) — sleeping 5s"
            sleep 5
        done

        log "running pg_basebackup -h $primary_fqdn -p $PG_PORT -U $repl_user -D $PGDATA"
        # -X stream: include WAL needed to make the backup
        #   self-consistent (no archive_command dependency)
        # -P: progress to stderr (visible in docker logs)
        # No -R: we don't want pg_basebackup's auto-generated
        #   recovery conf — primary_conninfo comes from
        #   voodu-50-streaming.conf via include_dir, which lets
        #   the operator override it via pg_config.
        if ! pg_basebackup -h "$primary_fqdn" -p "$PG_PORT" -U "$repl_user" -D "$PGDATA" -X stream -P; then
            log "pg_basebackup FAILED — leaving PGDATA empty so the next boot retries"
            exit 1
        fi

        unset PGPASSWORD

        # standby.signal tells postgres to come up in recovery
        # mode. Without this, postgres would treat the cloned
        # data dir as a primary and refuse to follow upstream
        # WAL.
        touch "$PGDATA/standby.signal"
        log "pg_basebackup complete + standby.signal armed"
    elif [ ! -f "$PGDATA/standby.signal" ]; then
        # M-P5 split-brain guard. PGDATA is populated AND we're
        # configured as a standby (ORDINAL != PRIMARY_ORDINAL),
        # but standby.signal is absent — this means PGDATA was
        # last used as a primary. Almost certainly the result of
        # a failover (PG_PRIMARY_ORDINAL flipped to a different
        # ordinal) where this pod USED to be the primary.
        #
        # If we proceeded, postgres would boot WITHOUT
        # standby.signal, ignore primary_conninfo, and start as
        # a writable primary — running in parallel with the new
        # primary on the same DNS round-robin. That's split-brain;
        # data divergence happens within seconds.
        #
        # Refuse to start. Operator runs vd postgres:rejoin to
        # pg_rewind this pod against the current primary and arm
        # standby.signal, then a normal restart resumes streaming.
        # Compose the volume name the way voodu's statefulset
        # reconciler does: voodu-<scope>-<name>-data-<ordinal>.
        # Bash inside the container can't see env from the host
        # but PG_NAME / PG_SCOPE_SUFFIX / ORDINAL are all set; the
        # scope segment is reconstructed from PG_SCOPE_SUFFIX
        # (".scope.voodu" → "scope") to give the operator an
        # exact copy-pasteable command.
        SCOPE_SEG="${PG_SCOPE_SUFFIX#.}"
        SCOPE_SEG="${SCOPE_SEG%.voodu}"

        if [ -n "$SCOPE_SEG" ]; then
            CONTAINER_NAME="${SCOPE_SEG}-${PG_NAME}.${ORDINAL}"
            VOLUME_NAME="voodu-${SCOPE_SEG}-${PG_NAME}-data-${ORDINAL}"
            REJOIN_REF="${SCOPE_SEG}/${PG_NAME}"
        else
            CONTAINER_NAME="${PG_NAME}.${ORDINAL}"
            VOLUME_NAME="voodu-${PG_NAME}-data-${ORDINAL}"
            REJOIN_REF="${PG_NAME}"
        fi

        log "ERROR: pod ordinal=$ORDINAL is configured as STANDBY (primary=$PRIMARY_ORDINAL),"
        log "       but PGDATA was last used as primary (no standby.signal found)."
        log "       This usually means a failover happened and this pod must be"
        log "       reattached to the new primary — OR the previous run left a"
        log "       stale volume after 'vd delete' (which preserves data)."
        log ""
        log "       Recovery (try in order):"
        log ""
        log "       1) Try pg_rewind against the current primary (preserves data"
        log "          divergence is small):"
        log ""
        log "             vd postgres:rejoin $REJOIN_REF --replica $ORDINAL"
        log ""
        log "       2) If pg_rewind fails (data divergence too large, or volume"
        log "          is from a previous session), wipe this pod's data volume."
        log "          The reconciler will recreate the pod and the wrapper will"
        log "          bootstrap a fresh standby via pg_basebackup:"
        log ""
        log "             docker rm -f $CONTAINER_NAME"
        log "             docker volume rm $VOLUME_NAME"
        log "             vd apply"
        log ""
        log "       3) For clean teardowns next time, use 'vd delete --prune'"
        log "          which removes data volumes too (default 'vd delete'"
        log "          preserves them for accidental-deletion safety)."
        exit 1
    else
        log "PGDATA already populated + standby.signal present — resuming as standby (subsequent boot)"
    fi

    # Runtime primary_conninfo. Written every boot so post-promote
    # standbys point at the CURRENT primary without needing the
    # asset re-rendered. The asset (voodu-50-streaming.conf, bind-
    # mounted read-only) only carries static replication tunables;
    # the dynamic line lives here in a writable PGDATA file.
    #
    # postgresql.conf will include_if_exists this file (wired by
    # the include_dir step below); include_if_exists is read AFTER
    # include_dir so this runtime value wins on conflict.
    primary_fqdn="${PG_NAME:?PG_NAME is required}-${PRIMARY_ORDINAL}${PG_SCOPE_SUFFIX:-.voodu}"
    repl_user="${PG_REPLICATION_USER:?PG_REPLICATION_USER is required}"
    repl_pass="${PG_REPLICATION_PASSWORD:?PG_REPLICATION_PASSWORD is required}"

    # Single-quote escaping in postgresql.conf string syntax: '' is
    # a literal '. We never put a real ' in passwords (auto-gen is
    # hex-only), but the substitution is correct in case of operator
    # overrides.
    escaped_pass="${repl_pass//\'/\'\'}"
    runtime_conf="$PGDATA/voodu-runtime.conf"

    cat > "$runtime_conf" <<EOF
# Runtime primary_conninfo — rewritten by voodu-postgres wrapper at every
# boot. Do not edit; changes are clobbered on next pod restart. To
# change the primary, use 'vd postgres:promote <ref> --replica <N>'.
primary_conninfo = 'host=${primary_fqdn} port=5432 user=${repl_user} password=${escaped_pass} application_name=voodu-postgres-standby'
EOF

    log "wrote primary_conninfo for $primary_fqdn to $runtime_conf"
else
    log "role=PRIMARY (ordinal=$ORDINAL)"

    # On the rare path where this pod was previously a STANDBY
    # (post-promote: pg_promote() removed standby.signal but the
    # runtime conf still has primary_conninfo from when this pod
    # was a follower), drop the file. primary_conninfo without
    # standby.signal is harmless but visually confusing in
    # describe / pg_settings output.
    runtime_conf="$PGDATA/voodu-runtime.conf"
    if [ -f "$runtime_conf" ]; then
        rm -f "$runtime_conf"
        log "removed stale $runtime_conf (this pod is now PRIMARY)"
    fi
fi

# Common from here: wire include_dir, symlink confs, hand off
# to the official postgres entrypoint.

# 1. Ensure include_dir is wired into the postgres-managed
#    postgresql.conf. Absolute path — points OUTSIDE PGDATA so
#    the asset bind-mounts (read-only) don't get touched by the
#    official docker-entrypoint's chown -R on PGDATA.
#
#    Skipped on first boot when the file doesn't exist yet
#    (initdb hasn't run on primary; pg_basebackup populated PGDATA
#    on standby — postgresql.conf inherited from primary already
#    has the directive if a previous boot wired it).
#
#    Idempotent — appended only once.
#
#    Cleanup: if a previous version of this entrypoint wrote
#    'include_dir = ...conf.d...' (PGDATA-relative path), comment
#    it out so we don't get duplicate parsing. Combined with the
#    absolute path below, postgres reads only the new location.
if [ -f "$PGDATA/postgresql.conf" ]; then
    if grep -q "^include_dir = 'conf.d'$" "$PGDATA/postgresql.conf"; then
        log "deprecating legacy include_dir = 'conf.d' (PGDATA-relative) — voodu-postgres now uses /etc/postgresql/conf.d"
        sed -i "s|^include_dir = 'conf.d'$|# include_dir = 'conf.d'  # deprecated by voodu-postgres|" "$PGDATA/postgresql.conf"
    fi

    if ! grep -q "^include_dir = '/etc/postgresql/conf.d'" "$PGDATA/postgresql.conf"; then
        log "wiring include_dir = '/etc/postgresql/conf.d' into postgresql.conf"
        {
            echo ""
            echo "# Added by voodu-postgres — picks up bind-mounted voodu-*.conf"
            echo "include_dir = '/etc/postgresql/conf.d'"
        } >> "$PGDATA/postgresql.conf"
    fi

    # Runtime override file — primary_conninfo for standbys, written
    # by the wrapper from env vars at every boot. Wired AFTER the
    # asset include_dir so its values win on conflict (the asset
    # doesn't set primary_conninfo today, but this future-proofs).
    # PGDATA-relative path → resolves to $PGDATA/voodu-runtime.conf.
    if ! grep -q "^include_if_exists = 'voodu-runtime.conf'" "$PGDATA/postgresql.conf"; then
        log "wiring include_if_exists = 'voodu-runtime.conf' (runtime primary_conninfo) into postgresql.conf"
        {
            echo ""
            echo "# Added by voodu-postgres — runtime primary_conninfo (wrapper-written at boot)"
            echo "include_if_exists = 'voodu-runtime.conf'"
        } >> "$PGDATA/postgresql.conf"
    fi

    # Remove stale symlinks from previous voodu-postgres versions
    # that put them under PGDATA/conf.d/. They point to bind-mounts
    # that may no longer exist (e.g. voodu-00-wal-archive.conf was
    # dropped when WAL archive subsystem was removed). Even if they
    # resolve, chown -R on PGDATA follows the symlinks and chokes
    # on the read-only mount targets. Just nuke the dir.
    if [ -d "$PGDATA/conf.d" ]; then
        log "removing legacy $PGDATA/conf.d (config now lives at /etc/postgresql/conf.d)"
        rm -rf "$PGDATA/conf.d"
    fi
fi

# 2. (Symlink dance removed.) Bind-mounts now land directly under
#    /etc/postgresql/conf.d/, which postgres reads via the
#    absolute include_dir set above. No PGDATA mutation = no
#    chown -R surprises = no stale-symlink rot across plugin
#    versions.

# 3. Ensure /backups (host bind-mount for pg_dump output) is owned
#    by postgres. Bind-mount of a fresh host dir lands as root:root
#    inside the container; pg_dump runs as postgres uid 999 and
#    needs write access. Idempotent — chown is a no-op if perms
#    already match. Skipped silently if /backups doesn't exist
#    (operator overrode the bind-mount or pre-1.0 deploys).
if [ -d /backups ]; then
    chown postgres:postgres /backups 2>/dev/null || \
        log "warn: could not chown /backups (continuing — pg_dump may fail with permission denied)"
fi

# 4. Hand off to the official postgres entrypoint. It honours
#    POSTGRES_USER/PASSWORD/DB env vars to run initdb on first
#    boot of the primary, wires pg_hba defaults, runs init
#    scripts under /docker-entrypoint-initdb.d/, and execs
#    postgres in foreground. On standbys it skips initdb
#    (PG_VERSION exists post-basebackup) and just execs postgres
#    which detects standby.signal and starts in recovery.
exec docker-entrypoint.sh postgres -p "$PG_PORT"
`
}
