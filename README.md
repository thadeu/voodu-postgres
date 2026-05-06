# voodu-postgres

Voodu plugin that expands a `postgres { … }` HCL block into a production-ready postgres cluster: 1 primary + N streaming-replication standbys, auto-generated passwords, dual `DATABASE_URL` injection (writer + reader endpoints, RDS-style).

Bare block produces a single hardened primary; `replicas = 3` flips it into a cluster with `pg_basebackup`-cloned standbys and `primary_conninfo`-driven WAL streaming.

## Table of contents

- [Quick start](#quick-start)
- [Configuration](#configuration)
  - [HCL contract](#hcl-contract)
  - [Plugin defaults](#plugin-defaults)
  - [Customising postgresql.conf via `pg_config`](#customising-postgresqlconf-via-pg_config)
  - [Custom image (extensions baked)](#custom-image-extensions-baked)
  - [Statefulset passthrough](#statefulset-passthrough)
- [High availability — streaming replication](#high-availability--streaming-replication)
  - [Cluster shape](#cluster-shape)
  - [Cold-start sequence](#cold-start-sequence)
  - [Write vs read endpoints](#write-vs-read-endpoints)
  - [Promote a standby](#promote-a-standby)
  - [Rejoining the old primary](#rejoining-the-old-primary)
- [Backup automation](#backup-automation)
  - [`vd pg:backups` (Heroku-style)](#vd-pgbackups-heroku-style-recommended)
  - [Retention (`--keep` / `--max-age`)](#retention---keep----max-age)
  - [`vd postgres:backup` / `vd postgres:restore` (legacy)](#vd-postgresbackup--vd-postgresrestore-legacy-pg_basebackup)
- [Connecting via `psql`](#connecting-via-psql)
- [Real-world examples](#real-world-examples)
  - [Single primary + Rails app](#single-primary--rails-app)
  - [3-replica cluster + Rails MultiDB](#3-replica-cluster--rails-multidb)
  - [Custom image with pgvector](#custom-image-with-pgvector)
  - [External access for DBeaver / TablePlus](#external-access-for-dbeaver--tableplus)
- [Plugin reference](#plugin-reference)
  - [Commands](#commands)
  - [Asset files emitted](#asset-files-emitted)
  - [Statefulset env vars](#statefulset-env-vars)
  - [Bucket keys (config)](#bucket-keys-config)
  - [Repo layout](#repo-layout)
  - [Development](#development)
- [Install & upgrade](#install--upgrade)
- [Storage](#storage)
- [License](#license)

---

## Quick start

```hcl
# voodu.hcl
postgres "clowk-lp" "db" {
  image    = "postgres:16"
  replicas = 3                  # 1 primary + 2 standbys
}
```

Apply:

```bash
vd apply -f voodu.hcl
```

Plugin emits:

- **`asset "clowk-lp" "db"`** — 4 files: bash entrypoint wrapper, postgresql.conf overrides, streaming-replication conf, replication-user init script
- **`statefulset "clowk-lp" "db"`** — 3 pods (`db-0..db-2`), 1 volume claim (`data`, per-pod), 4 asset bind-mounts, host bind-mount `/opt/voodu/backups/clowk-lp/db` → `/backups`, env vars wiring everything
- **2 config_set actions** (first apply only) — persists auto-gen `POSTGRES_PASSWORD` + `POSTGRES_REPLICATION_PASSWORD`

Wire your app:

```bash
vd postgres:link clowk-lp/db clowk-lp/web --reads
# → DATABASE_URL    on web (primary)
# → DATABASE_READ_URL on web (read pool spanning standbys)
```

Inspect:

```bash
vd postgres:info clowk-lp/db
```

---

## Configuration

### HCL contract

Every field is optional. Defaults shown.

```hcl
postgres "scope" "name" {
  image    = "postgres:latest"            # operator typically pins major (postgres:16)
  replicas = 1                            # 1 = single primary; >=2 = cluster
  database = "postgres"                   # initdb arg
  user     = "postgres"                   # superuser
  port     = 5432                         # listen port

  password         = ""                   # default empty → auto-gen 32-byte hex
                                          # operator-set value lives in HCL plaintext
  replication_user = "replicator"         # separate role, REPLICATION attribute only

  initdb_locale   = "C.UTF-8"             # initdb --locale
  initdb_encoding = "UTF8"                # initdb --encoding

  # postgresql.conf overrides (key/value, anti-injection guarded)
  pg_config = {
    max_connections = 200
    shared_buffers  = "256MB"
    log_connections = true
  }

  # Extensions: parsed + validated, but NOT auto-installed (M-P4
  # ships `vd postgres:exec` for explicit install).
  extensions = ["pgvector", "pg_stat_statements"]

  # Statefulset passthrough — anything the statefulset accepts
  # flows through unchanged. See "Statefulset passthrough" below.
}
```

Print the complete spec at any time:

```bash
voodu-postgres defaults
```

### Plugin defaults

Bare block — `postgres "data" "db" {}` — emits a statefulset with:

- `image = "postgres:latest"` (override to pin major in prod)
- `replicas = 1` (single primary)
- `command = ["bash", "/usr/local/bin/voodu-postgres-entrypoint"]` (role-aware wrapper)
- `ports = ["5432"]` (loopback by default — `vd postgres:expose` to publish)
- `volume_claims`: `data` at `/var/lib/postgresql/data` (per-pod, statefulset-style)
- `volumes`: host bind-mount `/opt/voodu/backups/<scope>/<name>` → `/backups` (where `vd pg:backups:capture` writes)
- `health_check = "pg_isready -U postgres -d postgres -p 5432"`
- env: `POSTGRES_USER` / `POSTGRES_DB` / `POSTGRES_PASSWORD` / `POSTGRES_INITDB_ARGS` / `PGDATA` / `PG_PORT` / `PG_NAME` / `PG_SCOPE_SUFFIX` / `PG_PRIMARY_ORDINAL` / `PG_REPLICATION_USER` / `PG_REPLICATION_PASSWORD`

### Customising postgresql.conf via `pg_config`

The `pg_config` map renders into a `voodu-99-overrides.conf` file that postgres loads via `include_dir`. "Last assignment wins" — operator overrides trump plugin defaults from `voodu-50-streaming.conf`.

```hcl
postgres "clowk-lp" "db" {
  pg_config = {
    # tuning
    max_connections      = 200
    shared_buffers       = "1GB"
    work_mem             = "16MB"
    effective_cache_size = "3GB"
    random_page_cost     = 1.1     # SSD storage

    # logging
    log_connections    = true
    log_disconnections = true
    log_min_messages   = "warning"
    log_statement      = "ddl"
  }
}
```

Type rules:

| HCL type | Renders as |
|---|---|
| `int` / `int64` / whole `float` | unquoted integer |
| fractional `float` | unquoted decimal |
| `bool` | `on` / `off` (postgres convention) |
| `string` | `'...'` (single-quoted, embedded `'` escaped as `''`) |

Keys validated as `^[a-z][a-z0-9_]*$` — anti-injection guard so an operator-controlled key can't smuggle additional directives.

### Custom image (extensions baked)

For extensions not in the official `postgres:` image (pgvector, postgis), point `image` at a custom build:

```hcl
postgres "clowk-lp" "db" {
  image = "ghcr.io/clowk/pg16-pgvector:1.2.0"
}
```

Two ways to produce that image:

**(A) Pre-built and pushed to a registry** — your CI runs `docker build` + `docker push`. HCL just references the tag.

**(B) Inline build via voodu** — the controller's statefulset kind accepts `dockerfile` / `workdir` / `path` / `lang { }`. voodu apply streams your source over SSH, runs `docker build`, tags `clowk-lp-db:latest`, deploys:

```hcl
postgres "clowk-lp" "db" {
  workdir    = "infra/postgres"
  dockerfile = "Dockerfile.pg"
  replicas   = 3

  lang {
    name = "generic"
    build_args = {
      PG_MAJOR = "16"
    }
  }
}
```

`infra/postgres/Dockerfile.pg`:

```dockerfile
FROM postgres:16
RUN apt-get update && apt-get install -y postgresql-16-pgvector \
    && rm -rf /var/lib/apt/lists/*
```

The same image must work as primary AND standby (standbys clone primary via `pg_basebackup`, no separate image).

Extensions: enable inside the database via your app's migration system (Rails `enable_extension :pgvector`, Django RunSQL, etc.) OR with a one-off `vd postgres:psql <ref> -c "CREATE EXTENSION IF NOT EXISTS pgvector"`.

### Statefulset passthrough

Anything the statefulset kind accepts flows through unchanged. Common cases:

```hcl
postgres "clowk-lp" "db" {
  image = "postgres:16"

  # extra env vars (operator-supplied, merged with plugin's)
  env = {
    TZ        = "America/Sao_Paulo"
    PGAPPNAME = "clowk-lp"
  }

  # config from shared buckets (e.g. shared AWS creds)
  env_from = ["aws/cli", "monitoring/secrets"]

  # custom health check (overrides plugin's pg_isready default)
  health_check = "pg_isready -U appuser -d appdata -p 5432"

  # kernel-level CPU/memory caps via cgroups
  resources {
    limits {
      cpu    = "2"      # 2 cpus (or "500m" for 0.5 cpu, k8s-style millicores)
      memory = "4Gi"    # 4 GiB (binary; "4G" for decimal SI; "1024" plain bytes)
    }
  }

  # build-mode (instead of image = "...") — Dockerfile + workdir + lang { }
  # see "Custom image (extensions baked)" below
}
```

Plugin-owned fields (`database`, `user`, `password`, `port`, `initdb_locale`, `initdb_encoding`, `pg_config`, `extensions`, `replication_user`) are stripped from the merged spec before emitting — they don't leak to the statefulset wire shape.

#### Resource limits

`resources { limits { ... } }` translates to `docker run --cpus=<n> --memory=<bytes>` on every replica pod. Two layers of constraint apply to postgres:

1. **Container-level (this block)** — kernel cap via cgroups. OOM-kills the postgres process if it exceeds memory limit. Sane budget = host RAM × 0.7 ÷ replicas, leaving headroom for other workloads.
2. **App-level (`pg_config`)** — postgres-internal allocations. Should be SMALLER than the container cap so postgres self-limits before the kernel kills it.

Recommended pairing for a `memory = "4Gi"` container limit:

```hcl
resources {
  limits {
    cpu    = "2"
    memory = "4Gi"
  }
}

pg_config = {
  shared_buffers       = "1GB"     # ~25% of container limit
  effective_cache_size = "3GB"     # ~75% of container limit
  work_mem             = "16MB"    # per-query, watch for high concurrency
  max_connections      = 200
}
```

Value formats accepted (k8s parity):

| Type | Form | Examples |
|---|---|---|
| CPU | decimal | `"2"`, `"1.5"`, `"0.25"` |
| CPU | millicores | `"500m"` (= 0.5), `"100m"` (= 0.1) |
| Memory | binary (1024-based, preferred) | `"4Gi"`, `"512Mi"`, `"256Ki"` |
| Memory | decimal SI (1000-based) | `"4G"`, `"500M"`, `"1500K"` |
| Memory | plain bytes | `"4294967296"` |

Omit `resources { }` entirely or leave individual fields empty for "no limit" — docker daemon defaults apply (effectively unlimited until host RAM is exhausted).

---

## High availability — streaming replication

### Cluster shape

```hcl
postgres "clowk-lp" "db" {
  image    = "postgres:16"
  replicas = 3
}
```

Produces 3 pods with stable identity:

| Pod | Role | DNS | Per-pod data volume |
|---|---|---|---|
| `clowk-lp-db.0` | primary | `db-0.clowk-lp.voodu` | `voodu-clowk-lp-db-data-0` |
| `clowk-lp-db.1` | standby | `db-1.clowk-lp.voodu` | `voodu-clowk-lp-db-data-1` |
| `clowk-lp-db.2` | standby | `db-2.clowk-lp.voodu` | `voodu-clowk-lp-db-data-2` |

Plus the round-robin shared alias `db.clowk-lp.voodu` resolving to all 3 pods (use sparingly — primary writes need pod-0 specifically).

Streaming config baked into `voodu-50-streaming.conf`:

```ini
hot_standby      = on
max_wal_senders  = 10
primary_conninfo = 'host=db-0.clowk-lp.voodu port=5432 user=replicator password=<auto-hex> application_name=voodu-postgres-standby'
wal_keep_size    = '1GB'
```

No replication slots — `wal_keep_size = 1GB` is more predictable. Standbys that fall behind beyond 1GB get re-cloned via `pg_basebackup` on next pod restart (wrapper detects empty PGDATA and re-runs basebackup).

### Cold-start sequence

```
t=0  pod-0 (primary) boots
     → official docker-entrypoint.sh detects empty PGDATA → initdb
     → /docker-entrypoint-initdb.d/00_create_replication.sh runs:
        - CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD '...'
        - append `host replication replicator all scram-sha-256` to pg_hba.conf
     → postgres ready

t=2s pod-1, pod-2 (standbys) boot in parallel
     → wrapper detects ORDINAL != PRIMARY_ORDINAL
     → loops on pg_isready waiting for db-0
     → pg_basebackup -h db-0.clowk-lp.voodu -X stream → clones data dir
     → touch standby.signal (postgres comes up in recovery mode)
     → docker-entrypoint.sh sees PG_VERSION exists → skips initdb
     → postgres starts, reads primary_conninfo from voodu-50-streaming.conf
     → connects to primary, starts streaming WAL
```

Standby retry: if primary isn't ready (race during first cluster apply), the wrapper retries `pg_isready` up to 60 times × 5s sleep (~5min budget) before giving up. In practice the primary's initdb finishes well within that window.

### Write vs read endpoints

`vd postgres:link <postgres> <consumer> --reads` emits two URLs on the consumer:

| Var | Targets | Use case |
|---|---|---|
| `DATABASE_URL` | `db-0.scope.voodu` (primary, ordinal 0) | All writes |
| `DATABASE_READ_URL` | `db-1.scope.voodu,db-2.scope.voodu,...` (libpq multi-host) | Read queries |

`DATABASE_READ_URL` uses postgres's native multi-host syntax with `target_session_attrs=any` — libpq tries hosts left-to-right; clients can shuffle the list for round-robin distribution.

Without `--reads`, only `DATABASE_URL` is emitted (single primary endpoint). Apps without read-replica logic just use that.

### Promote a standby

`vd postgres:promote` is a 2-step manual failover. Plugin owns ALL the postgres-side SQL — operator never types it:

```bash
# Step 1: PROMOTE — plugin runs lag check + pg_promote() + flips
#         PG_PRIMARY_ORDINAL + refreshes DATABASE_URL on every
#         linked consumer + rolling restart.
vd postgres:promote clowk-lp/db --replica 1

# Step 2: REJOIN — recover the OLD primary as a standby of the new one.
#         Required because the rolling restart hits pod-0 with
#         PG_PRIMARY_ORDINAL=1 but PGDATA still in primary state — the
#         wrapper's split-brain guard catches it and refuses to start.
vd postgres:rejoin clowk-lp/db --replica 0
```

What `promote` does internally:

1. **Lag check** — queries `pg_stat_replication` on the current primary. Refuses if any standby is behind (`max_lag_bytes > 0`) unless `--force` is passed.
2. **pg_promote** — runs `SELECT pg_promote(true, 60)` inside the target container. Postgres exits recovery mode and becomes the new primary.
3. **Wait** — polls `pg_is_in_recovery()` until it returns `false` (max 30s).
4. **Bucket flip** — sets `PG_PRIMARY_ORDINAL=N` so the next expand re-renders `streaming.conf` with the new primary FQDN.
5. **Refresh consumers** — every linked consumer's `DATABASE_URL[/_READ_URL]` is rewritten to point at the new primary; rolling restart picks up the change.

Flags:
- `--force` — promote despite replication lag (operator accepts data loss). Required when the current primary is unreachable (lag check itself would error).
- `--no-restart` — flip the bucket + URLs but skip the rolling restart on the postgres pods. Useful for staged promotions (verify, then restart on demand).

Legacy alias: `vd postgres:failover` still works (same handler), kept for backward-compat with scripts using the old name.

### Rejoining the old primary

`vd postgres:rejoin` runs `pg_rewind` against the current primary inside a one-shot container that shares the target pod's data volume (`docker run --volumes-from`). Steps:

1. `docker stop <container>` — pg_rewind requires the target offline
2. `docker run --rm --volumes-from <container> postgres:<ver> pg_rewind --target-pgdata=… --source-server="host=<primary> user=replicator …"`
3. `touch standby.signal` in the data dir
4. `docker start <container>` — pod boots as standby, picks up streaming from the new primary

**When `pg_rewind` fails**: divergence too large (WAL recycled past the divergence point) yields `could not find previous WAL record at…`. Fallback:

```bash
vd delete clowk-lp/db --replica 0 --prune    # wipes the data volume
vd apply -f voodu.hcl                         # wrapper bootstraps fresh standby via pg_basebackup
```

The wrapper script's split-brain guard prevents accidental writes during this window — if a pod is configured as standby (`ORDINAL != PG_PRIMARY_ORDINAL`) but PGDATA was last used as primary (no `standby.signal`), it exits 1 with a clear message pointing at `vd postgres:rejoin`.

---

## Backup automation

### `vd pg:backups` (Heroku-style, recommended)

`pg_dump` snapshots managed by the plugin. The host directory `/opt/voodu/backups/<scope>/<name>/` is bind-mounted at `/backups` inside every pod; backup files live there as `bNNN-<timestamp>.dump` (custom format, restorable via `pg_restore`).

#### Pre-flight (once per host)

```bash
sudo mkdir -p /opt/voodu/backups/clowk-lp/db
# chown is handled by the entrypoint wrapper at boot — no manual chown needed
```

#### Capture a snapshot

`pg:backups:capture` is **detached by default** — it spawns a sibling Docker container that runs `pg_dump` and returns the new backup ID immediately. The dump survives SSH drops, terminal closure, and Ctrl-C of the `vd` command.

```bash
# Default: detached, returns ID immediately
vd pg:backups:capture clowk-lp/db
# postgres clowk-lp/db: backup b008 capturing in background (container clowk-lp-db-backup-b008, source: ordinal 0). track via: vd pg:backups clowk-lp/db  |  vd pg:backups:logs clowk-lp/db b008 --follow

# Foreground with rolling progress
vd pg:backups:capture clowk-lp/db --follow

# From a standby (offloads work) + follow
vd pg:backups:capture clowk-lp/db --from-replica 1 --follow
```

Output with `--follow`:

```
capture: pg_dump → /backups/b008-20260505T143000Z.dump (db=postgres, user=postgres; raw size 487 MB, estimated dump ~243 MB, container clowk-lp-db-backup-b008)
  [3s] 12.0 MB written (~5% of estimate)
  [10s] 68.0 MB written (~28% of estimate)
  [25s] 156.0 MB written (~64% of estimate)
  [45s] 218.0 MB written (~90% of estimate)
done: 251.3 MB in 52s
postgres clowk-lp/db: backup b008 captured (b008-20260505T143000Z.dump, 251.3 MB, source: ordinal 0, elapsed 52s)
```

The `% of estimate` is a **hint, not a real percent**. pg_dump doesn't expose a true progress signal — the plugin queries `pg_database_size()` upfront and assumes ~50% compression (varies wildly with data shape). When the estimate is clearly wrong (>200%), the suffix is dropped silently and you just see bytes-written + elapsed. The trustworthy numbers are `<X> MB written` and `[<elapsed>s]`.

#### Container shape (capture internals)

Each capture spawns a one-shot container named `<scope>-<name>-backup-bNNN` that:

- Reuses the source pod's image (so `pg_dump` is available)
- Shares the source pod's network namespace via `--network container:<source>` (postgres reachable at `127.0.0.1`)
- Bind-mounts the same `/opt/voodu/backups/<scope>/<name>` host path at `/backups`
- Runs `pg_dump -F c -Z 6 -f /backups/<file>`; on non-zero exit, removes the partial file

Auto-prune: each `:capture` removes any **exited** sibling backup containers from prior runs (free up the bNNN slot for re-use after `:delete`). Running ones are never touched — concurrent captures with different IDs work fine.

> **Roadmap:** Once the controller exposes a runtime-dispatch action (`apply_manifest`), capture will migrate internally to emit a voodu `job` manifest instead of running `docker run` directly. UX stays identical; plumbing gets cleaner. See [the controller proposal](#) for status.

#### List (with status)

```bash
vd pg:backups clowk-lp/db
# === Backups for clowk-lp/db
#
#   ID     Status     Created at            Size
#   b007   complete   2026-05-04T02:00:00Z  245.3 MB
#   b008   running    2026-05-05T14:30:00Z  87.0 MB

# JSON for jq pipelines
vd pg:backups clowk-lp/db -o json | jq '.backups[] | {id, status, size_bytes}'
```

The list merges two data sources:

- **Files** in `/opt/voodu/backups/<scope>/<name>/` (read via host bind-mount — works without a running pod) → `complete` captures.
- **Backup containers** matched via `docker ps -a --filter name=<scope>-<name>-backup-` → in-progress (`running`) and recently-failed (`failed`).

Status reflects which side a given `bNNN` appears on:

| Status | Container | File | Meaning |
|---|---|---|---|
| `running` | running | partial / absent | capture in flight |
| `complete` | exited 0 (or pruned) | present | done, restorable |
| `failed` | exited != 0 | absent (wrapper removed) | needs investigation via `pg:backups:logs` |

#### Stream live logs

```bash
# One-shot read of accumulated container output
vd pg:backups:logs clowk-lp/db b008

# Tail in real-time (blocks until the container exits)
vd pg:backups:logs clowk-lp/db b008 --follow
```

Wraps `docker logs [-f] <scope>-<name>-backup-bNNN`. Works for `running`, `complete`, and `failed` captures — until the next `:capture` auto-prunes exited siblings.

#### Restore from a backup

```bash
# Local backup ID
vd pg:backups:restore clowk-lp/db b007 --yes

# http(s) URL — e.g. an S3 presigned URL pointing at a pg_dump file
vd pg:backups:restore clowk-lp/db https://my-bucket.s3.amazonaws.com/db.dump?... --yes
```

For URL inputs, the plugin downloads the file to a host temp dir, `docker cp`s it into `/tmp/<random>.dump` inside the primary, runs `pg_restore`, then cleans up both sides.

Runs `pg_restore --clean --if-exists --no-owner --no-privileges` against the primary's database. **Destructive of database content** (drops + recreates every object the dump touches), but **non-destructive of the cluster** — pods stay up, standbys replicate the restored state via streaming WAL.

Compared to the legacy `vd postgres:restore`:

| | `pg:backups:restore` (Heroku-style) | `postgres:restore` (legacy) |
|---|---|---|
| Source | `pg_dump` custom format (in `/backups/`) | `pg_basebackup` tar (operator-supplied path) |
| Destructive of | database content (objects in the dump) | entire cluster (PGDATA on every pod) |
| Cluster downtime | none — primary stays up | full — every pod stopped, wiped, restarted |
| Cross-version | yes (pg_dump portable) | no (pg_basebackup is exact bit copy) |
| Cross-host | yes (pg_dump portable) | no (same major version + SSL state) |

Pick `pg:backups:restore` for routine "rollback to last night" scenarios; `postgres:restore` for "salvage from a tar I have on a laptop after total host loss."

#### Download a backup file

Ship a backup off-host (laptop, S3 via host CLI, another voodu host):

```bash
vd pg:backups:download clowk-lp/db b007
# → ./b007-20260507T020000Z.dump in CWD

vd pg:backups:download clowk-lp/db b007 --to /srv/archive/db.dump
```

Bytes are copied verbatim via `docker cp`; the resulting file is a portable `pg_dump` custom format ready for `pg_restore` anywhere.

#### Delete a backup

```bash
vd pg:backups:delete clowk-lp/db b007 --yes
```

Removes `/backups/<filename>` inside the pod (host bind-mount → file disappears from disk). Sequence IDs **don't** renumber — deleting `b005` leaves a gap, next capture is `b008` (max+1 of remaining). Operator's responsibility to ensure no off-host sync (S3, rsync) is mid-flight against the file.

#### Schedule (paste-and-apply HCL helper)

`pg:backups:schedule` prints the cronjob block ready to paste into `voodu.hcl`:

```bash
vd pg:backups:schedule clowk-lp/db --at "0 3 * * *" --from-replica 1
```

```hcl
# Add this to your voodu.hcl, then run `vd apply`:

cronjob "clowk-lp" "db-backup" {
  schedule = "0 3 * * *"
  image    = "ghcr.io/clowk/voodu-cli:latest"

  command = ["bash", "-c", "vd pg:backups:capture clowk-lp/db --from-replica 1"]
}

# To remove the schedule later: delete the block + run `vd apply --prune`.
```

`pg:backups:schedule` itself emits no manifests and writes nothing — it's purely a templating helper. Voodu cronjobs are declarative (HCL is the source of truth); managed-schedule state plugin-side would diverge from HCL on the next `vd apply --prune`.

Combine with off-host sync (rclone, aws s3 sync, rsync) in a sibling cronjob mounting the same host path read-only.

#### Cancel an in-progress capture

```bash
# Stop everything in flight for clowk-lp/db
vd pg:backups:cancel clowk-lp/db
# postgres clowk-lp/db: stopped 1 capture

# Stop a specific capture
vd pg:backups:cancel clowk-lp/db b008
```

Runs `docker stop` on the matching `<scope>-<name>-backup-bNNN` containers. The container's `pg_dump` receives SIGTERM (10s grace), then SIGKILL. Postgres detects the dropped connection and rolls back — non-destructive of the cluster.

The container's wrapper script removes the partial dump file on non-zero exit, so a cancelled capture leaves no garbage in `/backups/`.

#### Retention (`--keep` / `--max-age`)

Backups grow unboundedly without intervention — every `:capture` writes a new `.dump` and leaves the exited job container behind. On a tight host (e.g. dev/test on a single 80GB HD) you want both:

- a **count cap** so the most recent N captures stay around regardless of cadence, and
- an **age cap** so really old snapshots get reaped even if you've barely been capturing.

Two CLI flags cover both axes; the plugin auto-prunes after each capture and on demand via `:prune`. Both flags accept the same shapes everywhere they appear.

**Flags:**

| Flag | Meaning | Examples |
|---|---|---|
| `--keep <N>` | Keep at most N most-recent backups | `--keep 30` |
| `--max-age <D>` | Drop anything older than D | `--max-age 7d`, `--max-age 2w`, `--max-age 168h` |
| `--retention <D>` | Alias for `--max-age` | same as above |

A backup is deleted if it fails **either** check (rank > N **or** age > D). Both flags are optional — pass none = no auto-prune.

**Persisted defaults via the config bucket:**

The plugin seeds `BACKUP_KEEP=30` on first apply. Subsequent `:capture` invocations pick this up automatically — no flags needed:

```bash
# First apply seeds BACKUP_KEEP=30 in the bucket
vd apply

# This single command captures + auto-prunes to 30 newest
vd pg:backups:capture clowk-lp/db
# postgres clowk-lp/db: backup b031 captured ...
# retention (keep 30): pruned 1 backup(s): b001
```

Override the persisted policy any time:

```bash
# Lower cap for a specific cluster
vd config clowk-lp/db set BACKUP_KEEP=14

# Add age cap on top
vd config clowk-lp/db set BACKUP_MAX_AGE=14d

# Disable auto-prune entirely (capture stops touching old files)
vd config clowk-lp/db set BACKUP_KEEP=0
vd config clowk-lp/db set BACKUP_MAX_AGE=
```

Precedence per axis:

1. **CLI flag** (`--keep`, `--max-age`) — wins for that axis only
2. **App-level bucket** (`vd config <scope>/<name> set ...`) — per-resource
3. **Scope-level bucket** (`vd config <scope> set ...`) — shared default for every resource in the scope
4. **Built-in default** — empty (no auto-prune); plugin seeds `BACKUP_KEEP=30` only at first apply, app-level

Per-axis means `--keep 5` for a one-off doesn't blow away the bucket's `BACKUP_MAX_AGE=14d` — only the count axis gets overridden for that invocation.

**Shared defaults via scope-level config:**

The controller's `ResolveConfig` merges **scope-level + app-level** (app wins on conflict) and the plugin reads through this — no `env_from` plumbing needed. Set the policy once for an entire scope:

```bash
# All postgres resources in scope `clowk-lp` inherit these
vd config clowk-lp set BACKUP_KEEP=14
vd config clowk-lp set BACKUP_MAX_AGE=7d

# Override one specific cluster
vd config clowk-lp/db set BACKUP_KEEP=30
```

> **Why not `env_from`:** the plugin reads policy via `GET /config` (controller's bucket store) BEFORE the capture container is spawned. `env_from` is a runtime container-env injection mechanism — different code path. Use scope-level config for shared defaults.

**On-demand prune (`:prune`):**

```bash
# Preview without touching disk — uses bucket defaults if no flags
vd pg:backups:prune clowk-lp/db --dry-run

# Tighter one-off
vd pg:backups:prune clowk-lp/db --keep 5 --yes

# Both axes, explicit
vd pg:backups:prune clowk-lp/db --keep 14 --max-age 7d --yes
```

`:prune` reads the bucket the same way as `:capture`. With `BACKUP_KEEP=30` already set, `vd pg:backups:prune clowk-lp/db --yes` is enough — no flags needed.

`--yes` is required for destructive runs; `--dry-run` skips the confirmation gate AND the disk operations, useful for cron previewing.

**What gets pruned:**

For each backup that fails the policy:

1. The `.dump` file in `/opt/voodu/backups/<scope>/<name>/` is removed.
2. The matching exited backup container (from `docker ps -a`) is `docker rm`'d so `vd get pd` stays clean.

Running containers are always skipped — never reap an in-flight capture.

**Schedule with retention inlined:**

`:schedule` accepts the same flags and inlines them into the cronjob's command, so the cron-driven captures self-trim:

```bash
vd pg:backups:schedule clowk-lp/db --at "0 3 * * *" --keep 30 --max-age 14d
```

```hcl
cronjob "clowk-lp" "db-backup" {
  schedule = "0 3 * * *"
  image    = "ghcr.io/clowk/voodu-cli:latest"

  command = ["bash", "-c", "vd pg:backups:capture clowk-lp/db --keep 30 --max-age 14d"]
}
```

You can also rely on the bucket-persisted defaults and skip the flags here — the cronjob will pick up `BACKUP_KEEP` / `BACKUP_MAX_AGE` from the config bucket the same way an interactive `:capture` does.

#### Off-site durability

```hcl
cronjob "clowk-lp" "db-backups-s3-sync" {
  schedule = "*/30 * * * *"
  image    = "amazon/aws-cli:latest"
  command  = ["s3", "sync", "/backups/", "s3://my-bucket/postgres/clowk-lp-db/"]
  volumes  = ["/opt/voodu/backups/clowk-lp/db:/backups:ro"]
  env_from = ["aws/cli"]
}
```

Backup files are immutable once written, so `s3 sync` never re-uploads.

### `vd postgres:backup` / `vd postgres:restore` (legacy, pg_basebackup)

The original commands run `pg_basebackup` (physical snapshot) instead of `pg_dump` (logical dump). They still work and are the fastest path for full-cluster restores, but require operator-supplied paths and don't integrate with the `/backups` directory.

```bash
# pg_basebackup tar to operator-chosen path
vd postgres:backup clowk-lp/db --destination /srv/backups/db-20260504.tar

# Restore (DESTRUCTIVE — wipes every pod's PGDATA)
vd postgres:restore clowk-lp/db --from /srv/backups/db.tar --yes
```

Output is a tar with `base.tar` + `pg_wal.tar` (`-X stream` includes WAL needed to make the backup self-consistent). Point-in-snapshot only — no PITR.

> **`pg:backups:restore` / `:download` / `:delete` / `:schedule` will replace this surface in the next iteration.**


---

## Real-world examples

### Single primary + Rails app

```hcl
# voodu.hcl
postgres "clowk-lp" "db" {
  image    = "postgres:16"
  database = "appdata"
  user     = "appuser"
}

deployment "clowk-lp" "web" {
  image = "ghcr.io/clowk/web:latest"

  env = {
    RAILS_ENV = "production"
  }

  ports = ["3000"]
}
```

```bash
vd apply -f voodu.hcl
vd postgres:link clowk-lp/db clowk-lp/web

# Rails web pod now has DATABASE_URL set:
# postgres://appuser:<hex>@db-0.clowk-lp.voodu:5432/appdata
```

### 3-replica cluster + Rails MultiDB

```hcl
postgres "clowk-lp" "db" {
  image    = "postgres:16"
  database = "appdata"
  user     = "appuser"
  replicas = 3                        # 1 primary + 2 standbys
}

deployment "clowk-lp" "web" {
  image = "ghcr.io/clowk/web:latest"
  ports = ["3000"]
}
```

```bash
vd apply -f voodu.hcl
vd postgres:link clowk-lp/db clowk-lp/web --reads

# web now has both:
# DATABASE_URL      = postgres://...@db-0.clowk-lp.voodu:5432/appdata
# DATABASE_READ_URL = postgres://...@db-1.clowk-lp.voodu:5432,db-2.clowk-lp.voodu:5432/appdata?target_session_attrs=any
```

`config/database.yml`:

```yaml
production:
  primary:
    url: <%= ENV["DATABASE_URL"] %>
  primary_replica:
    url: <%= ENV["DATABASE_READ_URL"] %>
    replica: true
```

`app/models/application_record.rb`:

```ruby
class ApplicationRecord < ActiveRecord::Base
  primary_abstract_class

  connects_to database: { writing: :primary, reading: :primary_replica }
end
```

### Custom image with pgvector

```hcl
postgres "clowk-lp" "db" {
  workdir    = "infra/postgres"
  dockerfile = "Dockerfile.pg"
  replicas   = 3

  lang {
    name = "generic"
  }
}
```

`infra/postgres/Dockerfile.pg`:

```dockerfile
FROM postgres:16
RUN apt-get update \
 && apt-get install -y postgresql-16-pgvector \
 && rm -rf /var/lib/apt/lists/*
```

```bash
vd apply -f voodu.hcl
# voodu streams the build context, runs `docker build`, tags
# clowk-lp-db:latest, deploys to the 3 pods.

# Enable in your app's migrations:
# Rails:  enable_extension :pgvector
# Django: from pgvector.django import VectorField

# OR inline:
vd postgres:psql clowk-lp/db -c "CREATE EXTENSION IF NOT EXISTS pgvector"
```

### External access for DBeaver / TablePlus

```bash
vd postgres:expose clowk-lp/db
# postgres clowk-lp/db now exposed on 0.0.0.0:<port> — pod restart triggered.
# Reminder: rotate password (vd postgres:new-password clowk-lp/db) if it
# ever leaked anywhere untrusted.

# Get the password
PW=$(vd config clowk-lp/db get POSTGRES_PASSWORD -o json | jq -r .POSTGRES_PASSWORD)

# Connect from your laptop
psql "postgres://postgres:$PW@my-vm-ip:5432/postgres"

# Done? un-expose
vd postgres:unexpose clowk-lp/db
```

⚠ Verify your firewall (ufw, security group, iptables) before exposing. The plugin flips the bind from `127.0.0.1` to `0.0.0.0`; the host firewall still controls who can reach the port from outside.

---

## Plugin reference

### Commands

| Command | Purpose |
|---|---|
| `vd postgres:link <provider> <consumer> [--reads]` | Wire `DATABASE_URL` (and optionally `DATABASE_READ_URL`) into a consumer |
| `vd postgres:unlink <provider> <consumer>` | Remove the link |
| `vd postgres:new-password <postgres> [--no-restart]` | Rotate superuser password + auto-refresh every linked consumer |
| `vd postgres:info <postgres> [-o json]` | Cluster topology snapshot (text or JSON) |
| `vd postgres:expose <postgres>` | Publish on 0.0.0.0:`<port>` (Internet-facing) |
| `vd postgres:unexpose <postgres>` | Return to 127.0.0.1:`<port>` (loopback only) |
| `vd postgres:promote <postgres> --replica <N> [--force] [--no-restart]` | Promote a standby to primary (plugin runs `pg_promote()` internally; refuses on lag without `--force`) |
| `vd postgres:rejoin <postgres> --replica <N>` | Re-attach a divergent pod as standby via `pg_rewind` |
| `vd postgres:psql <postgres> [--replica N] [-c "<sql>"]` | Drop into psql against the cluster (no password needed) |
| `vd pg:backups <postgres> [-o json]` | List backups + status (running/complete/failed) |
| `vd pg:backups:capture <postgres> [--from-replica N] [--follow] [--keep N] [--max-age D]` | Spawn pg_dump in a sibling container (detached default); auto-prunes after success when retention flags or `BACKUP_KEEP`/`BACKUP_MAX_AGE` set |
| `vd pg:backups:logs <postgres> <id> [--follow]` | docker logs on a backup container |
| `vd pg:backups:restore <postgres> <id\|url> --yes` | `pg_restore` from local id or http(s) URL (DESTRUCTIVE of db content) |
| `vd pg:backups:download <postgres> <id> [--to <path>]` | Copy a backup file from the pod to the host |
| `vd pg:backups:delete <postgres> <id> --yes` | Remove a backup file from `/backups/` |
| `vd pg:backups:prune <postgres> [--keep N] [--max-age D] [--dry-run] [--yes]` | Apply retention on demand (file + container cleanup) |
| `vd pg:backups:schedule <postgres> [--at <cron>] [--from-replica N] [--keep N] [--max-age D]` | Print cronjob HCL template for paste-and-apply (retention inlined into the captured command) |
| `vd pg:backups:cancel <postgres> [<id>]` | docker stop running capture(s) |
| `vd postgres:backup <postgres> --destination <path> [--from-replica N]` | (legacy) `pg_basebackup` snapshot to a tar file |
| `vd postgres:restore <postgres> --from <path> --yes` | (legacy) Restore PGDATA from a tar (DESTRUCTIVE of cluster) |
| `vd postgres:help` | Plugin overview |

Pass `--help` to any subcommand for full usage.

### Asset files emitted

The plugin emits one asset per postgres resource with 4 files:

| Key | Mount path | Loaded by |
|---|---|---|
| `entrypoint` | `/usr/local/bin/voodu-postgres-entrypoint` | `command = ["bash", ...]` |
| `pg_overrides_conf` | `/etc/postgresql/voodu-99-overrides.conf` | postgres `include_dir` (operator pg_config) |
| `streaming_conf` | `/etc/postgresql/voodu-50-streaming.conf` | postgres `include_dir` (primary_conninfo, hot_standby) |
| `init_replication_sh` | `/docker-entrypoint-initdb.d/00_create_replication.sh` | docker-entrypoint.sh on first boot of primary only |

The `voodu-NN-` prefixes order the include_dir scan: plugin defaults (50) load before operator overrides (99). "Last config wins" → operator pg_config trumps plugin defaults cleanly.

### Statefulset env vars

| Var | Source | Used by |
|---|---|---|
| `POSTGRES_USER` / `POSTGRES_DB` / `POSTGRES_PASSWORD` | plugin (HCL + auto-gen) | docker-entrypoint.sh (initdb) |
| `POSTGRES_INITDB_ARGS` | plugin (`--locale=... --encoding=...`) | docker-entrypoint.sh |
| `PGDATA` | plugin (`/var/lib/postgresql/data/pgdata`) | postgres |
| `PG_PORT` | plugin (HCL `port`) | wrapper script (`-p`) |
| `PG_NAME` / `PG_SCOPE_SUFFIX` | plugin (resource ref) | wrapper script (FQDN composition) |
| `PG_PRIMARY_ORDINAL` | plugin (default 0; M-P5 will flip via failover) | wrapper script (role detection) |
| `PG_REPLICATION_USER` / `PG_REPLICATION_PASSWORD` | plugin (HCL + auto-gen) | wrapper script (`pg_basebackup`), init script (`CREATE USER`) |
| `VOODU_REPLICA_ORDINAL` | controller (per-pod) | wrapper script (role detection) |

### Bucket keys (config)

| Key | Owner | Purpose |
|---|---|---|
| `POSTGRES_PASSWORD` | plugin auto-gen | Superuser password — read by `link`, rotated by `new-password` |
| `POSTGRES_REPLICATION_PASSWORD` | plugin auto-gen | Replication user password — read by streaming.conf renderer + `rejoin` |
| `POSTGRES_LINKED_CONSUMERS` | `vd postgres:link/unlink` | Comma-separated `<scope>/<name>` refs for `new-password`/`failover` fan-out |
| `PG_EXPOSE_PUBLIC` | `vd postgres:expose/unexpose` | `"true"` flips ports to 0.0.0.0; absent = loopback |
| `PG_PRIMARY_ORDINAL` | `vd postgres:failover` | Current primary ordinal (default `0`); flipped by failover, read by wrapper script + streaming.conf renderer |
| `BACKUP_KEEP` | seeded by plugin (default `30`); operator-tunable | Cap on backup count for auto-prune. `0` / empty disables count-based pruning. Read by `:capture` and `:prune`. |
| `BACKUP_MAX_AGE` | operator | Cap on backup age for auto-prune. Accepts `7d`, `2w`, `168h`. Empty disables age-based pruning. Read by `:capture` and `:prune`. |

Operator can read all of these via `vd config <ref>`. Setting them manually before first apply pre-seeds (useful for dev environments wanting deterministic passwords).

**Scope-level inheritance:** any of these keys can be set at the scope level (`vd config <scope> set KEY=VAL` — no `/<name>`) and the plugin will pick them up. App-level (`vd config <scope>/<name> set ...`) wins on conflict. Useful for shared defaults like `BACKUP_KEEP`/`BACKUP_MAX_AGE` across every postgres in a scope without touching each resource bucket.

**`env_from` does NOT apply to plugin reads.** `env_from` injects vars into the container's runtime environment; plugins read through the controller's `GET /config` (etcd store). For shared defaults, use scope-level config above.

### Repo layout

```
cmd/voodu-postgres/
  main.go               # entrypoint (subcommand switch)
  postgres.go           # postgresSpec parser/validator + plugin-owned strip
  entrypoint.go         # bash wrapper renderer (role-aware)
  pgconfig.go           # postgresql.conf overrides renderer
  password.go           # superuser password lifecycle
  replication.go        # replication password lifecycle + streaming.conf + init script
  controller.go         # invocationContext + controllerClient
  link.go               # cmdLink + cmdUnlink + URL builder + linked consumers
  new_password.go       # cmdNewPassword + auto-refresh consumers
  info.go               # cmdInfo (text + JSON snapshot)
  expose.go             # cmdExpose + cmdUnexpose
  help.go               # plugin overview
  *_test.go             # unit + integration tests
bin/                    # wrappers + the binary
plugin.yml              # plugin metadata (declares commands)
Makefile                # build / test / install-local
install                 # post-install hook
uninstall               # pre-uninstall hook
```

### Development

```bash
make build               # produces bin/voodu-postgres
make test                # go test ./...
make lint                # go vet ./...
make cross               # cross-compile linux/amd64 + linux/arm64

# Smoke test expand without dispatch
echo '{"kind":"postgres","scope":"x","name":"y","spec":{"replicas":3}}' \
  | bin/voodu-postgres expand | jq

# Smoke test help texts
bin/voodu-postgres link --help
bin/voodu-postgres expose --help

# Install into a local plugins root for E2E
make install-local PLUGINS_ROOT=/opt/voodu/plugins
```

---

## Install & upgrade

```bash
vd plugins:install thadeu/voodu-postgres
vd plugins:install thadeu/voodu-postgres --version 0.3.0
```

The plugin ships pre-built linux/amd64 + linux/arm64 binaries via GitHub Releases. The install hook downloads the right one for the host arch and drops it into `$VOODU_PLUGIN_DIR/postgres/bin/`.

## Storage

The plugin uses two storage shapes:

### Per-pod data (statefulset semantics)

Each pod gets its own `data` volume — postgres' authoritative data directory. Pod restarts re-attach to the same volume; scale-down preserves it; `vd delete --prune` is the only thing that destroys it.

| Claim | Volume name (ordinal 0) | Mount path | Survives |
|---|---|---|---|
| `data` | `voodu-<scope>-<name>-data-0` | `/var/lib/postgresql/data` | pod restart, scale-down |

Standbys (ordinal 1+) get their own per-pod `data` volume (`voodu-<scope>-<name>-data-1`, `-2`, …). Each pod is independent — losing one pod's data volume forces a re-bootstrap from the primary via `pg_basebackup`, but the others stay healthy.

### Backups directory (shared host bind-mount)

Every pod mounts `/opt/voodu/backups/<scope>/<name>/` from the host at `/backups` inside the container (rw on every pod). `vd pg:backups:capture` writes `pg_dump` files here; cronjobs sync the dir to off-host storage.

| Source | Container path | Mount mode | Default |
|---|---|---|---|
| host bind-mount | `/backups` | `:rw` (all pods) | `/opt/voodu/backups/<scope>/<name>` |

**Pre-flight (operator, once per host)**:

```bash
sudo mkdir -p /opt/voodu/backups/clowk-lp/db
```

Ownership (`postgres:postgres`, uid 999) is auto-fixed by the entrypoint wrapper at boot — no manual `chown` needed.

## License

MIT
