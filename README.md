# voodu-postgres

Voodu plugin that expands a `postgres { … }` HCL block into a production-ready postgres cluster: 1 primary + N streaming-replication standbys, WAL archive on by default, auto-generated passwords, dual `DATABASE_URL` injection (writer + reader endpoints, RDS-style).

Bare block produces a single hardened primary; `replicas = 3` flips it into a cluster with `pg_basebackup`-cloned standbys and `primary_conninfo`-driven WAL streaming.

## Table of contents

- [Quick start](#quick-start)
- [Configuration](#configuration)
  - [HCL contract](#hcl-contract)
  - [Plugin defaults](#plugin-defaults)
  - [Customising postgresql.conf via `pg_config`](#customising-postgresqlconf-via-pg_config)
  - [Custom image (extensions baked)](#custom-image-extensions-baked)
  - [WAL archive](#wal-archive)
  - [Statefulset passthrough](#statefulset-passthrough)
- [High availability — streaming replication](#high-availability--streaming-replication)
  - [Cluster shape](#cluster-shape)
  - [Cold-start sequence](#cold-start-sequence)
  - [Write vs read endpoints](#write-vs-read-endpoints)
  - [Promote a standby](#promote-a-standby)
  - [Rejoining the old primary](#rejoining-the-old-primary)
- [Backup automation](#backup-automation)
  - [`vd postgres:backup`](#vd-postgresbackup)
  - [`vd postgres:restore`](#vd-postgresrestore)
  - [WAL cleanup pattern](#wal-cleanup-pattern)
  - [Scheduled backups](#scheduled-backups)
- [Connecting via `psql`](#connecting-via-psql)
- [Real-world examples](#real-world-examples)
  - [Single primary + Rails app](#single-primary--rails-app)
  - [3-replica cluster + Rails MultiDB](#3-replica-cluster--rails-multidb)
  - [Custom image with pgvector](#custom-image-with-pgvector)
  - [WAL archive to S3](#wal-archive-to-s3)
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

- **`asset "clowk-lp" "db"`** — 5 files: bash entrypoint wrapper, postgresql.conf overrides, WAL archive defaults, streaming-replication conf, replication-user init script
- **`statefulset "clowk-lp" "db"`** — 3 pods (`db-0..db-2`), 2 volume claims (`data` + `wal-archive`), 5 asset bind-mounts, env vars wiring everything
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

  # WAL archive (on by default — see WAL section)
  wal_archive {
    enabled    = true
    mount_path = "/wal-archive"
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
- `volume_claims`: `data` at `/var/lib/postgresql/data` + `wal-archive` at `/wal-archive`
- `health_check = "pg_isready -U postgres -d postgres -p 5432"`
- env: `POSTGRES_USER` / `POSTGRES_DB` / `POSTGRES_PASSWORD` / `POSTGRES_INITDB_ARGS` / `PGDATA` / `PG_PORT` / `PG_NAME` / `PG_SCOPE_SUFFIX` / `PG_PRIMARY_ORDINAL` / `PG_REPLICATION_USER` / `PG_REPLICATION_PASSWORD`

### Customising postgresql.conf via `pg_config`

The `pg_config` map renders into a `voodu-99-overrides.conf` file that postgres loads via `include_dir`. "Last assignment wins" — operator overrides trump plugin defaults from `voodu-00-wal-archive.conf` and `voodu-50-streaming.conf`.

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

### WAL archive

WAL archive is on by default. Plugin emits a second `volume_claim` (`wal-archive` at `/wal-archive`) and a `voodu-00-wal-archive.conf` with:

```ini
wal_level       = replica
archive_mode    = on
archive_command = 'test ! -f /wal-archive/%f && cp %p /wal-archive/%f'
archive_timeout = 60
```

The default `archive_command` writes locally with an idempotency guard (rejects rewrites that would corrupt the archive on retry). Operator overrides via `pg_config`:

```hcl
postgres "clowk-lp" "db" {
  pg_config = {
    archive_command = "aws s3 cp %p s3://my-bucket/wal/%f"
  }

  env_from = ["aws/cli"]   # AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY
                           # via shared bucket pattern
}
```

Disable entirely (no second volume_claim, no archive overhead):

```hcl
postgres "clowk-lp" "db" {
  wal_archive {
    enabled = false
  }
}
```

**Cleanup is operator-owned** — see [Backup automation](#backup-automation) below.

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

Plugin-owned fields (`database`, `user`, `password`, `port`, `initdb_locale`, `initdb_encoding`, `pg_config`, `extensions`, `wal_archive`, `replication_user`) are stripped from the merged spec before emitting — they don't leak to the statefulset wire shape.

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

| Pod | Role | DNS | Volume claims |
|---|---|---|---|
| `clowk-lp-db.0` | primary | `db-0.clowk-lp.voodu` | `voodu-clowk-lp-db-data-0`, `voodu-clowk-lp-db-wal-archive-0` |
| `clowk-lp-db.1` | standby | `db-1.clowk-lp.voodu` | `voodu-clowk-lp-db-data-1`, `voodu-clowk-lp-db-wal-archive-1` |
| `clowk-lp-db.2` | standby | `db-2.clowk-lp.voodu` | `voodu-clowk-lp-db-data-2`, `voodu-clowk-lp-db-wal-archive-2` |

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

### `vd postgres:backup`

`pg_basebackup` snapshot to a tar file. Plugin spawns a one-shot container that shares the source pod's network namespace (`db-N.scope.voodu` resolves via voodu0 DNS) and streams the backup to the operator-supplied destination path on the host:

```bash
# Default: backup from the current primary
vd postgres:backup clowk-lp/db --destination /srv/backups/db-20260504.tar

# Offload from a standby (avoids write contention on primary)
vd postgres:backup clowk-lp/db --destination /srv/backups/db.tar --from-replica 1
```

Output is a tar with `base.tar` + `pg_wal.tar` (`-X stream` includes WAL needed to make the backup self-consistent — restore needs no archive_command access).

### `vd postgres:restore`

**DESTRUCTIVE** — wipes PGDATA on every pod, extracts the backup into the primary, then bootstraps standbys via `pg_basebackup`. Refuses to run without `--yes`.

```bash
# Latest restore (no PITR — replay all available WAL)
vd postgres:restore clowk-lp/db --from /srv/backups/db.tar --yes

# Point-in-time recovery to just before a bad migration
vd postgres:restore clowk-lp/db \
  --from /srv/backups/db.tar \
  --target-time "2026-05-04 14:30:00 UTC" \
  --yes
```

Workflow internals:

1. `docker stop` every pod (primary + standbys)
2. Wipe primary's PGDATA via `docker run --volumes-from`
3. Extract backup tar into PGDATA (one-shot container shares the volume)
4. (PITR) Append `recovery_target_time = '<ts>'` + `restore_command = 'cp /wal-archive/%f %p'` to `postgresql.auto.conf`, drop `recovery.signal`
5. `docker start` primary — postgres replays WAL on startup, promotes when target reached
6. Wipe each standby's PGDATA + start — wrapper's first-boot path bootstraps via `pg_basebackup` against the just-restored primary

**When pg_rewind/restore fails on a standby**: same fallback as M-P5 rejoin — `vd delete --replica N --prune` + re-apply forces a fresh standby bootstrap.

### WAL cleanup pattern

Plugin doesn't auto-prune the WAL archive — declare a cleanup cronjob:

```hcl
cronjob "clowk-lp" "wal-cleanup" {
  schedule = "0 2 * * *"               # daily at 02:00
  image    = "postgres:16"
  command  = ["bash", "-c", "find /wal-archive -mmin +10080 -delete"]
  # 10080 minutes = 7 days

  volumes = [
    "voodu-clowk-lp-db-wal-archive-0:/wal-archive:rw",
  ]
}
```

For S3/R2 archives (operator-overridden `archive_command`), no cleanup cronjob needed — set bucket lifecycle policy on the cloud side.

### Scheduled backups

Wrap `vd postgres:backup` in a voodu cronjob:

```hcl
cronjob "clowk-lp" "db-backup" {
  schedule = "0 3 * * *"               # daily at 03:00
  image    = "ghcr.io/clowk/voodu-cli:latest"   # has vd binary

  command = ["bash", "-c", "vd postgres:backup clowk-lp/db --destination /backup/db-$(date +%Y%m%d).tar --from-replica 1"]

  volumes = ["/srv/pg-backups:/backup:rw"]
}
```

S3/R2 upload — pipe through awscli or rclone in the cronjob command, or use a separate sync cronjob.

---

## Connecting via `psql`

`vd postgres:psql` shells into postgres without password — connection goes through the container's unix socket (trust auth in the stock pg_hba.conf):

```bash
# Interactive REPL on primary
vd postgres:psql clowk-lp/db

# Interactive on a standby (read-only)
vd postgres:psql clowk-lp/db --replica 1

# One-shot query
vd postgres:psql clowk-lp/db -c "SELECT version();"

# Replication status — debug standby lag
vd postgres:psql clowk-lp/db -c "SELECT * FROM pg_stat_replication;"

# CSV output via passthrough flag
vd postgres:psql clowk-lp/db -- --csv -c "SELECT * FROM pg_stat_database"

# Manual failover step 1
vd postgres:psql clowk-lp/db --replica 1 -c "SELECT pg_promote();"
```

Heroku-style — operator doesn't need to know password, host, or user. Plugin reads `POSTGRES_USER`/`POSTGRES_DB` from the statefulset env to compose the right `psql -U <user> -d <db>` invocation.

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

### WAL archive to S3

```hcl
# Shared AWS creds bucket (declared once)
asset "aws" "cli" {
  readme = file("./shared/aws-cli.README.md")
}
```

```bash
vd config aws/cli set AWS_ACCESS_KEY_ID=AKIAxxx
vd config aws/cli set AWS_SECRET_ACCESS_KEY=secretxxx
vd config aws/cli set AWS_DEFAULT_REGION=us-east-1
```

```hcl
postgres "clowk-lp" "db" {
  image    = "postgres:16-bullseye-aws"   # custom image with awscli baked
  replicas = 3

  pg_config = {
    archive_command = "aws s3 cp %p s3://my-bucket/wal/%f"
  }

  env_from = ["aws/cli"]   # AWS_ACCESS_KEY_ID etc. flow into the postgres pod
}
```

The plugin still emits `wal_archive` (volume claim + plugin default `archive_command` in `voodu-00-wal-archive.conf`). The operator's `pg_config` override lands in `voodu-99-overrides.conf` which postgres reads LAST → S3 command wins.

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
| `vd postgres:backup <postgres> --destination <path> [--from-replica N]` | `pg_basebackup` snapshot to a tar file |
| `vd postgres:restore <postgres> --from <path> [--target-time "<ts>"] --yes` | Restore from a tar (DESTRUCTIVE; PITR via `--target-time`) |
| `vd postgres:help` | Plugin overview |

Pass `--help` to any subcommand for full usage.

### Asset files emitted

The plugin emits one asset per postgres resource with 5 files:

| Key | Mount path | Loaded by |
|---|---|---|
| `entrypoint` | `/usr/local/bin/voodu-postgres-entrypoint` | `command = ["bash", ...]` |
| `pg_overrides_conf` | `/etc/postgresql/voodu-99-overrides.conf` | postgres `include_dir` (operator pg_config) |
| `wal_archive_conf` | `/etc/postgresql/voodu-00-wal-archive.conf` | postgres `include_dir` (plugin WAL defaults) |
| `streaming_conf` | `/etc/postgresql/voodu-50-streaming.conf` | postgres `include_dir` (primary_conninfo, hot_standby) |
| `init_replication_sh` | `/docker-entrypoint-initdb.d/00_create_replication.sh` | docker-entrypoint.sh on first boot of primary only |

The `voodu-NN-` prefixes order the include_dir scan: plugin defaults (00, 50) load first, operator overrides (99) load last. "Last config wins" → operator pg_config trumps plugin defaults cleanly.

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

Operator can read all of these via `vd config <ref>`. Setting them manually before first apply pre-seeds (useful for dev environments wanting deterministic passwords).

### Repo layout

```
cmd/voodu-postgres/
  main.go               # entrypoint (subcommand switch)
  postgres.go           # postgresSpec parser/validator + plugin-owned strip
  entrypoint.go         # bash wrapper renderer (role-aware)
  pgconfig.go           # postgresql.conf overrides renderer
  password.go           # superuser password lifecycle
  wal_archive.go        # WAL archive parser/validator/renderer
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

Per-pod volumes follow voodu's deterministic naming:

| Claim | Volume name (ordinal 0) | Mount path | Survives |
|---|---|---|---|
| `data` | `voodu-<scope>-<name>-data-0` | `/var/lib/postgresql/data` | pod restart, scale-down |
| `wal-archive` | `voodu-<scope>-<name>-wal-archive-0` | `/wal-archive` | pod restart, scale-down |

Standbys (ordinal 1+) get their own per-pod data + wal-archive volumes. Scale-down preserves volumes — operator opts into destruction via `vd delete --prune`.

## License

MIT
