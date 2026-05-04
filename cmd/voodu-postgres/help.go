// Plugin-level help text — what `vd postgres -h` shows the
// operator. Mirrors the voodu-redis printPluginOverview pattern:
// plain text on stdout (no envelope), the plugin owns the
// wording verbatim.

package main

import "fmt"

const pluginOverview = `vd postgres — managed postgres clusters on voodu

A macro plugin that expands a postgres { } HCL block into:
  - 1 asset (entrypoint script + 3 .conf files + 1 init script)
  - 1 statefulset (the postgres pods themselves)
  - 1+ config_set actions (auto-gen passwords on first apply)

Cluster shape: pod-0 = primary, pod-N+ = streaming-replication
standbys via pg_basebackup. WAL archive ON by default to a
sibling volume_claim. Operator points HCL at a custom image for
extensions baked (pgvector, postgis).

COMMANDS
  expand          (internal) macro entrypoint, called by vd apply
  link            wire DATABASE_URL into a consumer's bucket
  unlink          remove a previously-set DATABASE_URL
  new-password    rotate the superuser password + refresh consumers
  info            show cluster topology, ports, linked consumers
  expose          publish postgres on 0.0.0.0 (Internet-facing)
  unexpose        return postgres to 127.0.0.1 (loopback only)
  promote         promote a standby to primary (plugin runs pg_promote)
  rejoin          re-attach a divergent pod as standby (post-promote recovery)
  psql            interactive psql against the cluster (no password needed)
  backup          pg_basebackup snapshot to a tar file
  restore         restore from a tar (DESTRUCTIVE; supports PITR)
  help            this text

  Pass --help to any subcommand for full usage:
    vd postgres:link --help
    vd postgres:expose --help
    ...

QUICK START

  # 1. Declare a cluster
  cat > voodu.hcl <<EOF
  postgres "clowk-lp" "db" {
    image    = "postgres:16"
    replicas = 3                  # 1 primary + 2 standbys
  }
  EOF

  vd apply -f voodu.hcl

  # 2. Wire your app to it
  vd postgres:link clowk-lp/db clowk-lp/web --reads
  # → DATABASE_URL    = postgres://postgres:<hex>@db-0.clowk-lp.voodu:5432/postgres
  # → DATABASE_READ_URL = postgres://postgres:<hex>@db-1...,db-2.../postgres?target_session_attrs=any

  # 3. Inspect
  vd postgres:info clowk-lp/db

  # 4. Operator gets the password if needed (psql / DBeaver)
  vd config clowk-lp/db get POSTGRES_PASSWORD

  # 5. Rotate (auto-refreshes every linked consumer's URL)
  vd postgres:new-password clowk-lp/db

  # 6. Publish for external tools (DBeaver, Heroku Connect, ...)
  vd postgres:expose clowk-lp/db
  # ⚠ exposes 0.0.0.0:5432 — verify firewall first
  # Use vd postgres:unexpose to revoke

HCL CONTRACT (minimal → comprehensive)

  postgres "scope" "name" {
    # All fields optional. Defaults shown.
    image    = "postgres:latest"        # operator pins major in prod
    replicas = 1                        # 1 = single primary; >=2 cluster
    database = "postgres"               # initdb arg
    user     = "postgres"               # superuser
    port     = 5432                     # listen port

    # Plugin auto-generates these (HCL-overridable for password only):
    # password         = "<auto, 32-byte hex>"
    # replication_user = "replicator"   # default

    initdb_locale   = "C.UTF-8"
    initdb_encoding = "UTF8"

    # postgresql.conf overrides (key/value, anti-injection guarded)
    pg_config = {
      max_connections      = 200
      shared_buffers       = "256MB"
      log_connections      = true
    }

    # WAL archive — built-in, defaults to local volume + retention
    # via operator cronjob (see README).
    wal_archive {
      enabled    = true                 # default
      mount_path = "/wal-archive"       # default
    }

    # Statefulset passthroughs — anything the statefulset accepts
    # flows through unchanged: env, env_from, volumes, volume_claims,
    # cpu, memory, depends_on, networks, etc.
  }

DOCS
  https://github.com/thadeu/voodu-postgres
`

func printPluginOverview() {
	fmt.Print(pluginOverview)
}
