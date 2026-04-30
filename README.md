# voodu-postgres

Voodu macro plugin that expands a `postgres { … }` HCL block into a statefulset manifest.

The plugin is a **dumb alias of statefulset with sensible defaults** — every statefulset attribute the operator declares wins; the plugin only fills in what's missing. Custom config (postgresql.conf, pg_hba.conf, certs) flows through the `asset` kind, not via plugin knobs.

## Defaults

Bare block — `postgres "data" "main" {}` — produces:

```hcl
statefulset "data" "main" {
  image    = "postgres:15"
  replicas = 1

  env = {
    POSTGRES_DB = "main"
    PGDATA      = "/var/lib/postgresql/data/pgdata"
  }

  ports = ["5432"]

  volume_claim "data" {
    mount_path = "/var/lib/postgresql/data"
  }
}
```

Print the skeleton at any time:

```bash
voodu-postgres defaults
```

## Production hardening — via `asset`

Custom postgresql.conf / pg_hba.conf are declared as a sibling `asset` block and referenced from the postgres block via `${asset.<name>.<key>}`:

```hcl
asset "data" "pg-config" {
  postgresql_conf = file("./pg/postgresql.conf")
  pg_hba_conf     = url("https://r2.example.com/configs/pg_hba.conf")
}

postgres "data" "main" {
  image = "postgres:15-alpine"

  command = [
    "postgres",
    "-c", "config_file=/etc/postgresql/postgresql.conf",
    "-c", "hba_file=/etc/postgresql/pg_hba.conf",
  ]

  volumes = [
    "${asset.pg-config.postgresql_conf}:/etc/postgresql/postgresql.conf:ro",
    "${asset.pg-config.pg_hba_conf}:/etc/postgresql/pg_hba.conf:ro",
  ]
}
```

Server materialises the asset bytes (file content embedded by CLI, URL fetched + cached server-side), interpolates `${asset.pg-config.X}` to real host paths, mounts as bind volumes. Asset content drift triggers rolling restart automatically (the asset hash folds into the statefulset spec hash).

The plugin doesn't know any of this happened. It's a dumb alias — operator owns the wiring.

## Override anything

Every statefulset attribute is exposed by virtue of the alias model. Operator-declared values win on conflict.

```hcl
postgres "data" "main" {
  image = "postgres:15-alpine"

  # Bind on private interface only.
  ports = ["10.0.0.5:5432:5432"]

  # Custom env merges with defaults.
  env = {
    POSTGRES_INITDB_ARGS = "--data-checksums"
  }

  # Disable persistent storage (tmpfs-only — test fixtures).
  volume_claims = []
}
```

## Connecting from another app

Connection coordinates are NOT exposed via `${ref.…}` — secrets belong in `vd config set`:

```bash
PG_PASS=$(openssl rand -hex 16)
vd config set -s data -n main POSTGRES_PASSWORD=$PG_PASS
vd config set -s myapp DATABASE_URL="postgres://postgres:$PG_PASS@main-0.data:5432/main"
vd apply -f voodu.hcl
```

## Storage

Each ordinal of the underlying statefulset gets a docker named volume `voodu-<scope>-<name>-data-<ordinal>`. Volumes survive restarts, image bumps, scale-down, and `vd delete <kind>/<scope>/<name>`. Wipe explicitly:

```bash
vd delete statefulset/data/main --prune
```

## Install

JIT-installed by `vd apply` on first apply containing a `postgres { … }` block. Pin manually:

```bash
vd plugins:install postgres --repo thadeu/voodu-postgres
```

Override source repo per-block (forks, internal mirrors):

```hcl
postgres "data" "main" {
  _repo = "myorg/voodu-postgres-fork"
  image = "postgres:15-alpine"
}
```

## Plugin contract

`expand` reads stdin and writes a statefulset envelope:

```bash
echo '{"kind":"postgres","scope":"data","name":"main"}' | voodu-postgres expand
# {"status":"ok","data":{"kind":"statefulset",…}}
```

Merge rules:

- `env`: deep merge (operator adds keys, defaults preserved unless overwritten)
- everything else: operator-wins shallow
- `volume_claims = []` (or any explicitly empty list/map): default dropped

## Limitations

- Single-node only. Replication topology lands later.
- Plugin doesn't generate or rotate the postgres password — `vd config set` territory.

## Development

```bash
make build
make test
make cross

echo '{"kind":"postgres","scope":"data","name":"main"}' | bin/voodu-postgres expand
```

## License

MIT.
