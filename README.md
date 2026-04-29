# voodu-postgres

Voodu database plugin that materialises `database "<name>" { engine = "postgres" }` manifests into a single-node postgres statefulset.

## What it does

Given a manifest like:

```hcl
database "main" {
  engine  = "postgres"
  version = "15"

  params = {
    password = "optional-explicit-password"
  }
}
```

the plugin's `create` command POSTs an equivalent statefulset manifest to the voodu controller, scoped under `databases`:

```hcl
statefulset "databases" "main" {
  image    = "postgres:15"
  replicas = 1

  env = {
    POSTGRES_PASSWORD = "<resolved-or-generated>"
    POSTGRES_DB       = "main"
    PGDATA            = "/var/lib/postgresql/data/pgdata"
  }

  ports = ["5432"]

  volume_claim "data" {
    mount_path = "/var/lib/postgresql/data"
  }
}
```

Other apps reach the running database through voodu's per-pod alias: `${ref.database.main.host}` resolves to `main-0.databases`.

## Connection coordinates

After `vd apply -f voodu.hcl`, the plugin's `create` envelope is persisted at `/status/databases/<name>`. Available reference fields:

- `${ref.database.<name>.host}` — `<name>-0.databases` (DNS-resolvable on `voodu0`)
- `${ref.database.<name>.port}` — `5432`
- `${ref.database.<name>.username}` — `postgres`
- `${ref.database.<name>.password}` — generated hex (or whatever you set in `params.password`)
- `${ref.database.<name>.database}` — same as `<name>`
- `${ref.database.<name>.url}` — `postgres://postgres:PASSWORD@HOST:5432/DBNAME?sslmode=disable`

## Install

```bash
vd plugins:install postgres --repo thadeu/voodu-postgres
```

The install hook downloads the matching tagged binary from GitHub releases into `$VOODU_PLUGIN_DIR/bin/voodu-postgres`.

## Storage

Each ordinal of the underlying statefulset gets a docker named volume: `voodu-databases-<name>-data-<ordinal>`. Volumes survive:

- container restarts
- statefulset rolling restarts
- spec drift (image bumps, env changes)
- statefulset scale-down (the volume of the dropped ordinal stays)
- `vd delete database/<name>` (the database manifest is removed; the statefulset is destroyed; volumes are kept)

Volumes are destroyed only when the operator explicitly opts in:

```bash
vd delete statefulset/databases/<name> --prune
```

## Limitations

- Single-node only. Replication topology (primary at pod-0, followers running `pg_basebackup` against it) is on the roadmap.
- Re-create with no `params.password` generates a fresh password and the previous data becomes unreadable. Workaround: always pin a password via `params.password` once you have one in hand, or use the same `params` block across re-applies.
- `VOODU_DB_STORAGE` (e.g. `"10Gi"`) is currently informational. Docker volumes have no native quota; a future driver (loop-mount on a sized image, ZFS quota) will honour it.

## Development

```bash
make build         # builds bin/voodu-postgres for the host arch
make test          # runs unit tests
make lint          # go vet
make cross         # cross-compiles linux amd64 + arm64 to dist/
```

Local install against a voodu controller running on the same host:

```bash
sudo make install-local PLUGINS_ROOT=/opt/voodu/plugins
```

## License

MIT.
