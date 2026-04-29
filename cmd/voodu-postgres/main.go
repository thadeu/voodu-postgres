// Command voodu-postgres is the database plugin that materialises
// `database "<name>" { engine = "postgres" }` manifests into a
// single-node postgres statefulset.
//
// # Plugin contract
//
// Mirrors the contract documented in voodu's pkg/plugin: subcommands
// `create` and `destroy` invoked by the controller's
// DatabaseHandler. The reconciler injects:
//
//	VOODU_DB_NAME      — database manifest's name (also used as DB)
//	VOODU_DB_VERSION   — postgres major version (default "15")
//	VOODU_DB_STORAGE   — informational ("10Gi"); ignored on M-S4
//	VOODU_DB_PARAMS    — JSON map; supports {"password": "..."}
//	VOODU_CONTROLLER_URL — default http://127.0.0.1:8686
//
// On `create`, the plugin:
//
//   1. Reads (or generates) a postgres password
//   2. POSTs a statefulset manifest to /apply, scoped under
//      "databases", with replicas=1, a per-pod volume_claim
//      named "data", and the password in env
//   3. Emits an envelope with connection coordinates the
//      reference resolver (${ref.database.NAME.host}) consumes
//
// On `destroy`, the plugin DELETEs the statefulset manifest. The
// statefulset handler tears down pods but leaves volumes — the
// operator opts into data destruction explicitly via `vd delete
// --prune`. This matches what `vd delete database/...` should
// feel like: the plugin is responsible for the database manifest
// lifecycle, the underlying statefulset is just the building
// block.
//
// Single-node only on this version. Postgres replication needs
// init scripts that bootstrap pg-1+ from pg-0.scope, plus
// pg_hba.conf wiring — out of scope for the POC. The shape is
// laid for it: the statefulset already supports replicas, per-
// pod aliases, and rolling restart top-down. A future
// voodu-postgres iteration adds the bootstrap scripts.
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// version is overridden at build time via -ldflags so `voodu-postgres
// --version` matches plugin.yml. Default value is the dev-build
// marker; release pipeline pins it to the tag.
var version = "dev"

const (
	// pluginScope is the scope every database-plugin-managed
	// statefulset lives under. Hardcoded here so the database
	// kind (which is unscoped — `database "pg"` carries no
	// scope label) can still produce scoped statefulset names
	// without leaking the scope concept up to the operator.
	// `pg` becomes statefulset/databases/pg under the hood.
	pluginScope = "databases"

	// defaultPostgresVersion lands on the image tag when the
	// manifest doesn't pin one. Picked at one major behind
	// latest so brand-new releases don't break deploys silently.
	defaultPostgresVersion = "15"
)

func main() {
	if len(os.Args) < 2 {
		emitErr("usage: voodu-postgres <create|destroy|--version> [args...]")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "--version", "-v", "version":
		fmt.Println(version)
		return

	case "create":
		if err := cmdCreate(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	case "destroy":
		if err := cmdDestroy(); err != nil {
			emitErr(err.Error())
			os.Exit(1)
		}

	default:
		emitErr(fmt.Sprintf("unknown subcommand %q (want create|destroy)", os.Args[1]))
		os.Exit(1)
	}
}

// cmdCreate provisions a single postgres node by POSTing a
// statefulset manifest to /apply. Idempotent: a re-apply with
// the same VOODU_DB_NAME and same params is a no-op at the
// controller layer (statefulset spec hash unchanged → no rolling
// restart).
//
// The password is resolved with this precedence:
//
//   1. VOODU_DB_PARAMS["password"] — operator-supplied
//   2. previously-stored value at /config/databases/<name>/POSTGRES_PASSWORD
//      (this version skips this — would require config GET; left
//      as a follow-up. For now a re-create with no params
//      generates a fresh random password and the previous data
//      becomes unreadable. Acceptable for POC.)
//   3. fresh random hex
//
// The envelope's Data block carries fields that match the
// conventional ${ref.database.<name>.<field>} keys: host, port,
// username, password, database, url.
func cmdCreate() error {
	dbName := os.Getenv("VOODU_DB_NAME")
	if dbName == "" {
		return fmt.Errorf("VOODU_DB_NAME is required")
	}

	pgVersion := os.Getenv("VOODU_DB_VERSION")
	if pgVersion == "" {
		pgVersion = defaultPostgresVersion
	}

	password, err := resolvePassword()
	if err != nil {
		return fmt.Errorf("resolve password: %w", err)
	}

	manifest := buildStatefulsetManifest(dbName, pgVersion, password)

	if err := postManifest(manifest); err != nil {
		return fmt.Errorf("apply statefulset: %w", err)
	}

	host := dbName + "-0." + pluginScope

	connURL := fmt.Sprintf("postgres://postgres:%s@%s:5432/%s?sslmode=disable", password, host, dbName)

	emitOK(map[string]any{
		"host":     host,
		"port":     5432,
		"username": "postgres",
		"password": password,
		"database": dbName,
		"url":      connURL,
	})

	return nil
}

// cmdDestroy DELETEs the statefulset manifest. Volumes are
// preserved by default (statefulset semantics) — operator runs
// `vd delete statefulset/databases/<name> --prune` to wipe data.
// Idempotent: deleting a non-existent statefulset is a no-op
// (the handler treats missing as success).
func cmdDestroy() error {
	dbName := os.Getenv("VOODU_DB_NAME")
	if dbName == "" {
		return fmt.Errorf("VOODU_DB_NAME is required")
	}

	q := url.Values{}
	q.Set("kind", "statefulset")
	q.Set("scope", pluginScope)
	q.Set("name", dbName)

	endpoint := controllerURL() + "/apply?" + q.Encode()

	req, err := http.NewRequest(http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}

	resp, err := httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("controller delete: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("controller delete %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	emitOK(map[string]any{
		"name":    dbName,
		"deleted": true,
		"volumes": "preserved (vd delete --prune to wipe)",
	})

	return nil
}

// buildStatefulsetManifest composes the wire-shape JSON the
// controller expects on /apply: a single-element array of
// {"kind","scope","name","spec"} envelopes. The spec uses
// statefulset's image-mode, single replica, with one
// volume_claim for /var/lib/postgresql/data.
//
// PGDATA is set to a subdirectory of the mount because postgres
// rejects starting in a non-empty mountpoint that wasn't
// initialised by initdb — the docker-volume mount root often
// has a `lost+found` directory that triggers the rejection.
// `pgdata/` under the mount is empty on first boot and survives
// across restarts inside the same volume.
func buildStatefulsetManifest(dbName, pgVersion, password string) []map[string]any {
	spec := map[string]any{
		"image":    "postgres:" + pgVersion,
		"replicas": 1,
		"env": map[string]string{
			"POSTGRES_PASSWORD": password,
			"POSTGRES_DB":       dbName,
			"PGDATA":            "/var/lib/postgresql/data/pgdata",
		},
		"ports": []string{"5432"},
		"volume_claims": []map[string]any{
			{
				"name":       "data",
				"mount_path": "/var/lib/postgresql/data",
			},
		},
	}

	return []map[string]any{
		{
			"kind":  "statefulset",
			"scope": pluginScope,
			"name":  dbName,
			"spec":  spec,
		},
	}
}

// postManifest serialises the manifest list and POSTs it to
// /apply. prune=false because this single manifest doesn't
// represent the full state of the `databases` scope — other
// databases the operator has live there too. Without
// prune=false the controller would tear down sibling
// statefulsets on each plugin invocation, which would be
// catastrophic.
func postManifest(manifests any) error {
	body, err := json.Marshal(manifests)
	if err != nil {
		return err
	}

	endpoint := controllerURL() + "/apply?prune=false"

	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("controller post: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("controller %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	return nil
}

// resolvePassword pulls the password from VOODU_DB_PARAMS first
// (operator override) and falls back to a fresh random hex.
// 16 bytes = 128 bits is plenty for postgres role auth at the
// scale voodu targets.
func resolvePassword() (string, error) {
	raw := os.Getenv("VOODU_DB_PARAMS")
	if raw != "" {
		var params map[string]string

		if err := json.Unmarshal([]byte(raw), &params); err != nil {
			return "", fmt.Errorf("decode VOODU_DB_PARAMS: %w", err)
		}

		if pw, ok := params["password"]; ok && pw != "" {
			return pw, nil
		}
	}

	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}

	return hex.EncodeToString(b[:]), nil
}

// controllerURL resolves the voodu controller endpoint. Mirrors
// the CLI's resolution but only honours the env var (plugins
// don't have a flag layer).
func controllerURL() string {
	if v := os.Getenv("VOODU_CONTROLLER_URL"); v != "" {
		return strings.TrimRight(v, "/")
	}

	return "http://127.0.0.1:8686"
}

// httpClient is the shared http.Client for plugin → controller
// calls. Timeout is generous (30s) because /apply may serialise
// behind a reconciler retry queue under heavy load; lower
// values would surface false-failure on a healthy controller.
func httpClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second}
}

// emitOK writes a success envelope to stdout. The controller
// parses this into ev.Envelope.Data and persists under
// /status/databases/<name> — that's where ${ref.database.X.field}
// resolves from at deployment apply time.
func emitOK(data map[string]any) {
	enc := json.NewEncoder(os.Stdout)

	_ = enc.Encode(map[string]any{
		"status": "ok",
		"data":   data,
	})
}

// emitErr writes an error envelope. Both the message AND a
// non-zero exit code are needed: the controller checks exit
// first (cheaper), then parses the envelope to surface the
// message in `vd apply` output. Without exit non-zero a failed
// plugin would look like a successful one to the reconciler.
func emitErr(msg string) {
	enc := json.NewEncoder(os.Stderr)

	_ = enc.Encode(map[string]any{
		"status": "error",
		"error":  msg,
	})
}
