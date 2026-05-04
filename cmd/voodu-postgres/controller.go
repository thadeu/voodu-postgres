// Shared infra for the M-P4 dispatch commands (link, unlink,
// new-password, info, expose, unexpose). Mirrors the voodu-redis
// pattern — when the pkg/plugin/sdk lands, this whole file gets
// replaced by `import sdk`.
//
// The controller streams an envelope on stdin describing where
// to call back, the operator's args ride on os.Args, and the
// plugin emits a dispatchOutput with a list of side-effects
// (config_set / config_unset / etc.) the controller applies
// post-invoke.
//
// Two HTTP endpoints used by every command:
//
//   - GET /describe?kind=&scope=&name=  — the manifest's spec
//     (so we can read replicas count, env vars, etc.)
//   - GET /config?scope=&name=          — the merged config bucket
//     (so we can pull the auto-gen password back, list linked
//     consumers, etc.)

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// invocationContext is the JSON envelope the controller writes
// to stdin. Plugin reads it once at startup to learn where to
// call back. Args don't appear here — they arrive via os.Args.
type invocationContext struct {
	Plugin        string `json:"plugin"`
	Command       string `json:"command"`
	ControllerURL string `json:"controller_url,omitempty"`
	PluginDir     string `json:"plugin_dir,omitempty"`
	NodeName      string `json:"node_name,omitempty"`
}

// dispatchOutput is the envelope-data shape the controller
// expects on stdout for dispatch commands. Message is the
// operator-facing one-liner; Actions is the queue the controller
// applies post-invoke.
type dispatchOutput struct {
	Message string           `json:"message"`
	Actions []dispatchAction `json:"actions"`
}

// readInvocationContext decodes the JSON envelope on stdin.
// Empty stdin → falls back to env vars so direct CLI invocation
// (smoke testing without dispatch) still works.
func readInvocationContext() (*invocationContext, error) {
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		return nil, fmt.Errorf("read stdin: %w", err)
	}

	ctx := &invocationContext{}

	if len(raw) > 0 {
		if err := json.Unmarshal(raw, ctx); err != nil {
			return nil, fmt.Errorf("decode stdin: %w", err)
		}
	}

	if ctx.ControllerURL == "" {
		ctx.ControllerURL = os.Getenv("VOODU_CONTROLLER_URL")
	}

	if ctx.PluginDir == "" {
		ctx.PluginDir = os.Getenv("VOODU_PLUGIN_DIR")
	}

	return ctx, nil
}

// writeDispatchOutput encodes the dispatch result inside the
// standard plugin envelope.
func writeDispatchOutput(out dispatchOutput) error {
	enc := json.NewEncoder(os.Stdout)

	return enc.Encode(envelope{Status: "ok", Data: out})
}

// splitScopeName parses "scope/name" or just "name". Empty
// scope when no slash present. Mirrors voodu-redis splitScopeName
// — kept independent so the plugin doesn't import voodu internals.
func splitScopeName(ref string) (scope, name string) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", ""
	}

	if i := strings.Index(ref, "/"); i >= 0 {
		return ref[:i], ref[i+1:]
	}

	return "", ref
}

// hasHelpFlag scans args for -h / --help. Used by every
// dispatch command's prelude to short-circuit and print help
// text instead of trying to dispatch.
func hasHelpFlag(args []string) bool {
	for _, a := range args {
		if a == "-h" || a == "--help" {
			return true
		}
	}

	return false
}

// controllerClient is the tiny HTTP client the plugin uses to
// call back into the controller. Two endpoints today:
//
//   - GET /describe?kind=&scope=&name=  → manifest spec
//   - GET /config?scope=&name=          → merged config bucket
type controllerClient struct {
	baseURL string
	http    *http.Client
}

func newControllerClient(baseURL string) *controllerClient {
	return &controllerClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// fetchSpec returns the manifest spec for (kind, scope, name).
// Returns the spec map so callers can index by field name
// (env, replicas, etc.).
func (c *controllerClient) fetchSpec(kind, scope, name string) (map[string]any, error) {
	if c.baseURL == "" {
		return nil, fmt.Errorf("no controller_url available (set VOODU_CONTROLLER_URL or run via dispatch endpoint)")
	}

	u := fmt.Sprintf("%s/describe?kind=%s&scope=%s&name=%s",
		c.baseURL,
		url.QueryEscape(kind),
		url.QueryEscape(scope),
		url.QueryEscape(name),
	)

	resp, err := c.http.Get(u)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("describe %s/%s/%s: HTTP %d: %s", kind, scope, name, resp.StatusCode, body)
	}

	var env struct {
		Data struct {
			Manifest struct {
				Spec map[string]any `json:"spec"`
			} `json:"manifest"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, fmt.Errorf("decode describe response: %w", err)
	}

	return env.Data.Manifest.Spec, nil
}

// fetchConfig returns the merged config bucket as a string-typed
// map (POSTGRES_PASSWORD, POSTGRES_LINKED_CONSUMERS, etc.).
// Empty bucket surfaces as an empty map (not nil) so callers can
// range it safely.
func (c *controllerClient) fetchConfig(scope, name string) (map[string]string, error) {
	if c.baseURL == "" {
		return nil, fmt.Errorf("no controller_url available")
	}

	u := fmt.Sprintf("%s/config?scope=%s&name=%s",
		c.baseURL,
		url.QueryEscape(scope),
		url.QueryEscape(name),
	)

	resp, err := c.http.Get(u)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("config get %s/%s: HTTP %d: %s", scope, name, resp.StatusCode, body)
	}

	var env struct {
		Data struct {
			Vars map[string]string `json:"vars"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, fmt.Errorf("decode config response: %w", err)
	}

	if env.Data.Vars == nil {
		env.Data.Vars = map[string]string{}
	}

	return env.Data.Vars, nil
}
