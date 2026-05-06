// Tests for the M-P4 expose surface. The dispatch flow itself
// is straightforward (one config_set/config_unset action per
// command); the interesting half is the corresponding cmdExpand
// path that flips the ports binding when PG_EXPOSE_PUBLIC=true.
//
// Pinning here:
//
//   - Default (exposed=false) → ports = ["<port>"] (loopback)
//   - exposed=true → ports = ["0.0.0.0:<port>"] (internet-facing)
//   - The flip survives custom port via spec.Port
//
// The constant exposeFlagKey is shared between the dispatch
// command (writes it) and cmdExpand (reads it from req.Config).

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestComposeStatefulsetDefaults_PortsLoopbackByDefault(t *testing.T) {
	// Default exposed=false → ports = ["5432"] (voodu's
	// platform invariant prefixes 127.0.0.1 to bare port).
	spec := mustParse(t, nil)
	got := composeStatefulsetDefaults("s", "n", spec, "pw", "repl-pw", 0, false)

	ports, ok := got["ports"].([]any)
	if !ok || len(ports) != 1 {
		t.Fatalf("ports shape wrong: %v", got["ports"])
	}

	if ports[0] != "5432" {
		t.Errorf("expected loopback bare port '5432', got %v", ports[0])
	}
}

func TestComposeStatefulsetDefaults_PortsExposedFlipsTo0000(t *testing.T) {
	// exposed=true → ports = ["0.0.0.0:5432"] — voodu's
	// platform takes "0.0.0.0:" prefix verbatim and binds
	// public.
	spec := mustParse(t, nil)
	got := composeStatefulsetDefaults("s", "n", spec, "pw", "repl-pw", 0, true)

	ports := got["ports"].([]any)

	if ports[0] != "0.0.0.0:5432" {
		t.Errorf("expected '0.0.0.0:5432', got %v", ports[0])
	}
}

func TestComposeStatefulsetDefaults_ExposeHonoursCustomPort(t *testing.T) {
	// Operator override `port = 5433` + expose flip → "0.0.0.0:5433".
	spec := mustParse(t, map[string]any{"port": 5433})
	got := composeStatefulsetDefaults("s", "n", spec, "pw", "repl-pw", 0, true)

	ports := got["ports"].([]any)

	if ports[0] != "0.0.0.0:5433" {
		t.Errorf("expected '0.0.0.0:5433' with custom port, got %v", ports[0])
	}
}

func TestExposeFlagKey_PinsContractWithCmdExpand(t *testing.T) {
	// Sanity check: the bucket key the expose command writes
	// must match what cmdExpand reads. Hard-coded test of the
	// constant — if someone renames it on one side without the
	// other, this fails loudly.
	if exposeFlagKey != "PG_EXPOSE_PUBLIC" {
		t.Errorf("exposeFlagKey changed from PG_EXPOSE_PUBLIC to %q — verify cmdExpand reads same key", exposeFlagKey)
	}
}

func TestExposeHelp_MentionsSecurityCaveat(t *testing.T) {
	// The help text doubles as operator-facing documentation —
	// must remind them about password rotation when exposing.
	if !strings.Contains(exposeHelp, "rotate") || !strings.Contains(exposeHelp, "new-password") {
		t.Errorf("expose help text should remind about password rotation:\n%s", exposeHelp)
	}
}

// TestExposeHelp_MentionsHAConstraint pins the operator-facing
// docstring covers the "expose only with replicas=1" rule. With
// replicas > 1, voodu's reconciler binds 0.0.0.0:<port> on every
// pod and the second pod fails to start; the constraint is real
// and operators need a paper trail explaining why expose got
// refused. If the wording drifts away from the words below, the
// help page no longer matches what they'll see in error output.
func TestExposeHelp_MentionsHAConstraint(t *testing.T) {
	wantPhrases := []string{
		"replicas",
		"pgbouncer",
		"SSH tunnel",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(exposeHelp, want) {
			t.Errorf("expose help text missing %q (HA / replicas guidance):\n%s",
				want, exposeHelp)
		}
	}
}

// TestComposeExposeActions_RefusesWhenReplicasGreaterThanOne pins
// the safety net itself. The fetched-spec carries replicas=2 (or
// any >1), composeExposeActions must error before queuing the
// apply_manifest. Without this guard, the second pod boot would
// fail with "port already in use" — which is what the user
// originally hit and what we shipped this refusal to prevent.
//
// We exercise the helper via a stub controller server because
// composeExposeActions reaches out to /describe to read the
// replicas count. The stub returns a fixed spec; the helper must
// surface a useful error pointing at the alternatives.
func TestComposeExposeActions_RefusesWhenReplicasGreaterThanOne(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// /describe?kind=statefulset&scope=clowk-lp&name=db
		// returns a spec with replicas = 2.
		_, _ = w.Write([]byte(`{
  "status": "ok",
  "data": {
    "manifest": {
      "spec": {
        "replicas": 2,
        "env": {
          "PG_PORT": "5432"
        }
      }
    }
  }
}`))
	}))
	defer ts.Close()

	t.Setenv("VOODU_CONTROLLER_URL", ts.URL)

	_, err := composeExposeActions("clowk-lp", "db", true)
	if err == nil {
		t.Fatal("expected error when replicas > 1")
	}

	wantPhrases := []string{
		"replicas=2",
		"pgbouncer",
		"SSH tunnel",
	}

	for _, want := range wantPhrases {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error message missing %q (operator should see alternatives):\n%s",
				want, err.Error())
		}
	}
}

// TestComposeExposeActions_AllowsUnexposeRegardlessOfReplicas pins
// the asymmetric safety: rolling EXPOSE backwards (unexpose) is
// always safe regardless of replica count — flipping ports back
// to loopback never causes a port conflict. The guard is on
// `exposed=true` only; `exposed=false` should pass through.
func TestComposeExposeActions_AllowsUnexposeRegardlessOfReplicas(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
  "status": "ok",
  "data": {
    "manifest": {
      "spec": {
        "replicas": 3,
        "env": {
          "PG_PORT": "5432"
        }
      }
    }
  }
}`))
	}))
	defer ts.Close()

	t.Setenv("VOODU_CONTROLLER_URL", ts.URL)

	actions, err := composeExposeActions("clowk-lp", "db", false)
	if err != nil {
		t.Fatalf("unexpose with replicas=3 should succeed, got %v", err)
	}

	if len(actions) == 0 {
		t.Fatal("unexpose should still emit actions (config_unset + apply_manifest)")
	}
}
