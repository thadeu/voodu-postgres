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
