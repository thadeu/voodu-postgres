// Tests for the wal_archive surface: parser, validator, strategy
// dispatch, and conf renderer. The (asset, statefulset) plumbing —
// adding the asset bind + the strategy's volume contribution — is
// covered in main_test.go where the e2e expand flow lives. Local-
// strategy specifics (default destination, dangerous-path rejection,
// archive_command shape) live in strategy_local_test.go.
//
// Pinning here:
//
//   - Default spec is enabled with strategy = local.
//   - Operator can opt out via `enabled = false`.
//   - parseWALArchiveSpec maps `wal_archive = { ... }` cleanly.
//   - validateWALArchiveSpec dispatches to the strategy's Validate.
//   - selectStrategy returns localStrategy for "" and "local",
//     errors for unknown values.
//   - renderWALArchiveConf takes the strategy's plan and emits
//     a deterministic, alphabetically-ordered conf body.

package main

import (
	"strings"
	"testing"
)

func TestParseWALArchiveSpec_AbsentBlockReturnsNil(t *testing.T) {
	// Caller substitutes defaultWALArchiveSpec when nil.
	got, err := parseWALArchiveSpec(nil)
	if err != nil {
		t.Fatalf("parse nil: %v", err)
	}

	if got != nil {
		t.Errorf("expected nil for absent block, got %+v", got)
	}

	got, err = parseWALArchiveSpec(map[string]any{})
	if err != nil {
		t.Fatalf("parse empty map: %v", err)
	}

	if got != nil {
		t.Errorf("expected nil for spec without wal_archive key, got %+v", got)
	}
}

func TestParseWALArchiveSpec_EmptyBlockUsesDefaults(t *testing.T) {
	// `wal_archive = {}` — block declared but body empty.
	// Defaults fill in (enabled, strategy=local, empty
	// destination → caller substitutes default at expand).
	got, err := parseWALArchiveSpec(map[string]any{
		"wal_archive": map[string]any{},
	})

	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got == nil {
		t.Fatal("expected non-nil spec")
	}

	if !got.Enabled {
		t.Error("default Enabled should be true")
	}

	if got.Strategy != "local" {
		t.Errorf("default Strategy: got %q, want local", got.Strategy)
	}

	if got.Destination != "" {
		t.Errorf("default Destination should stay empty (caller substitutes), got %q", got.Destination)
	}
}

func TestParseWALArchiveSpec_OperatorOverrides(t *testing.T) {
	got, err := parseWALArchiveSpec(map[string]any{
		"wal_archive": map[string]any{
			"enabled":     false,
			"strategy":    "local",
			"destination": "/srv/pg/wal",
		},
	})

	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got.Enabled {
		t.Error("expected Enabled=false from operator override")
	}

	if got.Strategy != "local" {
		t.Errorf("Strategy: got %q", got.Strategy)
	}

	if got.Destination != "/srv/pg/wal" {
		t.Errorf("Destination: got %q", got.Destination)
	}
}

func TestParseWALArchiveSpec_DestinationOverride(t *testing.T) {
	got, err := parseWALArchiveSpec(map[string]any{
		"wal_archive": map[string]any{"destination": "/srv/custom/wal"},
	})

	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got.Destination != "/srv/custom/wal" {
		t.Errorf("Destination: got %q, want /srv/custom/wal", got.Destination)
	}
}

func TestParseWALArchiveSpec_StrategyEmptyKeepsDefault(t *testing.T) {
	// `strategy = ""` reverts to the default ("local") so an
	// operator who wants to be explicit-but-default doesn't
	// trigger the "unknown strategy" error.
	got, err := parseWALArchiveSpec(map[string]any{
		"wal_archive": map[string]any{"strategy": ""},
	})

	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got.Strategy != "local" {
		t.Errorf("empty strategy should keep default, got %q", got.Strategy)
	}
}

func TestParseWALArchiveSpec_DestinationEmptyKeepsForCallerToDefault(t *testing.T) {
	// `destination = ""` leaves Destination empty so the caller
	// (cmdExpand) substitutes strategy.DefaultDestination(scope, name)
	// at expand time. Same posture as image/database empty.
	got, err := parseWALArchiveSpec(map[string]any{
		"wal_archive": map[string]any{"destination": ""},
	})

	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got.Destination != "" {
		t.Errorf("empty destination should stay empty for caller-side default substitution, got %q", got.Destination)
	}
}

func TestParseWALArchiveSpec_TypeErrors(t *testing.T) {
	cases := []struct {
		name string
		spec any
		want string
	}{
		{"block-not-map", "string-instead-of-block", "wal_archive"},
		{"enabled-not-bool", map[string]any{"enabled": "yes"}, "enabled"},
		{"strategy-not-string", map[string]any{"strategy": 42}, "strategy"},
		{"destination-not-string", map[string]any{"destination": 42}, "destination"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseWALArchiveSpec(map[string]any{
				"wal_archive": tc.spec,
			})

			if err == nil {
				t.Fatalf("expected typed error for %s", tc.name)
			}

			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should mention %q, got: %v", tc.want, err)
			}
		})
	}
}

func TestSelectStrategy_LocalForEmptyAndExplicit(t *testing.T) {
	// "" and "local" both resolve to localStrategy. "" lets the
	// operator omit the field entirely without erroring.
	for _, name := range []string{"", "local"} {
		s, err := selectStrategy(name)
		if err != nil {
			t.Errorf("selectStrategy(%q): %v", name, err)
			continue
		}

		if s.Name() != "local" {
			t.Errorf("selectStrategy(%q).Name(): got %q, want local", name, s.Name())
		}
	}
}

func TestSelectStrategy_UnknownErrors(t *testing.T) {
	cases := []string{"s3", "r2", "rsync", "nfs", "cloud", "FOOBAR"}

	for _, name := range cases {
		_, err := selectStrategy(name)
		if err == nil {
			t.Errorf("selectStrategy(%q) should error (not implemented yet)", name)
			continue
		}

		if !strings.Contains(err.Error(), "unknown") {
			t.Errorf("error should say 'unknown', got: %v", err)
		}
	}
}

func TestValidateWALArchiveSpec_NilOK(t *testing.T) {
	// nil spec means "use defaults" — caller handles substitution.
	if err := validateWALArchiveSpec(nil); err != nil {
		t.Errorf("nil should validate (caller fills defaults): %v", err)
	}
}

func TestValidateWALArchiveSpec_DisabledSkipsChecks(t *testing.T) {
	// When disabled, destination is irrelevant — validator skips
	// strategy dispatch entirely.
	bad := &walArchiveSpec{Enabled: false, Strategy: "nonsense", Destination: "garbage"}
	if err := validateWALArchiveSpec(bad); err != nil {
		t.Errorf("disabled spec should skip checks: %v", err)
	}
}

func TestValidateWALArchiveSpec_UnknownStrategyRejected(t *testing.T) {
	spec := &walArchiveSpec{Enabled: true, Strategy: "s3"}

	err := validateWALArchiveSpec(spec)
	if err == nil {
		t.Fatal("expected error for unknown strategy")
	}

	if !strings.Contains(err.Error(), "unknown") {
		t.Errorf("error should say 'unknown', got: %v", err)
	}
}

func TestValidateWALArchiveSpec_DispatchesToLocalStrategy(t *testing.T) {
	// Spec with explicit strategy="local" + dangerous destination →
	// validator routes through localStrategy.Validate which rejects.
	spec := &walArchiveSpec{
		Enabled:     true,
		Strategy:    "local",
		Destination: "/etc/passwd",
	}

	err := validateWALArchiveSpec(spec)
	if err == nil {
		t.Fatal("expected error for dangerous destination via local strategy")
	}

	if !strings.Contains(err.Error(), "destination") {
		t.Errorf("error should mention 'destination', got: %v", err)
	}
}

func TestRenderWALArchiveConf_DisabledReturnsHeaderOnly(t *testing.T) {
	got := renderWALArchiveConf(&walArchiveSpec{Enabled: false}, walArchivePlan{})

	if !strings.Contains(got, "disabled") {
		t.Errorf("disabled spec should comment why:\n%s", got)
	}

	for _, line := range strings.Split(got, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		t.Errorf("disabled output should have no directive lines, got: %q", line)
	}
}

func TestRenderWALArchiveConf_NilSpecMatchesDisabled(t *testing.T) {
	// Defensive — nil shouldn't panic, behaves like disabled.
	got := renderWALArchiveConf(nil, walArchivePlan{})
	if !strings.Contains(got, "disabled") {
		t.Errorf("nil should render as disabled:\n%s", got)
	}
}

func TestRenderWALArchiveConf_EnabledEmitsRequiredDirectives(t *testing.T) {
	plan := localStrategy{}.Apply(defaultWALArchiveSpec(), "s", "n")
	got := renderWALArchiveConf(defaultWALArchiveSpec(), plan)

	wantLines := []string{
		"wal_level = replica",
		"archive_mode = on",
		"archive_timeout = 60",
		"archive_command = ", // value follows
	}

	for _, want := range wantLines {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

func TestRenderWALArchiveConf_ArchiveCommandFromPlan(t *testing.T) {
	// Strategy supplies the archive_command verbatim; renderer
	// just quotes it. Pin this so a future strategy change in
	// strategy_local.go flows through to the conf without a
	// renderer edit.
	plan := walArchivePlan{ArchiveCommand: "wal-g wal-push %p"}
	got := renderWALArchiveConf(defaultWALArchiveSpec(), plan)

	if !strings.Contains(got, "wal-g wal-push %p") {
		t.Errorf("archive_command should be the plan's verbatim string:\n%s", got)
	}
}

func TestRenderWALArchiveConf_AlphabeticalOrder(t *testing.T) {
	// Stable bytes across re-applies → asset digest doesn't flap.
	plan := localStrategy{}.Apply(defaultWALArchiveSpec(), "s", "n")
	got := renderWALArchiveConf(defaultWALArchiveSpec(), plan)

	// archive_command should appear before archive_mode (alpha sort).
	cmdIdx := strings.Index(got, "archive_command")
	modeIdx := strings.Index(got, "archive_mode")

	if cmdIdx < 0 || modeIdx < 0 {
		t.Fatalf("missing keys in:\n%s", got)
	}

	if cmdIdx > modeIdx {
		t.Errorf("expected archive_command before archive_mode (alphabetical):\n%s", got)
	}
}

func TestRenderWALArchiveConf_Deterministic(t *testing.T) {
	plan := localStrategy{}.Apply(defaultWALArchiveSpec(), "s", "n")
	a := renderWALArchiveConf(defaultWALArchiveSpec(), plan)
	b := renderWALArchiveConf(defaultWALArchiveSpec(), plan)

	if a != b {
		t.Error("render is not deterministic across calls")
	}
}
