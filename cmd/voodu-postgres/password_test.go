// Tests for resolveOrGeneratePassword. Covers the three precedence
// cases (HCL override > existing bucket > auto-gen) and pinning
// the auto-gen format (256-bit hex). The actual lifecycle smoke
// test (does the controller persist via config_set?) lives in
// main_test.go where we exercise cmdExpand end-to-end.

package main

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestResolveOrGeneratePassword_HCLOverrideWins(t *testing.T) {
	// `password = "explicit"` in HCL trumps everything. Even when
	// the bucket has a previous auto-gen value, operator-explicit
	// wins. isNew=false because we don't persist HCL values.
	spec := &postgresSpec{Password: "explicit-from-hcl"}
	config := map[string]string{passwordKey: "stale-from-bucket"}

	pw, isNew, err := resolveOrGeneratePassword(spec, config)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if pw != "explicit-from-hcl" {
		t.Errorf("HCL override lost: got %q", pw)
	}

	if isNew {
		t.Error("HCL override must not be flagged isNew (no persist)")
	}
}

func TestResolveOrGeneratePassword_ExistingBucketReused(t *testing.T) {
	// HCL empty + bucket has password → reuse, no generation.
	// This is the steady-state path on subsequent applies.
	spec := &postgresSpec{Password: ""}
	config := map[string]string{passwordKey: "deadbeef"}

	pw, isNew, err := resolveOrGeneratePassword(spec, config)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if pw != "deadbeef" {
		t.Errorf("bucket value lost: got %q, want deadbeef", pw)
	}

	if isNew {
		t.Error("reused password must not be flagged isNew")
	}
}

func TestResolveOrGeneratePassword_FirstApplyGenerates(t *testing.T) {
	// First apply: HCL empty, bucket empty → generate fresh.
	// isNew=true so the caller emits a config_set action.
	spec := &postgresSpec{Password: ""}
	config := map[string]string{}

	pw, isNew, err := resolveOrGeneratePassword(spec, config)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if pw == "" {
		t.Fatal("expected generated password, got empty string")
	}

	if !isNew {
		t.Error("freshly generated password must be flagged isNew")
	}

	// 256 bits = 32 bytes = 64 hex chars.
	if len(pw) != 64 {
		t.Errorf("expected 64 hex chars (32 bytes), got %d: %q", len(pw), pw)
	}

	if _, err := hex.DecodeString(pw); err != nil {
		t.Errorf("generated password not valid hex: %v (got %q)", err, pw)
	}
}

func TestResolveOrGeneratePassword_EmptyBucketStringTreatedAsAbsent(t *testing.T) {
	// `vd config set POSTGRES_PASSWORD=` (empty value) shouldn't
	// disable auth — postgres won't start without a password.
	// Plugin treats empty as "absent" and generates.
	spec := &postgresSpec{Password: ""}
	config := map[string]string{passwordKey: ""}

	pw, isNew, err := resolveOrGeneratePassword(spec, config)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if pw == "" {
		t.Error("empty bucket value should trigger generation, not be passed through")
	}

	if !isNew {
		t.Error("isNew should be true when bucket is effectively empty")
	}
}

func TestResolveOrGeneratePassword_NilSpecGenerates(t *testing.T) {
	// Defensive: nil spec shouldn't crash. Treats as "no HCL
	// override" and falls through to bucket / generate.
	pw, isNew, err := resolveOrGeneratePassword(nil, nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if !isNew || pw == "" {
		t.Errorf("nil spec + nil config should generate, got pw=%q isNew=%v", pw, isNew)
	}
}

func TestGeneratePassword_DistinctValues(t *testing.T) {
	// 256-bit collision is astronomically unlikely; two consecutive
	// calls returning the same value would mean crypto/rand is
	// broken. Pin against accidental seeding regression.
	a, err := generatePassword()
	if err != nil {
		t.Fatal(err)
	}

	b, err := generatePassword()
	if err != nil {
		t.Fatal(err)
	}

	if a == b {
		t.Errorf("two generations returned same value (entropy broken?): %q", a)
	}
}

func TestGeneratePassword_OnlyHexChars(t *testing.T) {
	pw, err := generatePassword()
	if err != nil {
		t.Fatal(err)
	}

	const hexChars = "0123456789abcdef"

	for _, r := range pw {
		if !strings.ContainsRune(hexChars, r) {
			t.Errorf("non-hex character %q in password %q", r, pw)
		}
	}
}
