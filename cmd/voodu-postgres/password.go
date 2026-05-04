// Password generation + resolution helpers shared by `cmdExpand`
// (idempotent reuse / first-apply generation) and (M-P4)
// `cmdNewPassword` (manual rotate).
//
// All randomness comes from crypto/rand — no math/rand fallback.
// Plugin authors who want a deterministic password (dev /
// test environment) can either:
//
//   - Set `password = "..."` in HCL — operator-explicit override
//     wins, password lives in the repo.
//   - Pre-set POSTGRES_PASSWORD via `vd config set <ref> set
//     POSTGRES_PASSWORD=...` before the first apply; the plugin
//     reads from config first and only generates when the bucket
//     is empty.

package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// passwordKey is the config bucket key holding the resolved
// password. Same name on both sides of the contract: cmdExpand
// writes it (first apply), cmdLink reads it (M-P4),
// `vd config get|set` surfaces it.
const passwordKey = "POSTGRES_PASSWORD"

// passwordEntropyBytes is the number of random bytes used when
// generating a fresh password (hex-encoded → 2x chars on the
// wire). 32 bytes = 256 bits, the same posture voodu-redis ships;
// hex form is alphanumeric so it round-trips through env files,
// postgres URLs, and shell scripts without escaping.
const passwordEntropyBytes = 32

// resolveOrGeneratePassword decides what password the postgres
// instance gets at boot. Precedence (highest first):
//
//  1. Operator-explicit `password = "..."` in HCL — never
//     persisted, lives in the repo. isNew=false.
//  2. Existing POSTGRES_PASSWORD in the controller-supplied
//     config bucket — reused across applies for stability.
//     isNew=false.
//  3. Generated 32-byte hex string — caller must emit a
//     config_set action so the next apply sees it via path 2.
//     isNew=true.
//
// Empty-string passwords in the bucket are treated as absent —
// operators who explicitly want no auth must remove the key
// entirely (`vd config <ref> unset POSTGRES_PASSWORD`), not set
// it to "". (Postgres itself will NOT start without a password
// when POSTGRES_PASSWORD is empty AND POSTGRES_HOST_AUTH_METHOD
// is unset, so this is just a defensive read — the plugin always
// supplies something non-empty.)
func resolveOrGeneratePassword(spec *postgresSpec, config map[string]string) (password string, isNew bool, err error) {
	if spec != nil && spec.Password != "" {
		return spec.Password, false, nil
	}

	if existing, ok := config[passwordKey]; ok && existing != "" {
		return existing, false, nil
	}

	fresh, err := generatePassword()
	if err != nil {
		return "", false, err
	}

	return fresh, true, nil
}

// generatePassword returns a hex-encoded 256-bit random string.
// crypto/rand fails closed — no fallback to weaker sources.
func generatePassword() (string, error) {
	buf := make([]byte, passwordEntropyBytes)

	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("read random: %w", err)
	}

	return hex.EncodeToString(buf), nil
}
