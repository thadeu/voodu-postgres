// Tests for the M-P4 info surface. composeInfoSnapshot is the
// pure function that builds the data shape; formatInfoText is
// the text renderer. Both get unit tests; the dispatch flow
// itself reads stdin + HTTPs the controller and is exercised
// in smoke E2E.

package main

import (
	"strings"
	"testing"
)

func TestComposeInfoSnapshot_DefaultsForMinimalSpec(t *testing.T) {
	// Empty spec / empty bucket: defaults flow through (postgres
	// user/db/port, replicator user, primary at 0, single replica,
	// loopback bind, no consumers).
	snap := composeInfoSnapshot("scope", "name", map[string]any{}, map[string]string{})

	if snap.Ref != "scope/name" {
		t.Errorf("ref: %q", snap.Ref)
	}

	if snap.Replicas != 1 {
		t.Errorf("replicas: %d", snap.Replicas)
	}

	if snap.PrimaryFQDN != "name-0.scope.voodu" {
		t.Errorf("primary fqdn: %q", snap.PrimaryFQDN)
	}

	if len(snap.StandbyFQDNs) != 0 {
		t.Errorf("expected no standbys for replicas=1, got %v", snap.StandbyFQDNs)
	}

	if snap.SuperUser != "postgres" || snap.Database != "postgres" || snap.Port != 5432 {
		t.Errorf("defaults wrong: %+v", snap)
	}

	if snap.ReplicationUser != "replicator" {
		t.Errorf("replication_user default: %q", snap.ReplicationUser)
	}

	if snap.PasswordRedacted != "<unset>" {
		t.Errorf("password redacted: %q", snap.PasswordRedacted)
	}

	if snap.Exposed {
		t.Error("expected loopback default")
	}
}

func TestComposeInfoSnapshot_StandbyFQDNsListed(t *testing.T) {
	spec := map[string]any{"replicas": 4}

	snap := composeInfoSnapshot("s", "db", spec, map[string]string{})

	want := []string{"db-1.s.voodu", "db-2.s.voodu", "db-3.s.voodu"}
	if len(snap.StandbyFQDNs) != len(want) {
		t.Fatalf("standby count: got %d, want %d", len(snap.StandbyFQDNs), len(want))
	}

	for i, w := range want {
		if snap.StandbyFQDNs[i] != w {
			t.Errorf("standby[%d]: got %q, want %q", i, snap.StandbyFQDNs[i], w)
		}
	}
}

func TestComposeInfoSnapshot_PrimaryOrdinalNonZero(t *testing.T) {
	// M-P5 failover flips PG_PRIMARY_ORDINAL — info should
	// reflect the current primary (e.g. pod-1) and exclude it
	// from the standby list.
	spec := map[string]any{
		"replicas": 3,
		"env":      map[string]any{"PG_PRIMARY_ORDINAL": "1"},
	}

	snap := composeInfoSnapshot("s", "db", spec, map[string]string{})

	if snap.PrimaryOrdinal != 1 {
		t.Errorf("primary ordinal: %d", snap.PrimaryOrdinal)
	}

	if snap.PrimaryFQDN != "db-1.s.voodu" {
		t.Errorf("primary fqdn: %q", snap.PrimaryFQDN)
	}

	want := []string{"db-0.s.voodu", "db-2.s.voodu"}
	if len(snap.StandbyFQDNs) != 2 || snap.StandbyFQDNs[0] != want[0] || snap.StandbyFQDNs[1] != want[1] {
		t.Errorf("standbys: got %v, want %v", snap.StandbyFQDNs, want)
	}
}

func TestComposeInfoSnapshot_WALArchiveDetected(t *testing.T) {
	spec := map[string]any{
		"volume_claims": []any{
			map[string]any{"name": "data", "mount_path": "/var/lib/postgresql/data"},
			map[string]any{"name": walArchiveClaimName, "mount_path": "/wal-archive"},
		},
	}

	snap := composeInfoSnapshot("s", "n", spec, map[string]string{})

	if !snap.WALArchiveEnabled {
		t.Error("wal_archive should be detected from volume_claim")
	}

	if snap.WALArchiveMountPath != "/wal-archive" {
		t.Errorf("mount path: %q", snap.WALArchiveMountPath)
	}
}

func TestComposeInfoSnapshot_WALArchiveDisabled(t *testing.T) {
	spec := map[string]any{
		"volume_claims": []any{
			map[string]any{"name": "data", "mount_path": "/var/lib/postgresql/data"},
		},
	}

	snap := composeInfoSnapshot("s", "n", spec, map[string]string{})

	if snap.WALArchiveEnabled {
		t.Error("wal_archive should be detected as disabled (no claim)")
	}
}

func TestComposeInfoSnapshot_ExposedFlag(t *testing.T) {
	config := map[string]string{"PG_EXPOSE_PUBLIC": "true"}
	snap := composeInfoSnapshot("s", "n", map[string]any{}, config)

	if !snap.Exposed {
		t.Error("expected Exposed=true when PG_EXPOSE_PUBLIC=true")
	}

	if !strings.Contains(snap.BindAddress, "0.0.0.0") {
		t.Errorf("bind address should mention 0.0.0.0: %q", snap.BindAddress)
	}
}

func TestComposeInfoSnapshot_LinkedConsumersSurfaced(t *testing.T) {
	config := map[string]string{linkedConsumersKey: "scope/web,scope/worker"}
	snap := composeInfoSnapshot("s", "n", map[string]any{}, config)

	if len(snap.LinkedConsumers) != 2 {
		t.Fatalf("expected 2 linked consumers, got: %v", snap.LinkedConsumers)
	}

	if snap.LinkedConsumers[0] != "scope/web" || snap.LinkedConsumers[1] != "scope/worker" {
		t.Errorf("consumers: %v", snap.LinkedConsumers)
	}
}

func TestComposeInfoSnapshot_PasswordsRedacted(t *testing.T) {
	config := map[string]string{
		passwordKey:            "abcdef0123456789abcdef",
		replicationPasswordKey: "deadbeef00112233",
	}

	snap := composeInfoSnapshot("s", "n", map[string]any{}, config)

	if !strings.HasSuffix(snap.PasswordRedacted, "...") || len(snap.PasswordRedacted) > 12 {
		t.Errorf("password not redacted properly: %q", snap.PasswordRedacted)
	}

	if !strings.HasSuffix(snap.ReplicationPasswordRedacted, "...") {
		t.Errorf("replication password not redacted: %q", snap.ReplicationPasswordRedacted)
	}
}

func TestRedactPassword(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", "<unset>"},
		{"shrt", "****"},
		{"12345678", "********"},
		{"123456789", "12345678..."},
		{"deadbeefcafe", "deadbeef..."},
	}

	for _, tc := range cases {
		got := redactPassword(tc.in)
		if got != tc.want {
			t.Errorf("redactPassword(%q): got %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFormatInfoText_IncludesCorePieces(t *testing.T) {
	snap := infoSnapshot{
		Ref:                         "clowk-lp/db",
		Image:                       "postgres:16",
		Replicas:                    3,
		PrimaryOrdinal:              0,
		PrimaryFQDN:                 "db-0.clowk-lp.voodu",
		StandbyFQDNs:                []string{"db-1.clowk-lp.voodu", "db-2.clowk-lp.voodu"},
		SuperUser:                   "appuser",
		Database:                    "appdata",
		Port:                        5432,
		ReplicationUser:             "replicator",
		PasswordRedacted:            "abcd1234...",
		ReplicationPasswordRedacted: "<unset>",
		WALArchiveEnabled:           true,
		WALArchiveMountPath:         "/wal-archive",
		Exposed:                     false,
		BindAddress:                 "127.0.0.1 (loopback only)",
		LinkedConsumers:             []string{"clowk-lp/web"},
	}

	text := formatInfoText(snap)

	wantLines := []string{
		"postgres clowk-lp/db",
		"image           postgres:16",
		"replicas        3",
		"primary         db-0.clowk-lp.voodu:5432",
		"standbys",
		"db-1.clowk-lp.voodu",
		"db-2.clowk-lp.voodu",
		"database        appdata",
		"super_user      appuser",
		"replication     replicator",
		"wal_archive     enabled @ /wal-archive",
		"linked consumers (1)",
		"clowk-lp/web",
	}

	for _, want := range wantLines {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in:\n%s", want, text)
		}
	}
}

func TestFormatInfoText_NoConsumersMessage(t *testing.T) {
	snap := infoSnapshot{
		Ref: "x/y", Image: "postgres:16", Replicas: 1,
		PrimaryFQDN: "y-0.x.voodu", SuperUser: "postgres", Database: "postgres",
		Port: 5432, ReplicationUser: "replicator",
		PasswordRedacted: "<unset>", ReplicationPasswordRedacted: "<unset>",
		BindAddress: "127.0.0.1",
	}

	text := formatInfoText(snap)
	if !strings.Contains(text, "linked consumers (none)") {
		t.Errorf("expected 'linked consumers (none)' message: %s", text)
	}
}

func TestParseInfoFlags(t *testing.T) {
	cases := []struct {
		in       []string
		wantJSON bool
		wantPos  int
	}{
		{[]string{"ref"}, false, 1},
		{[]string{"ref", "-o", "json"}, true, 1},
		{[]string{"ref", "--output", "json"}, true, 1},
		{[]string{"ref", "-o=json"}, true, 1},
		{[]string{"ref", "--output=json"}, true, 1},
		{[]string{"ref", "-o", "text"}, false, 1}, // text is the default; not json
	}

	for _, tc := range cases {
		pos, asJSON := parseInfoFlags(tc.in)
		if asJSON != tc.wantJSON {
			t.Errorf("asJSON for %v: got %v, want %v", tc.in, asJSON, tc.wantJSON)
		}

		if len(pos) != tc.wantPos {
			t.Errorf("pos count for %v: got %v, want %d", tc.in, pos, tc.wantPos)
		}
	}
}
