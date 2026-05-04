// strategy_local.go — local bind-mount WAL archive strategy.
//
// What it does: bind-mount a host directory at /wal-archive
// inside every postgres pod. Primary writes via archive_command
// (idempotent local cp); standbys mount it :rw too so a
// promoted-to-primary standby keeps writing into the SAME
// archive without a re-mount cycle.
//
// Why bind-mount and not per-pod volume_claim: WAL archive is
// CLUSTER-LEVEL state, not pod-level. Per-pod claims orphan WAL
// on the old primary's volume after `vd pg:promote`, leaving a
// PITR gap. A single shared host directory survives failover
// transparently.
//
// Pre-flight (operator responsibility, NOT auto-created by the
// plugin — apply has no privileged host access):
//
//	sudo mkdir -p <destination>
//	sudo chown 999:999 <destination>          # postgres uid/gid
//	sudo chmod 0700 <destination>             # WAL is sensitive
//
// Failure mode is loud: missing host dir → bind-mount produces
// empty container mount → archive_command logs "no such file or
// directory" on the first WAL switch.

package main

import (
	"fmt"
	"strings"
)

// localMountPath is the path INSIDE the container where the host
// directory gets bind-mounted. Internal convention — operator
// doesn't pick this. Picked /wal-archive because it's short,
// memorable, and outside any postgres-managed directory.
const localMountPath = "/wal-archive"

// localDestinationRoot is the prefix for the default local
// destination path. Final path = localDestinationRoot/<scope>/<name>.
// /opt/voodu/backups is conventional for voodu-managed durable
// state (mirrors /opt/voodu/plugins layout) and survives across
// container lifecycles + voodu upgrades.
const localDestinationRoot = "/opt/voodu/backups"

// localStrategy implements walArchiveStrategy for the local
// bind-mount case. Stateless — all state lives in walArchiveSpec.
type localStrategy struct{}

func (localStrategy) Name() string { return "local" }

// DefaultDestination puts the archive under
// /opt/voodu/backups/<scope>/<name>. Resource (scope, name)
// embeds in the path so multiple postgres resources on the same
// host don't collide.
//
// Unscoped resources land at /opt/voodu/backups/_unscoped/<name>
// (the leading dir disambiguates from scoped resources sharing
// names with unscoped ones — rare but cheap to defend against).
func (localStrategy) DefaultDestination(scope, name string) string {
	if scope == "" {
		return localDestinationRoot + "/_unscoped/" + name
	}

	return localDestinationRoot + "/" + scope + "/" + name
}

// Validate checks the operator-supplied destination. Empty is
// OK — Apply substitutes the default. Non-empty must be:
//
//  1. Absolute (starts with /).
//  2. Not a known dangerous root (/, /etc, /usr, /bin, …).
//
// Plugin does NOT check that the path EXISTS — that's a runtime
// concern. Apply must work without privileged host access.
func (localStrategy) Validate(spec *walArchiveSpec) error {
	if spec.Destination == "" {
		return nil
	}

	if !strings.HasPrefix(spec.Destination, "/") {
		return fmt.Errorf("wal_archive.destination %q is not absolute (local strategy requires an absolute host path)", spec.Destination)
	}

	if err := rejectDangerousLocalDestination(spec.Destination); err != nil {
		return err
	}

	return nil
}

// Apply emits the bind-mount + archive_command for the local
// strategy. Standbys carry the mount :rw too — see file header
// for why.
func (s localStrategy) Apply(spec *walArchiveSpec, scope, name string) walArchivePlan {
	dest := spec.Destination
	if dest == "" {
		dest = s.DefaultDestination(scope, name)
	}

	bindMount := dest + ":" + localMountPath + ":rw"

	// Idempotent local cp — recommended form from postgres docs.
	// The `test ! -f` guard rejects rewrites that would corrupt
	// the archive on retry.
	cmd := fmt.Sprintf("test ! -f %s/%%f && cp %%p %s/%%f", localMountPath, localMountPath)

	return walArchivePlan{
		Volumes:        []string{bindMount},
		ArchiveCommand: cmd,
	}
}

// rejectDangerousLocalDestination blocks footgun paths.
// Bind-mounting /etc or /usr at /wal-archive inside postgres
// would either leak host secrets (postgres user reads
// /etc/shadow via the mount) or corrupt the host (postgres
// writes WAL on top of system files).
//
// Allowed: /opt/..., /srv/..., /mnt/..., /var/lib/..., custom
// /data, /storage, etc. Plus operator's own subdirs anywhere
// under those.
func rejectDangerousLocalDestination(p string) error {
	// Exact-match dangerous roots.
	dangerousRoots := map[string]bool{
		"/":     true,
		"/etc":  true,
		"/usr":  true,
		"/bin":  true,
		"/sbin": true,
		"/lib":  true,
		"/proc": true,
		"/sys":  true,
		"/dev":  true,
		"/boot": true,
		"/root": true,
	}

	if dangerousRoots[p] {
		return fmt.Errorf("wal_archive.destination %q is a system root — pick a dedicated path (e.g. /opt/voodu/backups/...)", p)
	}

	// Prefix-match: any path under these is suspect.
	dangerousPrefixes := []string{"/etc/", "/usr/", "/bin/", "/sbin/", "/lib/", "/proc/", "/sys/", "/dev/", "/boot/", "/root/"}

	for _, prefix := range dangerousPrefixes {
		if strings.HasPrefix(p, prefix) {
			return fmt.Errorf("wal_archive.destination %q is under a system root — pick a dedicated path (e.g. /opt/voodu/backups/...)", p)
		}
	}

	return nil
}
