// Container naming + low-level docker shells used by failover,
// rejoin, and (M-P6) backup/restore.
//
// Plugins run server-side on the docker host, so direct docker
// CLI invocations are the path of least resistance. We keep the
// shell commands here so the operator-facing commands above stay
// focused on flow logic.

package main

import (
	"fmt"
	"io"
	"os/exec"
	"strconv"
)

// containerNameFor mirrors voodu's containers.ContainerName
// convention: <scope>-<name>.<ordinal> for scoped statefulsets,
// <name>.<ordinal> for unscoped. Used by failover/rejoin to
// reach a specific replica via docker exec / docker run.
func containerNameFor(scope, name string, ordinal int) string {
	base := name
	if scope != "" {
		base = scope + "-" + name
	}

	return base + "." + strconv.Itoa(ordinal)
}

// containerExists checks whether a container with this name is
// registered (running OR stopped). Wraps `docker inspect` so the
// error path is "container not found", suitable for pre-flight
// checks in failover/rejoin.
func containerExists(name string) bool {
	cmd := exec.Command("docker", "inspect", "--format", "{{.Id}}", name)
	cmd.Stderr = io.Discard

	return cmd.Run() == nil
}

// containerIsRunning returns true when the container is in the
// Running state. Distinct from containerExists — a stopped
// container exists but isn't running.
func containerIsRunning(name string) (bool, error) {
	cmd := exec.Command("docker", "inspect", "--format", "{{.State.Running}}", name)

	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("docker inspect %s: %w", name, err)
	}

	switch string(out) {
	case "true\n", "true":
		return true, nil
	case "false\n", "false":
		return false, nil
	}

	return false, fmt.Errorf("unexpected `docker inspect` output for %s: %q", name, string(out))
}
