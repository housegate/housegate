package testenv

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// StartInterserverGateway runs the pkg/interserver gateway (via the bazel-
// built cmd/igw binary) in a container on the keeper cluster's Docker
// network, reachable by peers at <alias>:9009. It forwards to target (the
// co-located CH's in-network interserver address, e.g. "ch-1:9009").
//
// Running the gateway as a container — rather than a host process — keeps
// every hop container-to-container (CH → gateway → CH), which is reliable
// on all Docker setups. A CH container cannot reliably dial back to a host
// process on a user-defined network (WSL2), so the host-process variant is
// only used by TestInterserverProxy, where the *test process* is the client.
func StartInterserverGateway(t *testing.T, cluster *KeeperCluster, alias, target string) {
	t.Helper()
	ctx := context.Background()
	bin := igwBinaryPath(t)
	dnet := cluster.DockerNetwork()

	req := testcontainers.ContainerRequest{
		Image:          "alpine:3.20",
		ExposedPorts:   []string{"9009/tcp"}, // only so the wait strategy can probe; peers use the alias
		Networks:       []string{dnet.Name},
		NetworkAliases: map[string][]string{dnet.Name: {alias}},
		Files: []testcontainers.ContainerFile{{
			HostFilePath:      bin,
			ContainerFilePath: "/igw",
			FileMode:          0o755,
		}},
		Cmd:        []string{"/igw", "-listen", ":9009", "-target", target},
		WaitingFor: wait.ForListeningPort("9009/tcp").WithStartupTimeout(30 * time.Second),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start interserver gateway %s -> %s: %v", alias, target, err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
}

// igwBinaryPath resolves the bazel-built cmd/igw binary from the test's
// runfiles. The binary is wired in as a data dependency of the
// integration_test target (see pkg/integration/BUILD.bazel).
func igwBinaryPath(t *testing.T) string {
	return runfileBinary(t, "pkg/integration/testenv/cmd/igw/igw_/igw")
}

// runfileBinary resolves a bazel-built binary from the test's runfiles by
// its rules_go output path (<pkg>/<name>_/<name>). Used to mount test-only
// gateway/proxy binaries into containers. Skips when not run under bazel.
func runfileBinary(t *testing.T, rel string) string {
	t.Helper()
	srcdir := os.Getenv("TEST_SRCDIR")
	ws := os.Getenv("TEST_WORKSPACE")
	if srcdir == "" || ws == "" {
		t.Skip("binary requires bazel runfiles (TEST_SRCDIR/TEST_WORKSPACE); run via `bazel test`")
	}
	p := filepath.Join(srcdir, ws, rel)
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("binary not found at %s: %v\n(add the go_binary to the integration_test `data`)", p, err)
	}
	return p
}
