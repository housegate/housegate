package testenv

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// cliBinaryRelPath is where we expect the clickhouse binary to live in
// the repo working tree. The path is repo-root-relative; ClickHouseCLI
// walks up from the test's cwd to find go.mod and then descends here.
//
// Operators install the binary once with:
//
//	curl -sL https://clickhouse.com/install.sh | bash
//	mv clickhouse tests/bin/
//
// CI does the same in a workflow step. The binary itself is gitignored.
var cliBinaryRelPath = filepath.Join("tests", "bin", "clickhouse")

// ClickHouseCLI returns the absolute path to the clickhouse fat binary
// under tests/bin/. If the binary is not installed the test is
// SKIPPED (not failed) — CLI coverage is conditional on the operator
// having pulled the binary down, and we want a missing binary to be a
// non-event on developer machines that do not bother with CLI tests.
//
// On Windows the binary has a .exe suffix.
func ClickHouseCLI(t *testing.T) string {
	t.Helper()

	root, err := findRepoRoot()
	if err != nil {
		t.Fatalf("find repo root: %v", err)
	}

	candidates := []string{
		filepath.Join(root, cliBinaryRelPath),
		filepath.Join(root, cliBinaryRelPath+"-client"),
	}
	if runtime.GOOS == "windows" {
		candidates = append(candidates, filepath.Join(root, cliBinaryRelPath+".exe"))
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}

	t.Skipf("clickhouse binary not found under %s — install with:\n"+
		"  curl -sL https://clickhouse.com/install.sh | bash && mv clickhouse %s",
		filepath.Join(root, "tests/bin"),
		filepath.Join(root, cliBinaryRelPath))
	return ""
}

// RunCLI runs `clickhouse client --query <query>` against the given
// proxy address. Returns trimmed stdout (with stderr merged in via
// CombinedOutput so CH exception messages surface) and any exec error;
// callers that need to assert on proxy-emitted exceptions inspect the
// error and the returned text.
//
// User and password are NOT passed: housegate's credential plugin
// replaces ClientHello.User/Password with whatever the ckh_manager
// provider configured, so anything we pass would be discarded. The
// default `default` user and empty password that clickhouse-client
// sends when --user/--password are omitted travel through the
// replacement path unchanged.
//
// Database IS passed because forward.Plugin uses it as-is to look up
// the indexer assignment; the proxy will reject hellos for unknown
// databases ("Database X doesn't exist") before credentials get a
// chance to be replaced.
//
// A 30s context bounds the whole exec — long-running queries should
// pass their own ctx via RunCLIContext if they need a different budget.
func RunCLI(t *testing.T, bin, proxyAddr, database, query string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return runCLIContext(ctx, t, bin, proxyAddr, database, query)
}

// RunCLICompressed is RunCLI with Native protocol compression forced on.
// clickhouse-client disables compression for localhost by default, while the
// proxy's production clients normally use it.
func RunCLICompressed(t *testing.T, bin, proxyAddr, database, query string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return runCLIContext(ctx, t, bin, proxyAddr, database, query, "--compression", "true")
}

// RunCLICompressedMultiquery is RunCLICompressed with multiple semicolon-
// separated queries executed on one client connection.
func RunCLICompressedMultiquery(t *testing.T, bin, proxyAddr, database, query string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return runCLIContext(
		ctx, t, bin, proxyAddr, database, query,
		"--compression", "true",
		"--multiquery",
	)
}

// RunCLIContext is RunCLI with a caller-supplied context for tests
// (large streams, cancellation) that need to control the timeout.
func RunCLIContext(ctx context.Context, t *testing.T, bin, proxyAddr, database, query string) (string, error) {
	t.Helper()
	return runCLIContext(ctx, t, bin, proxyAddr, database, query)
}

func runCLIContext(
	ctx context.Context,
	t *testing.T,
	bin, proxyAddr, database, query string,
	extraArgs ...string,
) (string, error) {
	t.Helper()
	host, port, err := net.SplitHostPort(proxyAddr)
	if err != nil {
		return "", fmt.Errorf("parse proxy addr %q: %w", proxyAddr, err)
	}

	// The clickhouse fat binary dispatches to its `client` subcommand
	// here. --host/--port override the localhost:9000 default — the
	// proxy binds an ephemeral port per test.
	args := []string{
		"client",
		"--host", host,
		"--port", port,
		"--database", database,
	}
	args = append(args, extraArgs...)
	args = append(args, "--query", query)
	cmd := exec.CommandContext(ctx, bin, args...)

	out, err := cmd.CombinedOutput()
	return strings.TrimRight(string(out), "\n"), err
}

func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod found walking up from %s", dir)
		}
		dir = parent
	}
}
