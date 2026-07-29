package housegate

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/network"
)

// minimalAgentConfig returns a config that satisfies cfg.Validate
// for agent mode. The signing key is a deterministic test key.
func minimalAgentConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.MetricsListen = "127.0.0.1:0"
	cfg.Agent.Mode = true
	cfg.Agent.Upstream = "127.0.0.1:1" // we won't dial it
	cfg.Agent.PrivateKeyHex = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	return &cfg
}

// minimalRouterOnlyConfig returns a config that satisfies cfg.Validate
// for a router-only server (no shard, no upstream — every session is
// forwarded to a peer via NetworkState). Validate requires
// network_state.source or redis_default_addr to be non-empty even when
// the caller injects a NetworkState via opts (validation runs before
// the opts-override path), so we set RedisDefaultAddr to a dummy
// non-connectable address; the injected InMemoryNetworkState means the
// router-only fallback never dials it.
//
// Tests that exercise the Options.NetworkState injection-bypass path
// clear RedisDefaultAddr and NetworkState.Source themselves — see
// TestNew_AcceptsInjectedNetworkState_BypassesSourceValidation below.
func minimalRouterOnlyConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.MetricsListen = "127.0.0.1:0"
	cfg.RedisDefaultAddr = "127.0.0.1:1" // satisfies Validate; never actually dialed
	// Empty upstream + no shard + not agent = router-only server.
	return &cfg
}

func TestNew_RequiresConfig(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("expected New(Options{}) to error on missing Config")
	}
}

func TestNew_AgentMode(t *testing.T) {
	p, err := New(Options{Config: minimalAgentConfig(t)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p == nil {
		t.Fatal("New returned nil Proxy")
	}
}

func TestNew_RouterOnlyMode_RequiresNetworkState(t *testing.T) {
	cfg := minimalRouterOnlyConfig(t)
	// Clear both source fields so Validate rejects the config — this
	// exercises the "no NetworkState configured" error path.
	cfg.NetworkState.Source = ""
	cfg.RedisDefaultAddr = ""
	_, err := New(Options{Config: cfg})
	if err == nil {
		t.Fatal("expected router-only New to error without NetworkState")
	}
	if !strings.Contains(err.Error(), "redis") && !strings.Contains(err.Error(), "NetworkState") && !strings.Contains(err.Error(), "network") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNew_RouterOnlyMode_AcceptsInjectedNetworkState(t *testing.T) {
	cfg := minimalRouterOnlyConfig(t)
	p, err := New(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p == nil {
		t.Fatal("New returned nil Proxy")
	}
}

// TestNew_AcceptsInjectedNetworkState_BypassesSourceValidation proves
// that Options.NetworkState injection makes the network_state.source
// validation rule unnecessary: a config with both NetworkState.Source
// AND RedisDefaultAddr empty is accepted iff a NetworkState is injected.
// This is the contract sentio-node relies on — the operator-visible
// source field is irrelevant in the embedded path.
func TestNew_AcceptsInjectedNetworkState_BypassesSourceValidation(t *testing.T) {
	cfg := minimalRouterOnlyConfig(t)
	cfg.NetworkState.Source = ""
	cfg.RedisDefaultAddr = ""

	p, err := New(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	})
	if err != nil {
		t.Fatalf("New with injected NetworkState should bypass source validation, got: %v", err)
	}
	if p == nil {
		t.Fatal("New returned nil Proxy")
	}
}

// TestNew_RejectsEmptySourceWithoutInjection is the negative companion
// to TestNew_AcceptsInjectedNetworkState_BypassesSourceValidation: same
// (empty source + empty redis_default_addr) config but no NetworkState
// injection — Validate must still reject it. Confirms the bypass only
// triggers when injection is provided.
func TestNew_RejectsEmptySourceWithoutInjection(t *testing.T) {
	cfg := minimalRouterOnlyConfig(t)
	cfg.NetworkState.Source = ""
	cfg.RedisDefaultAddr = ""

	_, err := New(Options{Config: cfg})
	if err == nil {
		t.Fatal("expected New to error when Source is empty and no NetworkState is injected")
	}
	if !strings.Contains(err.Error(), "network_state.source") {
		t.Fatalf("error did not reference network_state.source rule: %v", err)
	}
}

// TestRunWith_BindAndCancel proves the round-trip: New → RunWith on a
// :0 listener, cancel ctx, RunWith returns. Agent mode is the
// simplest mode to exercise here because it has no external deps.
func TestRunWith_BindAndCancel(t *testing.T) {
	p, err := New(Options{Config: minimalAgentConfig(t)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.RunWith(ctx, ln) }()

	// Wait for the RunWith goroutine to set p.addr — it does this
	// synchronously as the first action, but we still need to let the
	// scheduler run it. Poll instead of a fixed sleep to avoid CI flake.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && p.Addr() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if p.Addr() == nil {
		t.Fatal("Addr() == nil 2s after RunWith started")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("RunWith returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunWith did not return after ctx cancel")
	}
}
