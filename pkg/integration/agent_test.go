package integration

import (
	"context"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/registry"
)

// Agent-mode integration tests.
//
// Topology in all of these:
//
//	client → agent (signs each query as authTestKey1)
//	       → server (auth on, allowlist contains agent's address,
//	                 rewriter mock classifies the query so
//	                 PermissionCommitGateObserver clears it)
//	       → CH
//
// Auth-on at the server is the load-bearing piece: the only thing
// distinguishing this from "client → server → CH" is that the agent
// is the entity producing the JWS. If we tested with auth off the
// agent plugin's OnQuery hook would still fire but its work would be
// invisible — auth-on makes a missing or malformed token fatal, so the
// test actually proves the signer ran.

// TestAgent_PinnedUpstream_HappyPath exercises agent mode with an
// explicit cfg.Agent.Upstream — no NetworkState lookup, no Selector.
// This is the simplest deployment topology (the one used in
// hermetic / single-tenant deployments) and the only one where the
// agent and server know each other's wire address ahead of time.
//
// What fails if this regresses:
//   - agent.Plugin.OnQuery does not sign (server returns "no token")
//   - relay JWS validation at the server side regresses (signed token
//     rejected even though the address is allowlisted)
//   - agent dialer accidentally bypasses cfg.Agent.Upstream
func TestAgent_PinnedUpstream_HappyPath(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	rewriterOpt, _ := testenv.WithRewriterMock(t)
	server := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
	)

	agentProxy := testenv.StartAgentProxy(t, authTestKey1, server.Addr)

	conn := openConn(t, agentProxy.Addr)
	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("SELECT 1 through agent → server: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}
}

// TestAgent_SelectorPicksPermissioned exercises the auto-discovery
// path: the agent has no pinned Agent.Upstream, only a NetworkState
// that lists one bound indexer + a database permission for the agent's
// account. Selector's tier-1 (permissioned) walk picks the only
// candidate, and the agent dials it as if cfg.Agent.Upstream had been
// configured statically.
//
// Distinguishes from TestAgent_SelectorBootstrapFallback: this test
// grants permission so the selection lands in the permissioned tier
// and the bootstrap counter MUST NOT increment.
func TestAgent_SelectorPicksPermissioned(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	rewriterOpt, _ := testenv.WithRewriterMock(t)
	server := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), chEnv.Database, registry.DbAuthRead),
	)

	const metric = "clickhouse_proxy_agent_bootstrap_fallback_total"
	before := metricCounterValue(t, metric)

	agentProxy := testenv.StartAgentProxyWithSelector(t, authTestKey1,
		testenv.WithPeerAt(1, server),
		// Selector reads database permissions to decide tier-1 vs tier-2.
		// Granting the same permission as the server makes the agent pick
		// the server through the permissioned tier rather than bootstrap.
		testenv.WithDatabasePermission(signer.Address(), chEnv.Database, registry.DbAuthRead),
		testenv.WithLogicalDatabaseAt(chEnv.Database, 1),
	)

	conn := openConn(t, agentProxy.Addr)
	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("SELECT 1 through selector-driven agent → server: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}

	if delta := metricCounterValue(t, metric) - before; delta != 0 {
		t.Errorf("permissioned-tier selection should NOT bump %s; got delta=%v", metric, delta)
	}
}

// TestAgent_SelectorBootstrapFallback pins the tier-2 (bootstrap) path:
// an account with NO database permissions still gets routed to a bound
// indexer instead of erroring out, so a brand-new account can issue its
// first CREATE DATABASE through the proxy network.
//
// We assert two things:
//
//  1. The query succeeds (the dial reached the server).
//  2. clickhouse_proxy_agent_bootstrap_fallback_total incremented at
//     least once — the metric is the operator's signal that an account
//     fell through to bootstrap. Without it, an account that should be
//     in the permissioned tier could quietly degrade unnoticed.
//
// The server side intentionally runs auth-off because, with auth on
// and no permission grant, the commitgate PermissionObserver would
// reject the SELECT before we got to test the *agent's* selector
// behaviour.
func TestAgent_SelectorBootstrapFallback(t *testing.T) {
	server := testenv.StartServerProxy(t, chEnv.Addr) // auth-off, rewriter-off

	const metric = "clickhouse_proxy_agent_bootstrap_fallback_total"
	before := metricCounterValue(t, metric)

	agentProxy := testenv.StartAgentProxyWithSelector(t, authTestKey1,
		testenv.WithPeerAt(1, server),
		// NB: no WithDatabasePermission — the account has no perms, so
		// Selector's permissioned tier is empty and the bootstrap tier
		// fires.
	)

	conn := openConn(t, agentProxy.Addr)
	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("SELECT 1 through bootstrap-tier agent → server: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}

	if delta := metricCounterValue(t, metric) - before; delta < 1 {
		t.Errorf("bootstrap-tier selection should bump %s by ≥ 1; got delta=%v", metric, delta)
	}
}
