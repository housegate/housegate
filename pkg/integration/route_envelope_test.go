package integration

import (
	"context"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"housegate/housegate/pkg/integration/testenv"
	"housegate/housegate/pkg/route"
)

// TestRouteEnvelope_StripsAndPivots pins the __route__ envelope path
// the rewriter emits onto loopback remote() clauses for cross-indexer
// routing.
//
// Topology:
//
//	client → router (Stripper extracts __route__|<peer.Addr>|<real>
//	                 from hello.User, dialer pivots to peer.Addr)
//	       → peer (server-mode → CH)
//
// What this test catches that TestRouterOnly_ForwardsToPeer does not:
//
//   - TestRouterOnly_ForwardsToPeer exercises the *router-only*
//     deployment (no shard, no upstream) where every session is
//     pivoted to a peer via pickRandomBoundProxy. The Stripper plugin
//     never sees a __route__ envelope in that test — the dialer falls
//     through to NetworkState.
//   - This test exercises the *envelope* path: router has a normal
//     upstream and would dial it by default, but the __route__
//     envelope in the client's hello forces a pivot to peer.Addr.
//     This is the exact pattern the rewriter uses when emitting
//     remote('local-housegate', __route__|<peer>|<real>, ...) for
//     cross-indexer queries.
//
// Once Stripper sets RouteTarget, PluginChain skips every non-RouteAware
// plugin on the router side, so we don't need rewriter/auth/etc wired
// up. The peer is a vanilla auth-off server proxy — from its POV the
// router is just another client.
func TestRouteEnvelope_StripsAndPivots(t *testing.T) {
	peer := testenv.StartServerProxy(t, chEnv.Addr)
	router := testenv.StartServerProxy(t, chEnv.Addr) // router has its own upstream — Stripper must override it

	// Build the envelope the way the rewriter would: target = peer's
	// bound address, realUser = the underlying CH user the peer
	// expects in its hello.
	envelopedUser := route.FormatRouteUser(peer.Addr, chEnv.User)

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{router.Addr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: envelopedUser,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		t.Fatalf("clickhouse.Open with __route__ envelope: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("SELECT 1 via __route__ envelope: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}
}
