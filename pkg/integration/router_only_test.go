package integration

import (
	"context"
	"testing"

	"github.com/housegate/housegate/pkg/integration/testenv"
)

// TestRouterOnly_ForwardsToPeer verifies the router-only deployment
// role: a proxy with NO Shard and NO Upstream forwards every client
// session through to a peer proxy chosen from NetworkState's bound
// indexers.
//
// Topology:
//
//	client → router (no upstream) → peer (server-mode → CH)
//
// build.go::pickRandomBoundProxy is what selects `peer`; with only
// one IndexerInfo registered (via withPeerIndexer in testenv), every
// session lands on it. The router does not parse the protocol — it
// splices the TCP connection — so any working query through `peer`
// proves the router→peer hop is intact.
func TestRouterOnly_ForwardsToPeer(t *testing.T) {
	// 1) server-mode proxy connected to the real CH. The router below
	// will splice client TCP into this proxy.
	peer := testenv.StartServerProxy(t, chEnv.Addr)

	// 2) router-only proxy with no upstream of its own. Its
	// NetworkState gets exactly one IndexerInfo pointing at `peer`,
	// so pickRandomBoundProxy has a deterministic choice.
	router := testenv.StartRouterOnlyProxy(t, peer)

	// 3) client query through the router. SELECT 1 round-trips the
	// full chain; anything broken between router and peer surfaces
	// as a dial / EOF / handshake error.
	conn := openConn(t, router.Addr)
	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("SELECT 1 through router-only proxy: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}
}
