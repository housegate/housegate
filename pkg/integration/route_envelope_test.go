package integration

import (
	"context"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/route"
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
//
// Because a routed session skips credential replacement and auth,
// Stripper only honours an envelope the router itself would emit: the
// target is a housegate the network state advertises, and the inner
// user is a peer-relay envelope or the router's own static credential
// for that peer (here the ckh_manager credential WithCredentialReplace
// loads, which is chEnv's user).
func TestRouteEnvelope_StripsAndPivots(t *testing.T) {
	peer := testenv.StartServerProxy(t, chEnv.Addr)
	// router has its own upstream — Stripper must override it
	router := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithPeerAt(7, peer),
		testenv.WithCredentialReplace(),
	)

	// Build the envelope the way the rewriter would: target = peer's
	// bound address, realUser = the underlying CH user the peer
	// expects in its hello.
	envelopedUser := route.FormatRouteUser(peer.Addr, chEnv.User)
	v, err := routedSelectOne(router.Addr, envelopedUser, chEnv.Password)
	if err != nil {
		t.Fatalf("SELECT 1 via __route__ envelope: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}
}

// TestRouteEnvelope_RefusesClientChosenRoutes pins the fail-closed side:
// a client must not use the router as a relay to a host the network
// state does not advertise as a housegate, nor carry its own ClickHouse
// credentials through a route.
func TestRouteEnvelope_RefusesClientChosenRoutes(t *testing.T) {
	peer := testenv.StartServerProxy(t, chEnv.Addr)
	router := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithPeerAt(7, peer),
		testenv.WithCredentialReplace(),
	)

	cases := []struct {
		name, target, user, password string
	}{
		// The ClickHouse server itself, with valid ClickHouse credentials:
		// the router must never dial a non-advertised address.
		{"unadvertised clickhouse target", chEnv.Addr, chEnv.User, chEnv.Password},
		// An advertised peer, but with a user the router does not emit.
		{"foreign user to advertised peer", peer.Addr, "default", chEnv.Password},
		// An advertised peer with the right user but a guessed password.
		{"wrong password to advertised peer", peer.Addr, chEnv.User, "not-the-password"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := routedSelectOne(router.Addr, route.FormatRouteUser(tc.target, tc.user), tc.password)
			if err == nil {
				t.Fatalf("route to %s as %q must be refused, got SELECT 1 = %d", tc.target, tc.user, v)
			}
		})
	}
}

func routedSelectOne(routerAddr, user, password string) (uint8, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{routerAddr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: user,
			Password: password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()
	var v uint8
	err = conn.QueryRow(context.Background(), "SELECT 1").Scan(&v)
	return v, err
}
