package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/integration/testenv"
	peerenv "housegate/housegate/pkg/peer"
	"housegate/housegate/pkg/route"
)

// TestCombinedRouteAndPeerEnvelope pins the nesting of __route__ and
// __peer__ envelopes in the ClientHello user field — the exact pattern
// the rewriter emits onto loopback remote() clauses for cross-indexer
// routing.
//
// Topology:
//
//	client → router (Stripper extracts __route__|<peer.Addr>|...,
//	                 pivots to peer.Addr)
//	       → peer (credential plugin validates __peer__ JWS,
//	               marks IsPeerTrusted, replaces credentials)
//	       → CH
//
// What this catches that the separate route_envelope_test.go and
// peer_envelope_test.go do not:
//
//   - TestRouteEnvelope_StripsAndPivots only tests __route__ alone —
//     the pivoted connection arrives at the peer as a plain CH client,
//     not as a peer-trusted proxy.
//   - TestPeerEnvelope_BypassesAuthAndCommitgate only tests __peer__
//     alone — the envelope is parsed directly, not after a route
//     stripping.
//   - This test exercises the COMBINED path: the route Stripper peels
//     the outer __route__ layer, leaving __peer__|<addr> as the user,
//     and the peer's credential plugin then processes the inner peer
//     envelope. A regression in either plugin or in their ordering
//     (Stripper before credential) would break this.
//
// The peer has auth enabled (so PeerValidator is wired) and credential
// replacement on (so the __peer__ envelope gets replaced with CH creds
// after validation). The router is a plain server-mode proxy — its
// Stripper sets RouteTarget and the dialer pivots to peer.Addr.
// PluginChain on the router skips every non-RouteAware plugin.
func TestCombinedRouteAndPeerEnvelope(t *testing.T) {
	peerSigner, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	const peerIndexer uint64 = 42

	// Peer proxy: auth on + credential replace + indexer ID so it
	// validates the peer-relay JWS and swaps in real CH credentials.
	peerProxy := testenv.StartServerProxy(t, chEnv.Addr,
		authProxyConfig([]string{peerSigner.Address()}, false),
		testenv.WithCredentialReplace(),
		testenv.WithIndexerID(peerIndexer),
	)

	// Router proxy: normal server-mode with upstream CH. The Stripper
	// is always in the HelloPlugin chain; it will extract the route
	// envelope and pivot.
	router := testenv.StartServerProxy(t, chEnv.Addr)

	// Sign a peer-relay JWS with audience = peer's indexer ID so the
	// peer's credential plugin (PeerValidator) validates it.
	token, err := peerSigner.SignPeerLogin(strconv.FormatUint(peerIndexer, 10), 30*time.Second)
	if err != nil {
		t.Fatalf("SignPeerLogin: %v", err)
	}

	// Build the combined envelope:
	//   user     = "__route__|<peerProxy.Addr>|__peer__|<signerAddr>"
	//   password = peer-relay JWS
	//
	// The route Stripper on the router extracts the target and restores
	// hello.User = "__peer__|<signerAddr>" before forwarding to the peer.
	envelopeUser := route.FormatRouteUser(peerProxy.Addr, peerenv.FormatUser(peerSigner.Address()))

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{router.Addr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: envelopeUser,
			Password: token,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		t.Fatalf("clickhouse.Open with combined envelope: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("SELECT 1 via combined envelope: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}
}
