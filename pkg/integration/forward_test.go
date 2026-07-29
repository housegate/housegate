package integration

import (
	"context"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/integration/testenv"
)

// TestForward_PivotToPeerAtHello pins the forward.Plugin OnHello path:
// when hello.Database resolves to an indexer different from
// SelfIndexerID, the plugin dials the peer's address with a
// __peer__|<addr>|forwarded envelope, RebindToPeer swaps the upstream
// codec mid-handshake, and the rest of the connection flows through
// the peer.
//
// Topology:
//
//	             ┌── hello.Database = dbB (lives on indexer 2)
//	             ▼
//	client → proxyA (indexer 1, signs __peer__|<A>|forwarded)
//	       → proxyB (indexer 2, validates __peer__, marks
//	                 IsPeerTrusted + IsForwardedFromPeer)
//	       → CH
//
// Forwarded variant matters: __peer__|<addr>|forwarded tells proxyB
// that the SQL has NOT been rewritten yet, so its rewrite + auth chain
// MUST still run. The plain __peer__|<addr> variant (used by the
// rewriter's remote() loopback) opts out of rewrite + auth because the
// SQL on the wire is already rewritten. The two paths share the
// credential plugin's parsing but diverge on the chain filter — this
// test exercises the forwarded leg.
//
// What this catches that TestPeerEnvelope_BypassesAuthAndCommitgate
// does not:
//
//   - The pivot is initiated by proxyA's forward.Plugin, not by a
//     hand-rolled clickhouse-go connection. A regression in
//     forward.Plugin.OnHello (NetworkState lookup, peer signing,
//     RebindToPeer wiring) surfaces here.
//   - The envelope on the wire is the *forwarded* variant. A regression
//     that downgrades it to the legacy two-token form would silently
//     skip rewrite on proxyB, which the assertion (proxyB's
//     PermissionCommitGateObserver must clear the SELECT) catches.
//
// Both proxies hold the same relay key so they share an Address (the
// validator on proxyB allowlists the signer on proxyA — they happen to
// be the same key, which is the simplest way to make the test work
// without standing up a separate trust list).
func TestForward_PivotToPeerAtHello(t *testing.T) {
	peerSigner, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	signerAddr := peerSigner.Address()

	const (
		indexerA uint64 = 1
		indexerB uint64 = 2
	)
	// dbB is the logical database the test pretends "lives on indexer
	// B". We register it on the network state of *both* proxies so
	// proxyA's OnHello can resolve it (and decide to pivot) and so
	// proxyB's forward.Plugin sees it as locally owned (matches
	// SelfIndexerID) and proceeds to serve it.
	const dbB = "forward_test_db"

	// proxyB: the destination. Runs auth+rewriter so the peer-trust
	// filter on rewrite (RunOnPeerTrust=false) is meaningful — the
	// forwarded marker keeps rewrite + auth running, so the SELECT
	// must pass through both. We grant dbB to the signer so the
	// PermissionObserver clears the SELECT.
	rewriterB, _ := testenv.WithRewriterMock(t)
	proxyB := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterB,
		authProxyConfig([]string{signerAddr}, false),
		testenv.WithRelayKey(authTestKey1),
		testenv.WithIndexerID(indexerB),
		testenv.WithCredentialReplace(),
		testenv.WithExtraDatabases(dbB),
		testenv.WithLogicalDatabaseAt(dbB, indexerB),
	)

	// proxyA: the entry point. forward.Plugin sees hello.Database=dbB
	// → resolves indexer=2 != self=1 → pivots. Needs WithPeerAt so the
	// topology has proxyB's address; needs WithRelayKey so it can sign
	// the __peer__ JWS.
	proxyA := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithRelayKey(authTestKey1),
		testenv.WithIndexerID(indexerA),
		testenv.WithPeerAt(indexerB, proxyB),
		testenv.WithExtraDatabases(dbB),
		testenv.WithLogicalDatabaseAt(dbB, indexerB),
	)

	// Seed the physical database on CH so the forwarded hello carrying
	// Database=dbB does not get a "Database doesn't exist" from CH.
	seed := openConnNoDB(t, chEnv.Addr)
	if err := seed.Exec(context.Background(), "CREATE DATABASE IF NOT EXISTS "+dbB); err != nil {
		t.Fatalf("seed %s: %v", dbB, err)
	}
	t.Cleanup(func() { _ = seed.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbB) })

	// Open against proxyA pinned to dbB. The forward pivot happens at
	// hello time before any query — if it succeeds, the SELECT below
	// runs through proxyB and returns 1.
	conn := openSignedConnPinnedDB(t, proxyA.Addr, peerSigner, dbB)
	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("SELECT 1 via forward-pivot: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}
}
