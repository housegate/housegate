package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/integration/testenv"
	"housegate/housegate/pkg/peer"
)

// TestPeerEnvelope_BypassesAuthAndCommitgate pins the inbound side of
// the __peer__|<addr> envelope contract: when a connecting client
// presents a valid peer-relay JWS in the password and a __peer__
// envelope in the user field, the receiving proxy's credential plugin
// marks the session IsPeerTrusted and the chain's peer-trust filter
// skips auth, rewrite, and commitgate for the rest of the connection.
//
// We exploit this to prove the filter is wired:
//
//   - Server has Auth.Enabled = true (so PermissionCommitGateObserver
//     is auto-wired) and NO rewriter. With a normal client this is the
//     fail-closed posture from TestAuth_RejectsWithoutRewriter — every
//     statement is Unspecified and the observer rejects it.
//   - The peer envelope flips IsPeerTrusted. Auth (PeerTrustAware
//     opt-out) skips. Commitgate (PeerTrustAware opt-out) skips.
//     Without the filter respecting RunOnPeerTrust=false the SELECT
//     would still be rejected by the observer.
//
// If this test starts failing with "requires authenticated account" or
// similar, the peer-trust filter no longer fires on auth/commitgate —
// a regression in PluginChain that would silently break every
// cross-indexer rewriter remote() emission.
//
// We connect to the *external* port (not internal_listen) on purpose:
// the internal port pre-flags every session as peer-trusted, so a
// successful query there would also succeed without any envelope
// processing. External-port success is the load-bearing signal that
// the credential plugin's __peer__ branch actually fired.
func TestPeerEnvelope_BypassesAuthAndCommitgate(t *testing.T) {
	// The peer signer's address must be in AllowedAddresses so
	// EthValidator.ValidatePeerLogin clears the recovered signer.
	peerSigner, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	// SelfIndexerID = 42 — picked arbitrarily; the only invariant is
	// that the token's `aud` matches expectedAudience (which is the
	// server's own indexer id, stringified). Hardcoded to 42 here
	// instead of 0 so a regression that defaults expectedAud to "" no
	// longer matches by accident.
	const serverIndexer uint64 = 42

	server := testenv.StartServerProxy(t, chEnv.Addr,
		authProxyConfig([]string{peerSigner.Address()}, false),
		// Credential replace path: after the credential plugin strips
		// the __peer__ envelope it sets hello.User = "" so the
		// envelope cannot leak through to CH. The provider then
		// fills in the real CH creds the local upstream expects.
		// Without this, CH receives an empty username and rejects
		// the handshake.
		testenv.WithCredentialReplace(),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.IndexerID = serverIndexer
		}),
	)

	token, err := peerSigner.SignPeerLogin(strconv.FormatUint(serverIndexer, 10), 30*time.Second)
	if err != nil {
		t.Fatalf("SignPeerLogin: %v", err)
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{server.Addr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: peer.FormatUser(peerSigner.Address()),
			Password: token,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		t.Fatalf("clickhouse.Open with __peer__ envelope: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("SELECT 1 via __peer__ envelope: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}
}
