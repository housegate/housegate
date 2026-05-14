package peer

import (
	"fmt"
	"strconv"
	"time"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/credentials"
)

// SignPeerHello builds the (user, password) pair for a TCP handshake from one
// housegate to another's internal-port for a single credential path:
// either signer-based peer-relay JWS (when signer != nil) or static credentials
// (when signer == nil and fallback != nil). It does not chain — if signer is
// non-nil, fallback is ignored entirely; the caller is responsible for retrying
// via a second call with signer=nil to implement signer-fail → fallback policy.
// Returns an error when both signer and fallback are nil, or when the requested
// indexer has no static credential entry.
//
// The envelope is the legacy two-token form (peer.FormatUser). Use
// SignPeerHelloForwarded for forward-decision pivots — the receiving
// proxy needs to know the SQL is unrewritten so it runs rewrite/auth
// locally. See peer.ForwardedMarker.
func SignPeerHello(
	signer auth.PeerSigner,
	ttl time.Duration,
	targetIndexerId uint64,
	fallback credentials.CredentialProvider,
) (user, password string, err error) {
	return signPeerHello(signer, ttl, targetIndexerId, fallback, false)
}

// SignPeerHelloForwarded is the forward-pivot variant: emits the
// peer.FormatUserForwarded envelope so the receiving credential plugin
// flips IsForwardedFromPeer=true on the session, which in turn keeps
// the chain's peer-trust filter from skipping rewrite/auth.
//
// Same fallback rules as SignPeerHello: no static-credentials path
// when signer is nil — forward pivot only works when peer signing is
// wired. The fallback parameter is retained for symmetry.
func SignPeerHelloForwarded(
	signer auth.PeerSigner,
	ttl time.Duration,
	targetIndexerId uint64,
	fallback credentials.CredentialProvider,
) (user, password string, err error) {
	return signPeerHello(signer, ttl, targetIndexerId, fallback, true)
}

func signPeerHello(
	signer auth.PeerSigner,
	ttl time.Duration,
	targetIndexerId uint64,
	fallback credentials.CredentialProvider,
	forwarded bool,
) (user, password string, err error) {
	if signer != nil {
		audience := strconv.FormatUint(targetIndexerId, 10)
		token, signErr := signer.SignPeerLogin(audience, ttl)
		if signErr != nil {
			return "", "", fmt.Errorf("sign peer login (audience=%s): %w", audience, signErr)
		}
		if forwarded {
			return FormatUserForwarded(signer.Address()), token, nil
		}
		return FormatUser(signer.Address()), token, nil
	}
	if forwarded {
		// The forwarded marker is meaningless when the receiving end
		// has no PeerValidator wiring (static-credential path skips
		// the marker because IsPeerTrusted itself is never set).
		// Surface this as a clear error rather than silently falling
		// back to the legacy form.
		return "", "", fmt.Errorf("forward pivot requires a PeerSigner; static credentials are not supported")
	}
	if fallback == nil {
		return "", "", fmt.Errorf("no peer signer and no static credential provider")
	}
	u, p, credErr := fallback.GetCredentialForIndexer(targetIndexerId)
	if credErr != nil {
		return "", "", fmt.Errorf("no static credential for indexer %d: %w", targetIndexerId, credErr)
	}
	return u, p, nil
}
