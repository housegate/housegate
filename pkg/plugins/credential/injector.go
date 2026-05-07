// Package credential implements CredentialInjectorPlugin, which substitutes
// the credentials carried in a ClientHello with the upstream credentials
// supplied by a CredentialProvider.
//
// This decouples the client's view of the connection (often a service
// account with no real ClickHouse password — only a JWS) from the actual
// credentials needed to authenticate against the backing ClickHouse
// instance, which the proxy fetches from ckhmanager.
//
// The mapped credentials are also written to SessionState (MappedUser /
// MappedPassword) so downstream OnQuery plugins — most notably the rewriter
// — can read them without re-calling the provider.
//
// Peer-relay envelope: when ClientHello.User carries the peer.Prefix marker,
// the password field carries a peer-relay JWS issued by an upstream
// housegate proxy. The plugin verifies it via PeerValidator and marks the
// session SessionState.IsPeerTrusted, then proceeds to replace the
// credentials so the local CH dial uses the proxy's normal upstream
// account. Downstream plugins (auth/jws, rewrite) consult IsPeerTrusted
// and skip themselves — the upstream rewriter already produced final SQL.
package credential

import (
	"context"
	"fmt"
	"strconv"

	"sentioxyz/sentio-core/common/log"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/credentials"
	"housegate/housegate/pkg/peer"
	"housegate/housegate/pkg/plugin"
)

// Plugin replaces ClientHello.User / Password with credentials from a
// CredentialProvider. A nil Provider makes the plugin a no-op.
//
// When wired with a PeerValidator and GetSelfIndexerID, the plugin
// also recognises peer-relay envelopes and marks the session
// IsPeerTrusted. Both fields are optional; without them the plugin
// behaves exactly as before.
type Plugin struct {
	Provider credentials.CredentialProvider

	// PeerValidator verifies peer-relay JWS tokens carried in the
	// password of a __peer__|<addr> envelope. Nil disables peer auth
	// — a __peer__|<addr> hello will fall through to the regular
	// provider replace path, which will almost certainly fail at the
	// upstream CH (the audience check is the only line of defence
	// against an unknown signer with a valid-looking address; a nil
	// validator implies the operator has decided peer auth is not
	// required on this proxy).
	PeerValidator auth.PeerValidator

	// GetSelfIndexerID returns this proxy's indexer id, used as the
	// expected audience claim of inbound peer-relay JWS tokens. The
	// returned value is always formatted via strconv.FormatUint and
	// compared verbatim against the token's `aud` claim — including
	// id == 0, which the signer (peer.SignPeerHello) emits as the
	// literal string "0". Nil disables audience verification (sets
	// expected to ""), which only matches tokens with no audience —
	// avoid in production, the signer always sets one.
	GetSelfIndexerID func() uint64
}

func (p *Plugin) OnHello(ctx context.Context, sess chsession.Session, hello *chproto.ClientHello) error {
	_, logger := log.FromContext(ctx, "original_user", hello.User, "original_password", hello.Password)

	// Peer-relay envelope is recognised before provider replacement so
	// the JWS in the password is not lost. After validation the
	// regular provider path runs as usual so the local CH dial uses
	// the proxy's upstream account.
	if claimedAddr, forwarded, isPeer := peer.ParseUser(hello.User); isPeer {
		if err := p.validatePeer(ctx, sess, claimedAddr, hello.Password, forwarded); err != nil {
			return err
		}
		// Clear the peer envelope before provider replacement so
		// downstream code (and the upstream CH dial when no provider
		// is wired) sees a clean username.
		hello.User = ""
	}

	if p.Provider == nil {
		return nil
	}
	logger.Debugw("credential injector: fetching credentials from provider")

	username, password, err := p.Provider.GetDefaultCredential()
	if err != nil {
		return fmt.Errorf("credential injection: %w", err)
	}
	hello.User = username
	hello.Password = password

	state := sess.State()
	state.MappedUser = username
	state.MappedPassword = password

	logger.Debugw("credential injector: replacing hello credentials",
		"mapped_user", username,
		"mapped_password", password,
	)
	return nil
}

// validatePeer verifies the peer-relay JWS in the password and
// records the recovered signer address in SessionState. claimedAddr
// is the address embedded in the user envelope; it is logged but not
// trusted — the recovered address from the signature is authoritative.
//
// forwarded is the peer.ForwardedMarker bit parsed off the envelope:
// false for the rewriter's remote() loopback (SQL already rewritten
// upstream — local rewrite/auth must skip), true for a forward-decision
// pivot (SQL is the client's raw form — local rewrite/auth must run).
func (p *Plugin) validatePeer(ctx context.Context, sess chsession.Session, claimedAddr, jwsToken string, forwarded bool) error {
	_, logger := log.FromContext(ctx, "claimed_peer_address", claimedAddr, "forwarded", forwarded)

	if p.PeerValidator == nil {
		return fmt.Errorf("peer-relay envelope received but no PeerValidator wired")
	}
	if jwsToken == "" {
		return fmt.Errorf("peer-relay envelope: empty JWS token in password field")
	}

	expectedAud := ""
	if p.GetSelfIndexerID != nil {
		expectedAud = strconv.FormatUint(p.GetSelfIndexerID(), 10)
	}

	recovered, err := p.PeerValidator.ValidatePeerLogin(jwsToken, expectedAud)
	if err != nil {
		return fmt.Errorf("peer-relay validation: %w", err)
	}
	sess.State().SetPeerTrustForwarded(recovered, forwarded)
	logger.Infow("peer-relay session authenticated",
		"recovered_address", recovered,
		"audience", expectedAud,
	)
	return nil
}

var _ plugin.HelloPlugin = (*Plugin)(nil)
