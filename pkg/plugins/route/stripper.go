package routeplugin

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"

	"github.com/housegate/housegate/pkg/log"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/credentials"
	"github.com/housegate/housegate/pkg/peer"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/route"
)

// ErrRouteRefused is wrapped by every error Stripper returns for a
// __route__ envelope it will not honour.
var ErrRouteRefused = errors.New("route envelope refused")

// Stripper detects the __route__|<target>|<realUser> prefix in
// ClientHello.User, strips it in place, and stashes the target proxy
// address in SessionState.RouteTarget so the upstream dialer and the
// PluginChain can both observe the routed status.
//
// Stripper itself is RouteAware — it must run during OnHello on routed
// sessions because it is the plugin that sets the routed flag in the
// first place. Plugins ordered after Stripper that aren't RouteAware
// are skipped automatically by PluginChain.
//
// A routed session bypasses every non-RouteAware plugin, credential
// replacement and auth included, and the upstream dialer connects to
// the target verbatim. Stripper is therefore the gate between a
// client-chosen address and an outbound dial, and it fails closed:
//
//   - the target must be advertised by an indexer in Topology as its
//     housegate endpoint (registry.HousegateIndexersForEndpoint) — a
//     route is only ever proxy-to-proxy, never to a ClickHouse or any
//     other host;
//   - the inner user must be one this proxy itself emits for a route:
//     a peer-relay envelope (__peer__|..., whose JWS the target
//     validates), or exactly the static credential Credentials hands out
//     for the target indexer. Any other user — and so any client-supplied
//     ClickHouse credential — is refused.
//
// A zero Stripper (no Topology) refuses every route envelope.
type Stripper struct {
	// Topology resolves route targets to advertising indexers. Nil
	// refuses every route.
	Topology registry.Topology

	// Credentials is the provider the rewriter takes static remote()
	// credentials from when no peer signer is wired. Nil accepts only
	// peer-relay envelopes as the inner user.
	Credentials credentials.CredentialProvider
}

func (s Stripper) OnHello(ctx context.Context, sess chsession.Session, hello *chproto.ClientHello) error {
	targetAddr, realUser, isRoute := route.ParseRouteFromUser(hello.User)
	if isRoute && sess.State().IsInternalPort {
		return fmt.Errorf("%w: route envelope on internal-port (target=%q user=%q): "+
			"internal-port must never receive __route__; check peer config or firewall",
			ErrRouteRefused, targetAddr, realUser)
	}
	if !isRoute {
		return nil
	}
	_, logger := log.FromContext(ctx)
	if err := s.admit(targetAddr, realUser, hello.Password); err != nil {
		logger.Warnw("route stripper: refused route envelope",
			"target_addr", targetAddr,
			"real_user", realUser,
			"err", err,
		)
		return err
	}
	logger.Debugw("route stripper: extracted route target",
		"target_addr", targetAddr,
		"real_user", realUser,
	)
	hello.User = realUser
	sess.State().SetRouteTarget(targetAddr)
	return nil
}

// admit decides whether a route envelope is one this proxy emitted.
func (s Stripper) admit(targetAddr, realUser, password string) error {
	ids, err := registry.HousegateIndexersForEndpoint(s.Topology, targetAddr)
	if err != nil {
		return fmt.Errorf("%w: target: %v", ErrRouteRefused, err)
	}
	if _, _, isPeer := peer.ParseUser(realUser); isPeer {
		// The target's credential plugin verifies the peer-relay JWS.
		return nil
	}
	if s.Credentials != nil {
		for _, id := range ids {
			u, p, err := s.Credentials.GetCredentialForIndexer(id)
			if err != nil || u == "" {
				continue
			}
			userOK := subtle.ConstantTimeCompare([]byte(u), []byte(realUser)) == 1
			passOK := subtle.ConstantTimeCompare([]byte(p), []byte(password)) == 1
			if userOK && passOK {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: user %q is neither a peer-relay envelope nor this proxy's credential for the target",
		ErrRouteRefused, realUser)
}

// RunOnRouted reports that Stripper stays in the OnHello chain on routed
// sessions — it has to, because it's the plugin that marks them routed.
// Idempotent: a connection that arrives already-marked just doesn't
// match the prefix and falls through to a no-op.
func (Stripper) RunOnRouted() bool { return true }

var (
	_ plugin.HelloPlugin = Stripper{}
	_ plugin.RouteAware  = Stripper{}
)
