// Package forward implements the forward-decision plugin: at ClientHello time
// it looks up hello.Database in NetworkState. If the resolved indexer is this
// proxy (SelfIndexerID match), it does nothing and the chain proceeds locally.
// If the database is hosted on a different indexer, it dials the peer's
// internal-port, calls Session.RebindToPeer with a __peer__-envelope hello,
// and sets IsForwarding=true so ForwardAware-opt-out plugins (rewrite,
// commitgate) skip themselves for the rest of the connection.
//
// OnQuery detects standalone USE <db> statements and re-pivots the session
// in three flavours depending on where the new database lives relative to
// the session's current upstream:
//
//	previous → new       action
//	---------------------------------------------------------------
//	local    → local     no-op (CH handles USE itself)
//	local    → peer X    pivotToPeer(X)              (forward pivot)
//	peer X   → peer Y    pivotToPeer(Y)              (cross-peer pivot)
//	peer X   → peer X    no-op (already on right peer)
//	peer X   → local     rebindToLocal()             (return-home pivot)
//
// Complex SQL (non-USE) flows unmodified through the rewriter's remote() path.
package forward

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/housegate/housegate/pkg/log"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/credentials"
	"github.com/housegate/housegate/pkg/peer"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/registry"
)

// Plugin pivots a session to a peer's internal-port at handshake time when
// hello.Database resolves to a database hosted on a different indexer.
// Cross-DB SQL (without USE) still flows through the rewriter's remote() path.
type Plugin struct {
	Topology      registry.Topology
	Databases     registry.Databases
	SelfIndexerID uint64
	PeerSigner    auth.PeerSigner
	PeerTokenTTL  time.Duration
	// Fallback is the static credential provider used when PeerSigner is nil.
	// Both may be nil only in tests that never exercise the remote-dial path.
	Fallback credentials.CredentialProvider

	// LocalDialer dials this proxy's local upstream (the same target the
	// initial dialer would reach when no route plugin sets a target —
	// typically a cluster pool or cfg.Upstream). Required for the
	// remote→local rebind path triggered when a USE statement on a
	// previously-forwarded session targets a database hosted on this
	// proxy. nil disables that path: the USE will be sent to the current
	// (peer) upstream and CH will reject it because the database is
	// not local to that peer.
	LocalDialer func(ctx context.Context) (*chproto.Codec, error)

	// PhysicalDatabase mirrors cfg.Rewriter.PhysicalDatabase — the single
	// real ClickHouse database that hosts every logical database for this
	// deployment. Internal services (e.g. sentio analytic-server) connect
	// with hello.Database set to this physical name; that value is not in
	// NetworkState's logical-database registry, so OnHello short-circuits
	// rather than treating it as an unknown logical DB. Empty disables the
	// short-circuit (router-only deployments without a configured physical
	// database).
	PhysicalDatabase string

	// dialPeer / rebindFn / localRebindFn are injectable for tests;
	// production callers leave them nil and the plugin uses
	// defaultDial / defaultRebind / defaultLocalRebind.
	dialPeer      func(ctx context.Context, addr string) (*chproto.Codec, error)
	rebindFn      func(ctx context.Context, sess chsession.Session, up *chproto.Codec, hello *chproto.ClientHello) error
	localRebindFn func(ctx context.Context, sess chsession.Session, up *chproto.Codec, hello *chproto.ClientHello) error
}

func (p *Plugin) Name() string { return "forward" }

// OnHello implements plugin.HelloPlugin. It looks up hello.Database in
// NetworkState and pivots the session to a peer proxy when the database is
// hosted elsewhere. An empty database is deferred to the first USE statement
// (Phase 3 / Task 3.1); an unknown database returns a CH-shaped error.
//
// Peer-trusted sessions (IsPeerTrusted == true) are skipped: they arrived
// from another housegate that already made the routing decision.
func (p *Plugin) OnHello(ctx context.Context, sess chsession.Session, hello *chproto.ClientHello) error {
	if sess.State().IsPeerTrusted {
		return nil // already routed by the originating proxy
	}
	if hello.Database == "" {
		return nil // defer; first USE will re-evaluate (Task 3)
	}
	if p.PhysicalDatabase != "" && hello.Database == p.PhysicalDatabase {
		// Internal services (sentio analytic-server, indexer-side jobs)
		// connect with hello.Database set to the configured physical
		// ClickHouse database. That name is intentionally not in
		// NetworkState's logical-database registry — treat it as local
		// and let the rest of the chain (sessionstate, rewrite) handle
		// per-query logical→physical translation.
		return nil
	}
	info, ok := p.Databases.Get(hello.Database)
	if !ok {
		return fmt.Errorf("Code: 81. DB::Exception: Database %s doesn't exist", hello.Database)
	}
	if info.IndexerId == p.SelfIndexerID {
		return nil // serve locally
	}
	return p.pivotToPeer(ctx, sess, hello, info.IndexerId)
}

// pivotToPeer dials the peer's internal-port and drives RebindToPeer so
// subsequent client packets are forwarded transparently.
func (p *Plugin) pivotToPeer(ctx context.Context, sess chsession.Session, hello *chproto.ClientHello, peerIndexer uint64) error {
	peerAddr, ok := p.Topology.ProxyByIndexerId(peerIndexer)
	if !ok {
		return fmt.Errorf("indexer %d not found in topology", peerIndexer)
	}
	addr := fmt.Sprintf("%s:%d", peerAddr.Url, peerAddr.HousegatePort)

	dialer := p.dialPeer
	if dialer == nil {
		dialer = defaultDial
	}
	rebind := p.rebindFn
	if rebind == nil {
		rebind = defaultRebind
	}

	up, err := dialer(ctx, addr)
	if err != nil {
		return fmt.Errorf("dial peer %s: %w", addr, err)
	}

	// SignPeerHelloForwarded emits the __peer__|<addr>|forwarded envelope
	// so the receiving proxy's credential plugin sets
	// IsForwardedFromPeer=true. That flag tells the chain's peer-trust
	// filter to keep running rewrite + auth on the unrewritten client
	// SQL — without it, the peer would skip rewrite (legacy remote()
	// loopback contract) and the host's logical→physical table mapping
	// would never be applied.
	user, password, err := peer.SignPeerHelloForwarded(p.PeerSigner, p.PeerTokenTTL, peerIndexer, p.Fallback)
	if err != nil {
		if closer, ok := up.Conn().(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return fmt.Errorf("sign peer hello for %s: %w", addr, err)
	}

	peerHello := &chproto.ClientHello{
		Name:            hello.Name,
		Major:           hello.Major,
		Minor:           hello.Minor,
		ProtocolVersion: hello.ProtocolVersion,
		Database:        hello.Database,
		User:            user,
		Password:        password,
	}
	if err := rebind(ctx, sess, up, peerHello); err != nil {
		if closer, ok := up.Conn().(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return fmt.Errorf("rebind to peer %s: %w", addr, err)
	}

	sess.State().SetForwarding(true)
	sess.State().SetRouteTarget(addr)
	log.Infow("forward: pivoted session to peer at hello",
		"db", hello.Database, "peer", addr, "indexer", peerIndexer)
	return nil
}

// defaultDial dials a peer's internal-port and constructs a chproto.Codec
// suitable for an upstream-direction handshake.
func defaultDial(ctx context.Context, addr string) (*chproto.Codec, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return chproto.NewCodec(conn, chproto.DirToUpstream), nil
}

// defaultRebind delegates to Session.RebindToPeer.
func defaultRebind(ctx context.Context, sess chsession.Session, up *chproto.Codec, hello *chproto.ClientHello) error {
	return sess.RebindToPeer(ctx, up, hello)
}

// defaultLocalRebind delegates to Session.RebindToLocal.
func defaultLocalRebind(ctx context.Context, sess chsession.Session, up *chproto.Codec, hello *chproto.ClientHello) error {
	return sess.RebindToLocal(ctx, up, hello)
}

// OnQuery intercepts standalone USE statements. When USE targets a logical
// database hosted on a different peer than the session's current RouteTarget,
// pivots the session via pivotToPeer. Anything other than a bare USE
// (USE … SETTINGS …, multi-statement, etc.) is a no-op — the rewriter's
// maybeUpdateLogicalDatabase callback handles complex forms.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	newDB, ok := matchUse(qctx.OriginalSQL)
	if !ok {
		return nil
	}
	info, found := p.Databases.Get(newDB)
	if !found {
		return nil // let CH return the real "doesn't exist" error
	}

	sess := qctx.Session
	st := sess.State()

	if info.IndexerId == p.SelfIndexerID {
		// New database is local. If the session is currently forwarded
		// to a peer, we must rebind back to local CH — otherwise the
		// USE packet would be sent to the peer's CH which doesn't host
		// this database and will reject. If the session was never
		// forwarded, the local upstream is already correct.
		if !st.IsForwarding {
			return nil
		}
		return p.rebindToLocal(ctx, sess, newDB)
	}

	peerAddr, ok := p.Topology.ProxyByIndexerId(info.IndexerId)
	if !ok {
		return fmt.Errorf("indexer %d not found in topology", info.IndexerId)
	}
	newAddr := fmt.Sprintf("%s:%d", peerAddr.Url, peerAddr.HousegatePort)

	if newAddr == st.GetRouteTarget() {
		return nil // already on the right peer — skip
	}

	return p.pivotToPeer(ctx, sess, &chproto.ClientHello{
		Name:            st.ClientHostname,
		Major:           st.ClientVersion.Major,
		Minor:           st.ClientVersion.Minor,
		ProtocolVersion: st.ClientRevision,
		Database:        newDB,
	}, info.IndexerId)
}

// rebindToLocal is the remote→local counterpart of pivotToPeer. Dialed
// when a USE statement on a previously-forwarded session targets a
// database hosted on this proxy. Closes the peer upstream, dials local
// CH, runs the standard ClientHello→ServerHello handshake on the new
// codec, and clears IsForwarding + RouteTarget so the chain stops
// treating the session as forwarded.
//
// LocalDialer must be wired (build.go does this). nil LocalDialer
// returns an error rather than silently leaving the session pointing
// at the peer — the USE would otherwise hit a database that doesn't
// exist on the peer.
func (p *Plugin) rebindToLocal(ctx context.Context, sess chsession.Session, newDB string) error {
	if p.LocalDialer == nil {
		return fmt.Errorf("forward: USE %s targets local proxy but no LocalDialer is wired; cannot rebind back from peer", newDB)
	}

	rebind := p.localRebindFn
	if rebind == nil {
		rebind = defaultLocalRebind
	}

	up, err := p.LocalDialer(ctx)
	if err != nil {
		return fmt.Errorf("dial local CH for USE %s: %w", newDB, err)
	}

	st := sess.State()
	// hello.Database must be the PHYSICAL CH database, not the logical
	// `newDB` the user typed. In deployments that prefix many logical
	// DBs onto one physical DB, the logical name is not a real CH
	// database and CH would reject the hello with "Database does not
	// exist".
	//
	// We cannot read this from st.Database: when forward.OnHello
	// pivoted the session to a peer it set IsForwarding=true, which
	// causes the chain's ForwardAware filter to skip rewrite.OnHello
	// (the hook that would normally substitute logical→physical). So
	// st.Database carries the logical name from relay.handshake's
	// post-OnHello assignment. Use the configured PhysicalDatabase
	// directly. Empty PhysicalDatabase means no logical multiplexing
	// is configured — fall back to st.Database which is then the
	// real (physical) value.
	helloDB := st.Database
	if p.PhysicalDatabase != "" {
		helloDB = p.PhysicalDatabase
	}
	localHello := &chproto.ClientHello{
		Name:            st.ClientHostname,
		Major:           st.ClientVersion.Major,
		Minor:           st.ClientVersion.Minor,
		ProtocolVersion: st.ClientRevision,
		// Use the operator-managed upstream credentials the credential
		// plugin populated during the original OnHello. The peer envelope
		// must NOT appear here — local CH would reject __peer__|... as
		// an unknown user.
		User:     st.MappedUser,
		Password: st.MappedPassword,
		Database: helloDB,
	}

	prevPeer := st.GetRouteTarget()
	if err := rebind(ctx, sess, up, localHello); err != nil {
		if closer, ok := up.Conn().(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return fmt.Errorf("rebind to local for USE %s: %w", newDB, err)
	}

	log.Infow("forward: rebound session from peer to local",
		"db", newDB, "previous_peer", prevPeer)
	return nil
}

// RunOnForward implements plugin.ForwardAware. The forward plugin must keep
// running on already-forwarded sessions so OnQuery can re-route on USE.
func (*Plugin) RunOnForward() bool { return true }

// RunOnPeerTrust implements plugin.PeerTrustAware. Returns false — peer-trusted
// sessions arrived from another housegate that already made the routing
// decision; double-routing here would loop or duplicate work.
func (*Plugin) RunOnPeerTrust() bool { return false }

var (
	_ plugin.HelloPlugin    = (*Plugin)(nil)
	_ plugin.QueryPlugin    = (*Plugin)(nil)
	_ plugin.ForwardAware   = (*Plugin)(nil)
	_ plugin.PeerTrustAware = (*Plugin)(nil)
)
