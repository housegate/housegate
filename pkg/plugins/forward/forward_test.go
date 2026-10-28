package forward

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sitable"
)

// --------------------------------------------------------------------
// Test helpers
// --------------------------------------------------------------------

// fakeNS is a thin wrapper around InMemoryNetworkState with fluent
// builder methods for the common test patterns.
type fakeNS struct {
	s *network.InMemoryNetworkState
}

func newFakeNS(t *testing.T) *fakeNS {
	t.Helper()
	return &fakeNS{s: network.NewInMemoryNetworkState()}
}

// WithDatabase registers a logical database hosted by indexerID.
func (f *fakeNS) WithDatabase(name string, indexerID uint64) *fakeNS {
	f.s.DatabaseInfos[network.Database(name)] = network.DatabaseInfo{
		DatabaseId: name,
		IndexerId:  indexerID,
	}
	return f
}

// WithIndexer registers an indexer reachable at host:port.
func (f *fakeNS) WithIndexer(id uint64, host string, port uint16) *fakeNS {
	f.s.IndexerInfos[id] = network.IndexerInfo{
		IndexerId:           id,
		IndexerUrl:          host,
		ClickhouseProxyPort: port,
	}
	return f
}

// newTestForwardPlugin builds a *Plugin wired to ns and selfIndexerID.
// A real RelaySigner is used so the signer interface is satisfied; tests
// that do not exercise the peer-hello path never invoke it.
func newTestForwardPlugin(t *testing.T, ns *fakeNS, selfIndexerID uint64) *Plugin {
	t.Helper()
	// Use the same test key from pkg/auth peer tests.
	const testRelayKey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	signer, err := auth.NewRelaySigner(testRelayKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	reg := ns.s
	return &Plugin{
		Topology:      reg,
		Databases:     reg,
		SelfIndexerID: selfIndexerID,
		PeerSigner:    signer,
		PeerTokenTTL:  time.Minute,
	}
}

// newTestSession creates a minimal Session backed by a throwaway net.Pipe
// connection. Mirrors the pattern in pkg/plugins/route/stripper_test.go.
func newTestSession(t *testing.T) chsession.Session {
	t.Helper()
	clientConn, _ := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	return chsession.New(0, clientConn)
}

// pipedCodec returns a (*chproto.Codec, *chproto.Codec) pair via net.Pipe.
// A goroutine on peerEnd drains all writes so the codec end's writes don't
// block. Returns (codec, peerEnd) — peerEnd is the goroutine-drained side.
func pipedCodec(t *testing.T) (*chproto.Codec, net.Conn) {
	t.Helper()
	proxyEnd, peerEnd := net.Pipe()
	t.Cleanup(func() {
		_ = proxyEnd.Close()
		_ = peerEnd.Close()
	})
	// Drain peerEnd so writes to proxyEnd never block.
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := peerEnd.Read(buf); err != nil {
				return
			}
		}
	}()
	return chproto.NewCodec(proxyEnd, chproto.DirToUpstream), peerEnd
}

// --------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------

func TestForwardPlugin_OnHello_LocalDatabase_NoForward(t *testing.T) {
	ns := newFakeNS(t).
		WithDatabase("tenant1", 100). // tenant1 hosted on indexer 100
		WithIndexer(100, "self", 9000)
	p := newTestForwardPlugin(t, ns, 100) // self = 100

	sess := newTestSession(t)
	hello := &chproto.ClientHello{Database: "tenant1"}

	if err := p.OnHello(context.Background(), sess, hello); err != nil {
		t.Fatalf("OnHello: %v", err)
	}
	if sess.State().IsForwarding {
		t.Errorf("local DB must not set IsForwarding")
	}
	if sess.State().RouteTarget != "" {
		t.Errorf("local DB must leave RouteTarget empty, got %q", sess.State().RouteTarget)
	}
}

func TestForwardPlugin_OnHello_RemoteDatabase_TriggersRebind(t *testing.T) {
	ns := newFakeNS(t).
		WithDatabase("tenant2", 200).
		WithIndexer(200, "peer.internal", 9001)

	var dialedAddr string
	var rebindHello *chproto.ClientHello
	p := newTestForwardPlugin(t, ns, 100)
	p.dialPeer = func(_ context.Context, addr string) (*chproto.Codec, error) {
		dialedAddr = addr
		c, _ := pipedCodec(t) // returns a usable codec; the goroutine end drains
		return c, nil
	}
	p.rebindFn = func(_ context.Context, _ chsession.Session, _ *chproto.Codec, hello *chproto.ClientHello) error {
		rebindHello = hello
		return nil
	}

	sess := newTestSession(t)
	hello := &chproto.ClientHello{Database: "tenant2", User: "alice"}

	if err := p.OnHello(context.Background(), sess, hello); err != nil {
		t.Fatalf("OnHello: %v", err)
	}
	if !sess.State().IsForwarding {
		t.Errorf("remote DB must set IsForwarding")
	}
	if sess.State().RouteTarget != "peer.internal:9001" {
		t.Errorf("RouteTarget = %q want peer.internal:9001", sess.State().RouteTarget)
	}
	if dialedAddr != "peer.internal:9001" {
		t.Errorf("dialed addr = %q want peer.internal:9001", dialedAddr)
	}
	if rebindHello == nil {
		t.Fatal("rebinder not invoked")
	}
	// The peer envelope: User must be __peer__|<self-addr>, not the original "alice"
	if rebindHello.User == "alice" || !strings.HasPrefix(rebindHello.User, "__peer__|") {
		t.Errorf("rebind hello.User must be __peer__|...; got %q", rebindHello.User)
	}
	if rebindHello.Database != "tenant2" {
		t.Errorf("rebind hello.Database = %q want tenant2 (preserves original target)", rebindHello.Database)
	}
}

func TestForwardPlugin_OnHello_UnknownDatabase_Errors(t *testing.T) {
	ns := newFakeNS(t)
	p := newTestForwardPlugin(t, ns, 100)

	sess := newTestSession(t)
	hello := &chproto.ClientHello{Database: "nope"}

	err := p.OnHello(context.Background(), sess, hello)
	if err == nil {
		t.Fatalf("expected error for unknown database")
	}
	if !strings.Contains(err.Error(), "doesn't exist") {
		t.Errorf("error should explain DB doesn't exist; got %v", err)
	}
}

// TestForwardPlugin_OnHello_PhysicalDatabase_DefersToLocal verifies the
// physical-database short-circuit: internal services (sentio
// analytic-server etc.) connect with hello.Database set to the configured
// physical CH database name, which is intentionally absent from the
// logical-database registry. OnHello must defer to local rather than
// rejecting it as an unknown logical DB.
func TestForwardPlugin_OnHello_PhysicalDatabase_DefersToLocal(t *testing.T) {
	ns := newFakeNS(t)
	p := newTestForwardPlugin(t, ns, 100)
	p.PhysicalDatabase = "testnet"

	sess := newTestSession(t)
	hello := &chproto.ClientHello{Database: "testnet"}

	if err := p.OnHello(context.Background(), sess, hello); err != nil {
		t.Fatalf("OnHello on physical DB must defer, not error: %v", err)
	}
	if sess.State().IsForwarding {
		t.Errorf("physical DB must not set IsForwarding")
	}
	if sess.State().RouteTarget != "" {
		t.Errorf("physical DB must not set RouteTarget")
	}
}

func TestForwardPlugin_OnHello_EmptyDatabase_DefersToLocal(t *testing.T) {
	ns := newFakeNS(t)
	p := newTestForwardPlugin(t, ns, 100)

	sess := newTestSession(t)
	hello := &chproto.ClientHello{Database: ""}

	if err := p.OnHello(context.Background(), sess, hello); err != nil {
		t.Fatalf("OnHello empty DB must defer, not error: %v", err)
	}
	if sess.State().IsForwarding {
		t.Errorf("empty DB must not set IsForwarding")
	}
	if sess.State().RouteTarget != "" {
		t.Errorf("empty DB must not set RouteTarget")
	}
}

func TestForwardPlugin_OnHello_PeerTrusted_NoForward(t *testing.T) {
	// tenant2 is hosted on a remote indexer (200) — without IsPeerTrusted
	// this would normally trigger a pivot. With IsPeerTrusted=true the
	// guard at the top of OnHello must short-circuit and leave all state
	// untouched.
	ns := newFakeNS(t).
		WithDatabase("tenant2", 200).
		WithIndexer(200, "peer.internal", 9001)
	p := newTestForwardPlugin(t, ns, 100) // self=100, tenant2 hosted on 200

	sess := newTestSession(t)
	sess.State().IsPeerTrusted = true // simulate internal-port pre-flag
	hello := &chproto.ClientHello{Database: "tenant2"}

	if err := p.OnHello(context.Background(), sess, hello); err != nil {
		t.Fatalf("peer-trusted session must not error: %v", err)
	}
	if sess.State().IsForwarding {
		t.Errorf("peer-trusted session must not set IsForwarding")
	}
	if sess.State().RouteTarget != "" {
		t.Errorf("peer-trusted session must leave RouteTarget empty, got %q", sess.State().RouteTarget)
	}
}

// --------------------------------------------------------------------
// OnQuery USE-rebind tests
// --------------------------------------------------------------------

func TestForwardPlugin_OnQuery_USESameDb_NoRebind(t *testing.T) {
	ns := newFakeNS(t).
		WithDatabase("tenant2", 200).
		WithIndexer(200, "peer.internal", 9001)
	p := newTestForwardPlugin(t, ns, 100)

	var rebindCalled bool
	p.dialPeer = func(_ context.Context, _ string) (*chproto.Codec, error) {
		rebindCalled = true // dial reflects rebind attempt
		c, _ := pipedCodec(t)
		return c, nil
	}
	p.rebindFn = func(_ context.Context, _ chsession.Session, _ *chproto.Codec, _ *chproto.ClientHello) error {
		return nil
	}

	sess := newTestSession(t)
	sess.State().SetRouteTarget("peer.internal:9001") // already on the same peer
	sess.State().LogicalDatabase = "tenant2"

	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "USE tenant2"}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if rebindCalled {
		t.Errorf("USE same DB must not trigger rebind")
	}
}

func TestForwardPlugin_OnQuery_USEDifferentDb_TriggersRebind(t *testing.T) {
	ns := newFakeNS(t).
		WithDatabase("tenant2", 200).WithIndexer(200, "peer.internal", 9001).
		WithDatabase("tenant3", 300).WithIndexer(300, "other.internal", 9002)
	p := newTestForwardPlugin(t, ns, 100)

	var dialedAddr string
	p.dialPeer = func(_ context.Context, addr string) (*chproto.Codec, error) {
		dialedAddr = addr
		c, _ := pipedCodec(t)
		return c, nil
	}
	p.rebindFn = func(_ context.Context, _ chsession.Session, _ *chproto.Codec, _ *chproto.ClientHello) error {
		return nil
	}

	sess := newTestSession(t)
	sess.State().SetRouteTarget("peer.internal:9001") // currently on tenant2's peer
	sess.State().LogicalDatabase = "tenant2"

	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "USE tenant3"}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if dialedAddr != "other.internal:9002" {
		t.Errorf("expected dial to tenant3's peer; got %q", dialedAddr)
	}
	if sess.State().RouteTarget != "other.internal:9002" {
		t.Errorf("RouteTarget should update to new peer; got %q", sess.State().RouteTarget)
	}
}

func TestForwardPlugin_OnQuery_USELocalDb_NoRebind(t *testing.T) {
	ns := newFakeNS(t).
		WithDatabase("local_tenant", 100). // hosted on self
		WithIndexer(100, "self", 9000)
	p := newTestForwardPlugin(t, ns, 100) // self = indexer 100

	var dialCalled bool
	p.dialPeer = func(_ context.Context, _ string) (*chproto.Codec, error) {
		dialCalled = true
		c, _ := pipedCodec(t)
		return c, nil
	}
	p.rebindFn = func(_ context.Context, _ chsession.Session, _ *chproto.Codec, _ *chproto.ClientHello) error {
		return nil
	}

	sess := newTestSession(t)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "USE local_tenant"}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery local DB must not error: %v", err)
	}
	if dialCalled {
		t.Errorf("local DB must not trigger dial")
	}
	if sess.State().IsForwarding {
		t.Errorf("local DB must not flip IsForwarding")
	}
}

func TestForwardPlugin_OnQuery_NonUSE_NoOp(t *testing.T) {
	ns := newFakeNS(t).WithDatabase("tenant2", 200).WithIndexer(200, "peer.internal", 9001)
	p := newTestForwardPlugin(t, ns, 100)

	var rebindCalled bool
	p.dialPeer = func(_ context.Context, _ string) (*chproto.Codec, error) {
		rebindCalled = true
		c, _ := pipedCodec(t)
		return c, nil
	}
	p.rebindFn = func(_ context.Context, _ chsession.Session, _ *chproto.Codec, _ *chproto.ClientHello) error {
		return nil
	}

	sess := newTestSession(t)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1"}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if rebindCalled {
		t.Errorf("non-USE must not trigger rebind")
	}
	if sess.State().IsForwarding {
		t.Errorf("non-USE must not flip IsForwarding")
	}
}

// TestForwardPlugin_OnQuery_USELocalAfterPivot_TriggersLocalRebind is the
// remote→local counterpart of the cross-peer pivot test. After a session
// has been forwarded to a peer (USE remote_db), a subsequent USE that
// targets a database hosted on THIS proxy must rebind back to local CH —
// otherwise the USE goes to the peer's CH which doesn't have the
// database and rejects it.
func TestForwardPlugin_OnQuery_USELocalAfterPivot_TriggersLocalRebind(t *testing.T) {
	ns := newFakeNS(t).
		WithDatabase("local_db", 100). // hosted on self
		WithIndexer(100, "self", 9000)
	p := newTestForwardPlugin(t, ns, 100)

	var localDialCalled bool
	p.LocalDialer = func(_ context.Context) (*chproto.Codec, error) {
		localDialCalled = true
		c, _ := pipedCodec(t)
		return c, nil
	}
	var localRebindHello *chproto.ClientHello
	p.localRebindFn = func(_ context.Context, sess chsession.Session, _ *chproto.Codec, hello *chproto.ClientHello) error {
		localRebindHello = hello
		// Mimic Session.RebindToLocal's state mutations.
		st := sess.State()
		st.SetForwarding(false)
		st.SetRouteTarget("")
		return nil
	}

	sess := newTestSession(t)
	// Pre-condition: session was previously pivoted to a peer.
	sess.State().SetForwarding(true)
	sess.State().SetRouteTarget("peer.internal:9001")
	sess.State().MappedUser = "ch_admin"
	sess.State().MappedPassword = "ch_secret"
	// state.Database mirrors what relay.handshake records post-OnHello:
	// the LOGICAL name from the client's hello. Because forward.OnHello
	// set IsForwarding before rewrite.OnHello ran, the rewrite hook was
	// filtered out and never substituted the physical name. The plugin
	// must source the physical name from its own PhysicalDatabase
	// configuration instead.
	sess.State().Database = "remote_db"
	p.PhysicalDatabase = "shared_physical_db"

	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "USE local_db"}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	if !localDialCalled {
		t.Fatalf("USE on local DB after pivot must dial local CH")
	}
	if localRebindHello == nil {
		t.Fatalf("RebindToLocal must be invoked")
	}
	if localRebindHello.Database != "shared_physical_db" {
		t.Errorf("local rebind hello.Database=%q want shared_physical_db (physical name from Plugin.PhysicalDatabase, NOT the logical USE target nor the logical state.Database)",
			localRebindHello.Database)
	}
	if localRebindHello.User != "ch_admin" || localRebindHello.Password != "ch_secret" {
		t.Errorf("local rebind hello must use mapped credentials, not peer envelope; got user=%q pwd=%q",
			localRebindHello.User, localRebindHello.Password)
	}
	// Forward state cleared (mocked rebind sets these but verify the contract).
	if sess.State().IsForwarding {
		t.Errorf("IsForwarding must be cleared after rebind to local")
	}
	if sess.State().GetRouteTarget() != "" {
		t.Errorf("RouteTarget must be cleared after rebind to local; got %q",
			sess.State().GetRouteTarget())
	}
}

// TestForwardPlugin_OnQuery_USELocalDb_NotForwarded_NoRebind is a control
// for the test above: when a session is NOT in a forwarded state (fresh
// session, never pivoted), USE on a local database is a pure no-op.
// This keeps the path inexpensive on the common case.
func TestForwardPlugin_OnQuery_USELocalDb_NotForwarded_NoRebind(t *testing.T) {
	ns := newFakeNS(t).
		WithDatabase("local_db", 100).
		WithIndexer(100, "self", 9000)
	p := newTestForwardPlugin(t, ns, 100)

	var localDialCalled bool
	p.LocalDialer = func(_ context.Context) (*chproto.Codec, error) {
		localDialCalled = true
		c, _ := pipedCodec(t)
		return c, nil
	}

	sess := newTestSession(t)
	// IsForwarding stays false — this is a fresh session.
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "USE local_db"}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if localDialCalled {
		t.Errorf("USE local on never-forwarded session must NOT dial local CH (it is already there)")
	}
}

// TestForwardPlugin_OnQuery_USELocalAfterPivot_UsesPhysicalDB_NotLogical
// is the regression test for the "Database <logical> does not exist"
// error users hit after `CREATE DATABASE` + `USE` cycles.
//
// Repro: clickhouse-client reconnects on every USE. After the first USE
// pivoted the session to a peer (forward.OnHello → IsForwarding=true),
// rewrite.OnHello — the hook that rewrites hello.Database from logical
// to physical — was filtered out. relay.handshake then assigned
// state.Database = hello.Database (logical). When the next USE targets
// a database that is in fact local, rebindToLocal must NOT use that
// logical state.Database for the local hello: local CH only knows the
// physical name. The plugin's configured PhysicalDatabase wins.
func TestForwardPlugin_OnQuery_USELocalAfterPivot_UsesPhysicalDB_NotLogical(t *testing.T) {
	ns := newFakeNS(t).
		WithDatabase("local_db", 100).
		WithIndexer(100, "self", 9000)
	p := newTestForwardPlugin(t, ns, 100)
	p.PhysicalDatabase = "sentio_physical"

	p.LocalDialer = func(_ context.Context) (*chproto.Codec, error) {
		c, _ := pipedCodec(t)
		return c, nil
	}
	var localRebindHello *chproto.ClientHello
	p.localRebindFn = func(_ context.Context, sess chsession.Session, _ *chproto.Codec, hello *chproto.ClientHello) error {
		localRebindHello = hello
		st := sess.State()
		st.SetForwarding(false)
		st.SetRouteTarget("")
		return nil
	}

	sess := newTestSession(t)
	sess.State().SetForwarding(true)
	sess.State().SetRouteTarget("peer.internal:9001")
	sess.State().MappedUser = "ch_admin"
	sess.State().MappedPassword = "ch_secret"
	// Mirrors the production state after a forwarded handshake: rewrite
	// was filtered, so state.Database carries the LOGICAL name from
	// hello.Database, not the physical CH database.
	sess.State().Database = "remote_logical_db"

	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "USE local_db"}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if localRebindHello == nil {
		t.Fatalf("RebindToLocal must be invoked")
	}
	if localRebindHello.Database != "sentio_physical" {
		t.Errorf("local rebind hello.Database=%q want sentio_physical (PhysicalDatabase must override the logical state.Database)",
			localRebindHello.Database)
	}
}

// TestForwardPlugin_OnQuery_USELocalAfterPivot_NoPhysicalDB_FallsBackToState
// covers deployments without logical/physical multiplexing: when
// PhysicalDatabase is empty, state.Database is the real CH database
// and we use it directly.
func TestForwardPlugin_OnQuery_USELocalAfterPivot_NoPhysicalDB_FallsBackToState(t *testing.T) {
	ns := newFakeNS(t).
		WithDatabase("local_db", 100).
		WithIndexer(100, "self", 9000)
	p := newTestForwardPlugin(t, ns, 100)
	// PhysicalDatabase intentionally left empty.

	p.LocalDialer = func(_ context.Context) (*chproto.Codec, error) {
		c, _ := pipedCodec(t)
		return c, nil
	}
	var localRebindHello *chproto.ClientHello
	p.localRebindFn = func(_ context.Context, sess chsession.Session, _ *chproto.Codec, hello *chproto.ClientHello) error {
		localRebindHello = hello
		st := sess.State()
		st.SetForwarding(false)
		st.SetRouteTarget("")
		return nil
	}

	sess := newTestSession(t)
	sess.State().SetForwarding(true)
	sess.State().SetRouteTarget("peer.internal:9001")
	sess.State().MappedUser = "ch_admin"
	sess.State().MappedPassword = "ch_secret"
	sess.State().Database = "real_db"

	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "USE local_db"}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if localRebindHello == nil {
		t.Fatalf("RebindToLocal must be invoked")
	}
	if localRebindHello.Database != "real_db" {
		t.Errorf("local rebind hello.Database=%q want real_db (no PhysicalDatabase configured → use state.Database)",
			localRebindHello.Database)
	}
}

// TestForwardPlugin_OnQuery_USELocalAfterPivot_NoLocalDialer_Errors
// proves that when forward.Plugin is wired without a LocalDialer
// (router-only proxies, or misconfiguration), the rebind-to-local path
// surfaces a clear error rather than silently leaving the USE pointing
// at the peer.
func TestForwardPlugin_OnQuery_USELocalAfterPivot_NoLocalDialer_Errors(t *testing.T) {
	ns := newFakeNS(t).
		WithDatabase("local_db", 100).
		WithIndexer(100, "self", 9000)
	p := newTestForwardPlugin(t, ns, 100)
	// No LocalDialer wired.

	sess := newTestSession(t)
	sess.State().SetForwarding(true)
	sess.State().SetRouteTarget("peer.internal:9001")

	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "USE local_db"}
	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatalf("OnQuery must error when LocalDialer is nil and rebind-to-local is needed")
	}
	if !strings.Contains(err.Error(), "LocalDialer") {
		t.Errorf("error should mention LocalDialer; got: %v", err)
	}
}

// --------------------------------------------------------------------
// Replica-served routing (spec 2026-10-14 §11)
// --------------------------------------------------------------------

// replicaServedState is a table state serving rrdb (owned by indexer 200)
// from a member's copy on this host.
func replicaServedState() *sitable.Fake {
	return sitable.NewFake(sitable.Ordinary, sitable.Table{
		ID: "rrdb.a", Status: sitable.Replica, CopyTable: "rrdb__a__i3", ReplicaOf: 200,
	})
}

// replicaRouting is a host (indexer 100) whose table state serves rrdb, owned
// by indexer 200, plus a database other on indexer 200 that is not served and
// a local database local_db. Every dial is recorded.
func replicaRouting(t *testing.T, state sitable.TableState) (*Plugin, *[]string, *[]*chproto.ClientHello) {
	t.Helper()
	ns := newFakeNS(t).
		WithDatabase("rrdb", 200).
		WithDatabase("other", 200).
		WithDatabase("local_db", 100).
		WithIndexer(100, "self", 9000).
		WithIndexer(200, "owner.internal", 9001)
	p := newTestForwardPlugin(t, ns, 100)
	p.TableState = state
	p.PhysicalDatabase = "phys"
	var dials []string
	var localHellos []*chproto.ClientHello
	p.dialPeer = func(_ context.Context, addr string) (*chproto.Codec, error) {
		dials = append(dials, addr)
		c, _ := pipedCodec(t)
		return c, nil
	}
	p.rebindFn = func(_ context.Context, _ chsession.Session, _ *chproto.Codec, _ *chproto.ClientHello) error {
		return nil
	}
	p.LocalDialer = func(_ context.Context) (*chproto.Codec, error) {
		dials = append(dials, "local")
		c, _ := pipedCodec(t)
		return c, nil
	}
	p.localRebindFn = func(_ context.Context, sess chsession.Session, _ *chproto.Codec, hello *chproto.ClientHello) error {
		localHellos = append(localHellos, hello)
		sess.State().SetForwarding(false)
		sess.State().SetRouteTarget("")
		return nil
	}
	return p, &dials, &localHellos
}

func queryOn(sess chsession.Session, sql string) *plugin.QueryContext {
	return &plugin.QueryContext{Session: sess, OriginalSQL: sql}
}

const servingEndedMessage = "storage_integrity: database rrdb is no longer served from a copy on this host; reconnect to reach its owner (retryable)"

// TestForward_ReplicaServedHelloStaysLocal: a hello of a database the host
// serves from its member's copy is not pivoted to the owner; the session is
// marked. A database of the same owner that is not served still pivots.
func TestForward_ReplicaServedHelloStaysLocal(t *testing.T) {
	p, dials, _ := replicaRouting(t, replicaServedState())

	sess := newTestSession(t)
	if err := p.OnHello(context.Background(), sess, &chproto.ClientHello{Database: "rrdb"}); err != nil {
		t.Fatalf("OnHello(rrdb): %v", err)
	}
	snap := sess.State().Snapshot()
	if len(*dials) != 0 || snap.IsForwarding || snap.RouteTarget != "" {
		t.Fatalf("dials=%v forwarding=%v route=%q, want the session kept local", *dials, snap.IsForwarding, snap.RouteTarget)
	}
	if snap.ReplicaServedDatabase != "rrdb" {
		t.Fatalf("ReplicaServedDatabase = %q, want rrdb", snap.ReplicaServedDatabase)
	}

	other := newTestSession(t)
	if err := p.OnHello(context.Background(), other, &chproto.ClientHello{Database: "other"}); err != nil {
		t.Fatalf("OnHello(other): %v", err)
	}
	if got := other.State().Snapshot(); strings.Join(*dials, ",") != "owner.internal:9001" || !got.IsForwarding || got.ReplicaServedDatabase != "" {
		t.Fatalf("dials=%v forwarding=%v mark=%q, want a database that is not served pivoted to its owner unmarked", *dials, got.IsForwarding, got.ReplicaServedDatabase)
	}
}

// TestForward_USEOfAReplicaServedDatabaseStaysLocal: on a local session, USE of
// a served database dials nothing and marks the session; the USE itself then
// runs locally (rewrite maps it to the physical database).
func TestForward_USEOfAReplicaServedDatabaseStaysLocal(t *testing.T) {
	p, dials, _ := replicaRouting(t, replicaServedState())
	sess := newTestSession(t)
	if err := p.OnQuery(context.Background(), queryOn(sess, "USE rrdb")); err != nil {
		t.Fatalf("OnQuery(USE rrdb): %v", err)
	}
	snap := sess.State().Snapshot()
	if len(*dials) != 0 || snap.IsForwarding || snap.ReplicaServedDatabase != "rrdb" {
		t.Fatalf("dials=%v forwarding=%v mark=%q, want no dial and the session marked rrdb", *dials, snap.IsForwarding, snap.ReplicaServedDatabase)
	}
}

// TestForward_USEOfAReplicaServedDatabaseRebindsAForwardedSessionToLocal: a
// session forwarded to a peer comes back to the local ClickHouse through the
// existing return-home pivot (physical database, mapped credentials) and is
// marked.
func TestForward_USEOfAReplicaServedDatabaseRebindsAForwardedSessionToLocal(t *testing.T) {
	p, dials, localHellos := replicaRouting(t, replicaServedState())
	sess := newTestSession(t)
	sess.State().SetForwarding(true)
	sess.State().SetRouteTarget("owner.internal:9001")
	sess.State().MappedUser = "ch_admin"
	sess.State().MappedPassword = "ch_secret"
	if err := p.OnQuery(context.Background(), queryOn(sess, "USE rrdb")); err != nil {
		t.Fatalf("OnQuery(USE rrdb): %v", err)
	}
	if strings.Join(*dials, ",") != "local" || len(*localHellos) != 1 {
		t.Fatalf("dials=%v local rebinds=%d, want one rebind to the local ClickHouse", *dials, len(*localHellos))
	}
	if hello := (*localHellos)[0]; hello.Database != "phys" || hello.User != "ch_admin" || hello.Password != "ch_secret" {
		t.Fatalf("local hello = %+v, want the physical database and the mapped credentials", hello)
	}
	snap := sess.State().Snapshot()
	if snap.IsForwarding || snap.RouteTarget != "" || snap.ReplicaServedDatabase != "rrdb" {
		t.Fatalf("forwarding=%v route=%q mark=%q, want a local session marked rrdb", snap.IsForwarding, snap.RouteTarget, snap.ReplicaServedDatabase)
	}
}

// TestForward_ServingEndedIsARetryableRefusal: while the database is served, a
// marked session's statements pass; once the snapshot stops serving it, every
// statement but a USE is refused with the retryable 733 asking the client to
// reconnect.
func TestForward_ServingEndedIsARetryableRefusal(t *testing.T) {
	state := replicaServedState()
	p, _, _ := replicaRouting(t, state)
	sess := newTestSession(t)
	if err := p.OnHello(context.Background(), sess, &chproto.ClientHello{Database: "rrdb"}); err != nil {
		t.Fatalf("OnHello(rrdb): %v", err)
	}
	if err := p.OnQuery(context.Background(), queryOn(sess, "SELECT count() FROM a")); err != nil {
		t.Fatalf("a read while rrdb is served: %v", err)
	}
	state.Set() // the member revoked the lease: rrdb is no longer served
	for _, sql := range []string{"SELECT count() FROM a", "SELECT 1", "INSERT INTO a VALUES (1)", "SHOW TABLES"} {
		err := p.OnQuery(context.Background(), queryOn(sess, sql))
		var ce *chproto.ClientError
		if !errors.As(err, &ce) || ce.Code != chproto.CodeTableIsBeingRestarted || ce.Message != servingEndedMessage {
			t.Fatalf("OnQuery(%q) = %v, want 733 %q", sql, err, servingEndedMessage)
		}
	}
	state.Set(sitable.Table{ID: "rrdb.a", Status: sitable.Replica, CopyTable: "rrdb__a__i4", ReplicaOf: 200})
	if err := p.OnQuery(context.Background(), queryOn(sess, "SELECT count() FROM a")); err != nil {
		t.Fatalf("a read once rrdb is served again: %v", err)
	}
}

// TestForward_UseElsewhereClearsTheReplicaMark: a USE of a database hosted
// elsewhere and not served (pivoted to, or already the session's route
// target), or a USE of a local database, clears the mark, so the end of
// serving no longer refuses that session.
func TestForward_UseElsewhereClearsTheReplicaMark(t *testing.T) {
	for _, tc := range []struct {
		name, use, route, dials string
	}{
		{name: "pivot to the owner", use: "USE other", dials: "owner.internal:9001"},
		{name: "already routed to the owner", use: "USE other", route: "owner.internal:9001", dials: ""},
		{name: "local database", use: "USE local_db", dials: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := replicaServedState()
			p, dials, _ := replicaRouting(t, state)
			sess := newTestSession(t)
			if err := p.OnHello(context.Background(), sess, &chproto.ClientHello{Database: "rrdb"}); err != nil {
				t.Fatalf("OnHello(rrdb): %v", err)
			}
			if tc.route != "" {
				sess.State().SetRouteTarget(tc.route)
			}
			if err := p.OnQuery(context.Background(), queryOn(sess, tc.use)); err != nil {
				t.Fatalf("OnQuery(%s): %v", tc.use, err)
			}
			if got := sess.State().Snapshot().ReplicaServedDatabase; got != "" || strings.Join(*dials, ",") != tc.dials {
				t.Fatalf("mark=%q dials=%v, want the mark cleared and dials %q", got, *dials, tc.dials)
			}
			state.Set()
			if err := p.OnQuery(context.Background(), queryOn(sess, "SELECT 1")); err != nil {
				t.Fatalf("an unmarked session must not be refused when serving ends: %v", err)
			}
		})
	}
}

// TestForward_WithoutTableStateBehavesAsToday: with no table state the plugin
// pivots a hello and a USE of a remote database exactly as before replica
// reads, marks nothing, and never refuses a statement.
func TestForward_WithoutTableStateBehavesAsToday(t *testing.T) {
	p, dials, _ := replicaRouting(t, nil)
	sess := newTestSession(t)
	if err := p.OnHello(context.Background(), sess, &chproto.ClientHello{Database: "rrdb"}); err != nil {
		t.Fatalf("OnHello(rrdb): %v", err)
	}
	snap := sess.State().Snapshot()
	if strings.Join(*dials, ",") != "owner.internal:9001" || !snap.IsForwarding || snap.ReplicaServedDatabase != "" {
		t.Fatalf("dials=%v forwarding=%v mark=%q, want today's hello pivot", *dials, snap.IsForwarding, snap.ReplicaServedDatabase)
	}
	local := newTestSession(t)
	if err := p.OnQuery(context.Background(), queryOn(local, "USE rrdb")); err != nil {
		t.Fatalf("OnQuery(USE rrdb): %v", err)
	}
	if strings.Join(*dials, ",") != "owner.internal:9001,owner.internal:9001" || local.State().Snapshot().ReplicaServedDatabase != "" {
		t.Fatalf("dials=%v, want today's USE pivot and no mark", *dials)
	}
	marked := newTestSession(t)
	marked.State().SetReplicaServedDatabase("rrdb")
	if err := p.OnQuery(context.Background(), queryOn(marked, "SELECT 1")); err != nil {
		t.Fatalf("without a table state nothing is refused: %v", err)
	}
}

// TestForward_PeerTrustedSessionsIgnoreReplicaServing: a peer-trusted hello
// keeps its early return, and a session forwarded from a peer keeps today's
// USE pivot and is never refused: the entry proxy made the routing decision.
func TestForward_PeerTrustedSessionsIgnoreReplicaServing(t *testing.T) {
	state := replicaServedState()
	p, dials, _ := replicaRouting(t, state)

	loopback := newTestSession(t)
	loopback.State().SetPeerTrust("10.0.0.7:9000")
	if err := p.OnHello(context.Background(), loopback, &chproto.ClientHello{Database: "rrdb"}); err != nil {
		t.Fatalf("OnHello on a peer-trusted session: %v", err)
	}
	if got := loopback.State().Snapshot(); len(*dials) != 0 || got.IsForwarding || got.ReplicaServedDatabase != "" {
		t.Fatalf("dials=%v forwarding=%v mark=%q, want the peer-trusted hello untouched", *dials, got.IsForwarding, got.ReplicaServedDatabase)
	}

	forwarded := newTestSession(t)
	forwarded.State().SetPeerTrustForwarded("10.0.0.7:9000", true)
	if err := p.OnQuery(context.Background(), queryOn(forwarded, "USE rrdb")); err != nil {
		t.Fatalf("OnQuery(USE rrdb) on a forwarded-from-peer session: %v", err)
	}
	if got := forwarded.State().Snapshot(); strings.Join(*dials, ",") != "owner.internal:9001" || got.ReplicaServedDatabase != "" {
		t.Fatalf("dials=%v mark=%q, want today's pivot to the owner and no mark", *dials, got.ReplicaServedDatabase)
	}
	forwarded.State().SetReplicaServedDatabase("rrdb")
	state.Set()
	if err := p.OnQuery(context.Background(), queryOn(forwarded, "SELECT 1")); err != nil {
		t.Fatalf("a peer-trusted session is never refused by the replica rule: %v", err)
	}
}
