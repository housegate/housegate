package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/sistatement"
)

// The replica-failover tests (spec 2026-10-14 §11, Plan S3-C4 K46) run the
// agent's relay between a scripted client (net.Pipe) and fake upstreams on
// loopback ports: an owner and replica hosts speaking revision 54453, below
// the addendum, in timezone UTC.

const failoverDB = "db2"

// quarantinedRead is the refusal a quarantined owner's host answers for a
// read of its table.
var quarantinedRead = chproto.TableRefusedMessage(failoverDB+".t", "indexer_quarantined", "storage-integrity indexer 2 is quarantined")

// fakeUpstream is a ClickHouse-like server on a loopback port; serve runs
// once per accepted connection.
type fakeUpstream struct {
	t    *testing.T
	ln   net.Listener
	addr string

	mu      sync.Mutex
	hellos  []chproto.ClientHello
	queries []chproto.Query
}

func startFakeUpstream(t *testing.T, serve func(*fakeConn)) *fakeUpstream {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeUpstream{t: t, ln: ln, addr: ln.Addr().String()}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				serve(&fakeConn{up: f, conn: conn, codec: chproto.NewCodec(conn, chproto.DirFromClient)})
			}()
		}
	}()
	return f
}

func (f *fakeUpstream) seen() ([]chproto.ClientHello, []chproto.Query) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]chproto.ClientHello(nil), f.hellos...), append([]chproto.Query(nil), f.queries...)
}

// closedAddr is a loopback address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

type fakeConn struct {
	up    *fakeUpstream
	conn  net.Conn
	codec *chproto.Codec
}

// hello reads the ClientHello and answers a ServerHello named name.
func (c *fakeConn) hello(name string) bool {
	pkt, err := c.codec.ReadPacket(uint64(chproto.ClientHelloCode))
	if err != nil {
		return false
	}
	h, ok := pkt.Decoded.(*chproto.ClientHello)
	if !ok {
		return false
	}
	c.up.mu.Lock()
	c.up.hellos = append(c.up.hellos, *h)
	c.up.mu.Unlock()
	var b proto.Buffer
	(&proto.ServerHello{Name: name, Major: 24, Minor: 1, Revision: deferredTestRev, Timezone: "UTC", DisplayName: name}).EncodeAware(&b, deferredTestRev)
	if _, err := c.conn.Write(b.Buf); err != nil {
		return false
	}
	c.codec.SetRevision(deferredTestRev)
	c.codec.SetCompression(proto.CompressionDisabled)
	return true
}

// query reads the next Query packet and records it.
func (c *fakeConn) query() (*chproto.Query, bool) {
	pkt, err := c.codec.ReadPacket(uint64(chproto.ClientQueryCode))
	if err != nil || pkt.Type != uint64(chproto.ClientQueryCode) {
		return nil, false
	}
	q := pkt.Decoded.(*chproto.Query)
	c.up.mu.Lock()
	c.up.queries = append(c.up.queries, *q)
	c.up.mu.Unlock()
	return q, true
}

// data reads one client Data packet.
func (c *fakeConn) data() bool {
	pkt, err := c.codec.ReadPacket()
	return err == nil && pkt.Type == uint64(chproto.ClientDataCode)
}

func (c *fakeConn) exception(code int32, message string) {
	_ = c.codec.WriteException(&chproto.Exception{Code: proto.Error(code), Name: "DB::Exception", Message: message})
}

func (c *fakeConn) progress() {
	var b proto.Buffer
	b.PutUVarInt(uint64(proto.ServerCodeProgress))
	proto.Progress{Rows: 1, Bytes: 8, TotalRows: 1}.EncodeAware(&b, deferredTestRev)
	_, _ = c.conn.Write(b.Buf)
}

func (c *fakeConn) endOfStream() {
	_, _ = c.conn.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
}

// drain reads until the peer closes the connection.
func (c *fakeConn) drain() { _, _ = io.Copy(io.Discard, c.conn) }

// replicaServes answers every query with a Progress packet and EndOfStream.
func replicaServes(c *fakeConn) {
	if !c.hello("replica") {
		return
	}
	for {
		if _, ok := c.query(); !ok || !c.data() {
			return
		}
		c.progress()
		c.endOfStream()
	}
}

// ownerQuarantined answers every read with the quarantine refusal after its
// terminator and every other query with EndOfStream.
func ownerQuarantined(c *fakeConn) {
	if !c.hello("owner") {
		return
	}
	for {
		q, ok := c.query()
		if !ok || !c.data() {
			return
		}
		if strings.HasPrefix(q.Body, "SELECT") {
			c.exception(chproto.CodeQueryIsProhibited, quarantinedRead)
			continue
		}
		c.endOfStream()
	}
}

// fakeFailover classifies failures exactly as agent.ReplicaFailover does.
type fakeFailover struct {
	candidates []string

	mu      sync.Mutex
	asked   []string
	reports []string
}

func (f *fakeFailover) Candidates(_ context.Context, database string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, database)
	return f.candidates, nil
}

func (f *fakeFailover) Reason(phase FailoverPhase, cause error, exc *chproto.Exception) (string, bool) {
	switch {
	case exc == nil && cause != nil, exc != nil && phase == FailoverAtHello:
		return "owner_unreachable", true
	case exc != nil && phase == FailoverAtRead && chproto.IsIndexerQuarantinedReadMessage(exc.Message):
		return "owner_quarantined", true
	}
	return "", false
}

func (f *fakeFailover) FailedOver(_ context.Context, database, reason, _, to string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reports = append(f.reports, database+" "+reason+" "+to)
}

func (f *fakeFailover) state() (asked, reports []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.asked...), append([]string(nil), f.reports...)
}

// failoverClient is the client end of a relay under test.
type failoverClient struct {
	t     *testing.T
	conn  net.Conn
	codec *chproto.Codec
	relay *Relay
	done  chan error
}

// startFailoverRelay runs a relay whose dialer reaches owner and whose
// failover is f (nil: none), and returns its client end before the hello.
func startFailoverRelay(t *testing.T, owner string, f UpstreamFailover, hooks plugin.Hooks, obs PacketObserver) *failoverClient {
	t.Helper()
	clientEnd, relayEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })
	if hooks == nil {
		hooks = plugin.NoopHooks{}
	}
	dial := func(ctx context.Context, _ chsession.Session) (*chproto.Codec, error) {
		conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", owner)
		if err != nil {
			return nil, err
		}
		return chproto.NewCodec(conn, chproto.DirToUpstream), nil
	}
	r := NewRelay(chsession.New(1, relayEnd), hooks, obs, dial)
	r.failover = f
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	c := &failoverClient{t: t, conn: clientEnd, codec: chproto.NewCodec(clientEnd, chproto.DirToUpstream), relay: r, done: make(chan error, 1)}
	go func() { c.done <- r.Run(ctx) }()
	return c
}

// hello sends the ClientHello for database and returns the ServerHello's
// display name, or the error that ended the session.
func (c *failoverClient) hello(database string) (string, error) {
	c.t.Helper()
	_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := c.codec.WriteClientHello(&chproto.ClientHello{Name: "client", Major: 24, Minor: 1, ProtocolVersion: deferredTestRev, Database: database, User: "default"}); err != nil {
		return "", err
	}
	c.codec.SetServerHelloRevisionHint(deferredTestRev)
	pkt, err := c.codec.ReadPacket(uint64(chproto.ServerHelloCode), uint64(chproto.ServerExceptionCode))
	if err != nil {
		return "", err
	}
	srv, ok := pkt.Decoded.(*chproto.ServerHello)
	if !ok {
		c.t.Fatalf("hello answered %T, want a ServerHello", pkt.Decoded)
	}
	c.codec.SetRevision(deferredTestRev)
	c.codec.SetCompression(proto.CompressionDisabled)
	return srv.DisplayName, nil
}

func (c *failoverClient) mustHello(want string) {
	c.t.Helper()
	got, err := c.hello(failoverDB)
	if err != nil || got != want {
		c.t.Fatalf("hello = %q, %v; want the ServerHello of %q", got, err, want)
	}
}

func (c *failoverClient) sendQuery(id, sql string, settings ...chproto.Setting) {
	c.t.Helper()
	q := &chproto.Query{
		ID:       id,
		Body:     sql,
		Settings: settings,
		Info: proto.ClientInfo{
			ProtocolVersion: deferredTestRev, Major: 24, Minor: 1,
			Interface: proto.InterfaceTCP, Query: proto.ClientQueryInitial,
		},
		Stage:       proto.StageComplete,
		Compression: proto.CompressionDisabled,
	}
	if err := c.codec.WriteQuery(q); err != nil {
		c.t.Fatalf("write query %s: %v", id, err)
	}
}

func (c *failoverClient) sendEmptyData() {
	c.t.Helper()
	if err := c.codec.WriteEmptyDataBlock(); err != nil {
		c.t.Fatalf("write the empty Data block: %v", err)
	}
}

// run sends a query and its terminator and returns the packet types of its
// answer up to the terminal packet, with the Exception if it ended with one.
func (c *failoverClient) run(id, sql string, settings ...chproto.Setting) ([]uint64, *chproto.Exception, error) {
	c.t.Helper()
	c.sendQuery(id, sql, settings...)
	c.sendEmptyData()
	return c.answer()
}

func (c *failoverClient) answer() ([]uint64, *chproto.Exception, error) {
	c.t.Helper()
	var types []uint64
	for {
		_ = c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		pkt, err := c.codec.ReadPacket(uint64(chproto.ServerExceptionCode))
		if err != nil {
			return types, nil, err
		}
		types = append(types, pkt.Type)
		switch pkt.Type {
		case uint64(chproto.ServerEndOfStreamCode):
			return types, nil, nil
		case uint64(chproto.ServerExceptionCode):
			return types, pkt.Decoded.(*chproto.Exception), nil
		}
	}
}

// mustServe runs sql and requires the replica's Progress and EndOfStream.
func (c *failoverClient) mustServe(id, sql string) {
	c.t.Helper()
	types, exc, err := c.run(id, sql)
	if err != nil || exc != nil || len(types) != 2 || types[0] != uint64(chproto.ServerProgressCode) {
		c.t.Fatalf("%s: answer %v, exception %v, err %v; want the replica's Progress and EndOfStream", id, types, exc, err)
	}
}

// mustRefuse runs sql and requires exactly the Exception message want.
func (c *failoverClient) mustRefuse(id, sql, want string) {
	c.t.Helper()
	types, exc, err := c.run(id, sql)
	if err != nil || exc == nil || exc.Message != want || len(types) != 1 {
		c.t.Fatalf("%s: answer %v, exception %v, err %v; want only the Exception %q", id, types, exc, err, want)
	}
}

func (c *failoverClient) upstreamAddress() string {
	if named, ok := c.relay.sess.Upstream().Conn().(interface{ UpstreamAddress() string }); ok {
		return named.UpstreamAddress()
	}
	return ""
}

func requireNoFailover(t *testing.T, f *fakeFailover) {
	t.Helper()
	if asked, reports := f.state(); len(asked) != 0 || len(reports) != 0 {
		t.Fatalf("candidates were asked for %v and reports %v; this failure must never fail over", asked, reports)
	}
}

// TestAgentFailsOverAReadWhenTheOwnerIsQuarantined: an eligible read that
// the owner refuses as quarantined is replayed on the first replica host
// that takes it, whether the owner refused before or after the client's
// terminator: the client sees only the replica's answer and the session stays
// on the replica.
func TestAgentFailsOverAReadWhenTheOwnerIsQuarantined(t *testing.T) {
	t.Run("after the terminator", func(t *testing.T) {
		owner := startFakeUpstream(t, ownerQuarantined)
		replica := startFakeUpstream(t, replicaServes)
		f := &fakeFailover{candidates: []string{closedAddr(t), replica.addr}}
		c := startFailoverRelay(t, owner.addr, f, nil, nil)
		c.mustHello("owner")
		c.mustServe("q1", "SELECT count() FROM t")
		hellos, queries := replica.seen()
		if len(hellos) != 1 || hellos[0].Database != failoverDB || hellos[0].ProtocolVersion != deferredTestRev {
			t.Fatalf("replica hellos = %+v, want one for %s at the client's revision", hellos, failoverDB)
		}
		if len(queries) != 1 || queries[0].ID != "q1" || queries[0].Body != "SELECT count() FROM t" {
			t.Fatalf("replica queries = %+v, want the replayed q1", queries)
		}
		if asked, reports := f.state(); len(asked) != 1 || asked[0] != failoverDB || len(reports) != 1 || reports[0] != failoverDB+" owner_quarantined "+replica.addr {
			t.Fatalf("asked %v, reports %v", asked, reports)
		}
		if got := c.upstreamAddress(); got != replica.addr {
			t.Fatalf("session upstream = %q, want the replica %q", got, replica.addr)
		}
		c.mustServe("q2", "SELECT 1")
		if _, queries := replica.seen(); len(queries) != 2 {
			t.Fatalf("replica queries = %d, want the next query on the replica too", len(queries))
		}
	})

	t.Run("before the terminator", func(t *testing.T) {
		gotQuery := make(chan struct{}, 1)
		owner := startFakeUpstream(t, func(c *fakeConn) {
			if !c.hello("owner") {
				return
			}
			if _, ok := c.query(); !ok {
				return
			}
			c.exception(chproto.CodeQueryIsProhibited, quarantinedRead)
			c.drain()
		})
		replica := startFakeUpstream(t, func(c *fakeConn) {
			if !c.hello("replica") {
				return
			}
			if _, ok := c.query(); !ok {
				return
			}
			gotQuery <- struct{}{}
			if !c.data() {
				return
			}
			c.progress()
			c.endOfStream()
		})
		f := &fakeFailover{candidates: []string{replica.addr}}
		c := startFailoverRelay(t, owner.addr, f, nil, nil)
		c.mustHello("owner")
		c.sendQuery("q1", "WITH 1 AS x SELECT x")
		select {
		case <-gotQuery:
		case <-time.After(10 * time.Second):
			t.Fatal("the read never reached the replica")
		}
		c.sendEmptyData()
		types, exc, err := c.answer()
		if err != nil || exc != nil || len(types) != 2 {
			t.Fatalf("answer %v, exception %v, err %v; want the replica's answer with the terminator sent after the failover", types, exc, err)
		}
	})
}

// TestAgentFailsOverAtHelloWhenTheOwnerIsUnreachable: when the dial to the
// owner fails, or its host answers the hello with an Exception or closes the
// connection, the hello goes to the first candidate that completes it and the
// client's session runs there.
func TestAgentFailsOverAtHelloWhenTheOwnerIsUnreachable(t *testing.T) {
	for name, owner := range map[string]func(t *testing.T) string{
		"dial refused": closedAddr,
		"exception at hello": func(t *testing.T) string {
			return startFakeUpstream(t, func(c *fakeConn) {
				if _, err := c.codec.ReadPacket(uint64(chproto.ClientHelloCode)); err == nil {
					c.codec.SetRevision(deferredTestRev)
					c.exception(chproto.CodeQueryIsProhibited, "forward: dial peer for db2: connection refused")
				}
			}).addr
		},
		"closed at hello": func(t *testing.T) string {
			return startFakeUpstream(t, func(c *fakeConn) {
				_, _ = c.codec.ReadPacket(uint64(chproto.ClientHelloCode))
			}).addr
		},
	} {
		t.Run(name, func(t *testing.T) {
			replica := startFakeUpstream(t, replicaServes)
			f := &fakeFailover{candidates: []string{closedAddr(t), replica.addr}}
			c := startFailoverRelay(t, owner(t), f, nil, nil)
			c.mustHello("replica")
			c.mustServe("q1", "SELECT 1")
			hellos, _ := replica.seen()
			if len(hellos) != 1 || hellos[0].Database != failoverDB {
				t.Fatalf("replica hellos = %+v, want one for %s", hellos, failoverDB)
			}
			if _, reports := f.state(); len(reports) != 1 || reports[0] != failoverDB+" owner_unreachable "+replica.addr {
				t.Fatalf("reports = %v", reports)
			}
			if !c.relay.sess.State().ReplicaFailoverEligible {
				t.Fatal("a session failed over at hello must stay eligible for reads")
			}
		})
	}
}

// TestAgentNeverFailsOverAWrite: an INSERT is never failed over, whatever its
// owner answers, and it ends the session's eligibility, so a later read is
// not failed over either.
func TestAgentNeverFailsOverAWrite(t *testing.T) {
	owner := startFakeUpstream(t, func(c *fakeConn) {
		if !c.hello("owner") {
			return
		}
		for {
			if _, ok := c.query(); !ok || !c.data() {
				return
			}
			c.exception(chproto.CodeQueryIsProhibited, quarantinedRead)
		}
	})
	replica := startFakeUpstream(t, replicaServes)
	f := &fakeFailover{candidates: []string{replica.addr}}
	c := startFailoverRelay(t, owner.addr, f, nil, nil)
	c.mustHello("owner")
	c.mustRefuse("w1", "INSERT INTO t VALUES (1)", quarantinedRead)
	if c.relay.sess.State().ReplicaFailoverEligible {
		t.Fatal("an INSERT must end the session's failover eligibility")
	}
	c.mustRefuse("q1", "SELECT count() FROM t", quarantinedRead)
	requireNoFailover(t, f)
}

// TestAgentNeverFailsOverInsertSelect: INSERT ... SELECT is a write.
func TestAgentNeverFailsOverInsertSelect(t *testing.T) {
	owner := startFakeUpstream(t, func(c *fakeConn) {
		if !c.hello("owner") {
			return
		}
		if _, ok := c.query(); ok && c.data() {
			c.exception(chproto.CodeQueryIsProhibited, quarantinedRead)
		}
		c.drain()
	})
	replica := startFakeUpstream(t, replicaServes)
	f := &fakeFailover{candidates: []string{replica.addr}}
	c := startFailoverRelay(t, owner.addr, f, nil, nil)
	c.mustHello("owner")
	c.mustRefuse("w1", "INSERT INTO t SELECT * FROM s", quarantinedRead)
	requireNoFailover(t, f)
}

// TestAgentNeverFailsOverAfterData: once a packet of a read's answer reached
// the client, neither a later Exception nor a broken connection fails over.
func TestAgentNeverFailsOverAfterData(t *testing.T) {
	t.Run("then the quarantine refusal", func(t *testing.T) {
		owner := startFakeUpstream(t, func(c *fakeConn) {
			if !c.hello("owner") {
				return
			}
			if _, ok := c.query(); ok && c.data() {
				c.progress()
				c.exception(chproto.CodeQueryIsProhibited, quarantinedRead)
			}
			c.drain()
		})
		f := &fakeFailover{candidates: []string{startFakeUpstream(t, replicaServes).addr}}
		c := startFailoverRelay(t, owner.addr, f, nil, nil)
		c.mustHello("owner")
		types, exc, err := c.run("q1", "SELECT count() FROM t")
		if err != nil || exc == nil || exc.Message != quarantinedRead || len(types) != 2 {
			t.Fatalf("answer %v, exception %v, err %v; want the owner's Progress and its refusal", types, exc, err)
		}
		requireNoFailover(t, f)
	})

	t.Run("then the connection breaks", func(t *testing.T) {
		owner := startFakeUpstream(t, func(c *fakeConn) {
			if !c.hello("owner") {
				return
			}
			if _, ok := c.query(); ok && c.data() {
				c.progress()
			}
		})
		f := &fakeFailover{candidates: []string{startFakeUpstream(t, replicaServes).addr}}
		c := startFailoverRelay(t, owner.addr, f, nil, nil)
		c.mustHello("owner")
		types, _, err := c.run("q1", "SELECT count() FROM t")
		if err == nil || len(types) != 1 || types[0] != uint64(chproto.ServerProgressCode) {
			t.Fatalf("answer %v, err %v; want the owner's Progress and then the session closed", types, err)
		}
		requireNoFailover(t, f)
	})
}

// TestAgentNeverFailsOverASessionAfterSet: a SET holds server-side state a
// replay cannot reproduce, so the session's reads no longer fail over.
func TestAgentNeverFailsOverASessionAfterSet(t *testing.T) {
	owner := startFakeUpstream(t, ownerQuarantined)
	replica := startFakeUpstream(t, replicaServes)
	f := &fakeFailover{candidates: []string{replica.addr}}
	c := startFailoverRelay(t, owner.addr, f, nil, nil)
	c.mustHello("owner")
	if types, exc, err := c.run("s1", "SET max_threads = 1"); err != nil || exc != nil || len(types) != 1 {
		t.Fatalf("SET: answer %v, exception %v, err %v", types, exc, err)
	}
	c.mustRefuse("q1", "SELECT count() FROM t", quarantinedRead)
	requireNoFailover(t, f)
}

// TestAgentKeepsTheInjectedReadModeOnFailover: the replayed Query is the one
// the agent's plugins built, so the injected SQL_x_read_mode reaches the
// replica unchanged and never switches silently to safe.
func TestAgentKeepsTheInjectedReadModeOnFailover(t *testing.T) {
	owner := startFakeUpstream(t, ownerQuarantined)
	replica := startFakeUpstream(t, replicaServes)
	f := &fakeFailover{candidates: []string{replica.addr}}
	hooks := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{&sistatement.ReadModeInjector{Mode: "unsafe_latest"}}}
	c := startFailoverRelay(t, owner.addr, f, hooks, nil)
	c.mustHello("owner")
	c.mustServe("q1", "SELECT count() FROM t")
	_, ownerQueries := owner.seen()
	_, replicaQueries := replica.seen()
	if len(ownerQueries) != 1 || len(replicaQueries) != 1 {
		t.Fatalf("owner queries %d, replica queries %d; want one each", len(ownerQueries), len(replicaQueries))
	}
	mode := func(q chproto.Query) string {
		for _, s := range q.Settings {
			if s.Key == "SQL_x_read_mode" {
				return s.Value
			}
		}
		return ""
	}
	if got, want := mode(replicaQueries[0]), mode(ownerQueries[0]); want != "'unsafe_latest'" || got != want {
		t.Fatalf("replica read mode %q, owner read mode %q; want the injected 'unsafe_latest' on both", got, want)
	}
}

// TestAgentSurfacesTheOriginalErrorWhenEveryCandidateFails: when no candidate
// serves, a read gets the owner's own refusal and keeps the owner, and a
// hello ends with the original dial error.
func TestAgentSurfacesTheOriginalErrorWhenEveryCandidateFails(t *testing.T) {
	refusesHello := startFakeUpstream(t, func(c *fakeConn) {
		if _, err := c.codec.ReadPacket(uint64(chproto.ClientHelloCode)); err == nil {
			c.codec.SetRevision(deferredTestRev)
			c.exception(chproto.CodeQueryIsProhibited, "forward: dial peer for db2: connection refused")
		}
	})

	t.Run("read", func(t *testing.T) {
		owner := startFakeUpstream(t, ownerQuarantined)
		f := &fakeFailover{candidates: []string{closedAddr(t), refusesHello.addr}}
		c := startFailoverRelay(t, owner.addr, f, nil, nil)
		c.mustHello("owner")
		c.mustRefuse("q1", "SELECT count() FROM t", quarantinedRead)
		if types, exc, err := c.run("u1", "USE other"); err != nil || exc != nil || len(types) != 1 {
			t.Fatalf("the session must keep the owner after the failover found no replica: answer %v, exception %v, err %v", types, exc, err)
		}
		if asked, reports := f.state(); len(asked) != 1 || len(reports) != 0 {
			t.Fatalf("asked %v, reports %v; want one lookup and no failover", asked, reports)
		}
	})

	t.Run("hello", func(t *testing.T) {
		owner := closedAddr(t)
		f := &fakeFailover{candidates: []string{closedAddr(t), refusesHello.addr}}
		c := startFailoverRelay(t, owner, f, nil, nil)
		if _, err := c.hello(failoverDB); err == nil {
			t.Fatal("the hello must fail when no candidate completes it")
		}
		select {
		case err := <-c.done:
			if err == nil || !strings.HasPrefix(err.Error(), "dial upstream: dial tcp "+owner+": ") {
				t.Fatalf("Run = %v, want the original dial error of %s", err, owner)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not end")
		}
	})
}

func replicaFailoverMetric(t *testing.T, name, reason string) (float64, bool) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "reason" && label.GetValue() == reason {
					return m.GetCounter().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// TestAgentReplicaFailoverMetricsCountByReasonIncludingZeros: both counters
// export both reasons from startup, a served failover counts in
// clickhouse_proxy_agent_replica_failovers_total and a failover no candidate
// served in clickhouse_proxy_agent_replica_failover_exhausted_total.
func TestAgentReplicaFailoverMetricsCountByReasonIncludingZeros(t *testing.T) {
	for _, name := range []string{"clickhouse_proxy_agent_replica_failovers_total", "clickhouse_proxy_agent_replica_failover_exhausted_total"} {
		for _, reason := range []string{"owner_quarantined", "owner_unreachable"} {
			if _, ok := replicaFailoverMetric(t, name, reason); !ok {
				t.Fatalf("%s{reason=%q} is not exported", name, reason)
			}
		}
	}
	served := testutil.ToFloat64(agentReplicaFailoversTotal.WithLabelValues("owner_unreachable"))
	exhausted := testutil.ToFloat64(agentReplicaFailoverExhaustedTotal.WithLabelValues("owner_quarantined"))

	replica := startFakeUpstream(t, replicaServes)
	c := startFailoverRelay(t, closedAddr(t), &fakeFailover{candidates: []string{replica.addr}}, nil, NewMetricsObserver())
	c.mustHello("replica")
	if got := testutil.ToFloat64(agentReplicaFailoversTotal.WithLabelValues("owner_unreachable")); got != served+1 {
		t.Fatalf("failovers{owner_unreachable} = %g, want %g", got, served+1)
	}

	owner := startFakeUpstream(t, ownerQuarantined)
	c = startFailoverRelay(t, owner.addr, &fakeFailover{candidates: []string{closedAddr(t)}}, nil, NewMetricsObserver())
	c.mustHello("owner")
	c.mustRefuse("q1", "SELECT count() FROM t", quarantinedRead)
	if got := testutil.ToFloat64(agentReplicaFailoverExhaustedTotal.WithLabelValues("owner_quarantined")); got != exhausted+1 {
		t.Fatalf("failover_exhausted{owner_quarantined} = %g, want %g", got, exhausted+1)
	}
}

// TestRelayWithoutUpstreamFailoverIsUnchanged: without an UpstreamFailover
// the quarantine refusal reaches the client and the session keeps its owner,
// a failed dial ends the handshake with today's error, and no session is
// marked eligible.
func TestRelayWithoutUpstreamFailoverIsUnchanged(t *testing.T) {
	owner := startFakeUpstream(t, ownerQuarantined)
	c := startFailoverRelay(t, owner.addr, nil, nil, nil)
	c.mustHello("owner")
	if c.relay.sess.State().ReplicaFailoverEligible {
		t.Fatal("a relay without an UpstreamFailover must not mark the session eligible")
	}
	c.mustRefuse("q1", "SELECT count() FROM t", quarantinedRead)
	if types, exc, err := c.run("u1", "USE other"); err != nil || exc != nil || len(types) != 1 {
		t.Fatalf("the owner must keep serving the session: answer %v, exception %v, err %v", types, exc, err)
	}
	if c.relay.sess.State().LogicalDatabaseName() != "" {
		t.Fatal("a relay without an UpstreamFailover must not record a USE")
	}

	down := closedAddr(t)
	c = startFailoverRelay(t, down, nil, nil, nil)
	if _, err := c.hello(failoverDB); err == nil {
		t.Fatal("the hello must fail when the owner cannot be dialed")
	}
	select {
	case err := <-c.done:
		var opErr *net.OpError
		if err == nil || !strings.HasPrefix(err.Error(), "dial upstream: dial tcp "+down+": ") || !errors.As(err, &opErr) {
			t.Fatalf("Run = %v, want the dial error of %s wrapped as before", err, down)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not end")
	}
}

// TestAgentNeverFailsOverAClosedSession: when the client hangs up while its
// read is in flight, the relay closes the session and the upstream read
// fails; that failure is not the owner's and is never failed over.
func TestAgentNeverFailsOverAClosedSession(t *testing.T) {
	owner := startFakeUpstream(t, func(c *fakeConn) {
		if !c.hello("owner") {
			return
		}
		if _, ok := c.query(); ok && c.data() {
			c.drain()
		}
	})
	f := &fakeFailover{candidates: []string{startFakeUpstream(t, replicaServes).addr}}
	c := startFailoverRelay(t, owner.addr, f, nil, nil)
	c.mustHello("owner")
	c.sendQuery("q1", "SELECT count() FROM t")
	c.sendEmptyData()
	_ = c.conn.Close()
	select {
	case <-c.done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not end after the client hung up")
	}
	requireNoFailover(t, f)
}

// TestAgentTriesTheNextCandidateWhenOneCannotServeTheRead: replica_hosts is
// every enrolled indexer but the owner, so a candidate can take the hello
// and the read and still not serve it: its member does not serve the
// database, so it relays the quarantined owner's refusal, or its connection
// breaks before it answers. The read then moves on to the next candidate,
// and the failover is reported once, for the candidate that served. When
// none serves, the client gets the owner's original refusal and the failover
// counts as exhausted.
func TestAgentTriesTheNextCandidateWhenOneCannotServeTheRead(t *testing.T) {
	breaksAfterTheRead := func(c *fakeConn) {
		if c.hello("member") {
			if _, ok := c.query(); ok {
				_ = c.data()
			}
		}
	}

	t.Run("the next candidate serves", func(t *testing.T) {
		owner := startFakeUpstream(t, ownerQuarantined)
		refuses := startFakeUpstream(t, ownerQuarantined)
		breaks := startFakeUpstream(t, breaksAfterTheRead)
		replica := startFakeUpstream(t, replicaServes)
		f := &fakeFailover{candidates: []string{refuses.addr, breaks.addr, replica.addr}}
		served := testutil.ToFloat64(agentReplicaFailoversTotal.WithLabelValues("owner_quarantined"))
		c := startFailoverRelay(t, owner.addr, f, nil, NewMetricsObserver())
		c.mustHello("owner")
		c.mustServe("q1", "SELECT count() FROM t")
		for name, up := range map[string]*fakeUpstream{"refusing": refuses, "breaking": breaks, "serving": replica} {
			if _, queries := up.seen(); len(queries) != 1 || queries[0].ID != "q1" {
				t.Fatalf("%s candidate queries = %+v, want the replayed q1 once", name, queries)
			}
		}
		if asked, reports := f.state(); len(asked) != 1 || len(reports) != 1 || reports[0] != failoverDB+" owner_quarantined "+replica.addr {
			t.Fatalf("asked %v, reports %v; want one lookup and one report, for the candidate that served", asked, reports)
		}
		if got := testutil.ToFloat64(agentReplicaFailoversTotal.WithLabelValues("owner_quarantined")); got != served+1 {
			t.Fatalf("failovers{owner_quarantined} = %g, want %g", got, served+1)
		}
		if got := c.upstreamAddress(); got != replica.addr {
			t.Fatalf("session upstream = %q, want the replica %q", got, replica.addr)
		}
		c.mustServe("q2", "SELECT 1")
	})

	t.Run("no candidate serves", func(t *testing.T) {
		owner := startFakeUpstream(t, ownerQuarantined)
		first := startFakeUpstream(t, ownerQuarantined)
		second := startFakeUpstream(t, ownerQuarantined)
		f := &fakeFailover{candidates: []string{first.addr, second.addr}}
		served := testutil.ToFloat64(agentReplicaFailoversTotal.WithLabelValues("owner_quarantined"))
		exhausted := testutil.ToFloat64(agentReplicaFailoverExhaustedTotal.WithLabelValues("owner_quarantined"))
		c := startFailoverRelay(t, owner.addr, f, nil, NewMetricsObserver())
		c.mustHello("owner")
		c.mustRefuse("q1", "SELECT count() FROM t", quarantinedRead)
		for name, up := range map[string]*fakeUpstream{"first": first, "second": second} {
			if _, queries := up.seen(); len(queries) != 1 {
				t.Fatalf("%s candidate queries = %d, want the read tried once on each", name, len(queries))
			}
		}
		if asked, reports := f.state(); len(asked) != 1 || len(reports) != 0 {
			t.Fatalf("asked %v, reports %v; want one lookup and no report", asked, reports)
		}
		if got := testutil.ToFloat64(agentReplicaFailoverExhaustedTotal.WithLabelValues("owner_quarantined")); got != exhausted+1 {
			t.Fatalf("failover_exhausted{owner_quarantined} = %g, want %g", got, exhausted+1)
		}
		if got := testutil.ToFloat64(agentReplicaFailoversTotal.WithLabelValues("owner_quarantined")); got != served {
			t.Fatalf("failovers{owner_quarantined} = %g, want it unchanged at %g", got, served)
		}
		// The session stays on the last candidate that answered, which
		// routes it as the owner would.
		if types, exc, err := c.run("u1", "USE other"); err != nil || exc != nil || len(types) != 1 {
			t.Fatalf("after the failover found no replica: answer %v, exception %v, err %v", types, exc, err)
		}
	})
}

// TestClassifyFailoverStatementReadsTheMainStatementAfterWith: a WITH-led
// statement is a failover read only when the statement after its WITH list
// is a SELECT. ClickHouse also takes a WITH list before INSERT, so a WITH-led
// write, and anything the classifier cannot read, is not a read.
func TestClassifyFailoverStatementReadsTheMainStatementAfterWith(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want failoverStatement
	}{
		{"SELECT 1", failoverRead},
		{"WITH 1 AS x SELECT x", failoverRead},
		{"with x as (select 1) select * from x", failoverRead},
		{"WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r", failoverRead},
		{"WITH `a b` AS (SELECT 1), 'x' AS s, CAST(1 AS UInt8) AS c, (SELECT 2) AS d SELECT * FROM `a b`", failoverRead},
		{"WITH 'a\\'b' AS s SELECT s", failoverRead},
		{"/* c */ WITH x AS (SELECT 1) -- c\n SELECT * FROM x", failoverRead},
		{"WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", failoverOther},
		{"WITH 1 AS x INSERT INTO t SELECT x", failoverOther},
		{"WITH x AS (SELECT 1), y AS (SELECT 2) INSERT INTO t SELECT * FROM y", failoverOther},
		{"WITH x AS (SELECT 1) ALTER TABLE t DELETE WHERE 1", failoverOther},
		{"WITH x AS (SELECT 1) CREATE TABLE t AS SELECT * FROM x", failoverOther},
		{"WITH x AS (SELECT 1)", failoverOther},
		{"WITH x AS (SELECT 1", failoverOther},
		{"WITH", failoverOther},
		{"WITH x AS (SELECT 1) (SELECT 2)", failoverOther},
		{"INSERT INTO t WITH x AS (SELECT 1) SELECT * FROM x", failoverOther},
		{"USE db2", failoverUse},
	} {
		if got, _ := classifyFailoverStatement(tc.sql); got != tc.want {
			t.Errorf("classifyFailoverStatement(%q) = %d, want %d", tc.sql, got, tc.want)
		}
	}
}

// TestAgentNeverFailsOverAWithLedWrite: WITH ... INSERT is a write. It is not
// failed over, and it ends the session's eligibility, so a later read on the
// session is not failed over either.
func TestAgentNeverFailsOverAWithLedWrite(t *testing.T) {
	owner := startFakeUpstream(t, func(c *fakeConn) {
		if !c.hello("owner") {
			return
		}
		for {
			if _, ok := c.query(); !ok || !c.data() {
				return
			}
			c.exception(chproto.CodeQueryIsProhibited, quarantinedRead)
		}
	})
	replica := startFakeUpstream(t, replicaServes)
	f := &fakeFailover{candidates: []string{replica.addr}}
	c := startFailoverRelay(t, owner.addr, f, nil, nil)
	c.mustHello("owner")
	c.mustRefuse("w1", "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", quarantinedRead)
	if c.relay.sess.State().ReplicaFailoverEligible {
		t.Fatal("a WITH-led INSERT must end the session's failover eligibility")
	}
	c.mustRefuse("q1", "SELECT count() FROM t", quarantinedRead)
	requireNoFailover(t, f)
}
