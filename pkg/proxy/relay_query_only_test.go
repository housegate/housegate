package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

type queryOnlyHooks struct {
	plugin.NoopHooks
	host       *plugin.SnapshotQueryHostPlugin
	aborts     atomic.Int32
	completes  atomic.Int32
	clientData atomic.Int32
}

type queryOnlySource struct {
	admit func(context.Context, *chproto.Query) (plugin.SnapshotQueryHostAdmission, error)
}

func (s queryOnlySource) IsSnapshotQuery(*chproto.Query) (bool, error) { return true, nil }
func (s queryOnlySource) AdmitSnapshotQueryAtHost(ctx context.Context, q *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
	return s.admit(ctx, q)
}

func newQueryOnlyHost(t *testing.T, admit func(context.Context, *chproto.Query) (plugin.SnapshotQueryHostAdmission, error)) *plugin.SnapshotQueryHostPlugin {
	t.Helper()
	host, err := plugin.NewSnapshotQueryHostPlugin(queryOnlySource{admit: admit})
	if err != nil {
		t.Fatalf("NewSnapshotQueryHostPlugin: %v", err)
	}
	return host
}

func writeFragmented(t *testing.T, c net.Conn, raw []byte) {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("cannot fragment an empty packet")
	}
	_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write(raw[:1]); err != nil {
		t.Fatalf("write packet fragment: %v", err)
	}
	if len(raw) > 1 {
		_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if _, err := c.Write(raw[1:]); err != nil {
			t.Fatalf("write packet remainder: %v", err)
		}
	}
}

type firstQueryOnlyHooks struct {
	queryOnlyHooks
	calls atomic.Int32
}

func (h *firstQueryOnlyHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if h.calls.Add(1) == 1 {
		return h.host.OnQuery(context.Background(), qctx)
	}
	return nil
}

func (h *queryOnlyHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	return h.host.OnQuery(context.Background(), qctx)
}

func (h *queryOnlyHooks) OnQueryAbort(context.Context, *plugin.QueryContext) {
	h.aborts.Add(1)
}

func (h *queryOnlyHooks) OnQueryComplete(context.Context, chsession.Session) {
	h.completes.Add(1)
}

// A drained late marker belongs to a locally completed operation, so neither
// ClientData chain may observe it.
func (h *queryOnlyHooks) OnClientDataStrict(context.Context, *plugin.QueryContext, []byte) error {
	h.clientData.Add(1)
	return nil
}

func (h *queryOnlyHooks) OnClientData(context.Context, *plugin.QueryContext, []byte) error {
	h.clientData.Add(1)
	return nil
}

func TestRelayQueryOnly_SuccessNeverWritesUpstream(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	upstreamProxy, upstreamPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	defer upstreamProxy.Close()
	defer upstreamPeer.Close()

	sess := chsession.New(1, clientProxy)
	sess.Client().SetRevision(deferredTestRev)
	upstream := chproto.NewCodec(upstreamPeer, chproto.DirToUpstream)
	upstream.SetRevision(deferredTestRev)
	if err := sess.BindUpstream(context.Background(), upstream); err != nil {
		t.Fatalf("bind upstream: %v", err)
	}
	ran := make(chan struct{})
	h := &queryOnlyHooks{host: newQueryOnlyHost(t, func(context.Context, *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		return plugin.SnapshotQueryHostAdmission{Run: func(context.Context) error { close(ran); return nil }, CancelClient: func() {}, MaxControlBytes: 1024}, nil
	})}
	r := NewRelay(sess, h, nil, nil)
	done := make(chan error, 1)
	go func() { done <- r.clientToUpstream(context.Background()) }()
	if _, err := clientPeer.Write(encodeInsertQuery(t, "query-only-success", "INSERT INTO target SELECT 1")); err != nil {
		t.Fatalf("write Query: %v", err)
	}
	select {
	case <-ran:
	case err := <-done:
		t.Fatalf("client loop before query-only Run: %v", err)
	case <-time.After(time.Second):
		t.Fatal("query-only Run did not execute")
	}
	buf := make([]byte, 1)
	if _, err := io.ReadFull(clientPeer, buf); err != nil {
		t.Fatalf("read local EOS: %v", err)
	}
	if buf[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("local terminal=%d, want EndOfStream", buf[0])
	}
	if !r.queryOnlyLateMarkerAllowed {
		t.Fatal("successful no-Data query-only execution did not allow a late marker")
	}
	_ = upstreamProxy.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	if _, err := upstreamProxy.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("query-only wrote to upstream: %v", err)
	}
	if h.aborts.Load() != 0 || h.completes.Load() != 1 {
		t.Fatalf("hooks abort=%d complete=%d, want 0/1", h.aborts.Load(), h.completes.Load())
	}
	_ = clientPeer.Close()
	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("client loop=%v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client loop did not finish")
	}
}

func TestRelayQueryOnly_CancelOwnsReaderAndCancelsRun(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	sess := chsession.New(2, clientProxy)
	r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
	started := make(chan struct{})
	stopped := make(chan struct{})
	var canceled atomic.Int32
	host := newQueryOnlyHost(t, func(_ context.Context, _ *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		return plugin.SnapshotQueryHostAdmission{Run: func(ctx context.Context) error { close(started); <-ctx.Done(); close(stopped); return ctx.Err() }, CancelClient: func() { canceled.Add(1) }, MaxControlBytes: 1024}, nil
	})
	qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{ID: "cancel"}, Values: map[string]any{}}
	if err := host.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("host OnQuery: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- r.runQueryOnly(context.Background(), qctx) }()
	<-started
	var raw proto.Buffer
	raw.PutUVarInt(uint64(chproto.ClientCancelCode))
	if _, err := clientPeer.Write(raw.Buf); err != nil {
		t.Fatalf("write Cancel: %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Run did not receive cancellation")
	}
	buf := make([]byte, 1)
	if _, err := io.ReadFull(clientPeer, buf); err != nil {
		t.Fatalf("read cancellation EOS: %v", err)
	}
	if buf[0] != byte(chproto.ServerEndOfStreamCode) || canceled.Load() != 1 {
		t.Fatalf("terminal=%d CancelClient=%d, want EOS/1", buf[0], canceled.Load())
	}
	if err := <-done; err != nil {
		t.Fatalf("run query-only after cancel: %v", err)
	}
}

func TestRelayQueryOnly_LateMarkerIsDrainedAndNextQueryIsServed(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	upstreamProxy, upstreamPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	defer upstreamProxy.Close()
	defer upstreamPeer.Close()
	sess := chsession.New(4, clientProxy)
	sess.Client().SetRevision(deferredTestRev)
	upstream := chproto.NewCodec(upstreamPeer, chproto.DirToUpstream)
	upstream.SetRevision(deferredTestRev)
	if err := sess.BindUpstream(context.Background(), upstream); err != nil {
		t.Fatalf("bind upstream: %v", err)
	}
	// Only the first query is query-only, so the next one takes the ordinary
	// forwarding path and the cleared allowance is observable on the wire.
	h := &firstQueryOnlyHooks{queryOnlyHooks: queryOnlyHooks{host: newQueryOnlyHost(t, func(context.Context, *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		return plugin.SnapshotQueryHostAdmission{Run: func(context.Context) error { return nil }, CancelClient: func() {}, MaxControlBytes: 1024}, nil
	})}}
	r := NewRelay(sess, h, nil, nil)
	done := make(chan error, 1)
	go func() { done <- r.clientToUpstream(context.Background()) }()
	writeAllConn(t, clientPeer, encodeInsertQuery(t, "local", "INSERT INTO target SELECT 1"))
	if got := readExact(t, clientPeer, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("local terminal=%d, want EOS", got[0])
	}
	// Plan D2: one late empty marker is drained; it must never reach upstream.
	writeAllConn(t, clientPeer, encodeEmptyClientData(t))
	_ = upstreamProxy.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	if _, err := upstreamProxy.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("late marker reached upstream: %v", err)
	}
	// The next ordinary Query clears the allowance and is forwarded upstream.
	writeAllConn(t, clientPeer, encodeInsertQuery(t, "next", "SELECT 2"))
	_ = upstreamProxy.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := upstreamProxy.Read(make([]byte, 1)); err != nil {
		t.Fatalf("next Query did not reach upstream: %v", err)
	}
	if r.queryOnlyLateMarkerAllowed {
		t.Fatal("next Query did not clear the late-marker allowance")
	}
	if h.clientData.Load() != 0 {
		t.Fatalf("drained late marker fired %d ClientData hooks, want 0", h.clientData.Load())
	}
	clientPeer.Close()
	// The forwarded Query is still half-consumed on the unbuffered upstream
	// pipe; close it so the relay's blocked write fails and the loop returns.
	upstreamProxy.Close()
	<-done
}

func TestRelayQueryOnly_RejectsUnprovenancedPlan(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	sess := chsession.New(3, clientProxy)
	r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
	qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{ID: "forged"}, Values: map[string]any{}}
	qctx.QueryOnly = &plugin.QueryOnlyPlan{}
	if err := r.runQueryOnly(context.Background(), qctx); err == nil {
		t.Fatal("unprovenanced query-only plan was accepted")
	}
}

func TestRelayQueryOnly_CancelDuringHostAdmissionClearsActiveQuery(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	sess := chsession.New(5, clientProxy)
	r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
	entered := make(chan struct{})
	host := newQueryOnlyHost(t, func(ctx context.Context, _ *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		close(entered)
		<-ctx.Done()
		return plugin.SnapshotQueryHostAdmission{}, ctx.Err()
	})
	qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{ID: "admission-cancel"}, Values: map[string]any{}}
	if err := host.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("host OnQuery: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- r.runQueryOnly(context.Background(), qctx) }()
	<-entered
	var raw proto.Buffer
	raw.PutUVarInt(uint64(chproto.ClientCancelCode))
	if _, err := clientPeer.Write(raw.Buf); err != nil {
		t.Fatalf("write Cancel: %v", err)
	}
	if got := readExact(t, clientPeer, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("terminal=%d, want EOS", got[0])
	}
	if err := <-done; err != nil {
		t.Fatalf("run after admission cancellation: %v", err)
	}
	if _, active := r.currentActiveQuery(); active {
		t.Fatal("cancelled host admission left active query")
	}
	if !r.beginActiveQuery("next") {
		t.Fatal("next query could not claim cleared active state")
	}
	r.takeActiveQuery()
}

func TestRelayQueryOnly_RejectsNamedEmptyMarker(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	sess := chsession.New(6, clientProxy)
	r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
	started := make(chan struct{})
	host := newQueryOnlyHost(t, func(_ context.Context, _ *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		return plugin.SnapshotQueryHostAdmission{Run: func(ctx context.Context) error { close(started); <-ctx.Done(); return ctx.Err() }, CancelClient: func() {}, MaxControlBytes: 1024}, nil
	})
	qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{ID: "named-marker"}, Values: map[string]any{}}
	if err := host.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("host OnQuery: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- r.runQueryOnly(context.Background(), qctx) }()
	<-started
	raw := encodeEmptyClientData(t)
	name := []byte("tmp_external")
	raw[1] = byte(len(name)) // ClientData code is one byte in the supported protocol.
	raw = append(append(raw[:2:2], name...), raw[2:]...)
	if _, err := clientPeer.Write(raw); err != nil {
		t.Fatalf("write named marker: %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("named external-table marker was accepted")
	}
	if _, active := r.currentActiveQuery(); active {
		t.Fatal("named marker left active query")
	}
	// The refused packet was fully framed and this operation had not consumed
	// its marker yet, so plan D2's allowance still applies.
	if !r.queryOnlyLateMarkerAllowed {
		t.Fatal("first marker violation at a packet boundary denied the late-marker allowance")
	}
}

func TestRelayQueryOnly_SecondMarkerViolationDeniesLateMarkerAllowance(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	sess := chsession.New(19, clientProxy)
	sess.Client().SetRevision(deferredTestRev)
	r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
	started := make(chan struct{})
	host := newQueryOnlyHost(t, func(_ context.Context, _ *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		return plugin.SnapshotQueryHostAdmission{Run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}, CancelClient: func() {}, MaxControlBytes: 1024}, nil
	})
	qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{ID: "duplicate-marker"}, Values: map[string]any{}}
	if err := host.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("host OnQuery: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- r.runQueryOnly(context.Background(), qctx) }()
	<-started
	// The first marker is drained in flight; the second one is the violation.
	writeAllConn(t, clientPeer, append(encodeEmptyClientData(t), encodeEmptyClientData(t)...))
	if err := <-done; err == nil {
		t.Fatal("duplicate empty marker was accepted")
	}
	// This operation already consumed its one marker, so no further marker may
	// be drained after it ends.
	if r.queryOnlyLateMarkerAllowed {
		t.Fatal("duplicate marker violation granted a second late-marker allowance")
	}
}

func TestRelayQueryOnly_RunBlockingDrainsEmptyMarker(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	sess := chsession.New(8, clientProxy)
	sess.Client().SetRevision(deferredTestRev)
	r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
	started := make(chan struct{})
	host := newQueryOnlyHost(t, func(_ context.Context, _ *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		return plugin.SnapshotQueryHostAdmission{Run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}, CancelClient: func() {}, MaxControlBytes: 1024}, nil
	})
	qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{ID: "drain-marker"}, Values: map[string]any{}}
	if err := host.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("host OnQuery: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- r.runQueryOnly(context.Background(), qctx) }()
	<-started
	if _, err := clientPeer.Write(encodeEmptyClientData(t)); err != nil {
		t.Fatalf("write empty marker: %v", err)
	}
	_ = clientPeer.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
	if _, err := clientPeer.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("empty marker completed or failed query-only Run: %v", err)
	}
	var cancel proto.Buffer
	cancel.PutUVarInt(uint64(chproto.ClientCancelCode))
	if _, err := clientPeer.Write(cancel.Buf); err != nil {
		t.Fatalf("write Cancel: %v", err)
	}
	if got := readExact(t, clientPeer, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("terminal=%d, want EOS", got[0])
	}
	if err := <-done; err != nil {
		t.Fatalf("run query-only after marker+cancel: %v", err)
	}
}

func TestRelayQueryOnly_DelayedPacketAfterSuccessClosesSession(t *testing.T) {
	namedMarker := func(t *testing.T) []byte {
		raw := encodeEmptyClientData(t)
		name := []byte("tmp_external")
		raw[1] = byte(len(name))
		return append(append(raw[:2:2], name...), raw[2:]...)
	}
	nonemptyMarker := func(t *testing.T) []byte {
		raw := encodeEmptyClientData(t)
		raw[len(raw)-1] = 1
		return raw
	}
	var cancel proto.Buffer
	cancel.PutUVarInt(uint64(chproto.ClientCancelCode))
	for _, tc := range []struct {
		name string
		raw  func(*testing.T) []byte
	}{
		{name: "named_marker", raw: namedMarker},
		{name: "nonempty_marker", raw: nonemptyMarker},
		// The plan-D2 allowance covers exactly one late empty marker; the
		// second one has no query to belong to.
		{name: "second_empty_marker", raw: func(t *testing.T) []byte {
			return append(encodeEmptyClientData(t), encodeEmptyClientData(t)...)
		}},
		{name: "cancel", raw: func(*testing.T) []byte { return append([]byte(nil), cancel.Buf...) }},
		{name: "unknown", raw: func(*testing.T) []byte { return []byte{99} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientProxy, clientPeer := net.Pipe()
			upstreamProxy, upstreamPeer := net.Pipe()
			defer clientProxy.Close()
			defer clientPeer.Close()
			defer upstreamProxy.Close()
			defer upstreamPeer.Close()
			sess := chsession.New(14, clientProxy)
			sess.Client().SetRevision(deferredTestRev)
			upstream := chproto.NewCodec(upstreamPeer, chproto.DirToUpstream)
			upstream.SetRevision(deferredTestRev)
			if err := sess.BindUpstream(context.Background(), upstream); err != nil {
				t.Fatalf("bind upstream: %v", err)
			}
			h := &queryOnlyHooks{host: newQueryOnlyHost(t, func(context.Context, *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
				return plugin.SnapshotQueryHostAdmission{Run: func(context.Context) error { return nil }, CancelClient: func() {}, MaxControlBytes: 1024}, nil
			})}
			r := NewRelay(sess, h, nil, nil)
			done := make(chan error, 1)
			go func() { done <- r.clientToUpstream(context.Background()) }()
			writeAllConn(t, clientPeer, encodeInsertQuery(t, "local-success", "INSERT INTO target SELECT 1"))
			if got := readExact(t, clientPeer, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
				t.Fatalf("terminal=%d, want EOS", got[0])
			}
			writeFragmented(t, clientPeer, tc.raw(t))
			if err := <-done; err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("client loop=%v, want terminal fail-closed error", err)
			}
			_ = upstreamProxy.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
			if _, err := upstreamProxy.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("delayed %s reached upstream: %v", tc.name, err)
			}
		})
	}
}

func TestRelayQueryOnly_ControlReadFailureClearsActiveQuery(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{name: "oversized", raw: encodeInsertQuery(t, "oversized-control", strings.Repeat("x", 2048))},
		{name: "malformed", raw: []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientProxy, clientPeer := net.Pipe()
			upstreamProxy, upstreamPeer := net.Pipe()
			defer clientProxy.Close()
			defer clientPeer.Close()
			defer upstreamProxy.Close()
			defer upstreamPeer.Close()
			sess := chsession.New(11, clientProxy)
			sess.Client().SetRevision(deferredTestRev)
			upstream := chproto.NewCodec(upstreamPeer, chproto.DirToUpstream)
			upstream.SetRevision(deferredTestRev)
			if err := sess.BindUpstream(context.Background(), upstream); err != nil {
				t.Fatalf("bind upstream: %v", err)
			}
			started := make(chan struct{})
			h := &firstQueryOnlyHooks{queryOnlyHooks: queryOnlyHooks{host: newQueryOnlyHost(t, func(_ context.Context, _ *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
				return plugin.SnapshotQueryHostAdmission{Run: func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					return ctx.Err()
				}, CancelClient: func() {}, MaxControlBytes: 1024}, nil
			})}}
			r := NewRelay(sess, h, nil, nil)
			done := make(chan error, 1)
			go func() { done <- r.clientToUpstream(context.Background()) }()
			writeAllConn(t, clientPeer, encodeInsertQuery(t, "control-error", "INSERT INTO target SELECT 1"))
			<-started
			// The bad control packet is pipelined ahead of an ordinary Query. A
			// control read that stops mid-body leaves the client codec off a
			// packet boundary, so that pipelined Query must never be served.
			pipelined := append(append([]byte(nil), tc.raw...), encodeInsertQuery(t, "after-control-error", "SELECT 2")...)
			writeAllConn(t, clientPeer, pipelined)
			if exc := readServerException(t, clientPeer); exc.Message == "" {
				t.Fatalf("%s control failure returned an empty Exception", tc.name)
			}
			if r.queryOnlyLateMarkerAllowed {
				t.Fatalf("%s control failure granted the late-marker allowance", tc.name)
			}
			select {
			case err := <-done:
				if err == nil || errors.Is(err, io.EOF) {
					t.Fatalf("client loop=%v, want terminal control error", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%s control failure left the session reusable", tc.name)
			}
			_ = upstreamProxy.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
			if _, err := upstreamProxy.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("%s control failure served a later query upstream: %v", tc.name, err)
			}
			if _, active := r.currentActiveQuery(); active {
				t.Fatalf("%s control error left active query", tc.name)
			}
			if !r.beginActiveQuery("next") {
				t.Fatalf("%s control error blocked next query", tc.name)
			}
			r.takeActiveQuery()
		})
	}
}

func TestRelayQueryOnly_RunErrorClearsActiveQuery(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	sess := chsession.New(12, clientProxy)
	r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
	host := newQueryOnlyHost(t, func(context.Context, *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		return plugin.SnapshotQueryHostAdmission{Run: func(context.Context) error { return errors.New("durable host failure") }, CancelClient: func() {}, MaxControlBytes: 1024}, nil
	})
	qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{ID: "run-error"}, Values: map[string]any{}}
	if err := host.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("host OnQuery: %v", err)
	}
	if err := r.runQueryOnly(context.Background(), qctx); err == nil {
		t.Fatal("Run error was reported as local success")
	}
	if _, active := r.currentActiveQuery(); active {
		t.Fatal("Run error left active query")
	}
	if !r.queryOnlyLateMarkerAllowed {
		t.Fatal("Run error did not allow a late marker")
	}
	if !r.beginActiveQuery("next") {
		t.Fatal("Run error blocked next query")
	}
	r.takeActiveQuery()
}

func TestRelayQueryOnly_RunErrorDrainsPipelinedMarker(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	upstreamProxy, upstreamPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	defer upstreamProxy.Close()
	defer upstreamPeer.Close()
	sess := chsession.New(18, clientProxy)
	sess.Client().SetRevision(deferredTestRev)
	upstream := chproto.NewCodec(upstreamPeer, chproto.DirToUpstream)
	upstream.SetRevision(deferredTestRev)
	if err := sess.BindUpstream(context.Background(), upstream); err != nil {
		t.Fatalf("bind upstream: %v", err)
	}
	h := &firstQueryOnlyHooks{queryOnlyHooks: queryOnlyHooks{host: newQueryOnlyHost(t, func(context.Context, *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		return plugin.SnapshotQueryHostAdmission{Run: func(context.Context) error { return errors.New("durable host failure") }, CancelClient: func() {}, MaxControlBytes: 1024}, nil
	})}}
	r := NewRelay(sess, h, nil, nil)
	done := make(chan error, 1)
	go func() { done <- r.clientToUpstream(context.Background()) }()
	writeAllConn(t, clientPeer, encodeInsertQuery(t, "run-error-drain", "INSERT INTO target SELECT 1"))
	if exc := readServerException(t, clientPeer); exc.Message == "" {
		t.Fatal("query-only failure returned an empty Exception")
	}
	// A local failure carries the same allowance as a local success: the one
	// pipelined marker is drained instead of closing the connection.
	writeFragmented(t, clientPeer, encodeEmptyClientData(t))
	_ = upstreamProxy.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	if _, err := upstreamProxy.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("pipelined marker after Run error reached upstream: %v", err)
	}
	writeAllConn(t, clientPeer, encodeInsertQuery(t, "after-run-error", "SELECT 2"))
	_ = upstreamProxy.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := upstreamProxy.Read(make([]byte, 1)); err != nil {
		t.Fatalf("next Query after Run error did not reach upstream: %v", err)
	}
	if r.queryOnlyLateMarkerAllowed {
		t.Fatal("next Query did not clear the late-marker allowance")
	}
	if h.clientData.Load() != 0 {
		t.Fatalf("drained late marker fired %d ClientData hooks, want 0", h.clientData.Load())
	}
	clientPeer.Close()
	upstreamProxy.Close()
	<-done
}

func TestRelayQueryOnly_CancelThenNextQueryIsServed(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	upstreamProxy, upstreamPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	defer upstreamProxy.Close()
	defer upstreamPeer.Close()
	sess := chsession.New(7, clientProxy)
	sess.Client().SetRevision(deferredTestRev)
	upstream := chproto.NewCodec(upstreamPeer, chproto.DirToUpstream)
	upstream.SetRevision(deferredTestRev)
	if err := sess.BindUpstream(context.Background(), upstream); err != nil {
		t.Fatalf("bind upstream: %v", err)
	}
	started := make(chan struct{})
	h := &firstQueryOnlyHooks{queryOnlyHooks: queryOnlyHooks{host: newQueryOnlyHost(t, func(_ context.Context, _ *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		return plugin.SnapshotQueryHostAdmission{Run: func(ctx context.Context) error { close(started); <-ctx.Done(); return ctx.Err() }, CancelClient: func() {}, MaxControlBytes: 1024}, nil
	})}}
	r := NewRelay(sess, h, nil, nil)
	done := make(chan error, 1)
	go func() { done <- r.clientToUpstream(context.Background()) }()
	writeAllConn(t, clientPeer, encodeInsertQuery(t, "cancelled", "INSERT INTO target SELECT 1"))
	<-started
	var cancel proto.Buffer
	cancel.PutUVarInt(uint64(chproto.ClientCancelCode))
	writeAllConn(t, clientPeer, cancel.Buf)
	if got := readExact(t, clientPeer, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("cancel terminal=%d, want EOS", got[0])
	}
	// A canceled local operation ends at a supported terminal boundary, so the
	// next Query clears the allowance and is forwarded.
	writeAllConn(t, clientPeer, encodeInsertQuery(t, "ordinary-after-cancel", "SELECT 3"))
	_ = upstreamProxy.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := upstreamProxy.Read(make([]byte, 1)); err != nil {
		t.Fatalf("next query after cancel did not reach upstream: %v", err)
	}
	if r.queryOnlyLateMarkerAllowed {
		t.Fatal("next Query did not clear the late-marker allowance")
	}
	clientPeer.Close()
	upstreamProxy.Close()
	<-done
}
