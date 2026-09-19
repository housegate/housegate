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
	host      *plugin.SnapshotQueryHostPlugin
	aborts    atomic.Int32
	completes atomic.Int32
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
	if _, err := c.Write(raw[:1]); err != nil {
		t.Fatalf("write packet fragment: %v", err)
	}
	if len(raw) > 1 {
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
	if !r.queryOnlySessionIsTerminal() {
		t.Fatal("successful no-Data query-only execution did not mark session terminal")
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

func TestRelayQueryOnly_LocalSuccessMakesSessionTerminal(t *testing.T) {
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
	h := &queryOnlyHooks{host: newQueryOnlyHost(t, func(context.Context, *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		return plugin.SnapshotQueryHostAdmission{Run: func(context.Context) error { return nil }, CancelClient: func() {}, MaxControlBytes: 1024}, nil
	})}
	r := NewRelay(sess, h, nil, nil)
	done := make(chan error, 1)
	go func() { done <- r.clientToUpstream(context.Background()) }()
	if _, err := clientPeer.Write(encodeInsertQuery(t, "local", "INSERT INTO target SELECT 1")); err != nil {
		t.Fatalf("write local Query: %v", err)
	}
	if got := readExact(t, clientPeer, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("local terminal=%d, want EOS", got[0])
	}
	// INSERT ... SELECT has no mandatory ClientData terminator. A delayed
	// marker is therefore not allowed to create a reusable-session boundary.
	if _, err := clientPeer.Write(encodeEmptyClientData(t)); err != nil {
		t.Fatalf("write late query-only marker: %v", err)
	}
	if err := <-done; err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("client loop=%v, want terminal fail-closed error", err)
	}
	_ = upstreamProxy.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	if _, err := upstreamProxy.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("late marker reached upstream: %v", err)
	}
	if !r.queryOnlySessionIsTerminal() {
		t.Fatal("local query-only success did not mark session terminal")
	}
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
		{name: "empty_marker", raw: encodeEmptyClientData},
		{name: "named_marker", raw: namedMarker},
		{name: "nonempty_marker", raw: nonemptyMarker},
		{name: "cancel", raw: func(*testing.T) []byte { return append([]byte(nil), cancel.Buf...) }},
		{name: "unknown", raw: func(*testing.T) []byte { return []byte{99} }},
		{name: "query", raw: func(t *testing.T) []byte { return encodeInsertQuery(t, "late-query", "SELECT 2") }},
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
			if _, err := clientPeer.Write(encodeInsertQuery(t, "local-success", "INSERT INTO target SELECT 1")); err != nil {
				t.Fatalf("write local Query: %v", err)
			}
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
			defer clientProxy.Close()
			defer clientPeer.Close()
			sess := chsession.New(11, clientProxy)
			r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
			started := make(chan struct{})
			host := newQueryOnlyHost(t, func(_ context.Context, _ *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
				return plugin.SnapshotQueryHostAdmission{Run: func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					return ctx.Err()
				}, CancelClient: func() {}, MaxControlBytes: 1024}, nil
			})
			qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{ID: "control-error"}, Values: map[string]any{}}
			if err := host.OnQuery(context.Background(), qctx); err != nil {
				t.Fatalf("host OnQuery: %v", err)
			}
			done := make(chan error, 1)
			go func() { done <- r.runQueryOnly(context.Background(), qctx) }()
			<-started
			if _, err := clientPeer.Write(tc.raw); err != nil {
				t.Fatalf("write %s control: %v", tc.name, err)
			}
			if err := <-done; err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("run query-only error=%v, want non-EOF control error", err)
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
	if !r.beginActiveQuery("next") {
		t.Fatal("Run error blocked next query")
	}
	r.takeActiveQuery()
}

func TestRelayQueryOnly_CancelThenNextQueryClosesSession(t *testing.T) {
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
	if _, err := clientPeer.Write(encodeInsertQuery(t, "cancelled", "INSERT INTO target SELECT 1")); err != nil {
		t.Fatalf("write local Query: %v", err)
	}
	<-started
	var cancel proto.Buffer
	cancel.PutUVarInt(uint64(chproto.ClientCancelCode))
	if _, err := clientPeer.Write(cancel.Buf); err != nil {
		t.Fatalf("write Cancel: %v", err)
	}
	if got := readExact(t, clientPeer, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("cancel terminal=%d, want EOS", got[0])
	}
	if _, err := clientPeer.Write(encodeInsertQuery(t, "ordinary-after-cancel", "SELECT 3")); err != nil {
		t.Fatalf("write next Query: %v", err)
	}
	if err := <-done; err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("client loop=%v, want terminal fail-closed error", err)
	}
	_ = upstreamProxy.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	if _, err := upstreamProxy.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("next query after cancel reached upstream: %v", err)
	}
	if !r.queryOnlySessionIsTerminal() {
		t.Fatal("query-only cancellation did not mark session terminal")
	}
}
