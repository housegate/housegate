package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	chcompress "github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

func TestRelay_Handshake_PopulatesSessionState(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	// Pre-wire the upstream side to respond with a ServerHello once the
	// proxy forwards ClientHello.
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Drain the ClientHello the proxy forwards to us. Use a read deadline
		// so we don't block forever.
		_ = upstreamProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 4096)
		_, _ = upstreamProxy.Read(buf)

		// Write a ServerHello.
		srv := &proto.ServerHello{
			Name:        "test-server",
			Major:       24,
			Minor:       1,
			Revision:    54453,
			Timezone:    "UTC",
			DisplayName: "test-display",
		}
		var b proto.Buffer
		srv.EncodeAware(&b, 54453)
		_, _ = upstreamProxy.Write(b.Buf)
	}()

	// Client side: feed a ClientHello into the proxy's client side, then
	// drain any bytes the proxy writes back (the ServerHello echo).
	go func() {
		var b proto.Buffer
		(&proto.ClientHello{
			Name:            "test-client",
			Major:           1,
			Minor:           0,
			ProtocolVersion: 54453,
			Database:        "default",
			User:            "alice",
			Password:        "",
		}).Encode(&b)
		_, _ = clientProxy.Write(b.Buf)
		// Read (and discard) whatever the proxy sends back so WriteServerHello
		// does not block on the full pipe buffer.
		_ = clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = io.Copy(&bytes.Buffer{}, clientProxy)
	}()

	sess := chsession.New(1, proxyClient)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}

	r := &Relay{sess: sess, hooks: plugin.NoopHooks{}, obs: nil}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.handshake(ctx); err != nil {
		t.Fatalf("handshake: %v", err)
	}

	snap := sess.State().Snapshot()
	if snap.ClientRevision == 0 {
		t.Fatalf("ClientRevision not set after handshake: %+v", snap)
	}
	if snap.Database != "default" {
		t.Fatalf("Database=%q, want default", snap.Database)
	}
	// MappedUser should default to hello.User when no plugin overrides.
	if snap.MappedUser != "alice" {
		t.Fatalf("MappedUser=%q, want alice", snap.MappedUser)
	}

	<-done
}

func TestRelay_Run_QueryForwardAndEndOfStream(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	// --- upstream scripting ---
	go func() {
		// 1. Drain ClientHello from proxy.
		_ = upstreamProxy.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		drain := make([]byte, 4096)
		_, _ = upstreamProxy.Read(drain)
		// 2. Write ServerHello.
		srv := &proto.ServerHello{
			Name: "test", Major: 24, Minor: 1, Revision: 54453,
			Timezone: "UTC", DisplayName: "test",
		}
		var b proto.Buffer
		srv.EncodeAware(&b, 54453)
		_, _ = upstreamProxy.Write(b.Buf)
		// 3. Drain forwarded Query.
		_ = upstreamProxy.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, _ = upstreamProxy.Read(drain)
		// 4. Write EndOfStream back to client.
		var eos proto.Buffer
		eos.PutUVarInt(uint64(proto.ServerCodeEndOfStream))
		_, _ = upstreamProxy.Write(eos.Buf)
	}()

	// --- client scripting ---
	go func() {
		var b proto.Buffer
		(&proto.ClientHello{
			Name: "c", Major: 1, ProtocolVersion: 54453,
			Database: "default", User: "alice",
		}).Encode(&b)
		_, _ = clientProxy.Write(b.Buf)

		// After handshake, send a Query.
		time.Sleep(100 * time.Millisecond)
		var qb proto.Buffer
		(&proto.Query{
			ID: "qid", Body: "SELECT 1",
			Info: proto.ClientInfo{
				ProtocolVersion: 54453, Major: 24, Minor: 1,
				Interface: proto.InterfaceTCP,
				Query:     proto.ClientQueryInitial,
			},
		}).EncodeAware(&qb, 54453)
		_, _ = clientProxy.Write(qb.Buf)
	}()

	sess := chsession.New(1, proxyClient)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	_ = sess.BindUpstream(context.Background(), up)

	obs := &fakePacketObserver{}
	r := NewRelay(sess, plugin.NoopHooks{}, obs, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(ctx) }()

	// Read forwarded EndOfStream from the client side.
	_ = clientProxy.SetReadDeadline(time.Now().Add(1 * time.Second))
	// We need to drain the ServerHello first (which the handshake echoed),
	// then the EndOfStream.
	sink := make([]byte, 512)
	n, err := clientProxy.Read(sink)
	if err != nil {
		t.Fatalf("read ServerHello from client side: %v", err)
	}
	// The first byte(s) include ServerHello; continue reading until we see
	// the EOS type (5).
	gotEOS := false
	for !gotEOS {
		for i := 0; i < n; i++ {
			if sink[i] == byte(proto.ServerCodeEndOfStream) {
				gotEOS = true
				break
			}
		}
		if gotEOS {
			break
		}
		n, err = clientProxy.Read(sink)
		if err != nil {
			break
		}
	}
	if !gotEOS {
		t.Fatalf("did not see EndOfStream type (5) on client stream")
	}

	// Close the client side so Run exits.
	clientProxy.Close()
	select {
	case <-runErr:
	case <-time.After(1 * time.Second):
		t.Fatalf("Run did not return after client close")
	}

	// Wire-level observer: one Query ClientPacket, non-zero bytes each
	// direction, and at least EndOfStream on the server side. We don't
	// pin exact counts because the upstream→client path is chunk-based
	// and may collapse ServerHello + EndOfStream into one read depending
	// on net.Pipe scheduling.
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if !containsString(obs.clientPackets, "Query") {
		t.Errorf("clientPackets=%v, expected to include Query", obs.clientPackets)
	}
	if obs.bytes["client_to_upstream"] == 0 {
		t.Errorf("bytes[client_to_upstream]=0, expected >0 after forwarding Query")
	}
	if obs.bytes["upstream_to_client"] == 0 {
		t.Errorf("bytes[upstream_to_client]=0, expected >0 after receiving ServerHello+EOS")
	}
}

type fakePacketObserver struct {
	mu            sync.Mutex
	clientPackets []string
	serverPackets []string
	bytes         map[string]float64
	dataBlocks    []string
}

func (f *fakePacketObserver) ClientPacket(t string) {
	f.mu.Lock()
	f.clientPackets = append(f.clientPackets, t)
	f.mu.Unlock()
}

func (f *fakePacketObserver) ServerPacket(t string) {
	f.mu.Lock()
	f.serverPackets = append(f.serverPackets, t)
	f.mu.Unlock()
}

func (f *fakePacketObserver) BytesTransferred(direction string, n float64) {
	f.mu.Lock()
	if f.bytes == nil {
		f.bytes = make(map[string]float64)
	}
	f.bytes[direction] += n
	f.mu.Unlock()
}

func (f *fakePacketObserver) StreamingDataBlock(mode string) {
	f.mu.Lock()
	f.dataBlocks = append(f.dataBlocks, mode)
	f.mu.Unlock()
}

func containsString(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// exceptionRecordingHooks captures OnException + OnQueryComplete invocations so
// tests can assert that the relay decoded an upstream Exception packet
// and dispatched it through the chain before firing OnQueryComplete.
type exceptionRecordingHooks struct {
	plugin.NoopHooks
	mu             sync.Mutex
	exceptions     []*chproto.Exception
	querySuccesses int
	queryCompletes int
}

func (h *exceptionRecordingHooks) OnException(_ context.Context, _ chsession.Session, exc *chproto.Exception) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.exceptions = append(h.exceptions, exc)
	return nil
}

func (h *exceptionRecordingHooks) OnQuerySuccess(_ context.Context, _ chsession.Session, _ string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.querySuccesses++
}

func (h *exceptionRecordingHooks) OnQueryComplete(_ context.Context, _ chsession.Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queryCompletes++
}

// TestRelay_UpstreamException_FiresOnException drives a synthetic
// Exception packet through Relay.upstreamToClient and asserts that
// the decoded Exception is dispatched via Hooks.OnException with the
// expected fields, ahead of OnQueryComplete.
func TestRelay_UpstreamException_FiresOnException(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	// Client side: drain whatever the relay writes (the Exception
	// bytes are spliced through unchanged).
	go func() {
		_ = clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
		sink := make([]byte, 4096)
		for {
			if _, err := clientProxy.Read(sink); err != nil {
				return
			}
		}
	}()

	const rev = 54453
	sess := chsession.New(1, proxyClient)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	// BindUpstream without a handshake; we are unit-testing
	// upstreamToClient in isolation.
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}

	hooks := &exceptionRecordingHooks{}
	r := &Relay{sess: sess, hooks: hooks, obs: nil}

	// Encode an Exception packet on the upstream side.
	go func() {
		var b proto.Buffer
		b.PutUVarInt(uint64(proto.ServerCodeException))
		exc := proto.Exception{
			Code:    60,
			Name:    "DB::Exception",
			Message: "Table not found",
			Stack:   "",
		}
		exc.EncodeAware(&b, rev)
		_, _ = upstreamProxy.Write(b.Buf)
		// Close so upstreamToClient sees io.EOF and returns.
		_ = upstreamProxy.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := r.upstreamToClient(ctx)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("upstreamToClient: %v", err)
	}

	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if len(hooks.exceptions) != 1 {
		t.Fatalf("OnException fired %d times, want 1; queryCompletes=%d", len(hooks.exceptions), hooks.queryCompletes)
	}
	got := hooks.exceptions[0]
	if got.Message != "Table not found" {
		t.Errorf("Exception.Message=%q, want %q", got.Message, "Table not found")
	}
	if int(got.Code) != 60 {
		t.Errorf("Exception.Code=%d, want 60", got.Code)
	}
	if got.Name != "DB::Exception" {
		t.Errorf("Exception.Name=%q, want DB::Exception", got.Name)
	}
	if hooks.queryCompletes != 1 {
		t.Errorf("OnQueryComplete fired %d times, want 1", hooks.queryCompletes)
	}
	if hooks.querySuccesses != 0 {
		t.Errorf("OnQuerySuccess fired %d times for Exception, want 0", hooks.querySuccesses)
	}
}

func TestRelay_UpstreamEndOfStream_FiresQuerySuccess(t *testing.T) {
	hooks, err := runFramedUpstreamBytes(t, []byte{byte(chproto.ServerEndOfStreamCode)})
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("upstreamToClient: %v", err)
	}

	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.querySuccesses != 1 {
		t.Errorf("OnQuerySuccess fired %d times, want 1", hooks.querySuccesses)
	}
	if hooks.queryCompletes != 1 {
		t.Errorf("OnQueryComplete fired %d times, want 1", hooks.queryCompletes)
	}
	if len(hooks.exceptions) != 0 {
		t.Errorf("OnException fired %d times for EndOfStream, want 0", len(hooks.exceptions))
	}
}

func TestRelay_CoalescedProgressAndEndOfStream_FiresQuerySuccess(t *testing.T) {
	const rev = 54453
	var b proto.Buffer
	b.PutUVarInt(uint64(chproto.ServerProgressCode))
	(proto.Progress{Rows: 5, Bytes: 8, TotalRows: 13}).EncodeAware(&b, rev)
	b.PutUVarInt(uint64(chproto.ServerEndOfStreamCode))

	hooks, err := runFramedUpstreamBytes(t, b.Buf)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("upstreamToClient: %v", err)
	}

	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.querySuccesses != 1 {
		t.Errorf("OnQuerySuccess fired %d times, want 1", hooks.querySuccesses)
	}
	if hooks.queryCompletes != 1 {
		t.Errorf("OnQueryComplete fired %d times, want 1", hooks.queryCompletes)
	}
}

func TestRelay_FragmentedProgressPayloadByteCannotImpersonateEndOfStream(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	go func() {
		_, _ = io.Copy(io.Discard, clientProxy)
	}()

	const rev = 54453
	sess := chsession.New(1, proxyClient)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	hooks := &exceptionRecordingHooks{}
	r := &Relay{sess: sess, hooks: hooks}
	if !r.beginActiveQuery("qid-fragmented") {
		t.Fatal("beginActiveQuery unexpectedly reported an active query")
	}

	relayDone := make(chan error, 1)
	go func() {
		relayDone <- r.upstreamToClient(context.Background())
	}()

	var progress proto.Buffer
	progress.PutUVarInt(uint64(chproto.ServerProgressCode))
	(proto.Progress{Rows: uint64(chproto.ServerEndOfStreamCode)}).EncodeAware(&progress, rev)
	if len(progress.Buf) < 3 || progress.Buf[1] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("test fixture does not isolate payload 0x05: %x", progress.Buf)
	}

	// Send the Progress type and its rows value as separate TCP writes. The
	// second write is exactly one 0x05 byte, but it is Progress payload, not a
	// server packet. A raw-read heuristic would report false success here.
	if _, err := upstreamProxy.Write(progress.Buf[:1]); err != nil {
		t.Fatalf("write Progress type: %v", err)
	}
	if _, err := upstreamProxy.Write(progress.Buf[1:2]); err != nil {
		t.Fatalf("write isolated payload byte: %v", err)
	}
	time.Sleep(25 * time.Millisecond)
	hooks.mu.Lock()
	if hooks.querySuccesses != 0 || hooks.queryCompletes != 0 {
		gotSuccess, gotComplete := hooks.querySuccesses, hooks.queryCompletes
		hooks.mu.Unlock()
		t.Fatalf("isolated payload byte fired hooks: successes=%d completes=%d", gotSuccess, gotComplete)
	}
	hooks.mu.Unlock()

	terminal := append(append([]byte(nil), progress.Buf[2:]...), byte(chproto.ServerEndOfStreamCode))
	if _, err := upstreamProxy.Write(terminal); err != nil {
		t.Fatalf("finish Progress and EndOfStream: %v", err)
	}
	_ = upstreamProxy.Close()

	select {
	case err := <-relayDone:
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("upstreamToClient: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("upstreamToClient did not finish")
	}

	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.querySuccesses != 1 {
		t.Errorf("genuine EndOfStream fired OnQuerySuccess %d times, want 1", hooks.querySuccesses)
	}
	if hooks.queryCompletes != 1 {
		t.Errorf("genuine EndOfStream fired OnQueryComplete %d times, want 1", hooks.queryCompletes)
	}
}

func TestRelay_UnsupportedResultFallsBackToOpaqueBytes(t *testing.T) {
	for _, compression := range []proto.Compression{
		proto.CompressionDisabled,
		proto.CompressionEnabled,
	} {
		t.Run(compression.String(), func(t *testing.T) {
			clientProxy, proxyClient := net.Pipe()
			upstreamProxy, proxyUpstream := net.Pipe()
			defer clientProxy.Close()
			defer upstreamProxy.Close()

			const rev = chproto.MaxSupportedRevision
			sess := chsession.New(1, proxyClient)
			up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
			up.SetRevision(rev)
			up.SetCompression(compression)
			if err := sess.BindUpstream(context.Background(), up); err != nil {
				t.Fatalf("BindUpstream: %v", err)
			}

			hooks := &exceptionRecordingHooks{}
			r := &Relay{sess: sess, hooks: hooks}
			if !r.beginActiveQuery("qid-aggregate-state") {
				t.Fatal("beginActiveQuery unexpectedly reported an active query")
			}

			data := aggregateStateDataPacket(t, rev, compression)
			wire := append(append([]byte(nil), data...), byte(chproto.ServerEndOfStreamCode))

			relayDone := make(chan error, 1)
			go func() {
				relayDone <- r.upstreamToClient(context.Background())
			}()
			go func() {
				_ = upstreamProxy.SetWriteDeadline(time.Now().Add(time.Second))
				_, _ = upstreamProxy.Write(wire)
				_ = upstreamProxy.Close()
			}()

			got := make([]byte, len(wire))
			_ = clientProxy.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := io.ReadFull(clientProxy, got); err != nil {
				t.Fatalf("read opaque aggregate-state result: %v", err)
			}
			if !bytes.Equal(got, wire) {
				t.Fatalf("opaque aggregate-state result changed:\ngot  %x\nwant %x", got, wire)
			}

			select {
			case err := <-relayDone:
				if err != nil && !errors.Is(err, io.EOF) {
					t.Fatalf("upstreamToClient: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("upstreamToClient did not finish")
			}
		})
	}
}

func aggregateStateDataPacket(
	t *testing.T,
	rev int,
	compression proto.Compression,
) []byte {
	t.Helper()
	var body proto.Buffer
	(proto.BlockInfo{BucketNum: -1}).Encode(&body)
	body.PutUVarInt(1)
	body.PutUVarInt(1)
	body.PutString("state")
	body.PutString("AggregateFunction(sum, UInt64)")
	if proto.FeatureCustomSerialization.In(rev) {
		body.PutBool(false)
	}
	body.PutUInt64(45)

	var data proto.Buffer
	data.PutUVarInt(uint64(proto.ServerCodeData))
	data.PutString("")
	if compression == proto.CompressionEnabled {
		compressor := chcompress.NewWriter(0, chcompress.LZ4)
		if err := compressor.Compress(body.Buf); err != nil {
			t.Fatalf("compress aggregate state: %v", err)
		}
		data.Buf = append(data.Buf, compressor.Data...)
	} else {
		data.Buf = append(data.Buf, body.Buf...)
	}
	return append([]byte(nil), data.Buf...)
}

func TestRelay_OpaqueResultStopsAtNextQueryAndResumesPacketFraming(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	const rev = chproto.MaxSupportedRevision
	sess := chsession.New(1, proxyClient)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	up.SetCompression(proto.CompressionEnabled)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}

	hooks := &exceptionRecordingHooks{}
	r := &Relay{sess: sess, hooks: hooks}
	if !r.beginActiveQuery("qid-opaque") {
		t.Fatal("beginActiveQuery unexpectedly reported an active query")
	}

	first := aggregateStateDataPacket(t, rev, proto.CompressionEnabled)
	first = append(first, byte(chproto.ServerEndOfStreamCode))

	relayDone := make(chan error, 1)
	go func() {
		relayDone <- r.upstreamToClient(context.Background())
	}()
	go func() {
		_, _ = upstreamProxy.Write(first)
	}()

	gotFirst := make([]byte, len(first))
	_ = clientProxy.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(clientProxy, gotFirst); err != nil {
		t.Fatalf("read first opaque response: %v", err)
	}
	if !bytes.Equal(gotFirst, first) {
		t.Fatalf("first opaque response changed:\ngot  %x\nwant %x", gotFirst, first)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopped, err := r.stopOpaqueResponse(ctx)
	if err != nil {
		t.Fatalf("stopOpaqueResponse: %v", err)
	}
	if !stopped {
		t.Fatal("stopOpaqueResponse reported no opaque response")
	}
	if _, active := r.takeActiveQuery(); !active {
		t.Fatal("opaque query was not active at client-proven boundary")
	}
	hooks.OnQueryComplete(context.Background(), sess)

	if !r.beginActiveQuery("qid-framed-after-opaque") {
		t.Fatal("beginActiveQuery after opaque response reported an active query")
	}
	go func() {
		_, _ = upstreamProxy.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
		_ = upstreamProxy.Close()
	}()

	var terminal [1]byte
	_ = clientProxy.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(clientProxy, terminal[:]); err != nil {
		t.Fatalf("read framed EndOfStream after opaque response: %v", err)
	}
	if terminal[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("terminal byte=%d, want EndOfStream(%d)", terminal[0], chproto.ServerEndOfStreamCode)
	}

	select {
	case err := <-relayDone:
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("upstreamToClient: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("upstreamToClient did not finish")
	}

	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.querySuccesses != 1 {
		t.Errorf("framed response after opaque fallback fired OnQuerySuccess %d times, want 1", hooks.querySuccesses)
	}
	if hooks.queryCompletes != 2 {
		t.Errorf("two query lifecycles fired OnQueryComplete %d times, want 2", hooks.queryCompletes)
	}
}

type blockingQuerySuccessHooks struct {
	plugin.NoopHooks
	entered chan struct{}
	release chan struct{}
}

func (h *blockingQuerySuccessHooks) OnQuerySuccess(context.Context, chsession.Session, string) {
	close(h.entered)
	<-h.release
}

func TestRelay_QuerySuccessRunsBeforeEndOfStreamIsExposed(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	sess := chsession.New(1, proxyClient)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(54453)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}

	hooks := &blockingQuerySuccessHooks{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	r := &Relay{sess: sess, hooks: hooks}
	if !r.beginActiveQuery("qid-order") {
		t.Fatal("beginActiveQuery unexpectedly reported an active query")
	}

	upstreamDone := make(chan struct{})
	go func() {
		_, _ = upstreamProxy.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
		_ = upstreamProxy.Close()
		close(upstreamDone)
	}()
	relayDone := make(chan error, 1)
	go func() {
		relayDone <- r.upstreamToClient(context.Background())
	}()

	select {
	case <-hooks.entered:
	case <-time.After(time.Second):
		t.Fatal("OnQuerySuccess did not run")
	}

	_ = clientProxy.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	var got [1]byte
	if _, err := clientProxy.Read(got[:]); err == nil {
		t.Fatal("EndOfStream reached client before OnQuerySuccess returned")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("client read before success release = %v, want timeout", err)
	}
	_ = clientProxy.SetReadDeadline(time.Time{})

	close(hooks.release)
	if _, err := io.ReadFull(clientProxy, got[:]); err != nil {
		t.Fatalf("read EndOfStream after success release: %v", err)
	}
	if got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client byte = %d, want EndOfStream", got[0])
	}

	select {
	case err := <-relayDone:
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("upstreamToClient: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("upstreamToClient did not finish")
	}
	<-upstreamDone
}

func TestRelay_UpstreamMalformedException_DoesNotFireQuerySuccess(t *testing.T) {
	hooks, err := runFramedUpstreamBytes(t, []byte{byte(chproto.ServerExceptionCode)})
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("malformed Exception error = %v, want framing error", err)
	}

	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.querySuccesses != 0 {
		t.Errorf("OnQuerySuccess fired %d times for malformed Exception, want 0", hooks.querySuccesses)
	}
	if hooks.queryCompletes != 1 {
		t.Errorf("OnQueryComplete fired %d times, want 1", hooks.queryCompletes)
	}
	if len(hooks.exceptions) != 0 {
		t.Errorf("OnException fired %d times for undecodable Exception, want 0", len(hooks.exceptions))
	}
}

func runFramedUpstreamBytes(t *testing.T, raw []byte) (*exceptionRecordingHooks, error) {
	t.Helper()
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	go func() {
		sink := make([]byte, 64)
		for {
			if _, err := clientProxy.Read(sink); err != nil {
				return
			}
		}
	}()

	sess := chsession.New(1, proxyClient)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(54453)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}

	hooks := &exceptionRecordingHooks{}
	r := &Relay{sess: sess, hooks: hooks}
	if !r.beginActiveQuery("qid-outcome") {
		t.Fatal("beginActiveQuery unexpectedly reported an active query")
	}
	go func() {
		_, _ = upstreamProxy.Write(raw)
		_ = upstreamProxy.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return hooks, r.upstreamToClient(ctx)
}

// rewritingExceptionHooks mutates exc.Message in OnException to mimic
// what the rewrite plugin's RewriteErrorMessage path does.
type rewritingExceptionHooks struct {
	plugin.NoopHooks
	rewriteTo string
}

func (h *rewritingExceptionHooks) OnException(_ context.Context, _ chsession.Session, exc *chproto.Exception) error {
	exc.Message = h.rewriteTo
	return nil
}

// TestRelay_UpstreamException_RewrittenMessageReachesClient verifies
// that when OnException mutates exc.Message, the relay re-encodes the
// exception and forwards the rewritten message to the client (rather
// than the original raw bytes that were already on the wire).
func TestRelay_UpstreamException_RewrittenMessageReachesClient(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	const rev = 54453

	// Capture everything the relay writes to the client side, then
	// decode it as a server-direction Exception packet.
	clientGot := make(chan []byte, 1)
	go func() {
		_ = clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
		var buf bytes.Buffer
		sink := make([]byte, 4096)
		for {
			n, err := clientProxy.Read(sink)
			if n > 0 {
				buf.Write(sink[:n])
			}
			if err != nil {
				clientGot <- buf.Bytes()
				return
			}
		}
	}()

	sess := chsession.New(1, proxyClient)
	cli := sess.Client()
	cli.SetRevision(rev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}

	hooks := &rewritingExceptionHooks{rewriteTo: "Table tenant1.events not found"}
	r := &Relay{sess: sess, hooks: hooks, obs: nil}

	go func() {
		var b proto.Buffer
		b.PutUVarInt(uint64(proto.ServerCodeException))
		exc := proto.Exception{
			Code:    60,
			Name:    "DB::Exception",
			Message: "Table physical_db_1.tenant1_events not found",
			Stack:   "",
		}
		exc.EncodeAware(&b, rev)
		_, _ = upstreamProxy.Write(b.Buf)
		_ = upstreamProxy.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := r.upstreamToClient(ctx)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("upstreamToClient: %v", err)
	}
	_ = proxyClient.Close()

	raw := <-clientGot
	if len(raw) == 0 {
		t.Fatal("no bytes written to client")
	}
	if raw[0] != byte(chproto.ServerExceptionCode) {
		t.Fatalf("client got first byte %d, want ServerExceptionCode (%d)", raw[0], chproto.ServerExceptionCode)
	}
	rawReader := bytes.NewBuffer(raw)
	decoder := chproto.NewCodec(rawReader, chproto.DirToUpstream)
	decoder.SetRevision(rev)
	pkt, err := decoder.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("decode client Exception: %v; raw=%x", err, raw)
	}
	got, ok := pkt.Decoded.(*chproto.Exception)
	if !ok {
		t.Fatalf("client bytes decoded as %T, want *Exception", pkt.Decoded)
	}
	if got.Message != hooks.rewriteTo {
		t.Errorf("client Exception.Message=%q, want %q (the rewrite)", got.Message, hooks.rewriteTo)
	}
	if int(got.Code) != 60 {
		t.Errorf("client Exception.Code=%d, want 60", got.Code)
	}
	if got.Name != "DB::Exception" {
		t.Errorf("client Exception.Name=%q, want DB::Exception", got.Name)
	}
}

// TestWriteEndOfStreamToClient verifies the helper writes exactly one
// ServerEndOfStreamCode byte to its writer. This is the primitive used
// by the AbortWithSuccess branch in clientToUpstream.
func TestWriteEndOfStreamToClient(t *testing.T) {
	c, peer := net.Pipe()
	t.Cleanup(func() {
		_ = c.Close()
		_ = peer.Close()
	})

	go func() {
		_ = writeEndOfStreamToClient(c)
	}()

	buf := make([]byte, 4)
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	n, err := peer.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 byte, got %d", n)
	}
	if buf[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("expected ServerEndOfStreamCode (%d), got %d", chproto.ServerEndOfStreamCode, buf[0])
	}
}

// abortWithSuccessHooks is a Hooks impl whose OnQuery sets
// qctx.AbortWithSuccess=true and returns nil — emulating commitgate's
// behaviour after an Observer returns ErrAbortWithSuccess. It also counts
// OnQueryComplete invocations so the test can assert exactly-once firing.
type abortWithSuccessHooks struct {
	plugin.NoopHooks
	mu             sync.Mutex
	onQueryCalls   int
	queryAborts    int
	queryCompletes int
}

func (h *abortWithSuccessHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onQueryCalls++
	qctx.AbortWithSuccess = true
	return nil
}

func (h *abortWithSuccessHooks) OnQueryComplete(_ context.Context, _ chsession.Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queryCompletes++
}

func (h *abortWithSuccessHooks) OnQueryAbort(_ context.Context, _ *plugin.QueryContext) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queryAborts++
}

// TestRelay_ClientToUpstream_AbortWithSuccess plugs a fake plugin that
// sets qctx.AbortWithSuccess=true into the per-query loop, sends a real
// Query packet from a simulated client, and asserts:
//   - the client receives exactly one ServerEndOfStreamCode byte
//   - the simulated upstream conn receives zero bytes
//   - OnQueryComplete fires exactly once for the aborted query
//
// The test drives clientToUpstream directly (rather than the full Run)
// because the post-handshake Codec state is what we want to exercise.
func TestRelay_ClientToUpstream_AbortWithSuccess(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	const rev = 54453

	// Simulated upstream: drain anything written so writes don't block.
	// We assert later that nothing was written by counting bytes.
	upstreamBytes := make(chan int, 1)
	go func() {
		_ = upstreamProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 4096)
		total := 0
		for {
			n, err := upstreamProxy.Read(buf)
			total += n
			if err != nil {
				upstreamBytes <- total
				return
			}
		}
	}()

	// Simulated client: send a Query packet, then read the relay's
	// response into clientReadCh.
	clientReadCh := make(chan []byte, 1)
	go func() {
		// Query.EncodeAware prepends the ClientCodeQuery type byte itself —
		// do not add it twice.
		var qb proto.Buffer
		(&proto.Query{
			ID: "qid", Body: "CREATE TABLE t (a UInt8) ENGINE=Memory",
			Info: proto.ClientInfo{
				ProtocolVersion: rev, Major: 24, Minor: 1,
				Interface: proto.InterfaceTCP,
				Query:     proto.ClientQueryInitial,
			},
		}).EncodeAware(&qb, rev)
		_, _ = clientProxy.Write(qb.Buf)

		_ = clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 64)
		n, _ := clientProxy.Read(buf)
		clientReadCh <- buf[:n]
	}()

	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}

	hooks := &abortWithSuccessHooks{}
	r := &Relay{sess: sess, hooks: hooks, obs: nil}

	// Drive clientToUpstream until the simulated client closes.
	loopErrCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { loopErrCh <- r.clientToUpstream(ctx) }()

	// Read the relay's reply to the client. This should be exactly one
	// byte: ServerEndOfStreamCode.
	got := <-clientReadCh
	if len(got) != 1 {
		t.Fatalf("client got %d bytes, want 1; bytes=%v", len(got), got)
	}
	if got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client[0]=%d, want ServerEndOfStreamCode (%d)",
			got[0], chproto.ServerEndOfStreamCode)
	}

	// Tear down so clientToUpstream returns.
	clientProxy.Close()
	upstreamProxy.Close()
	select {
	case err := <-loopErrCh:
		if err != nil && !errors.Is(err, io.EOF) {
			// EOF (or wrapped EOF) is expected once the client side closes.
			t.Logf("clientToUpstream returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("clientToUpstream did not return after client close")
	}

	// Upstream must have received zero bytes — the AbortWithSuccess
	// branch skips up.WriteQuery entirely.
	upBytes := <-upstreamBytes
	if upBytes != 0 {
		t.Errorf("upstream received %d bytes, want 0", upBytes)
	}

	// Hook bookkeeping: OnQuery fired once, OnQueryComplete fired
	// exactly once for the aborted query.
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.onQueryCalls != 1 {
		t.Errorf("OnQuery fired %d times, want 1", hooks.onQueryCalls)
	}
	if hooks.queryCompletes != 1 {
		t.Errorf("OnQueryComplete fired %d times, want 1 (exactly once per query)", hooks.queryCompletes)
	}
	if hooks.queryAborts != 1 {
		t.Errorf("OnQueryAbort fired %d times, want 1 for non-forwarded query", hooks.queryAborts)
	}
}

type stagedInputHooks struct {
	plugin.NoopHooks
	mu             sync.Mutex
	onQueryCalls   int
	strictDataRaw  [][]byte
	strictComplete int
	inputCompletes int
	queryCompletes int
	queryAborts    int
}

func (h *stagedInputHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	h.mu.Lock()
	h.onQueryCalls++
	h.mu.Unlock()
	qctx.SuppressUpstreamExecution = true
	return nil
}

func (h *stagedInputHooks) OnClientDataStrict(_ context.Context, _ *plugin.QueryContext, raw []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.strictDataRaw = append(h.strictDataRaw, append([]byte(nil), raw...))
	return nil
}

func (h *stagedInputHooks) OnQueryInputCompleteStrict(context.Context, *plugin.QueryContext) error {
	h.mu.Lock()
	h.strictComplete++
	h.mu.Unlock()
	return nil
}

func (h *stagedInputHooks) OnQueryInputComplete(context.Context, *plugin.QueryContext) {
	h.mu.Lock()
	h.inputCompletes++
	h.mu.Unlock()
}

func (h *stagedInputHooks) OnQueryComplete(context.Context, chsession.Session) {
	h.mu.Lock()
	h.queryCompletes++
	h.mu.Unlock()
}

func (h *stagedInputHooks) OnQueryAbort(context.Context, *plugin.QueryContext) {
	h.mu.Lock()
	h.queryAborts++
	h.mu.Unlock()
}

func TestRelay_StagedNativeInsertPreservesSampleNegotiationAndSuppressesPayload(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	const rev = 54453
	sample := encodeServerSampleDataPacket(t, rev)
	nonEmpty := encodeNonEmptyClientDataPacket(t, rev)

	upstreamErrCh := make(chan error, 1)
	upstreamQueryCh := make(chan *chproto.Query, 1)
	upstreamDataCh := make(chan []byte, 1)
	go func() {
		codec := chproto.NewCodec(upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(rev)
		codec.SetCompression(proto.CompressionDisabled)

		pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil {
			upstreamErrCh <- err
			return
		}
		upstreamQueryCh <- pkt.Decoded.(*chproto.Query)
		if _, err := upstreamProxy.Write(sample); err != nil {
			upstreamErrCh <- err
			return
		}
		pkt, err = codec.ReadPacket()
		if err != nil {
			upstreamErrCh <- err
			return
		}
		upstreamDataCh <- append([]byte(nil), pkt.Raw...)
		if bytes.Contains(pkt.Raw, nonEmpty) {
			t.Errorf("ordinary upstream received staged payload bytes")
		}
		empty, err := chproto.ClientDataPacketIsEmpty(pkt.Raw, proto.CompressionDisabled)
		if err != nil {
			upstreamErrCh <- err
			return
		}
		if !empty {
			t.Errorf("ordinary upstream first Data packet after sample was not the zero-row terminator")
		}
		if _, err := upstreamProxy.Write([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
			upstreamErrCh <- err
			return
		}
		upstreamErrCh <- nil
	}()

	clientErrCh := make(chan error, 1)
	go func() {
		var qb proto.Buffer
		(&proto.Query{
			ID: "qid", Body: "INSERT INTO t FORMAT Native",
			Info: proto.ClientInfo{
				ProtocolVersion: rev, Major: 24, Minor: 1,
				Interface: proto.InterfaceTCP,
				Query:     proto.ClientQueryInitial,
			},
		}).EncodeAware(&qb, rev)
		if _, err := clientProxy.Write(qb.Buf); err != nil {
			clientErrCh <- err
			return
		}
		_ = clientProxy.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		gotSample := make([]byte, len(sample))
		if _, err := io.ReadFull(clientProxy, gotSample); err != nil {
			clientErrCh <- err
			return
		}
		if !bytes.Equal(gotSample, sample) {
			t.Errorf("client sample block mismatch")
		}
		if _, err := clientProxy.Write(nonEmpty); err != nil {
			clientErrCh <- err
			return
		}
		writer := chproto.NewCodec(clientProxy, chproto.DirFromClient)
		writer.SetCompression(proto.CompressionDisabled)
		if err := writer.WriteEmptyDataBlock(); err != nil {
			clientErrCh <- err
			return
		}
		_ = clientProxy.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := []byte{0}
		if _, err := io.ReadFull(clientProxy, buf); err != nil {
			clientErrCh <- err
			return
		}
		if buf[0] != byte(chproto.ServerEndOfStreamCode) {
			t.Errorf("client got server packet %d, want EndOfStream", buf[0])
		}
		clientErrCh <- nil
	}()

	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}

	hooks := &stagedInputHooks{}
	r := &Relay{sess: sess, hooks: hooks, obs: nil}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	loopErrCh := make(chan error, 2)
	go func() { loopErrCh <- r.clientToUpstream(ctx) }()
	go func() { loopErrCh <- r.upstreamToClient(ctx) }()

	select {
	case q := <-upstreamQueryCh:
		if q.ID != "qid" {
			t.Fatalf("upstream query id = %q, want qid", q.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ordinary upstream did not receive Query for sample-block negotiation")
	}
	select {
	case raw := <-upstreamDataCh:
		if bytes.Equal(raw, nonEmpty) {
			t.Fatal("ordinary upstream received staged payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ordinary upstream did not receive zero-row terminator")
	}
	if err := <-clientErrCh; err != nil {
		t.Fatalf("client flow: %v", err)
	}
	if err := <-upstreamErrCh; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}

	clientProxy.Close()
	upstreamProxy.Close()
	for i := 0; i < 2; i++ {
		select {
		case err := <-loopErrCh:
			if err != nil && !errors.Is(err, io.EOF) {
				t.Logf("relay loop returned: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("relay loop did not return after close")
		}
	}

	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if len(hooks.strictDataRaw) == 0 || !bytes.Equal(hooks.strictDataRaw[0], nonEmpty) {
		t.Fatalf("strict data captured %d packets, first must be the staged payload", len(hooks.strictDataRaw))
	}
	if hooks.strictComplete != 1 || hooks.inputCompletes != 1 || hooks.queryCompletes != 1 {
		t.Fatalf("completion counts strict/input/query = %d/%d/%d, want 1/1/1", hooks.strictComplete, hooks.inputCompletes, hooks.queryCompletes)
	}
	if hooks.queryAborts != 0 {
		t.Fatalf("OnQueryAbort calls = %d, want 0", hooks.queryAborts)
	}
}

type inputCompleteHooks struct {
	plugin.NoopHooks
	mu             sync.Mutex
	inputCompletes int
}

type pipelineHooks struct {
	plugin.NoopHooks
	mu       sync.Mutex
	queries  int
	abortIDs []string
}

type rejectedPipelineHooks struct {
	pipelineHooks
}

type strictQueryDecodeHooks struct {
	plugin.NoopHooks
}

func (*strictQueryDecodeHooks) RejectUndecodableQuery(chsession.Session) bool { return true }

func (h *rejectedPipelineHooks) OnQuery(_ context.Context, _ *plugin.QueryContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queries++
	return errors.New("reject first query")
}

func (h *pipelineHooks) OnQuery(_ context.Context, _ *plugin.QueryContext) error {
	h.mu.Lock()
	h.queries++
	h.mu.Unlock()
	return nil
}

func (h *pipelineHooks) OnQueryAbort(_ context.Context, qctx *plugin.QueryContext) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if qctx != nil && qctx.Query != nil {
		h.abortIDs = append(h.abortIDs, qctx.Query.ID)
	}
}

func TestRelay_ClientToUpstream_RejectsPipelinedQueryBeforeInputComplete(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	const rev = 54453
	upstreamPackets := make(chan []uint64, 1)
	go func() {
		codec := chproto.NewCodec(upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(rev)
		var types []uint64
		pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		if err == nil && pkt != nil {
			types = append(types, pkt.Type)
		}
		_ = upstreamProxy.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		pkt, err = codec.ReadPacket(uint64(chproto.ClientQueryCode))
		if err == nil && pkt != nil {
			types = append(types, pkt.Type)
		}
		upstreamPackets <- types
	}()
	go func() {
		for _, id := range []string{"query-a", "query-b"} {
			var qb proto.Buffer
			(&proto.Query{
				ID: id, Body: "INSERT INTO t VALUES (1)",
				Info: proto.ClientInfo{
					ProtocolVersion: rev, Major: 24, Minor: 1,
					Interface: proto.InterfaceTCP,
					Query:     proto.ClientQueryInitial,
				},
			}).EncodeAware(&qb, rev)
			_, _ = clientProxy.Write(qb.Buf)
		}
		buf := make([]byte, 1024)
		_ = clientProxy.SetReadDeadline(time.Now().Add(time.Second))
		_, _ = clientProxy.Read(buf)
		_ = clientProxy.Close()
	}()

	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	hooks := &pipelineHooks{}
	r := &Relay{sess: sess, hooks: hooks}
	err := r.clientToUpstream(context.Background())
	if err == nil || !strings.Contains(err.Error(), "before completing input") {
		t.Fatalf("clientToUpstream err = %v, want pipelined query rejection", err)
	}
	_ = proxyUpstream.Close()
	types := <-upstreamPackets
	if len(types) != 1 || types[0] != uint64(chproto.ClientQueryCode) {
		t.Fatalf("upstream packet types = %v, want only first Query", types)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.queries != 1 {
		t.Fatalf("OnQuery calls = %d, want only first query admitted", hooks.queries)
	}
	if len(hooks.abortIDs) != 1 || hooks.abortIDs[0] != "query-a" {
		t.Fatalf("abort IDs = %v, want [query-a]", hooks.abortIDs)
	}
}

func TestRelay_ClientToUpstream_RejectsNewQueryBeforeRejectedInputIsDrained(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	const rev = 54453
	upstreamBytes := make(chan int, 1)
	go func() {
		_ = upstreamProxy.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 1024)
		n, _ := upstreamProxy.Read(buf)
		upstreamBytes <- n
	}()
	go func() {
		writeQuery := func(id string) {
			var qb proto.Buffer
			(&proto.Query{
				ID: id, Body: "INSERT INTO t VALUES (1)",
				Info: proto.ClientInfo{
					ProtocolVersion: rev, Major: 24, Minor: 1,
					Interface: proto.InterfaceTCP,
					Query:     proto.ClientQueryInitial,
				},
			}).EncodeAware(&qb, rev)
			_, _ = clientProxy.Write(qb.Buf)
		}
		writeQuery("query-a")
		buf := make([]byte, 1024)
		_ = clientProxy.SetReadDeadline(time.Now().Add(time.Second))
		_, _ = clientProxy.Read(buf)
		writeQuery("query-b")
		_, _ = clientProxy.Read(buf)
		_ = clientProxy.Close()
	}()

	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	hooks := &rejectedPipelineHooks{}
	r := &Relay{sess: sess, hooks: hooks}
	err := r.clientToUpstream(context.Background())
	if err == nil || !strings.Contains(err.Error(), "before completing rejected input") {
		t.Fatalf("clientToUpstream err = %v, want rejected-input pipeline rejection", err)
	}
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes, want none", n)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.queries != 1 {
		t.Fatalf("OnQuery calls = %d, want only rejected query-a", hooks.queries)
	}
	if len(hooks.abortIDs) != 1 || hooks.abortIDs[0] != "query-a" {
		t.Fatalf("abort IDs = %v, want [query-a]", hooks.abortIDs)
	}
}

func TestRelay_ClientToUpstream_DrainsCompressedDataAfterQueryRejection(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	const rev = 54453
	upstreamBytes := make(chan int, 1)
	go func() {
		_ = upstreamProxy.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 1024)
		n, _ := upstreamProxy.Read(buf)
		upstreamBytes <- n
	}()
	go func() {
		var qb proto.Buffer
		(&proto.Query{
			ID: "query-a", Body: "INSERT INTO t VALUES (1)",
			Compression: proto.CompressionEnabled,
			Info: proto.ClientInfo{
				ProtocolVersion: rev, Major: 24, Minor: 1,
				Interface: proto.InterfaceTCP,
				Query:     proto.ClientQueryInitial,
			},
		}).EncodeAware(&qb, rev)
		_, _ = clientProxy.Write(qb.Buf)
		buf := make([]byte, 1024)
		_ = clientProxy.SetReadDeadline(time.Now().Add(time.Second))
		_, _ = clientProxy.Read(buf)

		writer := chproto.NewCodec(clientProxy, chproto.DirFromClient)
		writer.SetCompression(proto.CompressionEnabled)
		_ = writer.WriteEmptyDataBlock()
		_ = clientProxy.Close()
	}()

	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	hooks := &rejectedPipelineHooks{}
	r := &Relay{sess: sess, hooks: hooks}
	err := r.clientToUpstream(context.Background())
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("clientToUpstream: %v", err)
	}
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes, want none", n)
	}
}

func TestRelay_ClientToUpstream_StrictPolicyRejectsUndecodableQuery(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	upstreamBytes := make(chan int, 1)
	go func() {
		_ = upstreamProxy.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 1024)
		n, _ := upstreamProxy.Read(buf)
		upstreamBytes <- n
	}()
	go func() {
		malformed := append([]byte{byte(chproto.ClientQueryCode)}, bytes.Repeat([]byte{0xff}, 10)...)
		_, _ = clientProxy.Write(malformed)
		buf := make([]byte, 1024)
		_ = clientProxy.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, _ = clientProxy.Read(buf)
		_ = clientProxy.Close()
	}()

	const rev = 54453
	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	r := &Relay{sess: sess, hooks: &strictQueryDecodeHooks{}}
	err := r.clientToUpstream(context.Background())
	if err == nil || !strings.Contains(err.Error(), "strict query decode") {
		t.Fatalf("clientToUpstream err = %v, want strict query decode rejection", err)
	}
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes, want none", n)
	}
}

// strictInputCompleteFailHooks fails the pre-splice strict end-of-input hook,
// modelling a storage-integrity admission the consumer rejected.
type strictInputCompleteFailHooks struct {
	plugin.NoopHooks
	mu             sync.Mutex
	strictCalls    int
	inputCompletes int
}

func (h *strictInputCompleteFailHooks) OnQueryInputCompleteStrict(_ context.Context, _ *plugin.QueryContext) error {
	h.mu.Lock()
	h.strictCalls++
	h.mu.Unlock()
	return errors.New("admission rejected")
}

func (h *strictInputCompleteFailHooks) OnQueryInputComplete(_ context.Context, _ *plugin.QueryContext) {
	h.mu.Lock()
	h.inputCompletes++
	h.mu.Unlock()
}

// TestRelay_ClientToUpstream_StrictInputCompleteFailureBlocksTerminator pins the
// fail-closed boundary: when the strict end-of-input hook errors, the relay
// rejects the query, does NOT forward the terminating empty Data block upstream,
// and never fires the post-splice OnQueryInputComplete observer.
func TestRelay_ClientToUpstream_StrictInputCompleteFailureBlocksTerminator(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	const rev = 54453
	// Record every byte the relay forwards upstream so we can assert the
	// terminating block never arrived. The Query packet is forwarded first; the
	// terminating empty Data block must be withheld on strict failure.
	upstreamBytes := make(chan int, 1)
	go func() {
		n, _ := io.Copy(io.Discard, upstreamProxy)
		upstreamBytes <- int(n)
	}()
	go func() {
		var qb proto.Buffer
		(&proto.Query{
			ID: "qid", Body: "INSERT INTO t VALUES (1)",
			Info: proto.ClientInfo{
				ProtocolVersion: rev, Major: 24, Minor: 1,
				Interface: proto.InterfaceTCP,
				Query:     proto.ClientQueryInitial,
			},
		}).EncodeAware(&qb, rev)
		_, _ = clientProxy.Write(qb.Buf)
		writer := chproto.NewCodec(clientProxy, chproto.DirFromClient)
		writer.SetCompression(proto.CompressionDisabled)
		_ = writer.WriteEmptyDataBlock()
		_ = clientProxy.Close()
	}()

	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	hooks := &strictInputCompleteFailHooks{}
	r := &Relay{sess: sess, hooks: hooks}
	err := r.clientToUpstream(context.Background())
	if err == nil || !strings.Contains(err.Error(), "query input complete strict hook") {
		t.Fatalf("clientToUpstream err = %v, want strict-hook rejection", err)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.strictCalls != 1 {
		t.Fatalf("strict hook fired %d times, want 1", hooks.strictCalls)
	}
	if hooks.inputCompletes != 0 {
		t.Fatalf("post-splice OnQueryInputComplete fired %d times, want 0 on strict failure", hooks.inputCompletes)
	}
}

func (h *inputCompleteHooks) OnQueryInputComplete(_ context.Context, _ *plugin.QueryContext) {
	h.mu.Lock()
	h.inputCompletes++
	h.mu.Unlock()
}

func encodeNonEmptyClientDataPacket(t *testing.T, rev int) []byte {
	t.Helper()

	values := proto.ColUInt64{1, 2, 3}
	input := proto.Input{{Name: "v", Data: &values}}

	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	block := proto.Block{Rows: values.Rows(), Columns: len(input)}
	if err := block.EncodeBlock(&buf, rev, input); err != nil {
		t.Fatalf("encode non-empty client data block: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

func encodeServerSampleDataPacket(t *testing.T, rev int) []byte {
	t.Helper()

	values := proto.ColUInt64{}
	input := proto.Input{{Name: "v", Data: &values}}

	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeData))
	buf.PutString("")
	block := proto.Block{Rows: values.Rows(), Columns: len(input)}
	if err := block.EncodeBlock(&buf, rev, input); err != nil {
		t.Fatalf("encode server sample data block: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

func TestRelay_ClientToUpstream_FiresInputCompleteAfterEmptyData(t *testing.T) {
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	const rev = 54453
	go func() {
		_, _ = io.Copy(io.Discard, upstreamProxy)
	}()
	go func() {
		var qb proto.Buffer
		(&proto.Query{
			ID: "qid", Body: "INSERT INTO t VALUES (1)",
			Info: proto.ClientInfo{
				ProtocolVersion: rev, Major: 24, Minor: 1,
				Interface: proto.InterfaceTCP,
				Query:     proto.ClientQueryInitial,
			},
		}).EncodeAware(&qb, rev)
		_, _ = clientProxy.Write(qb.Buf)
		writer := chproto.NewCodec(clientProxy, chproto.DirFromClient)
		writer.SetCompression(proto.CompressionDisabled)
		_ = writer.WriteEmptyDataBlock()
		_ = clientProxy.Close()
	}()

	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	hooks := &inputCompleteHooks{}
	r := &Relay{sess: sess, hooks: hooks}
	err := r.clientToUpstream(context.Background())
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("clientToUpstream: %v", err)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.inputCompletes != 1 {
		t.Fatalf("OnQueryInputComplete fired %d times, want 1", hooks.inputCompletes)
	}
}

func TestRelay_ClientToUpstream_IgnoresInitialEmptyDataBeforeInsertPayload(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "bare native insert",
			sql:  "INSERT INTO t",
		},
		{
			name: "quoted identifier named select",
			sql:  "INSERT INTO t (`select`) FORMAT Native",
		},
		{
			name: "column list contains select token",
			sql:  "INSERT INTO t (select) FORMAT Native",
		},
		{
			name: "comment mentions select",
			sql:  "INSERT INTO t /* SELECT is only a comment */ FORMAT Native",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runStreamingInsertDataLifecycle(t, tt.sql)
		})
	}
}

func runStreamingInsertDataLifecycle(t *testing.T, sql string) {
	t.Helper()

	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	defer clientProxy.Close()
	defer upstreamProxy.Close()

	const rev = 54453
	go func() {
		_, _ = io.Copy(io.Discard, upstreamProxy)
	}()

	nonEmpty := encodeNonEmptyClientDataPacket(t, rev)
	clientErr := make(chan error, 1)
	go func() {
		var qb proto.Buffer
		(&proto.Query{
			ID: "qid", Body: sql,
			Info: proto.ClientInfo{
				ProtocolVersion: rev, Major: 24, Minor: 1,
				Interface: proto.InterfaceTCP,
				Query:     proto.ClientQueryInitial,
			},
		}).EncodeAware(&qb, rev)
		if err := clientProxy.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
			clientErr <- err
			return
		}
		if _, err := clientProxy.Write(qb.Buf); err != nil {
			clientErr <- err
			return
		}

		writer := chproto.NewCodec(clientProxy, chproto.DirFromClient)
		writer.SetCompression(proto.CompressionDisabled)
		if err := clientProxy.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
			clientErr <- err
			return
		}
		if err := writer.WriteEmptyDataBlock(); err != nil {
			clientErr <- err
			return
		}
		if err := clientProxy.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
			clientErr <- err
			return
		}
		if _, err := clientProxy.Write(nonEmpty); err != nil {
			clientErr <- err
			return
		}
		if err := clientProxy.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
			clientErr <- err
			return
		}
		if err := writer.WriteEmptyDataBlock(); err != nil {
			clientErr <- err
			return
		}
		_ = clientProxy.Close()
		clientErr <- nil
	}()

	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	hooks := &inputCompleteHooks{}
	r := &Relay{sess: sess, hooks: hooks}
	err := r.clientToUpstream(context.Background())
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("clientToUpstream: %v", err)
	}
	if err := <-clientErr; err != nil {
		t.Fatalf("client writer: %v", err)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.inputCompletes != 1 {
		t.Fatalf("OnQueryInputComplete fired %d times, want 1 after payload terminator", hooks.inputCompletes)
	}
}
