package sistatement

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// fakeUpstream is an in-process server leg: it speaks the ClientHello /
// ServerHello / addendum handshake through a chproto.Codec on one end of a net.Pipe, publishes the Query it receives and replays a scripted response.
type fakeUpstream struct {
	seen       chan *chproto.Query
	reply      func(srv *chproto.Codec, rev int) error
	handshakes atomic.Int32
	chunkMode  string
	negotiated chan chproto.AddendumResult
}

func (f *fakeUpstream) dial(_ context.Context, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	go f.serve(server)
	return client, nil
}

func (f *fakeUpstream) serve(conn net.Conn) {
	defer conn.Close()
	srv := chproto.NewCodec(conn, chproto.DirFromClient)
	pkt, err := srv.ReadPacket(uint64(chproto.ClientHelloCode))
	if err != nil {
		return
	}
	hello, ok := pkt.Decoded.(*chproto.ClientHello)
	if !ok {
		return
	}
	rev := int(hello.ProtocolVersion)
	srv.SetRevision(rev)
	f.handshakes.Add(1)
	if err := srv.WriteServerHello(&chproto.ServerHello{Name: "fake", Major: 26, Minor: 3, Revision: rev, Timezone: "UTC", DisplayName: "fake"}); err != nil {
		return
	}
	if chproto.SupportsChunkedPackets(rev) {
		var tail proto.Buffer
		mode := f.chunkMode
		if mode == "" {
			mode = "notchunked"
		}
		tail.PutString(mode)
		tail.PutString(mode)
		tail.PutUVarInt(0)
		tail.PutUInt64(0)
		if err := srv.WriteRawPacket(tail.Buf); err != nil {
			return
		}
	}
	if chproto.SupportsAddendum(rev) {
		mode := f.chunkMode
		if mode == "" {
			mode = "notchunked"
		}
		res, err := srv.NegotiateAddendum(chproto.AddendumOpts{ProposedRecv: mode, ProposedSend: mode})
		if err != nil {
			return
		}
		if f.negotiated != nil {
			f.negotiated <- res
		}
	}
	for {
		qpkt, err := srv.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil {
			return
		}
		q, ok := qpkt.Decoded.(*chproto.Query)
		if !ok {
			return
		}
		if _, err := srv.ReadPacket(); err != nil {
			return
		} // the client's empty terminator
		f.seen <- q
		if err := f.reply(srv, rev); err != nil {
			return
		}
	}
}

func writeServerBlock(srv *chproto.Codec, rev int, input proto.Input) error {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: 1, Columns: len(input)}).EncodeBlock(&buf, rev, input); err != nil {
		return err
	}
	return srv.WriteRawPacket(buf.Buf)
}

func writeEndOfStream(srv *chproto.Codec) error {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeEndOfStream))
	return srv.WriteRawPacket(buf.Buf)
}

// evalRow builds one (id, ts) result row; idAsString forces a wire-type mismatch.
func evalRow(v uint64, idAsString bool) proto.Input {
	ts := &proto.ColDateTime{Location: time.UTC}
	ts.Append(time.Unix(1758000000, 0).UTC())
	if idAsString {
		s := &proto.ColStr{}
		s.Append("1")
		return proto.Input{{Name: "id", Data: s}, {Name: "ts", Data: ts}}
	}
	id := &proto.ColUInt64{}
	id.Append(v)
	return proto.Input{{Name: "id", Data: id}, {Name: "ts", Data: ts}}
}

func evalRequest() ValuesEvaluation {
	return ValuesEvaluation{
		UpstreamAddress: "selected:9000", Hello: &chproto.ClientHello{Name: "c", Major: 1, Minor: 0, ProtocolVersion: testRevision, Database: "shop", User: "writer"},
		Schema: payloadexec.TableSchema{TableID: "shop.orders", Columns: []lthash.Column{
			{Name: "id", Type: "UInt64"}, {Name: "ts", Type: "DateTime('UTC')"},
		}},
		Account: "0xabc", Columns: []string{"id", "ts"}, Rows: "(1, toDateTime(1758000000))",
		Timeout: 3 * time.Second, MaxRows: 64, MaxBytes: 1 << 20,
	}
}

func newTestEvaluator(t *testing.T, reply func(*chproto.Codec, int) error) (*UpstreamValuesEvaluator, *fakeUpstream, *auth.RelaySigner) {
	t.Helper()
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	up := &fakeUpstream{seen: make(chan *chproto.Query, 64), reply: reply}
	ev := NewUpstreamValuesEvaluator(up.dial, signer)
	t.Cleanup(func() { _ = ev.Close() })
	return ev, up, signer
}

// TestUpstreamValuesEvaluator_HelperQuery pins the helper SQL text
// (including the doubled quotes of DateTime('UTC') inside the structure literal), the four settings, compression off and the agent auth token, and also proves multiple result blocks are concatenated in order and the pool reuses one handshake.
func TestUpstreamValuesEvaluator_HelperQuery(t *testing.T) {
	ev, up, signer := newTestEvaluator(t, func(srv *chproto.Codec, rev int) error {
		for _, v := range []uint64{1, 2} {
			if err := writeServerBlock(srv, rev, evalRow(v, false)); err != nil {
				return err
			}
		}
		return writeEndOfStream(srv)
	})
	var q *chproto.Query
	for i := 0; i < 2; i++ {
		blocks, err := ev.Evaluate(context.Background(), evalRequest())
		if err != nil {
			t.Fatalf("Evaluate #%d: %v", i, err)
		}
		if len(blocks) != 2 || len(blocks[0]) != 2 || blocks[0][0].Data.Rows() != 1 {
			t.Fatalf("#%d blocks = %+v", i, blocks)
		}
		q = <-up.seen
	}
	if up.handshakes.Load() != 1 {
		t.Fatalf("pool performed %d handshakes for two evaluations, want 1", up.handshakes.Load())
	}

	want := "SELECT `id`, `ts` FROM VALUES('`id` UInt64, `ts` DateTime(''UTC'')', (1, toDateTime(1758000000)))"
	if q.Body != want {
		t.Fatalf("helper SQL =\n  %q\nwant\n  %q", q.Body, want)
	}
	if q.Compression != proto.CompressionDisabled {
		t.Fatalf("helper query must disable compression, got %v", q.Compression)
	}
	got := map[string]chproto.Setting{}
	for _, s := range q.Settings {
		got[s.Key] = s
	}
	for key, value := range map[string]string{
		"max_execution_time": "3", "max_result_rows": "64", "max_result_bytes": "1048576", "max_block_size": "64",
	} {
		if got[key].Value != value {
			t.Fatalf("setting %s = %q, want %q", key, got[key].Value, value)
		}
	}
	tok := got[auth.AuthTokenSettingKey]
	if !tok.Custom || !strings.HasPrefix(tok.Value, "'") {
		t.Fatalf("auth token setting = %+v", tok)
	}
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	if _, err := validator.ValidateQuery(context.Background(), auth.QueryMeta{Settings: map[string]string{auth.AuthTokenSettingKey: strings.Trim(tok.Value, "'")}, SQL: q.Body}); err != nil {
		t.Fatalf("helper query is not signed by the agent key: %v", err)
	}
}

func TestUpstreamValuesEvaluator_Refusals(t *testing.T) {
	cases := []struct {
		name, wantErr string
		timeout       time.Duration
		reply         func(*chproto.Codec, int) error
	}{
		{"type mismatch", "wire type", 3 * time.Second, func(srv *chproto.Codec, rev int) error {
			if err := writeServerBlock(srv, rev, evalRow(1, true)); err != nil {
				return err
			}
			return writeEndOfStream(srv)
		}},
		{"exception relayed", "code=36", 3 * time.Second, func(srv *chproto.Codec, _ int) error {
			return srv.WriteException(&chproto.Exception{Code: 36, Name: "BAD_ARGUMENTS", Message: "is not a constant expression"})
		}},
		{"timeout", "", time.Second, func(srv *chproto.Codec, _ int) error {
			time.Sleep(2 * time.Second)
			return writeEndOfStream(srv)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, up, _ := newTestEvaluator(t, tc.reply)
			req := evalRequest()
			req.Timeout = tc.timeout
			start := time.Now()
			_, err := ev.Evaluate(context.Background(), req)
			if err == nil {
				t.Fatal("the evaluation must fail")
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
			if tc.name == "exception relayed" && !strings.Contains(err.Error(), "is not a constant expression") {
				t.Fatalf("err = %v, want the upstream message", err)
			}
			if tc.name == "timeout" && time.Since(start) > 1500*time.Millisecond {
				t.Fatalf("timeout was not enforced (%s)", time.Since(start))
			}
			<-up.seen
		})
	}
}

func TestUpstreamValuesEvaluator_SchemaAndAggregateLimits(t *testing.T) {
	for _, tc := range []struct {
		name, want        string
		maxRows, maxBytes uint64
		reply             func(*chproto.Codec, int) error
	}{
		{name: "empty", want: "no rows", reply: func(s *chproto.Codec, _ int) error { return writeEndOfStream(s) }},
		{name: "wrong name", want: "expected", reply: func(s *chproto.Codec, r int) error {
			row := evalRow(1, false)
			row[0].Name = "other"
			return writeServerBlock(s, r, row)
		}},
		{name: "zero-row wrong schema", want: "wire type", reply: func(s *chproto.Codec, r int) error {
			var b proto.Buffer
			b.PutUVarInt(uint64(proto.ServerCodeData))
			b.PutString("")
			if err := (proto.Block{Columns: 2}).EncodeBlock(&b, r, proto.Input{{Name: "id", Data: &proto.ColStr{}}, {Name: "ts", Data: &proto.ColDateTime{Location: time.UTC}}}); err != nil {
				return err
			}
			return s.WriteRawPacket(b.Buf)
		}},
		{name: "rows", want: "max_rows", maxRows: 1, reply: func(s *chproto.Codec, r int) error {
			if err := writeServerBlock(s, r, evalRow(1, false)); err != nil {
				return err
			}
			return writeServerBlock(s, r, evalRow(2, false))
		}},
		{name: "bytes", want: "max_payload_bytes", maxBytes: 80, reply: func(s *chproto.Codec, r int) error {
			for i := 0; i < 3; i++ {
				if err := writeServerBlock(s, r, evalRow(uint64(i), false)); err != nil {
					return err
				}
			}
			return writeEndOfStream(s)
		}},
		{name: "bounded frame", want: "max_payload_bytes", maxBytes: 128, reply: func(s *chproto.Codec, r int) error {
			data := &proto.ColStr{}
			data.Append(strings.Repeat("x", 128<<10))
			return writeServerBlock(s, r, proto.Input{{Name: "id", Data: data}})
		}},
		{name: "wrong count", want: "columns", reply: func(s *chproto.Codec, r int) error { return writeServerBlock(s, r, evalRow(1, false)[:1]) }},
		{name: "unexpected packet", want: "unexpected", reply: func(s *chproto.Codec, _ int) error {
			var b proto.Buffer
			b.PutUVarInt(uint64(proto.ServerCodePong))
			return s.WriteRawPacket(b.Buf)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev, up, _ := newTestEvaluator(t, tc.reply)
			req := evalRequest()
			if tc.maxRows != 0 {
				req.MaxRows = tc.maxRows
			}
			if tc.maxBytes != 0 {
				req.MaxBytes = tc.maxBytes
			}
			for i := 0; i < 2; i++ {
				if _, err := ev.Evaluate(context.Background(), req); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("err=%v want=%s", err, tc.want)
				}
			}
			if up.handshakes.Load() != 2 {
				t.Fatalf("failed connection reused or retried: %d", up.handshakes.Load())
			}
			if ev.idleCount != 0 {
				t.Fatal("failed evaluation retained a connection")
			}
		})
	}
}

func TestUpstreamValuesEvaluator_TimeoutCoversDialAndHandshake(t *testing.T) {
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"dial", "hello", "cancel"} {
		t.Run(stage, func(t *testing.T) {
			var calls atomic.Int32
			entered := make(chan struct{})
			ev := NewUpstreamValuesEvaluator(func(ctx context.Context, _ string) (net.Conn, error) {
				calls.Add(1)
				close(entered)
				if stage == "dial" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				c, s := net.Pipe()
				t.Cleanup(func() { s.Close() })
				return c, nil
			}, signer)
			defer ev.Close()
			req := evalRequest()
			req.Timeout = 3 * time.Second
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			if stage == "cancel" {
				ctx, cancel = context.WithCancel(context.Background())
				defer cancel()
				go func() { <-entered; cancel() }()
			}
			start := time.Now()
			if _, err := ev.Evaluate(ctx, req); err == nil {
				t.Fatal("expected deadline/cancellation")
			}
			if time.Since(start) > time.Second || calls.Load() != 1 {
				t.Fatalf("elapsed=%s calls=%d", time.Since(start), calls.Load())
			}
		})
	}
}

func TestUpstreamValuesEvaluator_CloseFencesActiveAndFutureDials(t *testing.T) {
	ready := make(chan struct{})
	release := make(chan struct{})
	ev, _, _ := newTestEvaluator(t, func(s *chproto.Codec, _ int) error { close(ready); <-release; return writeEndOfStream(s) })
	done := make(chan error, 1)
	go func() { _, err := ev.Evaluate(context.Background(), evalRequest()); done <- err }()
	<-ready
	if err := ev.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed active evaluation succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt active evaluation")
	}
	if _, err := ev.Evaluate(context.Background(), evalRequest()); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("err=%v", err)
	}
	ev.mu.Lock()
	defer ev.mu.Unlock()
	if ev.idleCount != 0 || len(ev.active) != 0 {
		t.Fatalf("idle=%d active=%d", ev.idleCount, len(ev.active))
	}
}

func TestUpstreamValuesEvaluator_CloseFencesInFlightDial(t *testing.T) {
	signer, _ := auth.NewRelaySigner(testKey)
	entered, release := make(chan struct{}), make(chan struct{})
	c, s := net.Pipe()
	defer s.Close()
	ev := NewUpstreamValuesEvaluator(func(context.Context, string) (net.Conn, error) { close(entered); <-release; return c, nil }, signer)
	done := make(chan error, 1)
	go func() { _, err := ev.Evaluate(context.Background(), evalRequest()); done <- err }()
	<-entered
	ev.Close()
	close(release)
	if err := <-done; err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("err=%v", err)
	}
	if ev.idleCount != 0 || len(ev.active) != 0 {
		t.Fatal("closed dial entered pool")
	}
}

func TestUpstreamValuesEvaluator_PoolIdentityAndTotalBound(t *testing.T) {
	ev, up, _ := newTestEvaluator(t, func(s *chproto.Codec, r int) error {
		if err := writeServerBlock(s, r, evalRow(1, false)); err != nil {
			return err
		}
		return writeEndOfStream(s)
	})
	var addresses []string
	dial := ev.dial
	ev.dial = func(ctx context.Context, address string) (net.Conn, error) {
		addresses = append(addresses, address)
		return dial(ctx, address)
	}
	req := evalRequest()
	for _, mutate := range []func(){func() {}, func() { req.UpstreamAddress = "peer-b:9000" }, func() { req.Hello.User = "second-user" }, func() { req.Hello.Database = "second-db" }, func() { req.Hello.Password = "new-credential" }, func() { req.Account = "second-account" }} {
		mutate()
		if _, err := ev.Evaluate(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if up.handshakes.Load() != 6 || addresses[0] != "selected:9000" || addresses[1] != "peer-b:9000" {
		t.Fatalf("handshakes=%d addresses=%v", up.handshakes.Load(), addresses)
	}
	for i := 0; i < maxTotalIdleEvaluatorConns+2; i++ {
		req.Hello.Database = fmt.Sprintf("db%d", i)
		if _, err := ev.Evaluate(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if ev.idleCount != maxTotalIdleEvaluatorConns || len(ev.idle) > maxTotalIdleEvaluatorConns {
		t.Fatalf("idle=%d keys=%d", ev.idleCount, len(ev.idle))
	}
}

func TestUpstreamValuesEvaluator_QuotingAndAllRevisionTiers(t *testing.T) {
	req := evalRequest()
	req.Schema.Columns = []lthash.Column{{Name: "a.b", Type: "UInt64"}, {Name: "a`b\\c'd", Type: "String"}}
	req.Columns = []string{"a.b", "a`b\\c'd"}
	req.Rows = "(1, 'x')"
	got, err := buildValuesQuery(req)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT `a.b`, `a``b\\\\c'd` FROM VALUES('`a.b` UInt64, `a``b\\\\\\\\c''d` String', (1, 'x'))"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	for _, revision := range []int{54429, 54454, 54460, 54470} {
		t.Run(fmt.Sprint(revision), func(t *testing.T) {
			ev, _, _ := newTestEvaluator(t, func(s *chproto.Codec, r int) error {
				if err := writeServerBlock(s, r, evalRow(1, false)); err != nil {
					return err
				}
				return writeEndOfStream(s)
			})
			req := evalRequest()
			req.Hello.ProtocolVersion = revision
			if _, err := ev.Evaluate(context.Background(), req); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUpstreamValuesEvaluator_Field3AndNonChunkedNegotiation(t *testing.T) {
	for _, mode := range []string{"chunked_optional", "chunked"} {
		t.Run(mode, func(t *testing.T) {
			ev, up, _ := newTestEvaluator(t, func(s *chproto.Codec, r int) error {
				var packet, body proto.Buffer
				packet.PutUVarInt(uint64(proto.ServerCodeData))
				packet.PutString("")
				if err := (proto.Block{Rows: 1, Columns: 2, Info: proto.BlockInfo{BucketNum: -1}}).EncodeBlock(&body, r, evalRow(1, false)); err != nil {
					return err
				}
				packet.Buf = append(packet.Buf, body.Buf[:7]...)
				packet.PutUVarInt(3)
				packet.PutUVarInt(1)
				packet.PutInt32(7)
				packet.PutUVarInt(0)
				packet.Buf = append(packet.Buf, body.Buf[8:]...)
				if err := s.WriteRawPacket(packet.Buf); err != nil {
					return err
				}
				return writeEndOfStream(s)
			})
			up.chunkMode = mode
			up.negotiated = make(chan chproto.AddendumResult, 1)
			req := evalRequest()
			req.Hello.ProtocolVersion = 54470
			blocks, err := ev.Evaluate(context.Background(), req)
			if err != nil || len(blocks) != 1 {
				t.Fatalf("blocks=%v err=%v", blocks, err)
			}
			res := <-up.negotiated
			if res.NegotiatedRecv != "notchunked" || res.NegotiatedSend != "notchunked" {
				t.Fatalf("unsafe bounded-framing negotiation: %+v", res)
			}
		})
	}
}

func TestUpstreamValuesEvaluator_RejectsDeclaredShapeBeforeSecondDecode(t *testing.T) {
	for _, tc := range []struct {
		columns, rows uint64
		want          string
	}{{2, 1000, "max_rows"}, {1000, 1, "columns"}} {
		var b proto.Buffer
		b.PutUVarInt(uint64(proto.ServerCodeData))
		b.PutString("")
		proto.BlockInfo{BucketNum: -1}.Encode(&b)
		b.PutUVarInt(tc.columns)
		b.PutUVarInt(tc.rows)
		if _, err := decodeServerDataColumns(b.Buf, testRevision, evalRequest()); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("shape %d/%d: %v", tc.columns, tc.rows, err)
		}
	}
}

// writeEvaluationControlBlock emits either the schema-only leading Data block
// or the protocol's zero-column, zero-row end-of-data marker.
func writeEvaluationControlBlock(srv *chproto.Codec, rev int, columns proto.Input) error {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeData))
	buf.PutString("")
	if err := (proto.Block{Columns: len(columns)}).EncodeBlock(&buf, rev, columns); err != nil {
		return err
	}
	return srv.WriteRawPacket(buf.Buf)
}

func TestUpstreamValuesEvaluator_CompleteSelectResponseReusesConnection(t *testing.T) {
	for _, progress := range []bool{false, true} {
		t.Run(fmt.Sprintf("progress=%v", progress), func(t *testing.T) {
			ev, up, _ := newTestEvaluator(t, func(srv *chproto.Codec, rev int) error {
				header := proto.Input{{Name: "id", Data: &proto.ColUInt64{}}, {Name: "ts", Data: &proto.ColDateTime{Location: time.UTC}}}
				if err := writeEvaluationControlBlock(srv, rev, header); err != nil {
					return err
				}
				for _, id := range []uint64{11, 22} {
					if err := writeServerBlock(srv, rev, evalRow(id, false)); err != nil {
						return err
					}
				}
				if err := writeEvaluationControlBlock(srv, rev, nil); err != nil {
					return err
				}
				if progress {
					var buf proto.Buffer
					buf.PutUVarInt(uint64(proto.ServerCodeProgress))
					// At testRevision (54460): three read counters, two write
					// counters, then elapsed nanoseconds.
					for i := 0; i < 6; i++ {
						buf.PutUVarInt(0)
					}
					if err := srv.WriteRawPacket(buf.Buf); err != nil {
						return err
					}
				}
				return writeEndOfStream(srv)
			})
			for query := 0; query < 2; query++ {
				blocks, err := ev.Evaluate(context.Background(), evalRequest())
				if err != nil {
					t.Fatalf("evaluation %d: %v", query, err)
				}
				if len(blocks) != 2 {
					t.Fatalf("evaluation %d returned %d blocks", query, len(blocks))
				}
				for i, want := range []uint64{11, 22} {
					if got := blocks[i][0].Data.(*proto.ColUInt64).Row(0); got != want {
						t.Fatalf("block %d id=%d want=%d", i, got, want)
					}
				}
			}
			if got := up.handshakes.Load(); got != 1 {
				t.Fatalf("two complete evaluations used %d handshakes", got)
			}
			if ev.idleCount != 1 {
				t.Fatalf("idle=%d", ev.idleCount)
			}
		})
	}
}

func TestUpstreamValuesEvaluator_EmptyDataMarkerIsNotEndOfStream(t *testing.T) {
	ev, _, _ := newTestEvaluator(t, func(srv *chproto.Codec, rev int) error {
		if err := writeServerBlock(srv, rev, evalRow(1, false)); err != nil {
			return err
		}
		if err := writeEvaluationControlBlock(srv, rev, nil); err != nil {
			return err
		}
		return srv.WriteException(&chproto.Exception{Code: 36, Name: "BAD_ARGUMENTS", Message: "failed after data marker"})
	})
	if _, err := ev.Evaluate(context.Background(), evalRequest()); err == nil || !strings.Contains(err.Error(), "failed after data marker") {
		t.Fatalf("err=%v", err)
	}
	if ev.idleCount != 0 {
		t.Fatal("connection reused without successful EndOfStream")
	}
}

func TestUpstreamValuesEvaluator_EmptyDataMarkerWithoutRowsIsRefused(t *testing.T) {
	ev, _, _ := newTestEvaluator(t, func(srv *chproto.Codec, rev int) error {
		if err := writeEvaluationControlBlock(srv, rev, nil); err != nil {
			return err
		}
		return writeEndOfStream(srv)
	})
	if _, err := ev.Evaluate(context.Background(), evalRequest()); err == nil || !strings.Contains(err.Error(), "no rows") {
		t.Fatalf("err=%v", err)
	}
}
