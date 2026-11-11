package integration

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func dateTime64Raw(t time.Time, precision proto.Precision) int64 {
	ticksPerSecond := int64(1e9) / precision.Scale()
	return t.Unix()*ticksPerSecond + int64(t.Nanosecond())/precision.Scale()
}

// insertNativeRaw sends one INSERT whose single Data block carries cols
// exactly as encoded, straight to ClickHouse over the native protocol, so a
// DateTime64 tick count reaches the server without passing through any Go
// time.Time conversion.
func insertNativeRaw(t *testing.T, table string, cols proto.Input) error {
	t.Helper()
	raw, err := net.DialTimeout("tcp", chEnv.Addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(30 * time.Second))
	codec := chproto.NewCodec(raw, chproto.DirToUpstream)
	hello := &chproto.ClientHello{
		Name: "dt64-raw", Major: 25, Minor: 8,
		ProtocolVersion: chproto.MaxSupportedRevision,
		Database:        chEnv.Database, User: chEnv.User, Password: chEnv.Password,
	}
	if err := codec.WriteClientHello(hello); err != nil {
		t.Fatal(err)
	}
	codec.SetServerHelloRevisionHint(int(hello.ProtocolVersion))
	pkt, err := codec.ReadPacket(uint64(chproto.ServerHelloCode), uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("read server hello: %v", err)
	}
	srv, ok := pkt.Decoded.(*chproto.ServerHello)
	if !ok {
		t.Fatalf("server hello = %T %+v", pkt.Decoded, pkt.Decoded)
	}
	rev := min(int(hello.ProtocolVersion), int(srv.Revision))
	codec.SetRevision(rev)
	if err := codec.SendAddendum(codec.ResolveUpstreamAddendum(chproto.AddendumResult{}, chproto.AddendumOpts{})); err != nil {
		t.Fatal(err)
	}
	if err := codec.WriteQuery(&chproto.Query{
		ID:   fmt.Sprintf("dt64-raw-%d", time.Now().UnixNano()),
		Body: "INSERT INTO " + table + " VALUES",
		Info: proto.ClientInfo{
			ProtocolVersion: rev, Major: 25, Minor: 8,
			Interface: proto.InterfaceTCP, Query: proto.ClientQueryInitial,
			InitialAddress: "127.0.0.1:0", ClientHostname: "dt64-raw", ClientName: "dt64-raw",
		},
	}); err != nil {
		t.Fatal(err)
	}
	var empty proto.Buffer
	empty.PutUVarInt(uint64(chproto.ClientDataCode))
	empty.PutString("")
	(&proto.Block{}).EncodeAware(&empty, rev)
	// The query's external-tables terminator, then wait for the sample block.
	if err := codec.WriteRawPacket(empty.Buf); err != nil {
		t.Fatal(err)
	}
	for {
		pkt, err := codec.ReadPacket(uint64(chproto.ServerExceptionCode))
		if err != nil {
			t.Fatalf("read insert header: %v", err)
		}
		if exc, ok := pkt.Decoded.(*chproto.Exception); ok {
			return fmt.Errorf("code=%d %s", exc.Code, exc.Message)
		}
		if pkt.Type == uint64(chproto.ServerDataCode) {
			break
		}
	}
	data, err := nativepayload.EncodeClientDataPacket(rev, cols)
	if err != nil {
		t.Fatal(err)
	}
	if err := codec.WriteRawPacket(data); err != nil {
		t.Fatal(err)
	}
	if err := codec.WriteRawPacket(empty.Buf); err != nil {
		t.Fatal(err)
	}
	for {
		pkt, err := codec.ReadPacket(uint64(chproto.ServerExceptionCode))
		if err != nil {
			t.Fatalf("read insert result: %v", err)
		}
		if exc, ok := pkt.Decoded.(*chproto.Exception); ok {
			return fmt.Errorf("code=%d %s", exc.Code, exc.Message)
		}
		if pkt.Type == uint64(chproto.ServerEndOfStreamCode) {
			return nil
		}
	}
}

// TestCHDateTime64NativeRawRange measures what ClickHouse does with a
// DateTime64 tick count outside the int64-nanosecond range (1677-09-21 to
// 2262-04-11) sent over Native, and pins the intake refusal against it (#343).
// Measured on 25.8 and 26.8: both store every tick count exactly as sent, at
// both ends and well past ClickHouse's own documented 1900..2299 range (25.8
// clamps only the rendering, 26.8 renders the true instant). So ClickHouse
// never refuses such a value; DecodeForIntake must, at both bounds, while the
// boundary tick counts themselves and DateTime64(9) stay admitted.
func TestCHDateTime64NativeRawRange(t *testing.T) {
	conn := openDirectCH(t)
	db := "hg_dt64_raw_range"
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })
	for _, p := range []proto.Precision{0, 3, 6, 8, 9} {
		maxRaw := int64(math.MaxInt64) / p.Scale()
		minRaw := int64(math.MinInt64) / p.Scale()
		cases := []struct {
			name     string
			raw      int64
			admitted bool
		}{
			{"int64-ns-min", minRaw, true},
			{"int64-ns-max", maxRaw, true},
			{"1900", dateTime64Raw(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), p), true},
		}
		if p < 9 {
			cases = append(cases, []struct {
				name     string
				raw      int64
				admitted bool
			}{
				{"int64-ns-min-1", minRaw - 1, false},
				{"int64-ns-max+1", maxRaw + 1, false},
				{"1600", dateTime64Raw(time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC), p), false},
				{"1899", dateTime64Raw(time.Date(1899, 12, 31, 0, 0, 0, 0, time.UTC), p), true},
				{"2299", dateTime64Raw(time.Date(2299, 12, 31, 23, 59, 59, 0, time.UTC), p), false},
				{"3000", dateTime64Raw(time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC), p), false},
			}...)
		}
		typeName := fmt.Sprintf("DateTime64(%d, 'UTC')", p)
		table := fmt.Sprintf("%s.p%d", db, p)
		mustExec(t, conn, fmt.Sprintf("CREATE TABLE %s (id UInt32, c %s) ENGINE = MergeTree ORDER BY id", table, typeName))
		schema := payloadexec.TableSchema{TableID: table, Columns: []lthash.Column{{Name: "id", Type: "UInt32"}, {Name: "c", Type: typeName}}}
		for i, tc := range cases {
			ids := proto.ColUInt32{uint32(i)}
			col := new(proto.ColDateTime64).WithPrecision(p).WithLocation(time.UTC)
			col.Data = append(col.Data, proto.DateTime64(tc.raw))
			input := proto.Input{{Name: "id", Data: &ids}, {Name: "c", Data: col}}
			if err := insertNativeRaw(t, table, input); err != nil {
				t.Fatalf("P=%d %s: ClickHouse refused tick count %d: %v", p, tc.name, tc.raw, err)
			}
			var stored int64
			var text string
			if err := conn.QueryRow(context.Background(), fmt.Sprintf("SELECT reinterpretAsInt64(c), toString(c) FROM %s WHERE id = %d", table, i)).Scan(&stored, &text); err != nil {
				t.Fatalf("P=%d %s read back: %v", p, tc.name, err)
			}
			t.Logf("P=%d %-14s sent=%d stored=%d rendered=%s", p, tc.name, tc.raw, stored, text)
			if stored != tc.raw {
				t.Errorf("P=%d %s: ClickHouse stored %d, sent %d", p, tc.name, stored, tc.raw)
			}
			payload, err := nativepayload.EncodeClientDataPacket(54465, input)
			if err != nil {
				t.Fatal(err)
			}
			_, err = nativepayload.DecodeForIntake(schema, 54465, payload)
			if tc.admitted && err != nil {
				t.Errorf("P=%d %s: DecodeForIntake = %v, want admitted", p, tc.name, err)
			}
			if !tc.admitted && !errors.Is(err, nativepayload.ErrDateTime64OutOfRange) {
				t.Errorf("P=%d %s: DecodeForIntake = %v, want ErrDateTime64OutOfRange", p, tc.name, err)
			}
		}
	}
}

// TestCHDateTime64OutOfRangeRewrittenByDecodeAndAppend records why intake
// refuses (#343): the replay-side write path, Native decode followed by a
// clickhouse-go batch Append (the SNode source write and the verifier
// materializer both do this), turns a 2299 tick count into a 1715 one. The
// stored value is in range, so every replica then reads and commits the same
// wrong row: consistent across nodes, but not the row the client sent.
func TestCHDateTime64OutOfRangeRewrittenByDecodeAndAppend(t *testing.T) {
	conn := openDirectCH(t)
	db := "hg_dt64_rewrite"
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })
	table := db + ".t"
	mustExec(t, conn, "CREATE TABLE "+table+" (id UInt32, c DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id")
	schema := payloadexec.TableSchema{TableID: table, Columns: []lthash.Column{{Name: "id", Type: "UInt32"}, {Name: "c", Type: "DateTime64(3, 'UTC')"}}}

	sent := time.Date(2299, 12, 31, 23, 59, 59, 999000000, time.UTC).UnixMilli()
	ids := proto.ColUInt32{1}
	col := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)
	col.Data = append(col.Data, proto.DateTime64(sent))
	payload, err := nativepayload.EncodeClientDataPacket(54465, proto.Input{{Name: "id", Data: &ids}, {Name: "c", Data: col}})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := nativepayload.Decode(schema, 54465, payload)
	if err != nil {
		t.Fatalf("replay-side Decode = %v, want it to keep admitting the row", err)
	}
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+table)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(rows[0].Values...); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	var stored int64
	var text string
	if err := conn.QueryRow(context.Background(), "SELECT reinterpretAsInt64(c), toString(c) FROM "+table).Scan(&stored, &text); err != nil {
		t.Fatal(err)
	}
	t.Logf("sent %d (2299-12-31 23:59:59.999), stored %d (%s)", sent, stored, text)
	// 25.8 renders it clamped to 1900; the tick count is what matters.
	if got := time.UnixMilli(stored).UTC(); stored == sent || got.Year() != 1715 {
		t.Fatalf("stored %d (%s, rendered %s); want the 1715 rewrite of %d", stored, got, text, sent)
	}
	if _, err := nativepayload.DecodeForIntake(schema, 54465, payload); !errors.Is(err, nativepayload.ErrDateTime64OutOfRange) {
		t.Fatalf("DecodeForIntake = %v, want ErrDateTime64OutOfRange", err)
	}
}
