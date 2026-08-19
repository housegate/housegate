package integration

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func TestReplayCHExecutorNativeTemporalScalarsMatchInProcessRoot(t *testing.T) {
	conn := openDirectCH(t)
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("load non-UTC test location: %v", err)
	}
	ctx := clickhouse.Context(context.Background(), clickhouse.WithUserLocation(shanghai))
	tableID := uniqueTable(t)
	schema := payloadexec.TableSchema{
		TableID: tableID,
		Columns: []lthash.Column{
			{Name: "day", Type: "Date"},
			{Name: "second", Type: "DateTime('UTC')"},
			{Name: "millisecond", Type: "DateTime64(3, 'UTC')"},
		},
	}
	chE := payloadexec.NewWithMaterializer(chReplayNetwork, chexec.NewMaterializer(chReplayNetwork, conn), schema)
	inE := payloadexec.NewWithMaterializer(chReplayNetwork, nativepayload.Materializer{NetworkID: chReplayNetwork}, schema)
	genCH, err := chE.GenesisSnapshot(0, "schema-1", "ch-26.x")
	if err != nil {
		t.Fatal(err)
	}
	genIn, err := inE.GenesisSnapshot(0, "schema-1", "ch-26.x")
	if err != nil {
		t.Fatal(err)
	}

	instant := time.Date(2026, time.July, 16, 12, 34, 56, 123000000, time.UTC)
	date := new(proto.ColDate)
	date.Append(instant)
	dateTime := &proto.ColDateTime{Location: time.UTC}
	dateTime.Append(instant)
	dateTime64 := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)
	dateTime64.Append(instant)
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: 1, Columns: 3}).EncodeBlock(&buf, 54460, proto.Input{
		{Name: "day", Data: date},
		{Name: "second", Data: dateTime},
		{Name: "millisecond", Data: dateTime64},
	}); err != nil {
		t.Fatalf("encode temporal Native payload: %v", err)
	}
	payload := append([]byte(nil), buf.Buf...)
	job := chReplayJob(genCH, tableID, "stmt-native-time-1", "probe", payload, "")
	job.Statements[0].SQL = "INSERT INTO " + tableID + " FORMAT Native"
	job.Statements[0].SQLHash = replay.DigestString(job.Statements[0].SQL)
	job.Statements[0].PayloadFormat = nativepayload.PayloadFormat
	job.Statements[0].ClientRevision = 54460
	prepared := []replay.PreparedStatement{{Statement: job.Statements[0], Payload: payload}}
	_, chRes, err := chE.ApplyContext(ctx, genCH, job, prepared)
	if err != nil {
		t.Fatalf("chexec temporal Native: %v", err)
	}
	_, inRes, err := inE.ApplyContext(context.Background(), genIn, job, prepared)
	if err != nil {
		t.Fatalf("in-process temporal Native: %v", err)
	}
	if chRes.ComputedStateRoot != inRes.ComputedStateRoot {
		t.Fatalf("temporal Native executor equivalence broken:\n  ch: %s\n  in: %s", chRes.ComputedStateRoot, inRes.ComputedStateRoot)
	}
}
