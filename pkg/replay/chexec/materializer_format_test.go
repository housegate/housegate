package chexec

import (
	"context"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// decodeRows is exercised without ClickHouse: it is the pure format branch
// in front of the scratch-table path.
func TestDecodeRowsBranchesOnPayloadFormat(t *testing.T) {
	schema := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	csv := replay.PreparedStatement{Statement: replay.Statement{StatementID: "0xabc:1:n", TargetTableID: "db.t", PayloadFormat: payloadexec.PayloadFormatCSVWithNames}, Payload: []byte("v\n1\n")}
	rows, err := decodeRows(context.Background(), schema, csv)
	if err != nil || len(rows) != 1 {
		t.Fatalf("csv branch: rows=%d err=%v", len(rows), err)
	}
	legacy := csv
	legacy.PayloadFormat = ""
	if _, err := decodeRows(context.Background(), schema, legacy); err == nil || !strings.Contains(err.Error(), "payload_format") {
		t.Fatalf("empty format must fail closed: %v", err)
	}
	native := csv
	native.PayloadFormat = "clickhouse-native-data-v1"
	native.ClientRevision = 0
	if _, err := decodeRows(context.Background(), schema, native); err == nil || !strings.Contains(err.Error(), "revision") {
		t.Fatalf("native branch without client_revision must fail closed: %v", err)
	}
	unknown := csv
	unknown.PayloadFormat = "future-v9"
	if _, err := decodeRows(context.Background(), schema, unknown); err == nil || !strings.Contains(err.Error(), "future-v9") {
		t.Fatalf("unknown format must fail closed naming the format: %v", err)
	}
}
