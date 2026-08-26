package chexec

import (
	"context"
	"reflect"
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

// TestChexecAdmissionEqualsTheColumnProfile is the Spec Q Q-D1 guard for this
// package: chexec must admit exactly what the authority admits, no more and no
// less. Its own list used prefix matches, which accept parameter spellings the
// authority (and the decoders it describes) reject.
func TestChexecAdmissionEqualsTheColumnProfile(t *testing.T) {
	for _, v := range payloadexec.AdmittedColumnTypeVectors() {
		if !supportedColumnType(v) {
			t.Errorf("supportedColumnType(%q) = false, authority admits it", v)
		}
	}
	for _, v := range []string{
		"FixedString(17)", "FixedString(0)", "FixedString(x)", "FixedString(-1)",
		"DateTime64(10)", "DateTime64()", "DateTime('Not/AZone')", "DateTime()",
		"Date32", "Nullable(String)", "Array(UInt64)", "IPv4", "Int128",
	} {
		if supportedColumnType(v) != payloadexec.SupportedColumnType(v) {
			t.Errorf("chexec and authority disagree on %q: chexec=%v authority=%v",
				v, supportedColumnType(v), payloadexec.SupportedColumnType(v))
		}
	}
}

// TestChexecScanDestMatchesTheProfileGoType proves the read-back destination is
// the Go type the authority declares, so ClickHouse read-back and Native decode
// feed lthash identical value types for the same row.
func TestChexecScanDestMatchesTheProfileGoType(t *testing.T) {
	for _, v := range payloadexec.AdmittedColumnTypeVectors() {
		p, err := payloadexec.ResolveColumnProfile(v)
		if err != nil {
			t.Fatalf("ResolveColumnProfile(%q): %v", v, err)
		}
		dest, err := newScanDest(v)
		if err != nil {
			t.Fatalf("newScanDest(%q): %v", v, err)
		}
		if got := reflect.TypeOf(dest).Elem(); got != p.GoType {
			t.Errorf("newScanDest(%q) -> *%s, profile GoType %s", v, got, p.GoType)
		}
	}
	for _, v := range []string{"FixedString(0)", "DateTime64(10)", "Nullable(String)", "IPv4"} {
		if _, err := newScanDest(v); err == nil {
			t.Errorf("newScanDest(%q) = nil error, want a rejection: the authority does not admit it", v)
		}
	}
}
