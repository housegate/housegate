package nativepayload

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func dateTime64RangeSchema(columnType string) payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: "tenant.dt64", Columns: []lthash.Column{
		{Name: "id", Type: "UInt32"},
		{Name: "c", Type: columnType},
	}}
}

func dateTime64RangePayload(t *testing.T, p proto.Precision, raws ...int64) []byte {
	t.Helper()
	ids := proto.ColUInt32{}
	col := new(proto.ColDateTime64).WithPrecision(p).WithLocation(time.UTC)
	for i, raw := range raws {
		ids = append(ids, uint32(i))
		col.Data = append(col.Data, proto.DateTime64(raw))
	}
	return encodeNativePayload(t, proto.Input{{Name: "id", Data: &ids}, {Name: "c", Data: col}})
}

func dateTime64TypeName(p proto.Precision) string {
	return "DateTime64(" + string(rune('0'+p)) + ", 'UTC')"
}

func dateTime64RawAt(t time.Time, p proto.Precision) int64 {
	return t.Unix()*(1e9/p.Scale()) + int64(t.Nanosecond())/p.Scale()
}

// TestDecodeForIntakeRefusesDateTime64OutsideInt64Nanoseconds: ClickHouse
// stores any DateTime64 tick count it receives over Native (measured on 25.8
// and 26.8, pkg/integration TestCHDateTime64NativeRawRange), but a count whose
// instant lies outside 1677-09-21..2262-04-11 cannot cross ch-go's
// int64-nanosecond decode and re-encode unchanged. Intake refuses it, at both
// ends, in any column (#343).
func TestDecodeForIntakeRefusesDateTime64OutsideInt64Nanoseconds(t *testing.T) {
	for _, p := range []proto.Precision{0, 3, 6, 8} {
		schema := dateTime64RangeSchema(dateTime64TypeName(p))
		maxRaw, minRaw := math.MaxInt64/p.Scale(), math.MinInt64/p.Scale()
		for _, raw := range []int64{
			minRaw, maxRaw, 0,
			dateTime64RawAt(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), p),
			dateTime64RawAt(time.Date(2262, 4, 11, 0, 0, 0, 0, time.UTC), p),
		} {
			if _, err := DecodeForIntake(schema, nativePayloadTestRevision, dateTime64RangePayload(t, p, raw)); err != nil {
				t.Errorf("P=%d raw %d: DecodeForIntake = %v, want admitted", p, raw, err)
			}
		}
		for _, raw := range []int64{
			minRaw - 1, maxRaw + 1,
			dateTime64RawAt(time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC), p),
			dateTime64RawAt(time.Date(2299, 12, 31, 23, 59, 59, 0, time.UTC), p),
		} {
			// The refused value is the second row, after an in-range one.
			_, err := DecodeForIntake(schema, nativePayloadTestRevision, dateTime64RangePayload(t, p, 0, raw))
			if !errors.Is(err, ErrDateTime64OutOfRange) {
				t.Fatalf("P=%d raw %d: DecodeForIntake = %v, want ErrDateTime64OutOfRange", p, raw, err)
			}
			if msg := err.Error(); !strings.Contains(msg, `column "c"`) || !strings.Contains(msg, "native row 1") {
				t.Errorf("P=%d raw %d: error %q does not name the column and row", p, raw, msg)
			}
		}
	}
	// DateTime64(9) is int64 nanoseconds: every tick count is representable.
	schema := dateTime64RangeSchema("DateTime64(9, 'UTC')")
	if _, err := DecodeForIntake(schema, nativePayloadTestRevision, dateTime64RangePayload(t, 9, math.MinInt64, math.MaxInt64)); err != nil {
		t.Errorf("DateTime64(9) extremes: DecodeForIntake = %v, want admitted", err)
	}
}

func TestDecodeForIntakeRefusesNullableDateTime64OutsideInt64Nanoseconds(t *testing.T) {
	schema := dateTime64RangeSchema("Nullable(DateTime64(3, 'UTC'))")
	// Row 0 is NULL (its nested tick count 0), row 1 carries raw.
	payload := func(raw int64) []byte {
		inner := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)
		inner.Data = append(inner.Data, 0, proto.DateTime64(raw))
		col := &proto.ColNullable[time.Time]{Nulls: proto.ColUInt8{1, 0}, Values: inner}
		ids := proto.ColUInt32{0, 1}
		return encodeNativePayload(t, proto.Input{{Name: "id", Data: &ids}, {Name: "c", Data: col}})
	}
	if _, err := DecodeForIntake(schema, nativePayloadTestRevision, payload(math.MaxInt64/1_000_000)); err != nil {
		t.Fatalf("in-range Nullable value: DecodeForIntake = %v", err)
	}
	if _, err := DecodeForIntake(schema, nativePayloadTestRevision, payload(math.MaxInt64/1_000_000+1)); !errors.Is(err, ErrDateTime64OutOfRange) {
		t.Fatalf("out-of-range Nullable value: DecodeForIntake = %v, want ErrDateTime64OutOfRange", err)
	}
}

// TestDecodeKeepsOutOfRangeDateTime64Semantics pins the replay-side decode the
// SNode source write, the verifier materializer and the byte-side scan share:
// it still admits an out-of-range value in an ordinary column and still hands
// back ch-go's wrapped instant. Changing either changes what the network
// commits for such a row, which only an executor-profile gate may do (#343).
func TestDecodeKeepsOutOfRangeDateTime64Semantics(t *testing.T) {
	schema := dateTime64RangeSchema("DateTime64(3, 'UTC')")
	raw := dateTime64RawAt(time.Date(2299, 12, 31, 23, 59, 59, 999000000, time.UTC), proto.PrecisionMilli)
	payload := dateTime64RangePayload(t, proto.PrecisionMilli, raw)
	rows, err := Decode(schema, nativePayloadTestRevision, payload)
	if err != nil {
		t.Fatalf("Decode = %v, want the replay lane to keep admitting the row", err)
	}
	got := rows[0].Values[1].(time.Time)
	if got.Year() != 1715 {
		t.Fatalf("Decode value = %s, want ch-go's wrapped 1715 instant", got)
	}
	if err := ValidateDecodable(schema, nativePayloadTestRevision, payload); err != nil {
		t.Fatalf("ValidateDecodable = %v, want admitted", err)
	}
}

func TestDecodeForIntakeMatchesDecodeInRange(t *testing.T) {
	schema := dateTime64RangeSchema("DateTime64(3, 'UTC')")
	payload := dateTime64RangePayload(t, proto.PrecisionMilli, 0, -1, math.MaxInt64/1_000_000, math.MinInt64/1_000_000)
	want, err := Decode(schema, nativePayloadTestRevision, payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeForIntake(schema, nativePayloadTestRevision, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("DecodeForIntake rows %d, Decode rows %d", len(got), len(want))
	}
	for i := range want {
		if !got[i].Values[1].(time.Time).Equal(want[i].Values[1].(time.Time)) || got[i].PartitionID != want[i].PartitionID || got[i].RawBytes != want[i].RawBytes {
			t.Fatalf("row %d: DecodeForIntake %+v, Decode %+v", i, got[i], want[i])
		}
	}
}
