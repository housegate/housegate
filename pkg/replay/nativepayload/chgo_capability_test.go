package nativepayload

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// This file pins the ch-go column-capability surface Spec Q's design rests on.
// Every result here is a property of the pinned fork
// (github.com/sentioxyz/ch-go v0.73.0-sentioxyz-20260629), not of housegate: a
// fork bump can silently change any of them, and the column profile in
// pkg/replay/payloadexec derives its admitted set from exactly these facts.

// TestChGoInfersFixedStringOnlyAtGeneratedWidths pins Spec Q measurement M1.
// proto.ColAuto.Infer resolves FixedString only at the seven widths
// inferGenerated enumerates (proto/col_auto_gen.go); every other width fails
// inference outright. Widening this set is a ch-go change, not a housegate
// change, and Spec Q Q-D7's Phase 2 widening targets exactly this membership.
func TestChGoInfersFixedStringOnlyAtGeneratedWidths(t *testing.T) {
	for _, w := range []int{8, 16, 32, 64, 128, 256, 512} {
		var c proto.ColAuto
		if err := c.Infer(proto.ColumnType(fmt.Sprintf("FixedString(%d)", w))); err != nil {
			t.Fatalf("FixedString(%d): Infer = %v, want nil", w, err)
		}
	}
	for _, w := range []int{1, 4, 10, 17, 33, 255, 1000} {
		var c proto.ColAuto
		if err := c.Infer(proto.ColumnType(fmt.Sprintf("FixedString(%d)", w))); err == nil {
			t.Fatalf("FixedString(%d): Infer = nil, want an inference error", w)
		}
	}
}

// TestChGoDecodedColumnsReportTheirDeclaredType pins measurements M2 and M4:
// which declarations survive inference with their spelling intact, and which
// ones ColAuto downcasts. The Decimal rows are the load-bearing ones — the
// decoded column loses the declared precision and scale, which is why
// nativeBlockColumnPositions cannot admit a Decimal payload today.
func TestChGoDecodedColumnsReportTheirDeclaredType(t *testing.T) {
	for _, tc := range []struct{ declared, reported string }{
		{"Date", "Date"},
		{"Date32", "Date32"},
		{"DateTime", "DateTime"},
		{"DateTime('UTC')", "DateTime('UTC')"},
		{"DateTime64(3)", "DateTime64(3)"},
		{"DateTime64(3, 'UTC')", "DateTime64(3, 'UTC')"},
		{"UUID", "UUID"},
		// M4: the decoded column loses the declared precision and scale.
		{"Decimal(9, 2)", "Decimal32"},
		{"Decimal(18, 4)", "Decimal64"},
		{"Decimal(18, 2)", "Decimal64"},
		{"Decimal(38, 4)", "Decimal128"},
		{"Decimal(76, 10)", "Decimal256"},
		{"Nullable(UInt64)", "Nullable(UInt64)"},
		{"Nullable(Decimal(38, 4))", "Nullable(Decimal128)"},
	} {
		var c proto.ColAuto
		if err := c.Infer(proto.ColumnType(tc.declared)); err != nil {
			t.Fatalf("%s: Infer = %v", tc.declared, err)
		}
		if got := string(c.Data.Type()); got != tc.reported {
			t.Fatalf("%s: decoded column reports %q, want %q", tc.declared, got, tc.reported)
		}
	}
}

// TestChGoNullableSeamShape pins measurement M3: IsElemNull is reachable by a
// plain non-generic interface assertion, while the inner Values column is
// reachable only through one exported-field reflection lookup, because
// ColNullable[T].Values is a generic field with no non-generic accessor. Spec Q
// Q-D6's decoder seam depends on both halves.
func TestChGoNullableSeamShape(t *testing.T) {
	for _, declared := range []string{"Nullable(UInt64)", "Nullable(String)", "Nullable(DateTime64(3, 'UTC'))"} {
		var c proto.ColAuto
		if err := c.Infer(proto.ColumnType(declared)); err != nil {
			t.Fatalf("%s: Infer = %v", declared, err)
		}
		if _, ok := c.Data.(interface{ IsElemNull(int) bool }); !ok {
			t.Fatalf("%s: decoded column does not expose IsElemNull(int) bool", declared)
		}
		field := reflect.ValueOf(c.Data).Elem().FieldByName("Values")
		if !field.IsValid() || !field.CanInterface() {
			t.Fatalf("%s: ColNullable.Values is not reachable by reflection", declared)
		}
		if _, ok := field.Interface().(proto.ColResult); !ok {
			t.Fatalf("%s: ColNullable.Values is not a proto.ColResult", declared)
		}
	}
}

// TestNativeDecoderRejectsUndecodableInferableTypes is the honest statement of
// the Native lane's reach: every declaration here infers in ch-go and none of
// them replays. It states both halves for every row — the column-type profile
// refuses the declaration, and nativeColumnValue independently has no case for
// the column ch-go inferred — which is Spec Q Q-D7's durable invariant that the
// validator is never wider than the decoder. Phase 2 has to move both halves
// together; deleting a row is the visible evidence a capability landed.
func TestNativeDecoderRejectsUndecodableInferableTypes(t *testing.T) {
	for _, tc := range []struct {
		declared string
		column   func() proto.ColInput
		// decoderErr is what nativeColumnValue says about the decoded column
		// itself. Both halves are asserted for every row: the column-type
		// profile must reject the declaration (Spec Q Q-D7 — the validator may
		// never be wider than the decoder), and the decoder must independently
		// still have no case for it. Phase 2 has to move both together.
		decoderErr string
	}{
		{
			declared:   "FixedString(16)",
			column:     func() proto.ColInput { c := new(proto.ColFixedStr16); c.Append([16]byte{'a'}); return c },
			decoderErr: "unsupported column type *proto.ColFixedStr16",
		},
		{
			declared:   "FixedString(64)",
			column:     func() proto.ColInput { c := new(proto.ColFixedStr64); c.Append([64]byte{'a'}); return c },
			decoderErr: "unsupported column type *proto.ColFixedStr64",
		},
		{
			declared: "Decimal(18, 4)",
			column:   func() proto.ColInput { c := new(proto.ColDecimal64); c.Append(proto.Decimal64(12345)); return c },
			// Decimal has a second obstacle beyond the missing decoder case: the
			// ColAuto downcast that erases its declared precision and scale,
			// pinned directly by TestChGoDecodedColumnsReportTheirDeclaredType.
			decoderErr: "unsupported column type *proto.ColDecimal64",
		},
	} {
		t.Run(tc.declared, func(t *testing.T) {
			schema := payloadexec.TableSchema{
				TableID: "tenant.capability",
				Columns: []lthash.Column{{Name: "c", Type: tc.declared}},
			}
			// Half one, Spec Q Q-D7: the profile is never wider than the
			// decoder, so the declaration is refused before any payload runs.
			if payloadexec.SupportedColumnType(tc.declared) {
				t.Errorf("%s decodes nowhere but is admitted by the profile — Q-D7 forbids exactly this", tc.declared)
			}
			payload := encodeNativePayload(t, proto.Input{{Name: "c", Data: tc.column()}})
			_, err := Decode(schema, nativePayloadTestRevision, payload)
			if err == nil {
				t.Fatalf("%s: Decode = nil, want a rejection", tc.declared)
			}
			if !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
				t.Fatalf("%s: Decode = %v, want it to unwrap to ErrUnsupportedColumnType", tc.declared, err)
			}

			// Half two: the decoder independently still has no case for the
			// column ch-go inferred, so the profile is not merely masking a
			// capability that has quietly arrived.
			decoded, ok := tc.column().(proto.ColResult)
			if !ok {
				t.Fatalf("%s: sample column is not a proto.ColResult", tc.declared)
			}
			if _, _, err := nativeColumnValue(decoded, 0); err == nil {
				t.Fatalf("%s: nativeColumnValue now decodes it — Q-D7 wants the profile widened with it", tc.declared)
			} else if !strings.Contains(err.Error(), tc.decoderErr) {
				t.Fatalf("%s: nativeColumnValue = %v, want an error containing %q", tc.declared, err, tc.decoderErr)
			}
		})
	}
}

// TestNativeDecoderDecodesV2Types is the other half of Q-D7 for the types
// admission rules v2 admits: each declaration the profile now admits decodes
// on the Native lane to exactly the profile's Go value type, NULL decodes to
// the untyped nil under Nullable(T) only, and the decoded rows hash.
func TestNativeDecoderDecodesV2Types(t *testing.T) {
	day := time.Date(1950, time.January, 2, 0, 0, 0, 0, time.UTC)
	id := uuid.MustParse("61f0c404-5cb3-11e7-907b-a6006ad3dba0")
	minI128 := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	maxU256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	for _, tc := range []struct {
		declared string
		column   func() proto.ColInput
		want     []any
	}{
		{"Date32", func() proto.ColInput { c := new(proto.ColDate32); c.Append(day); return c }, []any{day}},
		{"UUID", func() proto.ColInput { c := new(proto.ColUUID); c.Append(id); return c }, []any{id}},
		{"Int128", func() proto.ColInput {
			c := new(proto.ColInt128)
			c.Append(proto.Int128{Low: 0, High: 1 << 63})
			return c
		}, []any{minI128}},
		{"UInt128", func() proto.ColInput { c := new(proto.ColUInt128); c.Append(proto.UInt128{Low: 7}); return c }, []any{big.NewInt(7)}},
		{"Int256", func() proto.ColInput {
			c := new(proto.ColInt256)
			c.Append(proto.Int256{Low: proto.UInt128{Low: ^uint64(0), High: ^uint64(0)}, High: proto.UInt128{Low: ^uint64(0), High: ^uint64(0)}})
			return c
		}, []any{big.NewInt(-1)}},
		{"UInt256", func() proto.ColInput {
			c := new(proto.ColUInt256)
			c.Append(proto.UInt256{Low: proto.UInt128{Low: ^uint64(0), High: ^uint64(0)}, High: proto.UInt128{Low: ^uint64(0), High: ^uint64(0)}})
			return c
		}, []any{maxU256}},
		{"LowCardinality(String)", func() proto.ColInput {
			c := proto.NewLowCardinality(new(proto.ColStr))
			c.AppendArr([]string{"eu", "us", "eu", ""})
			return c
		}, []any{"eu", "us", "eu", ""}},
		{"Nullable(UInt64)", func() proto.ColInput {
			c := new(proto.ColUInt64).Nullable()
			c.AppendArr([]proto.Nullable[uint64]{proto.NewNullable[uint64](1), proto.Null[uint64](), proto.NewNullable[uint64](0)})
			return c
		}, []any{uint64(1), nil, uint64(0)}},
		{"Nullable(String)", func() proto.ColInput {
			c := new(proto.ColStr).Nullable()
			c.AppendArr([]proto.Nullable[string]{proto.Null[string](), proto.NewNullable("")})
			return c
		}, []any{nil, ""}},
		{"Nullable(DateTime64(3, 'UTC'))", func() proto.ColInput {
			c := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC).Nullable()
			c.AppendArr([]proto.Nullable[time.Time]{proto.NewNullable(time.UnixMilli(1700000000123).UTC()), proto.Null[time.Time]()})
			return c
		}, []any{time.UnixMilli(1700000000123).UTC(), nil}},
		{"Nullable(UUID)", func() proto.ColInput {
			c := new(proto.ColUUID).Nullable()
			c.AppendArr([]proto.Nullable[uuid.UUID]{proto.Null[uuid.UUID](), proto.NewNullable(id)})
			return c
		}, []any{nil, id}},
		{"Nullable(Int128)", func() proto.ColInput {
			c := new(proto.ColInt128).Nullable()
			c.AppendArr([]proto.Nullable[proto.Int128]{proto.NewNullable(proto.Int128{Low: 5}), proto.Null[proto.Int128]()})
			return c
		}, []any{big.NewInt(5), nil}},
	} {
		t.Run(tc.declared, func(t *testing.T) {
			schema := payloadexec.TableSchema{TableID: "tenant.capability", Columns: []lthash.Column{{Name: "c", Type: tc.declared}}}
			profile, err := payloadexec.ResolveColumnProfile(tc.declared)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := Decode(schema, nativePayloadTestRevision, encodeNativePayload(t, proto.Input{{Name: "c", Data: tc.column()}}))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if len(rows) != len(tc.want) {
				t.Fatalf("rows = %d, want %d", len(rows), len(tc.want))
			}
			for i, row := range rows {
				got := row.Values[0]
				if !profile.AcceptsValue(got) {
					t.Fatalf("row %d: %T is outside the profile (%s)", i, got, profile.GoType)
				}
				if !reflect.DeepEqual(got, tc.want[i]) {
					t.Fatalf("row %d = %#v, want %#v", i, got, tc.want[i])
				}
				if _, err := lthash.RowHash(schema.TableID, schema.Columns, row.Values); err != nil {
					t.Fatalf("row %d does not hash: %v", i, err)
				}
			}
		})
	}
}

// TestNativeDecoderRefusesNoncanonicalNullMapBytes: ClickHouse reads any
// non-zero null-map byte as NULL, ch-go only 1, so a byte other than 0 or 1
// would commit a value where ClickHouse stores NULL; the decoder refuses it.
func TestNativeDecoderRefusesNoncanonicalNullMapBytes(t *testing.T) {
	schema := payloadexec.TableSchema{TableID: "tenant.nulls", Columns: []lthash.Column{{Name: "c", Type: "Nullable(UInt64)"}}}
	for _, b := range []uint8{2, 0xff} {
		c := new(proto.ColUInt64).Nullable()
		c.Append(proto.NewNullable[uint64](42))
		c.Nulls[0] = b
		if rows, err := Decode(schema, nativePayloadTestRevision, encodeNativePayload(t, proto.Input{{Name: "c", Data: c}})); err == nil {
			t.Fatalf("null-map byte %d decoded to %#v, want a refusal", b, rows[0].Values)
		}
	}
}
