package nativepayload

import (
	"errors"
	"fmt"
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
			declared: "Date32",
			column: func() proto.ColInput {
				c := new(proto.ColDate32)
				c.Append(time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC))
				return c
			},
			decoderErr: "unsupported column type *proto.ColDate32",
		},
		{
			declared:   "UUID",
			column:     func() proto.ColInput { c := new(proto.ColUUID); c.Append(uuid.UUID{}); return c },
			decoderErr: "unsupported column type *proto.ColUUID",
		},
		{
			declared: "Decimal(18, 4)",
			column:   func() proto.ColInput { c := new(proto.ColDecimal64); c.Append(proto.Decimal64(12345)); return c },
			// Decimal has a second obstacle beyond the missing decoder case: the
			// ColAuto downcast that erases its declared precision and scale,
			// pinned directly by TestChGoDecodedColumnsReportTheirDeclaredType.
			decoderErr: "unsupported column type *proto.ColDecimal64",
		},
		{
			declared: "Nullable(UInt64)",
			column: func() proto.ColInput {
				c := new(proto.ColUInt64).Nullable()
				c.Append(proto.NewNullable(uint64(1)))
				return c
			},
			decoderErr: "unsupported column type *proto.ColNullable[uint64]",
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
