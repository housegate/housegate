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
// them replays. Each row also records which gate stops it — the column-type
// profile, or nativeColumnValue having no case — so Spec Q Phase 2 has to move
// both halves together. Deleting a row is the visible evidence a capability
// landed.
func TestNativeDecoderRejectsUndecodableInferableTypes(t *testing.T) {
	for _, tc := range []struct {
		declared string
		column   func() proto.ColInput
		// profileAdmits records which of the two gates rejects this
		// declaration. Spec Q Q-D7 forbids the true case as a durable state:
		// the profile must never admit a width or shape the Native lane cannot
		// decode, because that trades a loud startup refusal for a late replay
		// failure.
		profileAdmits bool
		wantErr       string
	}{
		{
			declared:      "FixedString(16)",
			column:        func() proto.ColInput { c := new(proto.ColFixedStr16); c.Append([16]byte{'a'}); return c },
			profileAdmits: true,
			wantErr:       "unsupported column type *proto.ColFixedStr16",
		},
		{
			declared:      "FixedString(64)",
			column:        func() proto.ColInput { c := new(proto.ColFixedStr64); c.Append([64]byte{'a'}); return c },
			profileAdmits: true,
			wantErr:       "unsupported column type *proto.ColFixedStr64",
		},
		{
			declared: "Date32",
			column: func() proto.ColInput {
				c := new(proto.ColDate32)
				c.Append(time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC))
				return c
			},
			wantErr: `unsupported column type "Date32"`,
		},
		{
			declared: "UUID",
			column:   func() proto.ColInput { c := new(proto.ColUUID); c.Append(uuid.UUID{}); return c },
			wantErr:  `unsupported column type "UUID"`,
		},
		{
			declared: "Decimal(18, 4)",
			column:   func() proto.ColInput { c := new(proto.ColDecimal64); c.Append(proto.Decimal64(12345)); return c },
			// The ColAuto downcast that makes Decimal unreachable even once it
			// is admitted is pinned directly by
			// TestChGoDecodedColumnsReportTheirDeclaredType; here it is simply
			// outside the profile.
			wantErr: `unsupported column type "Decimal(18, 4)"`,
		},
		{
			declared: "Nullable(UInt64)",
			column: func() proto.ColInput {
				c := new(proto.ColUInt64).Nullable()
				c.Append(proto.NewNullable(uint64(1)))
				return c
			},
			wantErr: `unsupported column type "Nullable(UInt64)"`,
		},
	} {
		t.Run(tc.declared, func(t *testing.T) {
			schema := payloadexec.TableSchema{
				TableID: "tenant.capability",
				Columns: []lthash.Column{{Name: "c", Type: tc.declared}},
			}
			if got := payloadexec.SupportedColumnType(tc.declared); got != tc.profileAdmits {
				t.Fatalf("%s: SupportedColumnType = %v, want %v", tc.declared, got, tc.profileAdmits)
			}
			payload := encodeNativePayload(t, proto.Input{{Name: "c", Data: tc.column()}})
			_, err := Decode(schema, nativePayloadTestRevision, payload)
			if err == nil {
				t.Fatalf("%s: Decode = nil, want %q", tc.declared, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("%s: Decode = %v, want an error containing %q", tc.declared, err, tc.wantErr)
			}
			if !tc.profileAdmits && !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
				t.Fatalf("%s: Decode = %v, want it to unwrap to ErrUnsupportedColumnType", tc.declared, err)
			}
		})
	}
}
