// Package nativepayload decodes ClickHouse Native ClientData wire payloads
// (payload_format "clickhouse-native-data-v1": the concatenated de-chunked
// raw bytes of every non-empty client Data packet of one INSERT) into the
// shared payloadexec row model. It lives under pkg/replay so replay
// executors (chexec, arbiter-core snode/verifier) can import it without
// pulling in the ingress package.
package nativepayload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

const PayloadFormat = replay.PayloadFormatClickHouseNativeData

var ErrUnsupported = errors.New("unsupported native payload")

// Materializer materializes captured ClickHouse Native ClientData packets
// into the shared payloadexec row model. It owns only payload decoding and
// _hg_row_id injection; payloadexec.Executor owns part, partition, manifest,
// and state-root assembly.
type Materializer struct {
	NetworkID string
	Revision  int
}

func (m Materializer) Materialize(ctx context.Context, schema payloadexec.TableSchema, st replay.PreparedStatement) ([]payloadexec.Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.NetworkID == "" {
		return nil, fmt.Errorf("native materializer network_id is required")
	}
	if st.PayloadRef == "" {
		return nil, fmt.Errorf("native materializer only replays payload-local INSERTs; statement has no payload")
	}
	if st.TargetTableID != schema.TableID {
		return nil, fmt.Errorf("statement target table %q does not match pinned schema table %q", st.TargetTableID, schema.TableID)
	}
	if len(st.Payload) == 0 {
		return nil, fmt.Errorf("native payload is empty")
	}
	revision := m.Revision
	if st.ClientRevision != 0 {
		revision = int(st.ClientRevision)
	}
	rows, err := Decode(schema, revision, st.Payload)
	if err != nil {
		return nil, err
	}
	for ordinal := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows[ordinal].RowID = payloadexec.RowID(m.NetworkID, schema.TableID, st.StatementID, uint64(ordinal))
	}
	return rows, nil
}

// Decode parses concatenated ClickHouse Native ClientData packets
// against the pinned table schema. Returned rows are schema-ordered,
// partitioned, and carry deterministic logical Native value bytes. RowID is
// intentionally left unset for Materialize to inject with the statement
// identity.
func Decode(schema payloadexec.TableSchema, revision int, payload []byte) ([]payloadexec.Row, error) {
	if schema.TableID == "" {
		return nil, fmt.Errorf("table_id is required")
	}
	if revision == 0 {
		return nil, fmt.Errorf("client protocol revision is required")
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("native payload contains no rows")
	}
	if err := validateNativePinnedSchema(schema); err != nil {
		return nil, err
	}
	key, err := payloadexec.ParsePartitionKey(schema)
	if err != nil {
		return nil, err
	}

	var out []payloadexec.Row
	pr := proto.NewReader(bytes.NewReader(payload))
	for {
		code, err := pr.UVarInt()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("native packet code: %w", err)
		}
		block, err := decodeNativeDataBlock(pr, revision, code)
		if err != nil {
			return nil, err
		}
		if block.Rows == 0 {
			return nil, fmt.Errorf("native payload contains an embedded zero-row Data control packet")
		}
		colPos, err := nativeBlockColumnPositions(schema, block.Columns)
		if err != nil {
			return nil, err
		}
		for rowIndex := 0; rowIndex < block.Rows; rowIndex++ {
			wireValues, rawBytes, err := block.Row(rowIndex)
			if err != nil {
				return nil, err
			}
			values := make([]any, len(schema.Columns))
			for schemaPos, blockPos := range colPos {
				values[schemaPos] = wireValues[blockPos]
			}
			if key.IsPartitioned() {
				if err := block.partitionValueRepresentable(colPos[key.ColumnIndex()], rowIndex); err != nil {
					return nil, fmt.Errorf("native row %d: partition column %q: %w", len(out), key.Column(), err)
				}
			}
			partitionID, err := key.PartitionID(values)
			if err != nil {
				return nil, fmt.Errorf("native row %d: %w", len(out), err)
			}
			out = append(out, payloadexec.Row{Values: values, PartitionID: partitionID, RawBytes: rawBytes})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("native payload contains no rows")
	}
	return out, nil
}

// ValidateDecodable verifies that a Native payload can be decoded
// under the pinned table schema before callers emit any publication evidence.
func ValidateDecodable(schema payloadexec.TableSchema, revision int, payload []byte) error {
	if _, err := Decode(schema, revision, payload); err != nil {
		return fmt.Errorf("%w: %w", ErrUnsupported, err)
	}
	return nil
}

func validateNativePinnedSchema(schema payloadexec.TableSchema) error {
	seen := map[string]struct{}{}
	for _, col := range schema.Columns {
		if col.Name == "" {
			return fmt.Errorf("schema contains an unnamed column")
		}
		if col.Name == "_hg_row_id" {
			return fmt.Errorf("schema must not include reserved _hg_row_id")
		}
		if _, dup := seen[col.Name]; dup {
			return fmt.Errorf("schema contains duplicate column %q", col.Name)
		}
		seen[col.Name] = struct{}{}
	}
	return nil
}

func nativeBlockColumnPositions(schema payloadexec.TableSchema, columns []lthash.Column) ([]int, error) {
	if len(columns) != len(schema.Columns) {
		return nil, fmt.Errorf("native block has %d columns, schema has %d", len(columns), len(schema.Columns))
	}
	blockByName := make(map[string]int, len(columns))
	for i, col := range columns {
		if col.Name == "_hg_row_id" {
			return nil, fmt.Errorf("native payload must not contain reserved _hg_row_id")
		}
		if _, dup := blockByName[col.Name]; dup {
			return nil, fmt.Errorf("native block contains duplicate column %q", col.Name)
		}
		blockByName[col.Name] = i
	}
	positions := make([]int, len(schema.Columns))
	for schemaPos, want := range schema.Columns {
		blockPos, ok := blockByName[want.Name]
		if !ok {
			return nil, fmt.Errorf("native block missing schema column %q", want.Name)
		}
		// Compare against the wire type the column-type authority declares for
		// this schema type rather than against the schema spelling itself
		// (Spec Q Q-D1). Two consequences: a schema declaring a type outside the
		// admitted profile now fails with ErrUnsupportedColumnType instead of a
		// confusing string mismatch, and a family whose decoded ch-go column
		// reports something other than its declaration has one place to say so.
		wantProfile, err := payloadexec.ResolveColumnProfile(want.Type)
		if err != nil {
			return nil, fmt.Errorf("native block column %q: %w", want.Name, err)
		}
		got := columns[blockPos]
		if got.Type != wantProfile.NativeWireType {
			return nil, fmt.Errorf("native block column %q type %q does not match schema type %q (expected wire type %q)",
				want.Name, got.Type, want.Type, wantProfile.NativeWireType)
		}
		positions[schemaPos] = blockPos
	}
	return positions, nil
}

type nativeDataBlock struct {
	Columns []lthash.Column
	Rows    int
	cols    []proto.ColResult
}

func decodeNativeDataBlock(pr *proto.Reader, revision int, code uint64) (*nativeDataBlock, error) {
	if code != uint64(proto.ClientCodeData) {
		return nil, fmt.Errorf("packet type %d is not ClientData", code)
	}
	if _, err := pr.Str(); err != nil {
		return nil, fmt.Errorf("native block name: %w", err)
	}
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(pr, revision, results.Auto()); err != nil {
		return nil, fmt.Errorf("decode native block: %w", err)
	}
	out := &nativeDataBlock{Rows: block.Rows}
	for _, rc := range results {
		col := rc.Data
		typeName := ""
		if col != nil {
			typeName = string(col.Type())
		}
		out.Columns = append(out.Columns, lthash.Column{Name: rc.Name, Type: typeName})
		out.cols = append(out.cols, col)
	}
	return out, nil
}

// partitionValueRepresentable refuses a DateTime64 partition value whose
// instant is outside the int64-nanosecond range (1677-09-21 to 2262-04-11).
// ch-go's DateTime64.Time multiplies the raw value up to nanoseconds and wraps
// silently there, so the decoded time.Time, and the partition id derived from
// it, would name a partition ClickHouse does not hold the row in.
func (b *nativeDataBlock) partitionValueRepresentable(col, row int) error {
	c, ok := b.cols[col].(*proto.ColDateTime64)
	if !ok {
		return nil
	}
	raw := int64(c.Data[row])
	scale := c.Precision.Scale()
	if raw > math.MaxInt64/scale || raw < math.MinInt64/scale {
		return fmt.Errorf("DateTime64 value %d at precision %d is outside the representable nanosecond range", raw, c.Precision)
	}
	return nil
}

func (b *nativeDataBlock) Row(i int) ([]any, uint64, error) {
	if i < 0 || i >= b.Rows {
		return nil, 0, fmt.Errorf("row %d out of range (rows=%d)", i, b.Rows)
	}
	values := make([]any, len(b.cols))
	var rawBytes uint64
	for c, col := range b.cols {
		v, bytes, err := nativeColumnValue(col, i)
		if err != nil {
			return nil, 0, fmt.Errorf("column %q (%s): %w", b.Columns[c].Name, b.Columns[c].Type, err)
		}
		values[c] = v
		rawBytes += bytes
	}
	return values, rawBytes, nil
}

// nativeColumnValue returns the row value and its deterministic logical Native
// value byte count. The count feeds signed part metadata; it intentionally does
// not attempt to reconstruct packet framing or compression bytes.
func nativeColumnValue(col proto.ColResult, i int) (any, uint64, error) {
	switch c := col.(type) {
	case *proto.ColUInt8:
		return (*c)[i], 1, nil
	case *proto.ColUInt16:
		return (*c)[i], 2, nil
	case *proto.ColUInt32:
		return (*c)[i], 4, nil
	case *proto.ColUInt64:
		return (*c)[i], 8, nil
	case *proto.ColInt8:
		return (*c)[i], 1, nil
	case *proto.ColInt16:
		return (*c)[i], 2, nil
	case *proto.ColInt32:
		return (*c)[i], 4, nil
	case *proto.ColInt64:
		return (*c)[i], 8, nil
	case *proto.ColFloat32:
		return (*c)[i], 4, nil
	case *proto.ColFloat64:
		return (*c)[i], 8, nil
	case *proto.ColStr:
		row := c.Row(i)
		return row, uint64(len(row)), nil
	case *proto.ColFixedStr32:
		row := c.Row(i)
		return append([]byte(nil), row[:]...), 32, nil
	case *proto.ColBool:
		return (*c)[i], 1, nil
	case *proto.ColDate:
		return c.Row(i), 2, nil
	case *proto.ColDateTime:
		return c.Row(i).UTC(), 4, nil
	case *proto.ColDateTime64:
		return c.Row(i).UTC(), 8, nil
	// Admission rules v2 (Spec Q Phase 2 revision).
	case *proto.ColDate32:
		// ColDate32 values are always UTC midnight (ch-go's Date32.Time).
		return c.Row(i).UTC(), 4, nil
	case *proto.ColUUID:
		return c.Row(i), 16, nil
	case *proto.ColInt128:
		v := c.Row(i)
		return wideInt([]uint64{v.Low, v.High}, true), 16, nil
	case *proto.ColUInt128:
		v := c.Row(i)
		return wideInt([]uint64{v.Low, v.High}, false), 16, nil
	case *proto.ColInt256:
		v := c.Row(i)
		return wideInt([]uint64{v.Low.Low, v.Low.High, v.High.Low, v.High.High}, true), 32, nil
	case *proto.ColUInt256:
		v := c.Row(i)
		return wideInt([]uint64{v.Low.Low, v.Low.High, v.High.Low, v.High.High}, false), 32, nil
	case *proto.ColLowCardinality[string]:
		// The dictionary is a wire encoding only: the value and its logical
		// byte count are those of the String it stands for.
		row := c.Row(i)
		return row, uint64(len(row)), nil
	default:
		if nullable, ok := col.(interface{ IsElemNull(int) bool }); ok {
			return nullableColumnValue(col, nullable, i)
		}
		return nil, 0, fmt.Errorf("unsupported column type %T", col)
	}
}

// nullableColumnValue decodes one row of a ch-go ColNullable[T] through a
// generic seam rather than a per-instantiation type switch (Spec Q Q-D6):
// IsElemNull answers NULL, and the exported Values field — reached by one
// reflection lookup, since ColNullable has no non-generic accessor for it —
// recurses into nativeColumnValue, so every inner type the switch above
// decodes is decodable under Nullable without a case of its own. A NULL is
// the untyped nil and counts one logical byte (its null-map byte); a
// non-NULL value counts that byte plus the inner value's.
//
// The null-map byte must be exactly 0 or 1. ClickHouse reads any non-zero
// byte as NULL while ch-go's IsElemNull recognizes only 1, so a payload with
// another byte would commit a value where ClickHouse stores NULL; it is
// refused instead.
func nullableColumnValue(col proto.ColResult, nullable interface{ IsElemNull(int) bool }, i int) (any, uint64, error) {
	holder := reflect.ValueOf(col)
	if holder.Kind() != reflect.Pointer || holder.IsNil() || holder.Elem().Kind() != reflect.Struct {
		return nil, 0, fmt.Errorf("unsupported nullable column type %T", col)
	}
	nulls, ok := holder.Elem().FieldByName("Nulls").Interface().(proto.ColUInt8)
	if !ok || i >= len(nulls) {
		return nil, 0, fmt.Errorf("unsupported nullable column type %T", col)
	}
	switch nulls[i] {
	case 0:
	case 1:
		if !nullable.IsElemNull(i) {
			return nil, 0, fmt.Errorf("nullable column %T disagrees with its null map", col)
		}
		return nil, 1, nil
	default:
		return nil, 0, fmt.Errorf("nullable null-map byte %d at row %d is neither 0 nor 1", nulls[i], i)
	}
	field := holder.Elem().FieldByName("Values")
	if !field.IsValid() || !field.CanInterface() {
		return nil, 0, fmt.Errorf("unsupported nullable column type %T", col)
	}
	inner, ok := field.Interface().(proto.ColResult)
	if !ok {
		return nil, 0, fmt.Errorf("unsupported nullable column type %T", col)
	}
	if _, nested := inner.(interface{ IsElemNull(int) bool }); nested {
		return nil, 0, fmt.Errorf("unsupported nested nullable column type %T", col)
	}
	v, n, err := nativeColumnValue(inner, i)
	if err != nil {
		return nil, 0, err
	}
	return v, n + 1, nil
}

// wideInt converts little-endian 64-bit limbs to a *big.Int, as two's
// complement when signed.
func wideInt(limbs []uint64, signed bool) *big.Int {
	n := new(big.Int)
	for j := len(limbs) - 1; j >= 0; j-- {
		n.Lsh(n, 64)
		n.Or(n, new(big.Int).SetUint64(limbs[j]))
	}
	bits := uint(64 * len(limbs))
	if signed && limbs[len(limbs)-1]>>63 == 1 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), bits))
	}
	return n
}
