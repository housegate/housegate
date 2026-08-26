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
			partitionID, err := payloadexec.PartitionIDForRow(schema, values)
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
	default:
		return nil, 0, fmt.Errorf("unsupported column type %T", col)
	}
}
