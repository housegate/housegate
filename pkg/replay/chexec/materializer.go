// Package chexec is the ClickHouse-backed materializer for the replay verifier
// (design Appendix C.3). It plugs into payloadexec.Executor via the Materializer
// seam: for each payload-local INSERT it executes the statement on a real
// (pinned) ClickHouse scratch table and reads the materialized rows back, so the
// computed state root reflects ClickHouse's own materialization (type coercion,
// defaults, future JSON shredding) rather than a wire-side approximation. All
// part -> partition -> state-root assembly stays in payloadexec; this package
// only supplies rows.
//
// MVP scope: payload-local INSERTs over the scalar whitelist. It materializes a
// fresh scratch table per statement ("materialize only the new block in an empty
// scratch table"), reads the rows back, and drops it. The heavy zero-copy
// pre-state mechanics (FREEZE/ATTACH, mutation scratch) of Appendix C.5 remain
// out of scope.
package chexec

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// rowIDColumn is the reserved physical row-instance identity column (§5.2).
const rowIDColumn = "_hg_row_id"

// Materializer implements payloadexec.Materializer against a real ClickHouse.
type Materializer struct {
	networkID string
	conn      clickhouse.Conn
	counter   atomic.Uint64
}

var _ payloadexec.Materializer = (*Materializer)(nil)

// NewMaterializer builds a ClickHouse-backed materializer. networkID must match
// the executor it is wired into (it derives the same _hg_row_id).
func NewMaterializer(networkID string, conn clickhouse.Conn) *Materializer {
	return &Materializer{networkID: networkID, conn: conn}
}

// Materialize executes one payload-local INSERT on a fresh scratch table and
// returns the rows ClickHouse actually materialized. _hg_row_id is the
// deterministic id the agent would have injected (§5.2); partition assignment
// and RawBytes are carried from the signed wire payload (keyed by row id) so the
// root stays comparable to the in-process executor, while the row VALUES come
// from ClickHouse's read-back.
func (m *Materializer) Materialize(ctx context.Context, schema payloadexec.TableSchema, st replay.PreparedStatement) ([]payloadexec.Row, error) {
	decoded, err := decodeRows(ctx, schema, st)
	if err != nil {
		return nil, err
	}
	for ordinal := range decoded {
		decoded[ordinal].RowID = payloadexec.RowID(m.networkID, schema.TableID, st.StatementID, uint64(ordinal))
	}
	if len(decoded) == 0 {
		return decoded, nil // no-op insert: nothing for ClickHouse to materialize
	}

	wire := make(map[string]payloadexec.Row, len(decoded))
	for _, r := range decoded {
		wire[string(r.RowID)] = r
	}

	scratch := fmt.Sprintf("_hg_replay_scratch_%d", m.counter.Add(1))
	// Register cleanup before CREATE: a CREATE that commits server-side but
	// returns a client error (timeout/reset) would otherwise leak the table.
	// DROP ... IF EXISTS is a no-op if the CREATE never landed.
	defer func() { _ = m.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+quoteIdent(scratch)) }()
	if err := m.createScratch(ctx, scratch, schema); err != nil {
		return nil, err
	}

	if err := m.insertRows(ctx, scratch, decoded); err != nil {
		return nil, err
	}
	return m.readBack(ctx, scratch, schema, wire)
}

// decodeRows selects the wire decoder from the statement's signed
// payload_format. Native is the production SI lane; CSVWithNames (and the
// empty legacy value) is kept for the in-process executor tests.
func decodeRows(_ context.Context, schema payloadexec.TableSchema, st replay.PreparedStatement) ([]payloadexec.Row, error) {
	switch st.PayloadFormat {
	case nativepayload.PayloadFormat:
		if st.ClientRevision == 0 {
			return nil, fmt.Errorf("statement %s: native payload requires client_revision", st.StatementID)
		}
		return nativepayload.Decode(schema, int(st.ClientRevision), st.Payload)
	case "", payloadexec.PayloadFormatCSVWithNames:
		return payloadexec.DecodeCSV(st.Payload, schema)
	default:
		return nil, fmt.Errorf("statement %s: unsupported payload_format %q", st.StatementID, st.PayloadFormat)
	}
}

func (m *Materializer) createScratch(ctx context.Context, table string, schema payloadexec.TableSchema) error {
	// Validate every column type against the supported set before interpolating
	// it into DDL: defense-in-depth against a malformed anchored-DDL type, and a
	// clear error instead of a confusing read-back scan failure later.
	for _, c := range schema.Columns {
		if !supportedColumnType(c.Type) {
			return fmt.Errorf("unsupported column type %q for ClickHouse executor (column %q)", c.Type, c.Name)
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (%s FixedString(32)", quoteIdent(table), rowIDColumn)
	for _, c := range schema.Columns {
		fmt.Fprintf(&b, ", %s %s", quoteIdent(c.Name), c.Type)
	}
	b.WriteString(") ENGINE = MergeTree ORDER BY tuple()")
	if err := m.conn.Exec(ctx, b.String()); err != nil {
		return fmt.Errorf("create scratch table %s: %w", table, err)
	}
	return nil
}

func (m *Materializer) insertRows(ctx context.Context, table string, rows []payloadexec.Row) error {
	batch, err := m.conn.PrepareBatch(ctx, "INSERT INTO "+quoteIdent(table))
	if err != nil {
		return fmt.Errorf("prepare insert batch: %w", err)
	}
	for i, r := range rows {
		args := make([]any, 0, len(r.Values)+1)
		args = append(args, r.RowID)
		args = append(args, r.Values...)
		if err := batch.Append(args...); err != nil {
			return fmt.Errorf("append row %d: %w", i, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send insert batch: %w", err)
	}
	return nil
}

func (m *Materializer) readBack(ctx context.Context, table string, schema payloadexec.TableSchema, wire map[string]payloadexec.Row) ([]payloadexec.Row, error) {
	var sel strings.Builder
	sel.WriteString("SELECT ")
	sel.WriteString(rowIDColumn)
	for _, c := range schema.Columns {
		sel.WriteString(", ")
		sel.WriteString(quoteIdent(c.Name))
	}
	fmt.Fprintf(&sel, " FROM %s", quoteIdent(table))

	chRows, err := m.conn.Query(ctx, sel.String())
	if err != nil {
		return nil, fmt.Errorf("read-back query: %w", err)
	}
	defer chRows.Close()

	out := make([]payloadexec.Row, 0, len(wire))
	for chRows.Next() {
		var rid []byte
		dests := make([]any, len(schema.Columns)+1)
		dests[0] = &rid
		holders := make([]any, len(schema.Columns))
		for i, c := range schema.Columns {
			p, err := newScanDest(c.Type)
			if err != nil {
				return nil, err
			}
			dests[i+1] = p
			holders[i] = p
		}
		if err := chRows.Scan(dests...); err != nil {
			return nil, fmt.Errorf("read-back scan: %w", err)
		}
		src, ok := wire[string(rid)]
		if !ok {
			return nil, fmt.Errorf("read-back row id %x not found among inserted rows", rid)
		}
		values := make([]any, len(schema.Columns))
		for i := range schema.Columns {
			v, err := derefScan(holders[i])
			if err != nil {
				return nil, err
			}
			values[i] = v
		}
		out = append(out, payloadexec.Row{
			RowID:       append([]byte(nil), rid...),
			Values:      values,
			PartitionID: src.PartitionID,
			RawBytes:    src.RawBytes,
		})
	}
	if err := chRows.Err(); err != nil {
		return nil, fmt.Errorf("read-back rows: %w", err)
	}
	if len(out) != len(wire) {
		return nil, fmt.Errorf("read-back returned %d rows, inserted %d", len(out), len(wire))
	}
	return out, nil
}

// supportedColumnType reports whether the executor can materialize and read back
// a column of this ClickHouse type. It is the single source of truth shared by
// createScratch (DDL validation) and newScanDest (read-back), kept in sync with
// payloadexec.parseValue's wire-side whitelist so the two executors admit the
// same schema set.
func supportedColumnType(typeName string) bool {
	switch {
	case typeName == "String", strings.HasPrefix(typeName, "FixedString("):
		return true
	case typeName == "Bool", typeName == "Float32", typeName == "Float64":
		return true
	case typeName == "UInt8", typeName == "UInt16", typeName == "UInt32", typeName == "UInt64":
		return true
	case typeName == "Int8", typeName == "Int16", typeName == "Int32", typeName == "Int64":
		return true
	default:
		return false
	}
}

// newScanDest returns a pointer of the Go type clickhouse-go scans this column
// into; the type must match what payloadexec/lthash expects so the canonical
// element bytes agree across the wire-side and read-back paths. FixedString(N)
// scans into []byte (exactly N bytes, NUL-padded) to match parseFixedString.
func newScanDest(typeName string) (any, error) {
	switch {
	case typeName == "String":
		return new(string), nil
	case strings.HasPrefix(typeName, "FixedString("):
		return new([]byte), nil
	case typeName == "Bool":
		return new(bool), nil
	case typeName == "Float32":
		return new(float32), nil
	case typeName == "Float64":
		return new(float64), nil
	case typeName == "UInt8":
		return new(uint8), nil
	case typeName == "UInt16":
		return new(uint16), nil
	case typeName == "UInt32":
		return new(uint32), nil
	case typeName == "UInt64":
		return new(uint64), nil
	case typeName == "Int8":
		return new(int8), nil
	case typeName == "Int16":
		return new(int16), nil
	case typeName == "Int32":
		return new(int32), nil
	case typeName == "Int64":
		return new(int64), nil
	default:
		return nil, fmt.Errorf("chexec: unsupported column type %q for ClickHouse read-back", typeName)
	}
}

func derefScan(p any) (any, error) {
	switch v := p.(type) {
	case *string:
		return *v, nil
	case *[]byte:
		// Defensive copy: clickhouse-go's FixedString column aliases a shared
		// row buffer. The []byte flows through lthash's []byte->kindString path,
		// matching the in-process parseFixedString encoding.
		return append([]byte(nil), *v...), nil
	case *bool:
		return *v, nil
	case *float32:
		return *v, nil
	case *float64:
		return *v, nil
	case *uint8:
		return *v, nil
	case *uint16:
		return *v, nil
	case *uint32:
		return *v, nil
	case *uint64:
		return *v, nil
	case *int8:
		return *v, nil
	case *int16:
		return *v, nil
	case *int32:
		return *v, nil
	case *int64:
		return *v, nil
	default:
		return nil, fmt.Errorf("chexec: unexpected scan destination %T", p)
	}
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
