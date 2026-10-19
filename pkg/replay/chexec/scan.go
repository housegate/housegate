package chexec

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// PartScanResult is one real ClickHouse part's row-content commitment.
type PartScanResult struct {
	PartName  string
	RowLtHash string
	RowCount  uint64
}

// ScanParts recomputes row LtHash commitments from named active parts of a real
// ClickHouse table. It folds rows through payloadexec.RowElementHash so byte-side
// scans and replay execution use the same row identity and encoding.
func ScanParts(ctx context.Context, conn clickhouse.Conn, qualifiedTable string, schema payloadexec.TableSchema, partNames []string) ([]PartScanResult, error) {
	if len(partNames) == 0 {
		return nil, fmt.Errorf("chexec: ScanParts requires at least one part name")
	}
	byPart, err := scanTable(ctx, conn, qualifiedTable, schema, partNames)
	if err != nil {
		return nil, err
	}
	return scanResults(qualifiedTable, partNames, byPart)
}

// ScanAllParts is ScanParts over every active part of the table: the same row
// derivation and refusals without the _part filter, sorted by part name. It is
// the install gate's scan of a scratch whose part names the caller does not
// know (spec 2026-10-14 §8.5 step 4). A part holds at least one row to be
// seen, so a table without rows gives an empty slice and no error, which is
// how a caller sees a rename race. It reads the whole table: only scratch
// tables and the rate-limited self-audit call it.
func ScanAllParts(ctx context.Context, conn clickhouse.Conn, qualifiedTable string, schema payloadexec.TableSchema) ([]PartScanResult, error) {
	byPart, err := scanTable(ctx, conn, qualifiedTable, schema, nil)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(byPart))
	for name := range byPart {
		names = append(names, name)
	}
	// scanResults sorts by part name.
	return scanResults(qualifiedTable, names, byPart)
}

// scanTable folds the rows of qualifiedTable into one accumulator per part:
// of the named parts, or of every active part when partNames is nil.
func scanTable(ctx context.Context, conn clickhouse.Conn, qualifiedTable string, schema payloadexec.TableSchema, partNames []string) (map[string]*partAccumulator, error) {
	db, table, ok := strings.Cut(qualifiedTable, ".")
	if !ok {
		return nil, fmt.Errorf("chexec: qualifiedTable must be db.table, got %q", qualifiedTable)
	}
	for _, c := range schema.Columns {
		if !supportedColumnType(c.Type) {
			return nil, fmt.Errorf("chexec: unsupported column type %q for part scan (column %q)", c.Type, c.Name)
		}
	}

	query, args := scanPartsQuery(db, table, schema, partNames)
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("part scan query: %w", err)
	}
	defer rows.Close()

	byPart := map[string]*partAccumulator{}
	for rows.Next() {
		part, rowHash, err := scanPartRow(rows, schema)
		if err != nil {
			return nil, err
		}
		acc := byPart[part]
		if acc == nil {
			acc = &partAccumulator{hash: lthash.New()}
			byPart[part] = acc
		}
		acc.hash.AddHash(rowHash)
		acc.count++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("part scan rows: %w", err)
	}
	return byPart, nil
}

type partAccumulator struct {
	hash  *lthash.Hash
	count uint64
}

// scanPartsQuery selects the rows of the named parts, or of every active part
// when partNames is nil (ScanAllParts).
func scanPartsQuery(db, table string, schema payloadexec.TableSchema, partNames []string) (string, []any) {
	var b strings.Builder
	b.WriteString("SELECT _part, ")
	b.WriteString(rowIDColumn)
	for _, c := range schema.Columns {
		b.WriteString(", ")
		b.WriteString(quoteIdent(c.Name))
	}
	fmt.Fprintf(&b, " FROM %s.%s", quoteIdent(db), quoteIdent(table))
	if partNames == nil {
		return b.String(), nil
	}
	b.WriteString(" WHERE _part IN (")

	args := make([]any, 0, len(partNames))
	for i, name := range partNames {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("?")
		args = append(args, name)
	}
	b.WriteString(")")
	return b.String(), args
}

type rowScanner interface {
	Scan(...any) error
}

func scanPartRow(rows rowScanner, schema payloadexec.TableSchema) (string, *lthash.Hash, error) {
	var part string
	var rid []byte
	dests := make([]any, len(schema.Columns)+2)
	dests[0] = &part
	dests[1] = &rid
	holders := make([]any, len(schema.Columns))
	for i, c := range schema.Columns {
		dest, err := newScanDest(c.Type)
		if err != nil {
			return "", nil, err
		}
		dests[i+2] = dest
		holders[i] = dest
	}
	if err := rows.Scan(dests...); err != nil {
		return "", nil, fmt.Errorf("part scan row: %w", err)
	}

	values := make([]any, len(schema.Columns))
	for i := range schema.Columns {
		value, err := derefScan(schema.Columns[i].Type, holders[i])
		if err != nil {
			return "", nil, err
		}
		values[i] = value
	}
	h, err := payloadexec.RowElementHash(schema, append([]byte(nil), rid...), values)
	if err != nil {
		return "", nil, fmt.Errorf("part %s: %w", part, err)
	}
	return part, h, nil
}

func scanResults(qualifiedTable string, partNames []string, byPart map[string]*partAccumulator) ([]PartScanResult, error) {
	out := make([]PartScanResult, 0, len(partNames))
	for _, name := range partNames {
		acc, ok := byPart[name]
		if !ok {
			return nil, fmt.Errorf("part %q not found (or empty) in %s", name, qualifiedTable)
		}
		out = append(out, PartScanResult{
			PartName:  name,
			RowLtHash: "0x" + hex.EncodeToString(acc.hash.Bytes()),
			RowCount:  acc.count,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartName < out[j].PartName })
	return out, nil
}
