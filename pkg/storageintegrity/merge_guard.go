package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrNativeMergesEnabled is the fail-closed sentinel returned when a guarded
// table still shows an active merge after STOP MERGES was issued: HouseGate must
// not proceed with storage-integrity ingest while ClickHouse's native merges can
// mutate the guarded part inventory out from under the integrity layer.
var ErrNativeMergesEnabled = errors.New("storageintegrity: native merges still active on a guarded table")

// MergeConn is the narrow ClickHouse-facing port the merge guard needs. It is
// deliberately minimal so the guard is unit-testable with an in-memory fake and
// so the core package does not depend on the full clickhouse-go Conn type; the
// runtime adapts a real connection to this port at the wiring boundary.
type MergeConn interface {
	Exec(ctx context.Context, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (MergeRows, error)
}

// MergeRows is the narrow rows port the verify probe reads.
type MergeRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// MergeTable identifies one merge-managed table.
type MergeTable struct {
	Database string
	Table    string
}

// MergeGuard re-asserts and verifies that ClickHouse background merges are
// stopped on the guarded storage-integrity tables (design: v1 requires STOP
// MERGES on both hg_safe and hg_unsafe so the integrity layer owns the active
// part inventory). It is a HouseGate-local ClickHouse-facing helper; it holds no
// Arbiter, journal, or intake state.
type MergeGuard struct {
	conn   MergeConn
	tables []MergeTable
}

// NewMergeGuard constructs a guard over the given connection and table set.
func NewMergeGuard(conn MergeConn, tables []MergeTable) *MergeGuard {
	return &MergeGuard{conn: conn, tables: tables}
}

// BuildStopMergesStatements returns one table-scoped `SYSTEM STOP MERGES` per
// guarded table, in the guard's table order. Each identifier is backtick-quoted;
// it never emits a bare global STOP.
func (g *MergeGuard) BuildStopMergesStatements() []string {
	stmts := make([]string, 0, len(g.tables))
	for _, t := range g.tables {
		stmts = append(stmts, fmt.Sprintf("SYSTEM STOP MERGES %s.%s", quoteMergeIdent(t.Database), quoteMergeIdent(t.Table)))
	}
	return stmts
}

// BuildVerifyMergesQuery returns the probe that detects any in-flight merge on
// the guarded tables. It selects database, table from system.merges filtered to
// exactly the guarded (database, table) pairs, so it neither misses a guarded
// table nor over-matches unrelated ones.
func (g *MergeGuard) BuildVerifyMergesQuery() string {
	preds := make([]string, 0, len(g.tables))
	for _, t := range g.tables {
		preds = append(preds, fmt.Sprintf("(database = %s AND table = %s)", quoteMergeString(t.Database), quoteMergeString(t.Table)))
	}
	where := "0"
	if len(preds) > 0 {
		where = strings.Join(preds, " OR ")
	}
	return "SELECT database, table FROM system.merges WHERE " + where
}

// AssertStopMerges issues every STOP MERGES statement, then runs the verify
// probe and fails closed (ErrNativeMergesEnabled) if any guarded table still
// shows an active merge. It is idempotent: re-asserting on a clean cluster
// succeeds, so it is safe to call at startup and on reconnect. A failed stop
// exec surfaces immediately, before the verify probe runs.
func (g *MergeGuard) AssertStopMerges(ctx context.Context) error {
	for _, stmt := range g.BuildStopMergesStatements() {
		if err := g.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("storageintegrity: STOP MERGES failed: %w", err)
		}
	}
	rows, err := g.conn.Query(ctx, g.BuildVerifyMergesQuery())
	if err != nil {
		return fmt.Errorf("storageintegrity: verify merges probe failed: %w", err)
	}
	defer rows.Close()
	var offenders []string
	for rows.Next() {
		var db, tbl string
		if err := rows.Scan(&db, &tbl); err != nil {
			return fmt.Errorf("storageintegrity: scan merges probe: %w", err)
		}
		offenders = append(offenders, db+"."+tbl)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("storageintegrity: read merges probe: %w", err)
	}
	if len(offenders) > 0 {
		return fmt.Errorf("%w: %s", ErrNativeMergesEnabled, strings.Join(offenders, ", "))
	}
	return nil
}

// quoteMergeIdent backtick-quotes a ClickHouse identifier, doubling any embedded
// backtick. It is a tiny local helper so the core package does not import the
// clickhouse-go layer just to quote identifiers.
func quoteMergeIdent(id string) string {
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}

// quoteMergeString single-quotes a ClickHouse string literal for the probe's
// WHERE clause, escaping embedded quotes and backslashes.
func quoteMergeString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}
