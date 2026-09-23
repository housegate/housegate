package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrNativeMergesEnabled is the fail-closed sentinel returned when a guarded
// table shows an active merge: HouseGate must not proceed with
// storage-integrity ingest while ClickHouse's native merges can mutate the
// guarded part inventory out from under the integrity layer.
var ErrNativeMergesEnabled = errors.New("storageintegrity: native merges still active on a guarded table")

// ErrMergeSettingNotPinned is the fail-closed sentinel returned when a guarded
// table does not pin max_bytes_to_merge_at_max_space_in_pool = 0 in its engine
// settings. That pinned setting is what keeps native background merges off; the
// guard refuses to re-enable ClickHouse's merge scheduler on a table without it.
var ErrMergeSettingNotPinned = errors.New("storageintegrity: guarded table does not pin max_bytes_to_merge_at_max_space_in_pool = 0")

// PinnedMergeSetting is the engine setting that keeps native background merges
// off on every storage-integrity table (the physical-table lifecycle pins it at
// creation and verifies it at startup).
const PinnedMergeSetting = "max_bytes_to_merge_at_max_space_in_pool"

// MergeConn is the narrow ClickHouse-facing port the merge guard needs. It is
// deliberately minimal so the guard is unit-testable with an in-memory fake and
// so the core package does not depend on the full clickhouse-go Conn type; the
// runtime adapts a real connection to this port at the wiring boundary.
type MergeConn interface {
	Exec(ctx context.Context, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (MergeRows, error)
}

// MergeRows is the narrow rows port the guard's probes read.
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

// MergeGuard keeps ClickHouse native merges off the guarded storage-integrity
// tables (hg_safe and hg_unsafe) so the integrity layer owns the active part
// inventory, and keeps ClickHouse's outdated-part cleanup running.
//
// Merges are kept off by the pinned engine setting
// max_bytes_to_merge_at_max_space_in_pool = 0, not by SYSTEM STOP MERGES.
// SYSTEM STOP MERGES also stops ClickHouse from deleting outdated parts, so the
// parts a promotion's REPLACE PARTITION retires stay on disk forever and come
// back as active parts after any ClickHouse restart (their block ranges are not
// covered by the replacement parts). That silently duplicated verified rows in
// hg_safe and wedged every later promotion on its shadow closure check. The
// guard therefore verifies the pinned setting, issues SYSTEM START MERGES
// (which also undoes a STOP left by an older HouseGate), and verifies that no
// merge is running. It holds no Arbiter, journal, or intake state.
//
// Residual gap: an explicit OPTIMIZE ... FINAL issued directly against
// ClickHouse merges regardless of the pinned setting. HouseGate never issues it
// and its storage-integrity rewrite refuses it on ordinary sessions; merges preserve row content, so
// data roots are unaffected, and the source's promotion reports the resulting
// part-name divergence.
type MergeGuard struct {
	conn   MergeConn
	tables []MergeTable
}

// NewMergeGuard constructs a guard over the given connection and table set.
func NewMergeGuard(conn MergeConn, tables []MergeTable) *MergeGuard {
	return &MergeGuard{conn: conn, tables: tables}
}

// BuildStartMergesStatements returns one table-scoped `SYSTEM START MERGES`
// per guarded table, in the guard's table order. Each identifier is
// backtick-quoted; it never emits a bare global START.
func (g *MergeGuard) BuildStartMergesStatements() []string {
	stmts := make([]string, 0, len(g.tables))
	for _, t := range g.tables {
		stmts = append(stmts, fmt.Sprintf("SYSTEM START MERGES %s.%s", quoteMergeIdent(t.Database), quoteMergeIdent(t.Table)))
	}
	return stmts
}

// BuildEngineSettingsQuery returns the probe that reads engine_full for exactly
// the guarded tables.
func (g *MergeGuard) BuildEngineSettingsQuery() string {
	return "SELECT database, name, engine_full FROM system.tables WHERE " + g.tableFilter("name")
}

// BuildVerifyMergesQuery returns the probe that detects any in-flight merge on
// the guarded tables. It selects database, table from system.merges filtered to
// exactly the guarded (database, table) pairs, so it neither misses a guarded
// table nor over-matches unrelated ones.
func (g *MergeGuard) BuildVerifyMergesQuery() string {
	return "SELECT database, table FROM system.merges WHERE " + g.tableFilter("table")
}

func (g *MergeGuard) tableFilter(tableColumn string) string {
	preds := make([]string, 0, len(g.tables))
	for _, t := range g.tables {
		preds = append(preds, fmt.Sprintf("(database = %s AND %s = %s)", quoteMergeString(t.Database), tableColumn, quoteMergeString(t.Table)))
	}
	if len(preds) == 0 {
		return "0"
	}
	return strings.Join(preds, " OR ")
}

// AssertStopMerges keeps native merges off the guarded tables and fails closed
// otherwise. The name predates the switch from SYSTEM STOP MERGES to the pinned
// engine setting and is kept because hosts implement the same method. In order:
//
//  1. every guarded table must exist and pin max_bytes_to_merge_at_max_space_in_pool = 0
//     (ErrMergeSettingNotPinned otherwise, before any statement is issued);
//  2. SYSTEM START MERGES on every guarded table, so ClickHouse deletes outdated
//     parts and a STOP left by an older HouseGate is undone;
//  3. no guarded table may show an in-flight merge (ErrNativeMergesEnabled).
//
// It is idempotent and safe to call at startup and on every reassert tick.
func (g *MergeGuard) AssertStopMerges(ctx context.Context) error {
	if err := g.verifyPinnedSetting(ctx); err != nil {
		return err
	}
	for _, stmt := range g.BuildStartMergesStatements() {
		if err := g.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("storageintegrity: START MERGES failed: %w", err)
		}
	}
	offenders, err := g.queryPairs(ctx, g.BuildVerifyMergesQuery())
	if err != nil {
		return fmt.Errorf("storageintegrity: verify merges probe failed: %w", err)
	}
	if len(offenders) > 0 {
		names := make([]string, 0, len(offenders))
		for _, o := range offenders {
			names = append(names, o[0]+"."+o[1])
		}
		return fmt.Errorf("%w: %s", ErrNativeMergesEnabled, strings.Join(names, ", "))
	}
	return nil
}

func (g *MergeGuard) verifyPinnedSetting(ctx context.Context) error {
	rows, err := g.conn.Query(ctx, g.BuildEngineSettingsQuery())
	if err != nil {
		return fmt.Errorf("storageintegrity: engine settings probe failed: %w", err)
	}
	defer rows.Close()
	engines := make(map[MergeTable]string, len(g.tables))
	for rows.Next() {
		var db, tbl, engine string
		if err := rows.Scan(&db, &tbl, &engine); err != nil {
			return fmt.Errorf("storageintegrity: scan engine settings probe: %w", err)
		}
		engines[MergeTable{Database: db, Table: tbl}] = engine
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("storageintegrity: read engine settings probe: %w", err)
	}
	var bad []string
	for _, t := range g.tables {
		engine, ok := engines[t]
		if !ok {
			bad = append(bad, t.Database+"."+t.Table+" (table missing)")
			continue
		}
		if engineSettings(engine)[PinnedMergeSetting] != "0" {
			bad = append(bad, t.Database+"."+t.Table)
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("%w: %s", ErrMergeSettingNotPinned, strings.Join(bad, ", "))
	}
	return nil
}

func (g *MergeGuard) queryPairs(ctx context.Context, query string) ([][2]string, error) {
	rows, err := g.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]string
	for rows.Next() {
		var a, b string
		if err := rows.Scan(&a, &b); err != nil {
			return nil, err
		}
		out = append(out, [2]string{a, b})
	}
	return out, rows.Err()
}

// engineSettings parses the trailing SETTINGS clause of a system.tables
// engine_full value into name -> value. ClickHouse renders it as
// "... SETTINGS a = 1, b = 'x'"; values here are compared as rendered.
func engineSettings(engineFull string) map[string]string {
	out := map[string]string{}
	idx := strings.LastIndex(engineFull, " SETTINGS ")
	if idx < 0 {
		return out
	}
	for _, kv := range strings.Split(engineFull[idx+len(" SETTINGS "):], ",") {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(name)] = strings.TrimSpace(value)
	}
	return out
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
