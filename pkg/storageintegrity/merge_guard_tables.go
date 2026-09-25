package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// MergeGuardDatabases are the two databases whose tables the per-table guard
// enumerates. They equal the Spec C D2 physical homes.
var MergeGuardDatabases = []string{"hg_safe", "hg_unsafe"}

// ErrMergeGuardTableMissing reports an Active table whose hg_safe or
// hg_unsafe table does not exist.
var ErrMergeGuardTableMissing = errors.New("storageintegrity: guarded table missing")

// MergeGuardReport is one per-table assertion pass (spec 2026-09-24 §9.4).
// Tables holds exactly one entry per requested Active table id: nil when both
// of its hg_* tables exist, pin the merge setting and show no merge. Other
// maps each present hg_* table that belongs to no requested id (a Pending or
// Gone table's) to its outcome; absence there is never an error.
type MergeGuardReport struct {
	Tables map[string]error
	Other  map[MergeTable]error
}

// AssertTables keeps native merges off every table present in hg_safe and
// hg_unsafe and reports per logical table. activeTableIDs are required: both
// of their physical tables must exist. Every other present hg_* table is
// checked the same way but may be absent. The returned error is set only when
// the enumeration itself fails; it then applies to every table.
//
// Order per pass, as in AssertStopMerges: read engine settings, START MERGES
// on every present table that pins the setting (a table that does not is never
// started), then verify that none of them shows an in-flight merge.
func (g *MergeGuard) AssertTables(ctx context.Context, activeTableIDs []string) (MergeGuardReport, error) {
	report := MergeGuardReport{Tables: map[string]error{}, Other: map[MergeTable]error{}}
	engines, err := g.presentEngines(ctx)
	if err != nil {
		return report, err
	}
	owner := map[MergeTable]string{}
	for _, id := range activeTableIDs {
		phys := PhysicalTableName(id)
		for _, db := range MergeGuardDatabases {
			owner[MergeTable{Database: db, Table: phys}] = id
		}
	}
	tableErr := map[MergeTable]error{}
	var healthy []MergeTable
	for table, engine := range engines {
		if engineSettings(engine)[PinnedMergeSetting] != "0" {
			tableErr[table] = fmt.Errorf("%w: %s.%s", ErrMergeSettingNotPinned, table.Database, table.Table)
			continue
		}
		healthy = append(healthy, table)
	}
	sort.Slice(healthy, func(i, j int) bool {
		if healthy[i].Database != healthy[j].Database {
			return healthy[i].Database < healthy[j].Database
		}
		return healthy[i].Table < healthy[j].Table
	})
	var started []MergeTable
	for _, table := range healthy {
		stmt := fmt.Sprintf("SYSTEM START MERGES %s.%s", quoteMergeIdent(table.Database), quoteMergeIdent(table.Table))
		if err := g.conn.Exec(ctx, stmt); err != nil {
			tableErr[table] = fmt.Errorf("storageintegrity: START MERGES failed: %w", err)
			continue
		}
		started = append(started, table)
	}
	if len(started) > 0 {
		offenders, err := g.queryPairs(ctx, "SELECT database, table FROM system.merges WHERE "+tableFilter(started, "table"))
		if err != nil {
			for _, table := range started {
				tableErr[table] = fmt.Errorf("storageintegrity: verify merges probe failed: %w", err)
			}
		}
		for _, o := range offenders {
			table := MergeTable{Database: o[0], Table: o[1]}
			tableErr[table] = fmt.Errorf("%w: %s.%s", ErrNativeMergesEnabled, table.Database, table.Table)
		}
	}
	for _, id := range activeTableIDs {
		var errs []error
		phys := PhysicalTableName(id)
		for _, db := range MergeGuardDatabases {
			table := MergeTable{Database: db, Table: phys}
			if _, ok := engines[table]; !ok {
				errs = append(errs, fmt.Errorf("%w: %s.%s", ErrMergeGuardTableMissing, db, phys))
				continue
			}
			errs = append(errs, tableErr[table])
		}
		report.Tables[id] = errors.Join(errs...)
	}
	for table := range engines {
		if _, ok := owner[table]; !ok {
			report.Other[table] = tableErr[table]
		}
	}
	return report, nil
}

func (g *MergeGuard) presentEngines(ctx context.Context) (map[MergeTable]string, error) {
	quoted := make([]string, len(MergeGuardDatabases))
	for i, db := range MergeGuardDatabases {
		quoted[i] = quoteMergeString(db)
	}
	rows, err := g.conn.Query(ctx, "SELECT database, name, engine_full FROM system.tables WHERE database IN ("+strings.Join(quoted, ", ")+")")
	if err != nil {
		return nil, fmt.Errorf("storageintegrity: engine settings probe failed: %w", err)
	}
	defer rows.Close()
	engines := map[MergeTable]string{}
	for rows.Next() {
		var db, tbl, engine string
		if err := rows.Scan(&db, &tbl, &engine); err != nil {
			return nil, fmt.Errorf("storageintegrity: scan engine settings probe: %w", err)
		}
		engines[MergeTable{Database: db, Table: tbl}] = engine
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storageintegrity: read engine settings probe: %w", err)
	}
	return engines, nil
}

func tableFilter(tables []MergeTable, tableColumn string) string {
	preds := make([]string, 0, len(tables))
	for _, t := range tables {
		preds = append(preds, fmt.Sprintf("(database = %s AND %s = %s)", quoteMergeString(t.Database), tableColumn, quoteMergeString(t.Table)))
	}
	if len(preds) == 0 {
		return "0"
	}
	return strings.Join(preds, " OR ")
}
