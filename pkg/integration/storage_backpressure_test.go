package integration

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// chMergeConn adapts clickhouse-go to the narrow sicore.MergeConn port the
// same way sentio-node's storageintegrityadapter.NewMergeConn does.
type chMergeConn struct{ conn clickhouse.Conn }

func (c chMergeConn) Exec(ctx context.Context, query string, args ...any) error {
	return c.conn.Exec(ctx, query, args...)
}

func (c chMergeConn) Query(ctx context.Context, query string, args ...any) (sicore.MergeRows, error) {
	return c.conn.Query(ctx, query, args...)
}

func bpTableSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "v", Type: "UInt64"},
		},
	}
}

func TestPartsPressureGuard_AgainstRealSystemParts(t *testing.T) {
	ctx := context.Background()
	conn := openDirectCH(t)
	unsafeDB := "hg_unsafe_bp_" + uniqueTable(t)
	safeDB := "hg_safe_bp_" + uniqueTable(t)
	table := "db__t"
	unpartitioned := "db__u"
	restoreTable := "db__restore"
	for _, query := range []string{
		"CREATE DATABASE IF NOT EXISTS " + unsafeDB,
		"CREATE DATABASE IF NOT EXISTS " + safeDB,
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", unsafeDB, table),
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), v UInt64) ENGINE = MergeTree ORDER BY (_hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", unsafeDB, unpartitioned),
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", unsafeDB, restoreTable),
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", safeDB, table),
		fmt.Sprintf("SYSTEM STOP MERGES %s.%s", unsafeDB, table),
		fmt.Sprintf("SYSTEM STOP MERGES %s.%s", unsafeDB, unpartitioned),
		fmt.Sprintf("SYSTEM STOP MERGES %s.%s", unsafeDB, restoreTable),
	} {
		mustExec(t, conn, query)
	}
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+unsafeDB+" SYNC")
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+safeDB+" SYNC")
	})

	// Three single-row inserts into p0 make three active parts; p1, the
	// unpartitioned table, and the safe table each receive one part.
	for i := 1; i <= 3; i++ {
		mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p0', %d)", unsafeDB, table, i, i))
	}
	for i := 10; i <= 12; i++ {
		mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'tuple()', %d)", unsafeDB, table, i, i))
	}
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p1', 9)", unsafeDB, table, 9))
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 7)", unsafeDB, unpartitioned, 7))
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p0', 1)", safeDB, table, 1))

	guard := sicore.NewPartsPressureGuard(chMergeConn{conn: conn}, sicore.PartsPressureConfig{
		UnsafeDatabase:        unsafeDB,
		SafeDatabase:          safeDB,
		SoftPartsPerPartition: 3,
		HardPartsPerPartition: 5,
	})
	snapshot, err := guard.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if snapshot[sicore.PartsKey{Database: unsafeDB, Table: table, Partition: "p_p0"}] != 3 ||
		snapshot[sicore.PartsKey{Database: unsafeDB, Table: table, Partition: "p_tuple()"}] != 3 ||
		snapshot[sicore.PartsKey{Database: unsafeDB, Table: table, Partition: "p_p1"}] != 1 ||
		snapshot[sicore.PartsKey{Database: unsafeDB, Table: unpartitioned, Partition: "all"}] != 1 ||
		snapshot[sicore.PartsKey{Database: safeDB, Table: table, Partition: "p_p0"}] != 1 {
		t.Fatalf("snapshot = %v", snapshot)
	}
	if err := guard.Allow(table, "p_p0"); !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatalf("p_p0 at soft limit must be refused: %v", err)
	}
	if err := guard.Allow(table, "p_tuple()"); !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatalf("partitioned tuple() value at soft limit must be refused: %v", err)
	}
	if err := guard.Allow(table, "p_p1"); err != nil {
		t.Fatalf("p_p1 below soft must be allowed: %v", err)
	}
	if err := guard.Allow(unpartitioned, "all"); err != nil {
		t.Fatalf("unpartitioned below soft must be allowed: %v", err)
	}

	// Guard keys must exactly match the logical partition IDs derived from the
	// admitted row payload.
	partitions, err := sicore.PayloadPartitionIDs(bpTableSchema(), sicore.EncodingCSVWithNames, 0, []byte("p,v\np0,1\np1,2\n"))
	if err != nil || len(partitions) != 2 || partitions[0] != "p_p0" || partitions[1] != "p_p1" {
		t.Fatalf("PayloadPartitionIDs = %v err=%v", partitions, err)
	}

	// Exact cleanup proof uses real system.parts names, not an aggregate count.
	// A queued replacement whose baseline includes the cleaned name must converge
	// after DROP PART and the replacement's later visibility.
	activeNames := func(target, partition string) map[string]bool {
		rows, err := conn.Query(ctx,
			"SELECT name FROM system.parts WHERE database = ? AND table = ? AND partition = ? AND active ORDER BY name",
			unsafeDB, target, partition,
		)
		if err != nil {
			t.Fatalf("query active part names: %v", err)
		}
		defer rows.Close()
		out := map[string]bool{}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan active part name: %v", err)
			}
			out[name] = true
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read active part names: %v", err)
		}
		return out
	}
	before := activeNames(table, "p1")
	cleaner, err := guard.Reserve(ctx, table, []string{"p_p1"})
	if err != nil {
		t.Fatalf("cleaner Reserve: %v", err)
	}
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p1', 10)", unsafeDB, table, 10))
	afterInsert := activeNames(table, "p1")
	candidateName := ""
	for name := range afterInsert {
		if !before[name] {
			candidateName = name
			break
		}
	}
	if candidateName == "" {
		t.Fatalf("new p1 candidate not found: before=%v after=%v", before, afterInsert)
	}
	candidate := sicore.CandidatePart{TableID: "db.t", PartitionID: "p_p1", PartName: candidateName}
	if err := cleaner.Commit(candidate); err != nil {
		t.Fatalf("cleaner Commit: %v", err)
	}
	queued, err := guard.Reserve(ctx, table, []string{"p_p1"})
	if err != nil {
		t.Fatalf("queued Reserve: %v", err)
	}
	if err := cleaner.PrepareCleanupProof(ctx, []sicore.CandidatePart{candidate}); err != nil {
		t.Fatalf("PrepareCleanupProof: %v", err)
	}
	mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s.%s DROP PART '%s'", unsafeDB, table, candidateName))
	if err := cleaner.ReleaseCleaned(ctx); err != nil {
		t.Fatalf("ReleaseCleaned: %v", err)
	}
	if err := queued.Commit(); err != nil {
		t.Fatalf("queued Commit: %v", err)
	}
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p1', 11)", unsafeDB, table, 11))
	if _, err := guard.Refresh(ctx); err != nil {
		t.Fatalf("replacement Refresh: %v", err)
	}
	probe, err := guard.Reserve(ctx, table, []string{"p_p1"})
	if err != nil {
		t.Fatalf("stable soft-1 inventory retained phantom cleanup debt: %v", err)
	}
	probe.Release()

	// Restart restoration uses one exact inventory and charges an ACK2
	// candidate that has never been observed locally even while its detached
	// part is absent. The debt clears only after that exact name is attached and
	// observed; unrelated count growth cannot stand in for name visibility.
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'restore', 1)", unsafeDB, restoreTable, 101))
	restoreBefore := activeNames(restoreTable, "restore")
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'restore', 2)", unsafeDB, restoreTable, 102))
	restoreAfter := activeNames(restoreTable, "restore")
	restoreCandidateName := ""
	for name := range restoreAfter {
		if !restoreBefore[name] {
			restoreCandidateName = name
			break
		}
	}
	if restoreCandidateName == "" {
		t.Fatalf("restore candidate not found: before=%v after=%v", restoreBefore, restoreAfter)
	}
	detachedPartName := restoreCandidateName
	nameFields := strings.Split(detachedPartName, "_")
	if len(nameFields) < 4 {
		t.Fatalf("unexpected MergeTree part name %q", detachedPartName)
	}
	block, err := strconv.Atoi(nameFields[len(nameFields)-3])
	if err != nil {
		t.Fatalf("parse MergeTree part block from %q: %v", detachedPartName, err)
	}
	// ClickHouse 25.8 allocates the next block while preserving the partition
	// prefix. The controlled table has block 1 active and block 2 detached; an
	// unrelated insert takes block 3 before ATTACH makes the delayed exact name
	// visible as block 4.
	nameFields[len(nameFields)-3] = strconv.Itoa(block + 2)
	nameFields[len(nameFields)-2] = strconv.Itoa(block + 2)
	restoreCandidateName = strings.Join(nameFields, "_")
	mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s.%s DETACH PART '%s'", unsafeDB, restoreTable, detachedPartName))
	restoreGuard := sicore.NewPartsPressureGuard(chMergeConn{conn: conn}, sicore.PartsPressureConfig{
		UnsafeDatabase: unsafeDB, SafeDatabase: safeDB,
		SoftPartsPerPartition: 3, HardPartsPerPartition: 5,
	})
	if _, err := restoreGuard.RestoreBatch(ctx, []sicore.PartsRestoreRecord{{
		StatementID: "restored-finalized", Table: restoreTable, PartitionIDs: []string{"p_restore"},
		Candidates: []sicore.CandidatePart{{
			TableID: "db.restore", PartitionID: "p_restore", PartName: restoreCandidateName,
		}},
		Finalized: true,
	}}); err != nil {
		t.Fatalf("RestoreBatch against detached candidate: %v", err)
	}
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'restore', 3)", unsafeDB, restoreTable, 103))
	unrelatedNames := activeNames(restoreTable, "restore")
	unrelatedName := ""
	for name := range unrelatedNames {
		if !restoreBefore[name] {
			unrelatedName = name
			break
		}
	}
	if unrelatedName == "" {
		t.Fatalf("unrelated restore part not found: base=%v active=%v", restoreBefore, unrelatedNames)
	}
	if _, err := restoreGuard.Reserve(ctx, restoreTable, []string{"p_restore"}); !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatalf("unrelated growth absorbed unseen exact debt: %v", err)
	}
	mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s.%s ATTACH PART '%s'", unsafeDB, restoreTable, detachedPartName))
	attachedNames := activeNames(restoreTable, "restore")
	if !attachedNames[restoreCandidateName] {
		t.Fatalf("attached candidate changed exact name: want=%s active=%v", restoreCandidateName, attachedNames)
	}
	if _, err := restoreGuard.Refresh(ctx); err != nil {
		t.Fatalf("attached candidate Refresh: %v", err)
	}
	baseName := ""
	for name := range restoreBefore {
		baseName = name
		break
	}
	if baseName == "" {
		t.Fatal("restore base part not found")
	}
	mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s.%s DROP PART '%s'", unsafeDB, restoreTable, baseName))
	mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s.%s DROP PART '%s'", unsafeDB, restoreTable, unrelatedName))
	if _, err := restoreGuard.Refresh(ctx); err != nil {
		t.Fatalf("post-base-cleanup Refresh: %v", err)
	}
	postRestoreProbe, err := restoreGuard.Reserve(ctx, restoreTable, []string{"p_restore"})
	if err != nil {
		t.Fatalf("exact attached visibility did not clear restored debt: %v", err)
	}
	postRestoreProbe.Release()
}
