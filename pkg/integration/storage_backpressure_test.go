package integration

import (
	"context"
	"errors"
	"fmt"
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
	for _, query := range []string{
		"CREATE DATABASE IF NOT EXISTS " + unsafeDB,
		"CREATE DATABASE IF NOT EXISTS " + safeDB,
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", unsafeDB, table),
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), v UInt64) ENGINE = MergeTree ORDER BY (_hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", unsafeDB, unpartitioned),
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", safeDB, table),
		fmt.Sprintf("SYSTEM STOP MERGES %s.%s", unsafeDB, table),
		fmt.Sprintf("SYSTEM STOP MERGES %s.%s", unsafeDB, unpartitioned),
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
}
