package storageintegrity

import (
	"context"
	"strings"
	"testing"

	"housegate/housegate/pkg/lthash"
)

func TestClickHousePartInspectorParsesActiveParts(t *testing.T) {
	conn := &fakeHashQueryConn{rows: &fakeHashRows{
		columns: []string{"partition_id", "name", "hash_of_all_files", "rows", "bytes_on_disk"},
		types:   []string{"String", "String", "String", "UInt64", "UInt64"},
		values: [][]any{
			{"p1", "all_1_1_0", "phys-aaa", uint64(3), uint64(120)},
			{"p1", "all_2_2_0", "phys-bbb", uint64(2), uint64(80)},
		},
	}}
	insp := ClickHousePartInspector{
		Conn:    conn,
		TableID: "hg_safe.events",
		SchemaHashFor: func(_ context.Context, _, _, _ string) (string, error) {
			return "schema-xyz", nil
		},
	}
	parts, err := insp.ActiveParts(context.Background(), "hg_safe", "events", []string{"p1"})
	if err != nil {
		t.Fatalf("ActiveParts: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 descriptors, got %d", len(parts))
	}
	// Query must target system.parts, filter active + the requested partition.
	for _, want := range []string{"FROM system.parts", "active", "partition_id IN ('p1')", "hash_of_all_files", "bytes_on_disk"} {
		if !strings.Contains(conn.query, want) {
			t.Fatalf("query missing %q:\n%s", want, conn.query)
		}
	}
	d := parts[0]
	if d.PartitionID != "p1" || d.PartName != "all_1_1_0" || d.PartPhysHash != "phys-aaa" || d.Rows != 3 || d.Bytes != 120 {
		t.Fatalf("descriptor[0] = %+v", d)
	}
	if d.TableID != "hg_safe.events" || d.SchemaHash != "schema-xyz" {
		t.Fatalf("descriptor[0] identity = %+v", d)
	}
	// A complete descriptor yields a valid cache key bound to the row-hash version.
	key, ok := d.CacheKey()
	if !ok {
		t.Fatalf("expected a valid cache key for a populated phys hash")
	}
	if key.RowHashVersion != lthash.RowHashVersion() || key.TableID != "hg_safe.events" || key.SchemaHash != "schema-xyz" || key.PartPhysHash != "phys-aaa" {
		t.Fatalf("cache key = %+v", key)
	}
}

func TestClickHousePartInspectorEmptyChecksumDegrades(t *testing.T) {
	// Guard #5: a part whose hash_of_all_files is empty must NOT produce a cache
	// key (ok=false) so the caller bypasses the cache and folds its rows. The
	// inspector itself must not error — it just leaves PartPhysHash empty.
	conn := &fakeHashQueryConn{rows: &fakeHashRows{
		columns: []string{"partition_id", "name", "hash_of_all_files", "rows", "bytes_on_disk"},
		types:   []string{"String", "String", "String", "UInt64", "UInt64"},
		values: [][]any{
			{"p1", "all_1_1_0", "  ", uint64(3), uint64(120)}, // whitespace-only → empty
		},
	}}
	insp := ClickHousePartInspector{Conn: conn, TableID: "hg_safe.events"}
	parts, err := insp.ActiveParts(context.Background(), "hg_safe", "events", nil)
	if err != nil {
		t.Fatalf("ActiveParts must not error on empty checksum: %v", err)
	}
	if len(parts) != 1 || parts[0].PartPhysHash != "" {
		t.Fatalf("expected one descriptor with empty phys, got %+v", parts)
	}
	if _, ok := parts[0].CacheKey(); ok {
		t.Fatalf("empty phys hash must not yield a usable cache key")
	}
}

func TestClickHousePartInspectorDefaultTableIDMatchesFold(t *testing.T) {
	// When TableID is unset, the descriptor's TableID must equal the string the
	// row fold uses: normalizeTableID(qualifiedTable(db,table)).
	conn := &fakeHashQueryConn{rows: &fakeHashRows{
		columns: []string{"partition_id", "name", "hash_of_all_files", "rows", "bytes_on_disk"},
		types:   []string{"String", "String", "String", "UInt64", "UInt64"},
		values:  [][]any{{"p1", "all_1_1_0", "phys-aaa", uint64(1), uint64(10)}},
	}}
	insp := ClickHousePartInspector{Conn: conn}
	parts, err := insp.ActiveParts(context.Background(), "hg_promote", "events_shadow", nil)
	if err != nil {
		t.Fatalf("ActiveParts: %v", err)
	}
	want := normalizeTableID(qualifiedTable("hg_promote", "events_shadow"))
	if parts[0].TableID != want {
		t.Fatalf("default TableID = %q, want %q", parts[0].TableID, want)
	}
}

func TestClickHousePartInspectorRequiresConn(t *testing.T) {
	var insp ClickHousePartInspector
	if _, err := insp.ActiveParts(context.Background(), "db", "t", nil); err == nil {
		t.Fatalf("expected error when Conn is nil")
	}
}
