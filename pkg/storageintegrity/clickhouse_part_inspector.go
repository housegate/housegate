package storageintegrity

import (
	"context"
	"fmt"
	"sort"

	"housegate/housegate/pkg/lthash"
)

// PartDescriptor is the live metadata of one active ClickHouse part, read from
// system.parts. It is the authoritative source of a part's identity for the
// caching fast paths: PartName / PartitionID / Rows / Bytes always come from
// here (never from a cache), and PartPhysHash (system.parts.hash_of_all_files)
// is the content address used to key the part-LtHash cache.
type PartDescriptor struct {
	Database    string
	Table       string
	TableID     string
	SchemaHash  string
	PartitionID string
	PartName    string
	// PartPhysHash is system.parts.hash_of_all_files. It may be empty on some
	// storage/backup states or very old parts; an empty value means "no usable
	// content address" and callers MUST bypass the cache for that part (fold its
	// rows) rather than key on an empty hash.
	PartPhysHash string
	Rows         uint64
	Bytes        uint64
}

// CacheKey builds the PartLtHashKey for this descriptor. ok is false when the
// part has no usable content address (empty PartPhysHash) — the caller must
// then bypass the cache entirely.
func (d PartDescriptor) CacheKey() (PartLtHashKey, bool) {
	phys := normalizeCachePhysHash(d.PartPhysHash)
	if phys == "" {
		return PartLtHashKey{}, false
	}
	key := PartLtHashKey{
		RowHashVersion: lthash.RowHashVersion(),
		TableID:        d.TableID,
		SchemaHash:     d.SchemaHash,
		PartPhysHash:   phys,
	}
	return key, key.valid()
}

// ClickHouseSchemaHashReader resolves the live schema hash of a physical table
// from system.columns, folded through the shared nativeSchemaHash. It satisfies
// SchemaColumnReader for the caching part scanner. The value is a cache-key
// namespacing device only (it never leaves the cache); it just has to be stable
// across calls for a given table and to change when a column's name/type
// changes — both hold because the query orders columns by name and
// nativeSchemaHash folds name+type. A schema change (out of MVP scope) yields a
// different key, so stale-schema hits cannot occur.
type ClickHouseSchemaHashReader struct {
	Conn HashQueryConn
}

func (r ClickHouseSchemaHashReader) SchemaHash(ctx context.Context, database, table, tableID string) (string, error) {
	if r.Conn == nil {
		return "", fmt.Errorf("clickhouse query connection is required")
	}
	rows, err := r.Conn.Query(ctx, "SELECT name, type FROM system.columns WHERE database = "+
		sqlStringLiteral(database)+" AND table = "+sqlStringLiteral(table)+" ORDER BY name")
	if err != nil {
		return "", fmt.Errorf("query system.columns for %s.%s: %w", database, table, err)
	}
	defer rows.Close()
	var cols []lthash.Column
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return "", fmt.Errorf("scan system.columns for %s.%s: %w", database, table, err)
		}
		cols = append(cols, lthash.Column{Name: name, Type: typ})
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("system.columns rows for %s.%s: %w", database, table, err)
	}
	if len(cols) == 0 {
		return "", fmt.Errorf("no columns found for %s.%s", database, table)
	}
	return nativeSchemaHash(tableID, cols), nil
}

// PartInspector reads live active-part metadata from ClickHouse. It is the
// system.parts front door for the fast paths: cheap metadata (no row scan) that
// yields the content address (hash_of_all_files) and the authoritative row
// count used to fail closed against a cached row-LtHash.
type PartInspector interface {
	// ActiveParts returns one descriptor per active part of database.table,
	// optionally filtered to the given partition IDs (empty = all partitions).
	ActiveParts(ctx context.Context, database, table string, partitionIDs []string) ([]PartDescriptor, error)
}

// ClickHousePartInspector reads system.parts for active-part metadata. It does
// not read row data; the row-LtHash comes from the cache (hit) or a targeted
// scan (miss) elsewhere. SchemaResolver, when set, supplies the schema hash for
// each table so descriptors carry a complete cache key; when nil, PartPhysHash
// is still populated but SchemaHash is empty (callers that need the cache must
// supply the schema hash themselves).
type ClickHousePartInspector struct {
	Conn HashQueryConn
	// TableID overrides the logical table id stamped on descriptors; empty uses
	// normalizeTableID(qualifiedTable(database, table)) — the same normalization
	// the row fold applies, so the cache key's TableID matches the fold input.
	TableID string
	// SchemaHashFor resolves the schema hash for a (database, table). Optional.
	SchemaHashFor func(ctx context.Context, database, table, tableID string) (string, error)
}

func (r ClickHousePartInspector) ActiveParts(ctx context.Context, database, table string, partitionIDs []string) ([]PartDescriptor, error) {
	if r.Conn == nil {
		return nil, fmt.Errorf("clickhouse query connection is required")
	}
	if database == "" || table == "" {
		return nil, fmt.Errorf("part inspector requires database and table")
	}
	tableID := r.TableID
	if tableID == "" {
		tableID = normalizeTableID(qualifiedTable(database, table))
	}
	var schemaHash string
	if r.SchemaHashFor != nil {
		var err error
		schemaHash, err = r.SchemaHashFor(ctx, database, table, tableID)
		if err != nil {
			return nil, fmt.Errorf("resolve schema hash for %s.%s: %w", database, table, err)
		}
	}

	query := "SELECT partition_id, name, hash_of_all_files, rows, bytes_on_disk FROM system.parts " +
		"WHERE database = " + sqlStringLiteral(database) + " AND table = " + sqlStringLiteral(table) + " AND active"
	if len(partitionIDs) > 0 {
		query += " AND partition_id IN (" + sqlStringList(partitionIDs) + ")"
	}
	query += " ORDER BY partition_id, name"

	rows, err := r.Conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query system.parts for %s.%s: %w", database, table, err)
	}
	defer rows.Close()

	var out []PartDescriptor
	for rows.Next() {
		var (
			partitionID  string
			name         string
			hashOfFiles  string
			rowCount     uint64
			bytesOnDisk  uint64
		)
		if err := rows.Scan(&partitionID, &name, &hashOfFiles, &rowCount, &bytesOnDisk); err != nil {
			return nil, fmt.Errorf("scan system.parts row for %s.%s: %w", database, table, err)
		}
		out = append(out, PartDescriptor{
			Database:     database,
			Table:        table,
			TableID:      tableID,
			SchemaHash:   schemaHash,
			PartitionID:  partitionID,
			PartName:     name,
			PartPhysHash: normalizeCachePhysHash(hashOfFiles),
			Rows:         rowCount,
			Bytes:        bytesOnDisk,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("system.parts rows for %s.%s: %w", database, table, err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PartitionID != out[j].PartitionID {
			return out[i].PartitionID < out[j].PartitionID
		}
		return out[i].PartName < out[j].PartName
	})
	return out, nil
}
