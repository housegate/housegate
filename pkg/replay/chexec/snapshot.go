package chexec

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"
)

// snapshotRestorer restores authenticated read tables onto restricted scratch
// relations. It is constructed explicitly and is not wired into production
// build.go; the snapshot-query lane stays default-off.
type snapshotRestorer struct {
	admin   clickhouse.Conn
	factory func(context.Context, []snapshotquery.Relation) (clickhouse.Conn, error)
}

// NewSnapshotRestorer returns the B2 HG ScratchRestorer. admin is used only for
// scratch DDL; readerFactory must provision a distinct restricted reader.
func NewSnapshotRestorer(admin clickhouse.Conn, readerFactory func(context.Context, []snapshotquery.Relation) (clickhouse.Conn, error)) (snapshotquery.ScratchRestorer, error) {
	if admin == nil || isNilConn(admin) {
		return nil, fmt.Errorf("chexec: snapshot restorer admin connection is required")
	}
	if readerFactory == nil {
		return nil, fmt.Errorf("chexec: snapshot restorer reader factory is required")
	}
	return &snapshotRestorer{admin: admin, factory: readerFactory}, nil
}

func (r *snapshotRestorer) Restore(ctx context.Context, manifest replay.SafeSnapshotManifest, schemaArtifact replay.AuthenticatedSnapshotQuerySchemaV1, schemas []payloadexec.TableSchema, reads replay.SnapshotReadSet, parts []snapshotquery.VerifiedPart) (snapshotquery.ReadSnapshot, error) {
	if ctx == nil {
		return nil, fmt.Errorf("chexec: restore context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	digest, err := replay.AuthenticatedSnapshotQuerySchemaDigestV1(schemaArtifact)
	if err != nil {
		return nil, fmt.Errorf("chexec: schema artifact: %w", err)
	}
	profile, err := snapshotquery.ValidateAuthenticatedSchemaProfile(manifest, reads.ReadSnapshot, schemaArtifact, digest)
	if err != nil {
		return nil, fmt.Errorf("chexec: restore schema: %w", err)
	}
	projected := profile.Schemas()
	if err := sameSchemas(schemas, projected); err != nil {
		return nil, err
	}
	expected, production, err := expectedReadParts(manifest, reads)
	if err != nil {
		return nil, err
	}
	verified, err := verifyParts(expected, parts)
	if err != nil {
		return nil, err
	}
	relations, err := scratchRelations(reads)
	if err != nil {
		return nil, err
	}
	if err := r.materialize(ctx, relations, projected, verified); err != nil {
		return nil, err
	}
	reader, err := r.factory(ctx, cloneRelations(relations))
	if err != nil {
		_ = dropScratch(r.admin, relations)
		return nil, fmt.Errorf("chexec: reader factory: %w", err)
	}
	if reader == nil || isNilConn(reader) || sameConn(reader, r.admin) {
		_ = dropScratch(r.admin, relations)
		return nil, fmt.Errorf("chexec: reader factory must provision a distinct restricted connection")
	}
	return &snapshotHandle{
		manifest:   manifest,
		artifact:   profile.SchemaArtifact(),
		schemas:    cloneTableSchemas(projected),
		relations:  relations,
		production: production,
		admin:      r.admin,
		reader:     reader,
		partCount:  len(verified),
	}, nil
}

func (r *snapshotRestorer) materialize(ctx context.Context, relations []snapshotquery.Relation, schemas []payloadexec.TableSchema, parts []snapshotquery.VerifiedPart) error {
	byID := make(map[string]payloadexec.TableSchema, len(schemas))
	for _, schema := range schemas {
		byID[schema.TableID] = schema
	}
	seenDB := map[string]struct{}{}
	for _, rel := range relations {
		if _, ok := seenDB[rel.Database]; !ok {
			if err := r.admin.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(rel.Database)); err != nil {
				return fmt.Errorf("chexec: create scratch database: %w", err)
			}
			seenDB[rel.Database] = struct{}{}
		}
		schema, ok := byID[rel.TableID]
		if !ok {
			return fmt.Errorf("chexec: scratch table %q has no schema projection", rel.TableID)
		}
		if err := r.admin.Exec(ctx, createScratchSQL(rel, schema)); err != nil {
			return fmt.Errorf("chexec: create scratch table: %w", err)
		}
	}
	relByTable := map[string]snapshotquery.Relation{}
	for _, rel := range relations {
		relByTable[rel.TableID] = rel
	}
	for _, part := range parts {
		rel, ok := relByTable[part.Entry.TableID]
		if !ok {
			return fmt.Errorf("chexec: verified part %q has no scratch relation", part.Entry.TableID)
		}
		stmt := fmt.Sprintf("ALTER TABLE %s.%s ATTACH PART %s FROM %s", quoteIdent(rel.Database), quoteIdent(rel.Table), quoteString(part.Entry.PartName), quoteString(part.LocalPath))
		if err := r.admin.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("chexec: attach scratch part: %w", err)
		}
	}
	return nil
}

type snapshotHandle struct {
	manifest   replay.SafeSnapshotManifest
	artifact   replay.AuthenticatedSnapshotQuerySchemaV1
	schemas    []payloadexec.TableSchema
	relations  []snapshotquery.Relation
	production []string
	admin      clickhouse.Conn
	reader     clickhouse.Conn
	partCount  int
	closed     bool
}

func (h *snapshotHandle) Manifest() replay.SafeSnapshotManifest { return h.manifest }
func (h *snapshotHandle) SchemaArtifact() replay.AuthenticatedSnapshotQuerySchemaV1 {
	out := h.artifact
	out.Artifact = h.artifact.Artifact.Clone()
	return out
}
func (h *snapshotHandle) Schemas() []payloadexec.TableSchema { return cloneTableSchemas(h.schemas) }
func (h *snapshotHandle) Relations() []snapshotquery.Relation {
	return cloneRelations(h.relations)
}

func (h *snapshotHandle) QueryRows(ctx context.Context, sql string, schema payloadexec.TableSchema) (snapshotquery.RowStream, error) {
	if h.closed {
		return nil, fmt.Errorf("chexec: snapshot handle is closed")
	}
	if ctx == nil {
		return nil, fmt.Errorf("chexec: query context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	folded := strings.ToLower(sql)
	if strings.Contains(folded, "remote(") || strings.Contains(folded, "remotesecure(") {
		return nil, fmt.Errorf("chexec: remote catalog access is forbidden on restored readers")
	}
	for _, name := range h.production {
		if name != "" && strings.Contains(folded, strings.ToLower(name)) {
			return nil, fmt.Errorf("chexec: production relation %q is forbidden on restored readers", name)
		}
	}
	rows, err := h.reader.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &connRowStream{rows: rows, schema: schema}, nil
}

func (h *snapshotHandle) Close() error {
	if h.closed {
		return nil
	}
	if h.reader != nil {
		if err := h.reader.Close(); err != nil {
			return err
		}
	}
	if err := dropScratch(h.admin, h.relations); err != nil {
		return err
	}
	h.closed = true
	return nil
}

type connRowStream struct {
	rows   driverRows
	schema payloadexec.TableSchema
}

type driverRows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

func (s *connRowStream) Next(context.Context) ([]any, error) {
	if s.rows == nil {
		return nil, io.EOF
	}
	if !s.rows.Next() {
		if err := s.rows.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	dest := make([]any, len(s.schema.Columns))
	ptrs := make([]any, len(dest))
	for i := range dest {
		ptrs[i] = &dest[i]
	}
	if err := s.rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	return dest, nil
}

func (s *connRowStream) Close() error {
	if s.rows == nil {
		return nil
	}
	return s.rows.Close()
}

func expectedReadParts(manifest replay.SafeSnapshotManifest, reads replay.SnapshotReadSet) (map[string]replay.PartManifestEntry, []string, error) {
	tables := make(map[string]replay.TableManifest, len(manifest.Tables))
	for _, table := range manifest.Tables {
		tables[table.TableID] = table
	}
	expected := make(map[string]replay.PartManifestEntry)
	var production []string
	if len(reads.Tables) == 0 {
		return nil, nil, fmt.Errorf("chexec: restore requires at least one read table")
	}
	for _, table := range reads.Tables {
		manifestTable, ok := tables[table.TableID]
		if !ok {
			return nil, nil, fmt.Errorf("chexec: read table %q is absent from the manifest", table.TableID)
		}
		if table.SchemaHash != manifestTable.SchemaHash {
			return nil, nil, fmt.Errorf("chexec: read table %q schema_hash mismatch", table.TableID)
		}
		if strings.TrimSpace(table.Database) == "" || strings.TrimSpace(table.Table) == "" {
			return nil, nil, fmt.Errorf("chexec: read table %q logical name is required", table.TableID)
		}
		production = append(production, table.Database+"."+table.Table)
		if len(table.ActiveParts) != len(manifestTable.ActiveParts) {
			return nil, nil, fmt.Errorf("chexec: read table %q does not name every active part", table.TableID)
		}
		manifestParts := make(map[string]replay.PartManifestEntry, len(manifestTable.ActiveParts))
		for _, part := range manifestTable.ActiveParts {
			manifestParts[partKey(part.TableID, part.PartitionID, part.PartName)] = part
		}
		for _, part := range table.ActiveParts {
			key := partKey(part.TableID, part.PartitionID, part.PartName)
			want, ok := manifestParts[key]
			if !ok {
				return nil, nil, fmt.Errorf("chexec: read part %s is absent from the manifest", key)
			}
			got := replay.PartManifestEntry{
				TableID: part.TableID, PartitionID: part.PartitionID, PartName: part.PartName,
				PartPhysHash: part.PartPhysHash, PartRowLtHash: part.PartRowLtHash,
				RowCount: part.RowCount, Bytes: part.Bytes,
			}
			if !samePartIdentity(got, want) {
				return nil, nil, fmt.Errorf("chexec: read part %s does not match the manifest", key)
			}
			if _, dup := expected[key]; dup {
				return nil, nil, fmt.Errorf("chexec: duplicate read part %s", key)
			}
			expected[key] = want
		}
	}
	return expected, production, nil
}

func verifyParts(expected map[string]replay.PartManifestEntry, parts []snapshotquery.VerifiedPart) ([]snapshotquery.VerifiedPart, error) {
	if len(parts) != len(expected) {
		return nil, fmt.Errorf("chexec: verified part set does not match the read descriptor")
	}
	seen := make(map[string]struct{}, len(parts))
	out := make([]snapshotquery.VerifiedPart, 0, len(parts))
	for _, part := range parts {
		key := partKey(part.Entry.TableID, part.Entry.PartitionID, part.Entry.PartName)
		want, ok := expected[key]
		if !ok {
			return nil, fmt.Errorf("chexec: unexpected verified part %s", key)
		}
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("chexec: duplicate verified part %s", key)
		}
		if !samePartIdentity(part.Entry, want) {
			return nil, fmt.Errorf("chexec: verified part %s does not match the manifest", key)
		}
		if err := verifyPartFile(part); err != nil {
			return nil, err
		}
		seen[key] = struct{}{}
		out = append(out, part)
	}
	return out, nil
}

func verifyPartFile(part snapshotquery.VerifiedPart) error {
	info, err := os.Lstat(part.LocalPath)
	if err != nil {
		return fmt.Errorf("chexec: verified part %q: %w", part.Entry.PartName, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("chexec: verified part %q is not a regular file", part.Entry.PartName)
	}
	if uint64(info.Size()) != part.Entry.Bytes {
		return fmt.Errorf("chexec: verified part %q size mismatch", part.Entry.PartName)
	}
	raw, err := os.ReadFile(part.LocalPath)
	if err != nil {
		return fmt.Errorf("chexec: verified part %q: %w", part.Entry.PartName, err)
	}
	if uint64(len(raw)) != part.Entry.Bytes || replay.DigestBytes(raw) != part.Entry.PartPhysHash {
		return fmt.Errorf("chexec: verified part %q physical hash mismatch", part.Entry.PartName)
	}
	return nil
}

func scratchRelations(reads replay.SnapshotReadSet) ([]snapshotquery.Relation, error) {
	out := make([]snapshotquery.Relation, 0, len(reads.Tables))
	used := map[string]struct{}{}
	for _, table := range reads.Tables {
		db, err := randomIdent("db")
		if err != nil {
			return nil, err
		}
		name, err := randomIdent("t")
		if err != nil {
			return nil, err
		}
		if db == table.Database || name == table.Table {
			return nil, fmt.Errorf("chexec: scratch name collided with logical identity")
		}
		key := db + "." + name
		if _, ok := used[key]; ok {
			return nil, fmt.Errorf("chexec: duplicate scratch relation")
		}
		used[key] = struct{}{}
		out = append(out, snapshotquery.Relation{TableID: table.TableID, Database: db, Table: name})
	}
	return out, nil
}

func randomIdent(prefix string) (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return "_hg_sq_" + prefix + "_" + hex.EncodeToString(buf[:]), nil
}

func createScratchSQL(rel snapshotquery.Relation, schema payloadexec.TableSchema) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(quoteIdent(rel.Database))
	b.WriteString(".")
	b.WriteString(quoteIdent(rel.Table))
	b.WriteString(" (")
	b.WriteString(quoteIdent(rowIDColumn))
	b.WriteString(" FixedString(32)")
	for _, col := range schema.Columns {
		b.WriteString(", ")
		b.WriteString(quoteIdent(col.Name))
		b.WriteString(" ")
		b.WriteString(col.Type)
	}
	b.WriteString(") ENGINE = MergeTree ORDER BY tuple()")
	return b.String()
}

func dropScratch(admin clickhouse.Conn, relations []snapshotquery.Relation) error {
	var first error
	for _, rel := range relations {
		stmt := "DROP TABLE IF EXISTS " + quoteIdent(rel.Database) + "." + quoteIdent(rel.Table)
		if err := admin.Exec(context.Background(), stmt); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func sameSchemas(got, want []payloadexec.TableSchema) error {
	if len(got) != len(want) {
		return fmt.Errorf("chexec: supplied schemas are not the complete authenticated projection")
	}
	for i := range want {
		if got[i].TableID != want[i].TableID || got[i].PartitionBy != want[i].PartitionBy || len(got[i].Columns) != len(want[i].Columns) {
			return fmt.Errorf("chexec: supplied schemas are not the complete authenticated projection")
		}
		for j := range want[i].Columns {
			if got[i].Columns[j] != want[i].Columns[j] {
				return fmt.Errorf("chexec: supplied schemas are not the complete authenticated projection")
			}
		}
	}
	return nil
}

func cloneTableSchemas(in []payloadexec.TableSchema) []payloadexec.TableSchema {
	out := append([]payloadexec.TableSchema{}, in...)
	for i := range out {
		out[i].Columns = append([]lthash.Column{}, in[i].Columns...)
	}
	return out
}

func cloneRelations(in []snapshotquery.Relation) []snapshotquery.Relation {
	return append([]snapshotquery.Relation{}, in...)
}

func partKey(tableID, partitionID, partName string) string {
	return tableID + "\x00" + partitionID + "\x00" + partName
}

func samePartIdentity(a, b replay.PartManifestEntry) bool {
	return a.TableID == b.TableID && a.PartitionID == b.PartitionID && a.PartName == b.PartName &&
		a.PartPhysHash == b.PartPhysHash && a.PartRowLtHash == b.PartRowLtHash &&
		a.RowCount == b.RowCount && a.Bytes == b.Bytes
}

func quoteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "\\'") + "'"
}

func isNilConn(conn clickhouse.Conn) bool {
	if conn == nil {
		return true
	}
	v := reflect.ValueOf(conn)
	return v.Kind() == reflect.Pointer && v.IsNil()
}

func sameConn(a, b clickhouse.Conn) bool {
	if a == nil || b == nil || isNilConn(a) || isNilConn(b) {
		return false
	}
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if va.Kind() != reflect.Pointer || vb.Kind() != reflect.Pointer {
		return false
	}
	return va.Pointer() == vb.Pointer()
}
