package chexec

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"
)

const snapshotTestKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type panicConn struct{}

func (panicConn) Contributors() []string { panic("clickhouse used") }
func (panicConn) ServerVersion() (*clickhouse.ServerVersion, error) {
	panic("clickhouse used")
}
func (panicConn) Select(context.Context, any, string, ...any) error { panic("clickhouse used") }
func (panicConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	panic("clickhouse used")
}
func (panicConn) QueryRow(context.Context, string, ...any) driver.Row { panic("clickhouse used") }
func (panicConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	panic("clickhouse used")
}
func (panicConn) Exec(context.Context, string, ...any) error { panic("clickhouse used") }
func (panicConn) AsyncInsert(context.Context, string, bool, ...any) error {
	panic("clickhouse used")
}
func (panicConn) Ping(context.Context) error { panic("clickhouse used") }
func (panicConn) Stats() driver.Stats        { panic("clickhouse used") }
func (panicConn) Close() error               { panic("clickhouse used") }

type recordingConn struct {
	execs   []string
	queries []string
	closed  int
	closeEr error
}

func (c *recordingConn) Contributors() []string                            { return nil }
func (c *recordingConn) ServerVersion() (*clickhouse.ServerVersion, error) { return nil, nil }
func (c *recordingConn) Select(context.Context, any, string, ...any) error { return nil }
func (c *recordingConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	c.queries = append(c.queries, query)
	return emptyRows{}, nil
}
func (c *recordingConn) QueryRow(context.Context, string, ...any) driver.Row { return errRow{} }
func (c *recordingConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errors.New("unused")
}
func (c *recordingConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return nil
}
func (c *recordingConn) AsyncInsert(context.Context, string, bool, ...any) error { return nil }
func (c *recordingConn) Ping(context.Context) error                              { return nil }
func (c *recordingConn) Stats() driver.Stats                                     { return driver.Stats{} }
func (c *recordingConn) Close() error {
	c.closed++
	return c.closeEr
}

type emptyRows struct{}

func (emptyRows) Next() bool           { return false }
func (emptyRows) Scan(...any) error    { return nil }
func (emptyRows) ScanStruct(any) error { return nil }
func (emptyRows) ColumnTypes() []driver.ColumnType {
	return nil
}
func (emptyRows) Totals(...any) error { return nil }
func (emptyRows) Columns() []string   { return nil }
func (emptyRows) Close() error        { return nil }
func (emptyRows) Err() error          { return nil }
func (emptyRows) HasData() bool       { return false }

type errRow struct{}

func (errRow) Err() error           { return errors.New("unused") }
func (errRow) Scan(...any) error    { return errors.New("unused") }
func (errRow) ScanStruct(any) error { return errors.New("unused") }

type restoreFixture struct {
	manifest replay.SafeSnapshotManifest
	pin      replay.SnapshotPin
	artifact replay.AuthenticatedSnapshotQuerySchemaV1
	schemas  []payloadexec.TableSchema
	reads    replay.SnapshotReadSet
	parts    []snapshotquery.VerifiedPart
	partFile string
}

func TestNewSnapshotRestorerRequiresDependencies(t *testing.T) {
	_, err := NewSnapshotRestorer(nil, func(context.Context, []snapshotquery.Relation) (clickhouse.Conn, error) {
		return &recordingConn{}, nil
	})
	if err == nil {
		t.Fatal("nil admin accepted")
	}
	_, err = NewSnapshotRestorer(panicConn{}, nil)
	if err == nil {
		t.Fatal("nil reader factory accepted")
	}
}

func TestRestoreRefusesBeforeClickHouse(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*testing.T, *restoreFixture)
	}{
		{"missing-part", func(_ *testing.T, f *restoreFixture) { f.parts = nil }},
		{"duplicate-part", func(_ *testing.T, f *restoreFixture) { f.parts = append(f.parts, f.parts[0]) }},
		{"extra-part", func(t *testing.T, f *restoreFixture) {
			payload := []byte("extra-bytes-xx")
			extra := f.parts[0]
			extra.Entry.PartName = "extra"
			extra.Entry.Bytes = uint64(len(payload))
			extra.Entry.PartPhysHash = replay.DigestBytes(payload)
			extra.LocalPath = writePart(t, extra.Entry.Bytes, payload)
			f.parts = append(f.parts, extra)
		}},
		{"row-count", func(_ *testing.T, f *restoreFixture) { f.parts[0].Entry.RowCount++ }},
		{"bytes", func(_ *testing.T, f *restoreFixture) { f.parts[0].Entry.Bytes++ }},
		{"phys-hash", func(_ *testing.T, f *restoreFixture) { f.parts[0].Entry.PartPhysHash = replay.DigestString("other") }},
		{"row-lthash", func(_ *testing.T, f *restoreFixture) {
			f.parts[0].Entry.PartRowLtHash = replay.DigestString("other-rows")
		}},
		{"corrupt-bytes", func(t *testing.T, f *restoreFixture) {
			if err := os.WriteFile(f.partFile, []byte("tampered-part-bytes"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"removed-read-table", func(_ *testing.T, f *restoreFixture) {
			tables := f.manifest.Tables[:0]
			for _, table := range f.manifest.Tables {
				if table.TableID != "R" {
					tables = append(tables, table)
				}
			}
			f.manifest.Tables = tables
		}},
		{"schema-substitution", func(_ *testing.T, f *restoreFixture) {
			f.schemas[0].Columns[0].Type = "String"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRestoreFixture(t, 1)
			tc.mut(t, &f)
			restorer, err := NewSnapshotRestorer(panicConn{}, func(context.Context, []snapshotquery.Relation) (clickhouse.Conn, error) {
				t.Fatal("reader factory called")
				return nil, errors.New("unreachable")
			})
			if err != nil {
				t.Fatal(err)
			}
			handle, err := restorer.Restore(context.Background(), f.manifest, f.artifact, f.schemas, f.reads, f.parts)
			if err == nil || handle != nil {
				t.Fatalf("want refusal before QueryRows, handle=%v err=%v", handle, err)
			}
		})
	}
}

func TestRestoreEmptyReadTableAndRetainsExactObject(t *testing.T) {
	f := newRestoreFixture(t, 0)
	admin := &recordingConn{}
	reader := &recordingConn{}
	restorer, err := NewSnapshotRestorer(admin, func(_ context.Context, rels []snapshotquery.Relation) (clickhouse.Conn, error) {
		if len(rels) != 1 || rels[0].TableID != "R" {
			t.Fatalf("relations=%+v", rels)
		}
		if rels[0].Database == "tenant" || rels[0].Table == "events" {
			t.Fatal("scratch names reused logical names")
		}
		return reader, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := restorer.Restore(context.Background(), f.manifest, f.artifact, f.schemas, f.reads, f.parts)
	if err != nil {
		t.Fatal(err)
	}
	got := handle.SchemaArtifact()
	got.Artifact.Tables[0].Columns[0].DefaultExpression = "now()"
	if reflect.DeepEqual(got.Artifact, handle.SchemaArtifact().Artifact) {
		t.Fatal("handle exposed a mutable schema artifact")
	}
	if handle.SchemaArtifact().Artifact.Tables[0].TableID != "R" || len(handle.Schemas()) != 3 {
		t.Fatal("complete projection was not retained")
	}
	if _, err := handle.QueryRows(context.Background(), "SELECT value FROM tenant.events", handle.Schemas()[0]); err == nil {
		t.Fatal("production relation SQL accepted")
	}
	if _, err := handle.QueryRows(context.Background(), "SELECT * FROM remote('x', tenant.events)", handle.Schemas()[0]); err == nil {
		t.Fatal("remote() SQL accepted")
	}
	if err := handle.Close(); err != nil || reader.closed != 1 {
		t.Fatal("close", err, reader.closed)
	}
}

func TestRestoreRefusesAdminAsReaderAndRetainsCloseUncertainty(t *testing.T) {
	f := newRestoreFixture(t, 0)
	admin := &recordingConn{}
	restorer, err := NewSnapshotRestorer(admin, func(context.Context, []snapshotquery.Relation) (clickhouse.Conn, error) {
		return admin, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restorer.Restore(context.Background(), f.manifest, f.artifact, f.schemas, f.reads, f.parts); err == nil {
		t.Fatal("admin reused as reader")
	}
	reader := &recordingConn{closeEr: errors.New("reader close unknown")}
	restorer, err = NewSnapshotRestorer(&recordingConn{}, func(context.Context, []snapshotquery.Relation) (clickhouse.Conn, error) {
		return reader, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := restorer.Restore(context.Background(), f.manifest, f.artifact, f.schemas, f.reads, f.parts)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err == nil {
		t.Fatal("unknown reader close reported success")
	}
}

func newRestoreFixture(t *testing.T, rowCount uint64) restoreFixture {
	t.Helper()
	schemas := []payloadexec.TableSchema{
		{TableID: "R", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}},
		{TableID: "U", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}},
		{TableID: "W", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}},
	}
	exec := payloadexec.New("network-b2", schemas...)
	manifest, err := exec.GenesisSnapshot(3, "schema-b2", "executor-b2")
	if err != nil {
		t.Fatal(err)
	}
	var parts []snapshotquery.VerifiedPart
	var partFile string
	if rowCount > 0 {
		payload := []byte("authenticated-part-body")
		partFile = writePart(t, uint64(len(payload)), payload)
		entry := replay.PartManifestEntry{
			TableID:       "R",
			PartitionID:   "all",
			PartName:      "all_0_0_0",
			PartPhysHash:  replay.DigestBytes(payload),
			PartRowLtHash: replay.DigestString("row-lthash"),
			RowCount:      rowCount,
			Bytes:         uint64(len(payload)),
		}
		for i := range manifest.Tables {
			if manifest.Tables[i].TableID == "R" {
				manifest.Tables[i].ActiveParts = []replay.PartManifestEntry{entry}
			}
		}
		sealed, err := manifest.Seal()
		if err != nil {
			t.Fatal(err)
		}
		manifest = sealed
		parts = []snapshotquery.VerifiedPart{{Entry: entry, LocalPath: partFile}}
	}
	pin := replay.SnapshotPin{
		NetworkID:        "network-b2",
		KeeperShardID:    1,
		SnapshotID:       manifest.SnapshotID,
		SafeBlockSeq:     manifest.SafeBlockSeq,
		ManifestRoot:     manifest.ManifestRoot,
		StateRoot:        manifest.StateRoot,
		SchemaSnapshotID: manifest.SchemaSnapshotID,
		SchemaRoot:       manifest.SchemaRoot,
	}
	tables := make([]replay.SnapshotQueryTableSchemaV1, 0, len(schemas))
	for _, schema := range schemas {
		cols := make([]replay.SnapshotQuerySchemaColumnV1, len(schema.Columns))
		for i, col := range schema.Columns {
			cols[i] = replay.SnapshotQuerySchemaColumnV1{Name: col.Name, Type: col.Type, Generation: replay.SnapshotQueryColumnGenerationOrdinary}
		}
		tables = append(tables, replay.SnapshotQueryTableSchemaV1{TableID: schema.TableID, SchemaHash: payloadexec.TableSchemaHash(pin.NetworkID, schema), PartitionBy: schema.PartitionBy, Columns: cols})
	}
	artifact := replay.SnapshotQuerySchemaArtifactV1{
		Kind: replay.SnapshotQuerySchemaArtifactKindV1, Version: replay.SnapshotQuerySchemaArtifactVersionV1,
		NetworkID: pin.NetworkID, KeeperShardID: pin.KeeperShardID, SnapshotID: pin.SnapshotID,
		ManifestRoot: pin.ManifestRoot, SchemaSnapshotID: pin.SchemaSnapshotID, SchemaRoot: pin.SchemaRoot,
		Tables: tables,
	}
	digest, err := replay.SnapshotQuerySchemaArtifactDigestV1(artifact)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSnapshotSchemaCertificateSigner(snapshotTestKey)
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.SignSnapshotSchemaCertificateV1(digest)
	if err != nil {
		t.Fatal(err)
	}
	authenticated := replay.AuthenticatedSnapshotQuerySchemaV1{Artifact: artifact, AuthorityJWS: token}
	readParts := make([]replay.SnapshotReadPart, 0, len(parts))
	for _, part := range parts {
		readParts = append(readParts, replay.SnapshotReadPart{
			TableID: part.Entry.TableID, PartitionID: part.Entry.PartitionID, PartName: part.Entry.PartName,
			PartPhysHash: part.Entry.PartPhysHash, PartRowLtHash: part.Entry.PartRowLtHash,
			RowCount: part.Entry.RowCount, Bytes: part.Entry.Bytes,
		})
	}
	var rRoot []replay.PartitionCommitment
	for _, table := range manifest.Tables {
		if table.TableID == "R" {
			rRoot = append([]replay.PartitionCommitment{}, table.PartitionRoots...)
		}
	}
	reads := replay.SnapshotReadSet{
		ReadSnapshot: pin,
		Tables: []replay.SnapshotReadTable{{
			Database: "tenant", Table: "events", TableID: "R",
			SchemaHash:     payloadexec.TableSchemaHash(pin.NetworkID, schemas[0]),
			PartitionRoots: rRoot, ActiveParts: readParts,
		}},
	}
	cloned := append([]payloadexec.TableSchema{}, schemas...)
	return restoreFixture{manifest: manifest, pin: pin, artifact: authenticated, schemas: cloned, reads: reads, parts: parts, partFile: partFile}
}

func writePart(t *testing.T, wantBytes uint64, payload []byte) string {
	t.Helper()
	if uint64(len(payload)) != wantBytes {
		t.Fatalf("payload %d want %d", len(payload), wantBytes)
	}
	path := filepath.Join(t.TempDir(), "part.bin")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

var (
	_ clickhouse.Conn = panicConn{}
	_ clickhouse.Conn = (*recordingConn)(nil)
)
