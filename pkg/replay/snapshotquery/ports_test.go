package snapshotquery

import (
	"context"
	"io"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

type testRowStream struct{}

func (testRowStream) Next(context.Context) ([]any, error) { return nil, io.EOF }
func (testRowStream) Close() error                        { return nil }

type testReadSnapshot struct{}

func (testReadSnapshot) Manifest() replay.SafeSnapshotManifest { return replay.SafeSnapshotManifest{} }
func (testReadSnapshot) SchemaArtifact() replay.AuthenticatedSnapshotQuerySchemaV1 {
	return replay.AuthenticatedSnapshotQuerySchemaV1{}
}
func (testReadSnapshot) Schemas() []payloadexec.TableSchema { return nil }
func (testReadSnapshot) Relations() []Relation              { return nil }
func (testReadSnapshot) QueryRows(context.Context, string, payloadexec.TableSchema) (RowStream, error) {
	return testRowStream{}, nil
}
func (testReadSnapshot) Close() error { return nil }

type testReadStore struct{}

func (testReadStore) Open(context.Context, replay.SnapshotPin, replay.SnapshotReadSet, string) (ReadSnapshot, error) {
	return testReadSnapshot{}, nil
}

func TestLeafPortSignatures(t *testing.T) {
	var _ RowStream = testRowStream{}
	var _ ReadSnapshot = testReadSnapshot{}
	var _ SnapshotReadStore = testReadStore{}
	relation := Relation{TableID: "db.t", Database: "db", Table: "t"}
	if relation.TableID != "db.t" || relation.Database != "db" || relation.Table != "t" {
		t.Fatal("relation fields changed")
	}
	rows, err := (testReadSnapshot{}).QueryRows(context.Background(), "SELECT id FROM db.t", payloadexec.TableSchema{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rows.Next(context.Background()); err != io.EOF {
		t.Fatalf("row stream successful end=%v, want io.EOF", err)
	}
}
