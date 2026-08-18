package schemaregistry

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

const networkStateLoaderTestNetwork = "schema-registry-testnet"

type fakeTableSchemas struct {
	latest map[string]registry.TableSchema
	exact  map[string]registry.TableSchema
}

func (f fakeTableSchemas) TableSchema(databaseID, tableID string, version uint32) (registry.TableSchema, bool) {
	schema, ok := f.exact[fmt.Sprintf("%s/%s@%d", databaseID, tableID, version)]
	return schema, ok
}

func (f fakeTableSchemas) LatestTableSchema(databaseID, tableID string) (registry.TableSchema, bool) {
	schema, ok := f.latest[databaseID+"/"+tableID]
	return schema, ok
}

type fakeSchemaLoader struct {
	schemas []payloadexec.TableSchema
	err     error
}

func (f fakeSchemaLoader) Load(context.Context, []TableRef) ([]payloadexec.TableSchema, error) {
	return f.schemas, f.err
}

func TestNetworkStateLoaderLoad(t *testing.T) {
	ref := TableRef{TableID: "orders.t", Database: "hg_unsafe", Table: "orders__t"}
	schema := payloadexec.TableSchema{
		TableID:     ref.TableID,
		PartitionBy: "day",
		Columns: []lthash.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "day", Type: "Date"},
		},
	}
	schemaJSON := `{"table_id":"orders.t","partition_by":"day","columns":[{"name":"id","type":"UInt64"},{"name":"day","type":"Date"}]}`
	hash := payloadexec.TableSchemaHash(networkStateLoaderTestNetwork, schema)
	declared := registry.TableSchema{
		DatabaseId: "orders",
		TableId:    "t",
		Version:    2,
		SchemaHash: hash,
		SchemaJson: schemaJSON,
	}

	tests := []struct {
		name       string
		source     fakeTableSchemas
		cross      Loader
		want       []payloadexec.TableSchema
		wantErr    error
		wantAnyErr bool
	}{
		{
			name: "happy path uses latest version",
			source: fakeTableSchemas{
				latest: map[string]registry.TableSchema{"orders/t": declared},
				exact: map[string]registry.TableSchema{
					"orders/t@1": {
						DatabaseId: "orders", TableId: "t", Version: 1,
						SchemaHash: "0xstale", SchemaJson: `{}`,
					},
					"orders/t@2": declared,
				},
			},
			want: []payloadexec.TableSchema{schema},
		},
		{
			name:    "content missing",
			source:  fakeTableSchemas{latest: map[string]registry.TableSchema{}, exact: map[string]registry.TableSchema{}},
			wantErr: ErrSchemaContentMissing,
		},
		{
			name: "latest points at missing content",
			source: fakeTableSchemas{
				latest: map[string]registry.TableSchema{"orders/t": declared},
				exact:  map[string]registry.TableSchema{},
			},
			wantErr: ErrSchemaContentMissing,
		},
		{
			name: "hash mismatch",
			source: fakeTableSchemas{
				latest: map[string]registry.TableSchema{"orders/t": declared},
				exact: map[string]registry.TableSchema{"orders/t@2": {
					DatabaseId: "orders", TableId: "t", Version: 2,
					SchemaHash: "0xdeadbeef", SchemaJson: schemaJSON,
				}},
			},
			wantErr: ErrSchemaHashMismatch,
		},
		{
			name: "malformed json",
			source: fakeTableSchemas{
				latest: map[string]registry.TableSchema{"orders/t": declared},
				exact: map[string]registry.TableSchema{"orders/t@2": {
					DatabaseId: "orders", TableId: "t", Version: 2,
					SchemaHash: hash, SchemaJson: `{`,
				}},
			},
			wantAnyErr: true,
		},
		{
			name: "clickhouse drift",
			source: fakeTableSchemas{
				latest: map[string]registry.TableSchema{"orders/t": declared},
				exact:  map[string]registry.TableSchema{"orders/t@2": declared},
			},
			cross: fakeSchemaLoader{schemas: []payloadexec.TableSchema{{
				TableID:     ref.TableID,
				PartitionBy: "day",
				Columns: []lthash.Column{
					{Name: "id", Type: "String"},
					{Name: "day", Type: "Date"},
				},
			}}},
			wantErr: ErrClickHouseDrift,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loader := NewNetworkStateLoader(tc.source, networkStateLoaderTestNetwork)
			if tc.cross != nil {
				loader.withCrossCheckLoader(tc.cross)
			}
			got, err := loader.Load(context.Background(), []TableRef{ref})
			if tc.wantAnyErr {
				if err == nil {
					t.Fatal("Load succeeded, want malformed-content error")
				}
				return
			}
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Load error = %v, want errors.Is(%v)", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if len(got) != 1 || got[0].TableID != schema.TableID ||
				got[0].PartitionBy != schema.PartitionBy ||
				len(got[0].Columns) != len(schema.Columns) {
				t.Fatalf("Load = %+v, want %+v", got, tc.want)
			}
			for i := range schema.Columns {
				if got[0].Columns[i] != schema.Columns[i] {
					t.Fatalf("Load columns[%d] = %+v, want %+v", i, got[0].Columns[i], schema.Columns[i])
				}
			}
			if gotHash := payloadexec.TableSchemaHash(networkStateLoaderTestNetwork, got[0]); gotHash != declared.SchemaHash {
				t.Fatalf("loaded schema hash = %s, declared %s", gotHash, declared.SchemaHash)
			}
		})
	}
}

func TestNetworkStateLoaderRejectsSchemaContentForDifferentTable(t *testing.T) {
	ref := TableRef{TableID: "orders.t", Database: "hg_unsafe", Table: "orders__t"}
	other := payloadexec.TableSchema{
		TableID: "other.table",
		Columns: []lthash.Column{{Name: "id", Type: "UInt64"}},
	}
	otherJSON := `{"table_id":"other.table","columns":[{"name":"id","type":"UInt64"}]}`
	declared := registry.TableSchema{
		DatabaseId: "orders",
		TableId:    "t",
		Version:    1,
		SchemaHash: payloadexec.TableSchemaHash(networkStateLoaderTestNetwork, other),
		SchemaJson: otherJSON,
	}
	loader := NewNetworkStateLoader(fakeTableSchemas{
		latest: map[string]registry.TableSchema{"orders/t": declared},
		exact:  map[string]registry.TableSchema{"orders/t@1": declared},
	}, networkStateLoaderTestNetwork)

	if _, err := loader.Load(context.Background(), []TableRef{ref}); !errors.Is(err, ErrSchemaIdentityMismatch) {
		t.Fatalf("Load error = %v, want errors.Is(ErrSchemaIdentityMismatch)", err)
	}
}

func TestNetworkStateLoaderRejectsDeclarationForDifferentCoordinates(t *testing.T) {
	ref := TableRef{TableID: "orders.t", Database: "hg_unsafe", Table: "orders__t"}
	schema := payloadexec.TableSchema{
		TableID: ref.TableID,
		Columns: []lthash.Column{{Name: "id", Type: "UInt64"}},
	}
	declared := registry.TableSchema{
		DatabaseId: "other",
		TableId:    "table",
		Version:    1,
		SchemaHash: payloadexec.TableSchemaHash(networkStateLoaderTestNetwork, schema),
		SchemaJson: `{"table_id":"orders.t","columns":[{"name":"id","type":"UInt64"}]}`,
	}
	loader := NewNetworkStateLoader(fakeTableSchemas{
		latest: map[string]registry.TableSchema{"orders/t": declared},
		exact:  map[string]registry.TableSchema{"orders/t@1": declared},
	}, networkStateLoaderTestNetwork)

	if _, err := loader.Load(context.Background(), []TableRef{ref}); !errors.Is(err, ErrSchemaIdentityMismatch) {
		t.Fatalf("Load error = %v, want errors.Is(ErrSchemaIdentityMismatch)", err)
	}
}
