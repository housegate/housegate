package sitable

import "github.com/housegate/housegate/pkg/replay/payloadexec"

// StaticVersion is the constant version of every Static snapshot.
const StaticVersion uint64 = 1

// Static is the TableState of a fixed configured table set: every listed
// table is Active, every other table is Ordinary, the version is always
// StaticVersion and Changed never fires. It serves the standalone binary,
// tests, and every host that does not inject a TableState.
type Static struct {
	snap Snapshot
	ids  []string
}

// NewStatic builds the static state. schemas carries the schemas loaded at
// startup, keyed by table id; a listed table without one stays Active with a
// zero schema, and the signed lane refuses writes to it.
func NewStatic(tableIDs []string, schemas map[string]payloadexec.TableSchema, networkID string) *Static {
	tables := make([]Table, 0, len(tableIDs))
	for _, id := range tableIDs {
		t := Table{ID: id, Status: Active}
		if schema, ok := schemas[id]; ok {
			t.Schema = schema
			t.SchemaHash = payloadexec.TableSchemaHash(networkID, schema)
		}
		tables = append(tables, t)
	}
	return &Static{snap: NewSnapshot(StaticVersion, Ordinary, tables), ids: append([]string(nil), tableIDs...)}
}

func (s *Static) Current() Snapshot { return s.snap }

func (s *Static) Changed() <-chan struct{} { return neverChanged }

// TableIDs returns the configured ids in configuration order.
func (s *Static) TableIDs() []string { return append([]string(nil), s.ids...) }

// Schemas returns the startup schemas of the listed tables that have one.
func (s *Static) Schemas() []payloadexec.TableSchema {
	var out []payloadexec.TableSchema
	for _, id := range s.ids {
		if t, ok := s.snap.Schema(id); ok && t.SchemaHash != "" {
			out = append(out, t.Schema)
		}
	}
	return out
}

var _ TableState = (*Static)(nil)
