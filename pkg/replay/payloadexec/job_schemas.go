package payloadexec

import (
	"encoding/json"
	"fmt"

	"github.com/housegate/housegate/pkg/replay"
)

// DecodeTableSchemaJSON decodes one registry-committed schema. It uses the
// same lenient encoding/json decoding the arbiter applied when it admitted
// the schema, so both sides agree on the decoded semantic fields that
// TableSchemaHash commits to; it additionally requires the schema to name
// tableID and to use only admitted column types.
func DecodeTableSchemaJSON(tableID, schemaJSON string) (TableSchema, error) {
	var schema TableSchema
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return TableSchema{}, fmt.Errorf("table %s: decode schema_json: %w", tableID, err)
	}
	if schema.TableID != tableID {
		return TableSchema{}, fmt.Errorf("table %s: schema_json names table %q", tableID, schema.TableID)
	}
	if err := ValidateTableSchemaColumns(schema); err != nil {
		return TableSchema{}, err
	}
	return schema, nil
}

// ResolveJobSchemas returns every schema a job may use: static first, then
// the job's TableSchemas, then its TableSetTransition.Adds. A job-carried
// schema replaces a static one with the same table id and is authoritative:
// the executor (validateAppendInputs) always requires it to match the hash
// the previous safe snapshot commits to, so a carried schema can only make a
// verifier refuse, never change what it accepts. A table that was retired and
// recreated under the same name with a different schema (spec D9) only gets
// its new schema this way for a block whose TableSchemas or
// TableSetTransition.Adds actually carries it — typically the block(s) that
// target it. A block that neither targets the table nor carries its schema
// still resolves the STALE static entry here; the executor treats that
// specific case (static-only, mismatched, untargeted) as unresolved rather
// than refusing, so an untargeted table's recreation does not stall every
// other block on the chain.
func ResolveJobSchemas(static []TableSchema, job replay.ReplayJob) (map[string]TableSchema, error) {
	out := make(map[string]TableSchema, len(static)+len(job.TableSchemas))
	for _, s := range static {
		out[s.TableID] = s
	}
	carried := job.TableSchemas
	if job.TableSetTransition != nil {
		carried = append(append([]replay.ReplayTableSchema(nil), carried...), job.TableSetTransition.Adds...)
	}
	for _, ts := range carried {
		schema, err := DecodeTableSchemaJSON(ts.TableID, ts.SchemaJSON)
		if err != nil {
			return nil, err
		}
		out[ts.TableID] = schema
	}
	return out, nil
}

// carriedTableIDs returns the table ids a job resolves authoritatively itself
// (TableSchemas or TableSetTransition.Adds), as opposed to a dynamic
// executor's static fallback. validateAppendInputs uses it to tell the two
// apart: a carried schema is always checked strictly against the committed
// hash, while a static-only resolution is a fallback that may be stale.
func carriedTableIDs(job replay.ReplayJob) map[string]bool {
	ids := make(map[string]bool, len(job.TableSchemas))
	for _, ts := range job.TableSchemas {
		ids[ts.TableID] = true
	}
	if job.TableSetTransition != nil {
		for _, add := range job.TableSetTransition.Adds {
			ids[add.TableID] = true
		}
	}
	return ids
}

// schemasForJob is the executor's schema view for one job. A static
// executor (New, NewWithMaterializer) refuses the job-carried fields it
// cannot honour rather than silently ignoring them.
func (e *Executor) schemasForJob(job replay.ReplayJob) (map[string]TableSchema, error) {
	if !e.dynamic {
		if job.TableSetTransition != nil || len(job.TableSchemas) != 0 {
			return nil, fmt.Errorf("executor has a static table set; build it with NewDynamic to replay table-set transitions or job-carried schemas")
		}
		return e.tables, nil
	}
	static := make([]TableSchema, 0, len(e.tables))
	for _, id := range e.sortedTableIDs() {
		static = append(static, e.tables[id])
	}
	return ResolveJobSchemas(static, job)
}

// SchemaHashes is the replay.SchemaHashSource (and replay.JobSchemaHashSource)
// of a verifier whose table set follows the chain: Tables answer directly and
// ForJob adds every schema the job carries, with the same precedence
// ResolveJobSchemas gives the executor.
type SchemaHashes struct {
	NetworkID string
	Tables    []TableSchema
}

// TableSchemaHash answers from the static tables only.
func (s SchemaHashes) TableSchemaHash(tableID string) (string, bool) {
	for _, t := range s.Tables {
		if t.TableID == tableID {
			return TableSchemaHash(s.NetworkID, t), true
		}
	}
	return "", false
}

// ForJob resolves the job-scoped schema set.
func (s SchemaHashes) ForJob(job replay.ReplayJob) (replay.SchemaHashSource, error) {
	schemas, err := ResolveJobSchemas(s.Tables, job)
	if err != nil {
		return nil, err
	}
	return resolvedSchemaHashes{networkID: s.NetworkID, schemas: schemas}, nil
}

type resolvedSchemaHashes struct {
	networkID string
	schemas   map[string]TableSchema
}

func (r resolvedSchemaHashes) TableSchemaHash(tableID string) (string, bool) {
	schema, ok := r.schemas[tableID]
	if !ok {
		return "", false
	}
	return TableSchemaHash(r.networkID, schema), true
}

var _ replay.JobSchemaHashSource = SchemaHashes{}
