package sistatement

import (
	"fmt"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// resolveTargetTableID returns the logical "<db>.<table>" id: the SQL's own
// database when qualified, else sessionDB. Unqualified + no session database
// is an error (the SI lane needs an unambiguous table id to look up the
// declared schema).
func resolveTargetTableID(sql, sessionDB string) (string, error) {
	target, err := sicore.ResolveInsertTarget(sql, sessionDB)
	if err != nil {
		return "", fmt.Errorf("sistatement: %w", err)
	}
	return target.CanonicalID(), nil
}

// insertColumnList parses the optional "(c1, c2, ...)" list that follows the
// INSERT target. explicit=false when there is no list.
func insertColumnList(sql string) ([]string, bool, error) {
	cols, explicit, err := sicore.InsertColumnList(sql)
	if err != nil {
		return nil, explicit, fmt.Errorf("sistatement: %w", err)
	}
	return cols, explicit, nil
}

// sampleColumnsFor builds the sample block columns: schema order when the
// INSERT has no column list, else the SQL's order — which must name every
// declared column exactly once (the Native decoder requires the full column
// set, so a subset would only fail later at the ingress).
func sampleColumnsFor(schema payloadexec.TableSchema, listed []string) ([]chproto.SampleColumn, error) {
	byName := make(map[string]string, len(schema.Columns))
	for _, c := range schema.Columns {
		if c.Name == "_hg_row_id" {
			continue
		}
		byName[c.Name] = c.Type
	}
	if len(byName) == 0 {
		return nil, fmt.Errorf("sistatement: declared schema for %s has no columns", schema.TableID)
	}
	if len(listed) == 0 {
		out := make([]chproto.SampleColumn, 0, len(byName))
		for _, c := range schema.Columns {
			if c.Name == "_hg_row_id" {
				continue
			}
			out = append(out, chproto.SampleColumn{Name: c.Name, Type: c.Type})
		}
		return out, nil
	}
	seen := make(map[string]bool, len(listed))
	out := make([]chproto.SampleColumn, 0, len(listed))
	for _, name := range listed {
		typ, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("sistatement: INSERT lists unknown column %q for %s", name, schema.TableID)
		}
		if seen[name] {
			return nil, fmt.Errorf("sistatement: INSERT lists column %q twice", name)
		}
		seen[name] = true
		out = append(out, chproto.SampleColumn{Name: name, Type: typ})
	}
	for _, c := range schema.Columns {
		if c.Name != "_hg_row_id" && !seen[c.Name] {
			return nil, fmt.Errorf("sistatement: SI INSERT into %s must list every declared column (missing %q)", schema.TableID, c.Name)
		}
	}
	return out, nil
}

// matchUse returns (database, true, nil) for a standalone USE statement and
// preserves parser errors that the signed lane must reject fail-closed.
func matchUse(sql string) (string, bool, error) {
	return sicore.ParseUseDatabaseStrict(sql)
}
