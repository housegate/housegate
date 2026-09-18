package snapshotquery

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/housegate/housegate/pkg/replay"
)

// LogicalTableName contains exact logical identifier values, not SQL spellings
// or scratch coordinates. TableID is opaque, including when it contains dots.
type LogicalTableName struct{ TableID, Database, Table string }

// SnapshotLogicalNames is additional trusted historical configuration for ONE
// full pin/O. O authenticates column semantics, NOT these names. Production must
// retain/recover original protected mappings independently for source/verifier;
// setting these fields on arbitrary names does not authenticate provenance.
type SnapshotLogicalNames struct {
	Pin                  replay.SnapshotPin
	SchemaArtifactDigest string
	Tables               []LogicalTableName
}

func validIdentifier(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0) && strings.TrimSpace(s) != ""
}
func freezeLogicalNames(n SnapshotLogicalNames) (map[string]LogicalTableName, error) {
	if err := validatePin(n.Pin); err != nil {
		return nil, err
	}
	if !canonicalDigest(n.SchemaArtifactDigest) || len(n.Tables) == 0 || len(n.Tables) > replay.SnapshotQuerySchemaMaxTables {
		return nil, fmt.Errorf("invalid scoped logical-name catalog")
	}
	out := make(map[string]LogicalTableName, len(n.Tables))
	pairs := map[[2]string]bool{}
	for _, v := range n.Tables {
		if !validIdentifier(v.TableID) || !validIdentifier(v.Database) || !validIdentifier(v.Table) {
			return nil, fmt.Errorf("invalid logical table identity")
		}
		key := [2]string{v.Database, v.Table}
		if _, ok := out[v.TableID]; ok || pairs[key] {
			return nil, fmt.Errorf("duplicate logical table ID or name")
		}
		out[v.TableID] = v
		pairs[key] = true
	}
	return out, nil
}
