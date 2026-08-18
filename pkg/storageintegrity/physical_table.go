package storageintegrity

import (
	"errors"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ErrPhysicalTableNameCollision reports a frozen schema set whose D2 logical
// to physical mapping is not injective.
var ErrPhysicalTableNameCollision = errors.New("storage_integrity: physical table name collision")

// PhysicalTableName maps a logical storage-integrity table id to the physical
// hg_unsafe/hg_safe name. It mirrors arbiter-core ddl.CHTableName (D2 freeze).
func PhysicalTableName(tableID string) string {
	return strings.ReplaceAll(tableID, ".", "__")
}

// ValidatePhysicalTableNames rejects schema sets that would route distinct
// logical tables into the same hg_unsafe/hg_safe table (and Keeper root in the
// source role). Call it once on the complete authoritative startup schema set.
func ValidatePhysicalTableNames(tables []payloadexec.TableSchema) error {
	owners := make(map[string]string, len(tables))
	for _, table := range tables {
		physical := PhysicalTableName(table.TableID)
		if previous, ok := owners[physical]; ok {
			return fmt.Errorf("%w: logical table ids %q and %q both map to %q", ErrPhysicalTableNameCollision, previous, table.TableID, physical)
		}
		owners[physical] = table.TableID
	}
	return nil
}
