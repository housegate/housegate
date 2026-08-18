package storageintegrity

import "strings"

// PhysicalTableName maps a logical storage-integrity table id to the physical
// hg_unsafe/hg_safe name. It mirrors arbiter-core ddl.CHTableName (D2 freeze).
func PhysicalTableName(tableID string) string {
	return strings.ReplaceAll(tableID, ".", "__")
}
