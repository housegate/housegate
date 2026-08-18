package forward

import sicore "github.com/housegate/housegate/pkg/storageintegrity"

// matchUse returns (databaseName, true) when sql is a standalone USE statement.
// Returns ("", false) for anything more elaborate (USE x SETTINGS y=1,
// multi-statement, comments, etc.).
func matchUse(sql string) (string, bool) {
	return sicore.ParseUseDatabase(sql)
}
