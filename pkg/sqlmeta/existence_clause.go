package sqlmeta

import "strconv"

// ExistenceClause records whether a statement carried an
// existence-check clause: IF NOT EXISTS for the CREATE family,
// IF EXISTS for the DROP / TRUNCATE family. The two are mutually
// exclusive so one type expresses both. Statements with no such
// clause report ExistenceClauseUnspecified.
//
// Mirrors protos.ExistenceClause but is a sqlmeta-owned type so
// downstream consumers (plugins, commitgate observers) do not have
// to import the protobuf package.
type ExistenceClause int

// Constants are kept numerically aligned with protos.ExistenceClause
// so conversions are a plain cast. Adding new values is fine;
// renumbering is not.
const (
	ExistenceClauseUnspecified ExistenceClause = 0
	ExistenceClauseIfNotExists ExistenceClause = 1 // CREATE ... IF NOT EXISTS
	ExistenceClauseIfExists    ExistenceClause = 2 // DROP / TRUNCATE ... IF EXISTS
)

// String returns a short human label, matching the proto enum names
// minus the EXISTENCE_CLAUSE_ prefix. Falls back to a numeric form
// for unknown values so logs stay readable when the proto enum grows.
func (e ExistenceClause) String() string {
	switch e {
	case ExistenceClauseUnspecified:
		return "UNSPECIFIED"
	case ExistenceClauseIfNotExists:
		return "IF_NOT_EXISTS"
	case ExistenceClauseIfExists:
		return "IF_EXISTS"
	default:
		return "EXISTENCE_CLAUSE(" + strconv.Itoa(int(e)) + ")"
	}
}
