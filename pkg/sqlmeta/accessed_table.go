package sqlmeta

// AccessedTable is one table the SQL referenced before rewrite,
// with the rewriter's best-effort resolution of its physical
// database under the active TableNameRewrite mode. Mirrors proto
// message AccessedTable (rewriter.proto tag 12 child).
//
// OriginalDatabase / OriginalTable are exactly what the SQL
// contained (empty database when the table was unqualified).
// LogicalDatabase / PhysicalDatabase are populated when the
// rewriter has enough context to resolve them; both can
// legitimately be empty (see proto comment for the cases).
//
// IsRemote is true iff the rewriter routed (or would have routed)
// this access through `remote(addr, db, table, user, password)`.
type AccessedTable struct {
	OriginalDatabase string
	OriginalTable    string
	LogicalDatabase  string
	PhysicalDatabase string
	IsRemote         bool
}
