package sessionstate

// Config tunes the state tracker plugin.
//
// Post-rewriter migration the state plugin is purely observational at
// hello time: it records ClientHello.Database into
// SessionState.LogicalDatabase so the rewriter (and any downstream
// plugin) sees the user's intended database name. Wire-level
// hello.Database substitution and USE rewriting both moved to the
// rewrite plugin, which is the one canonical owner of physical-vs-
// logical mapping. Config is kept as a struct (rather than removed)
// so future per-plugin tuning has a place to land without an API
// churn.
type Config struct{}
