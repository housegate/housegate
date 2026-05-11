package rewrite

import "housegate/housegate/pkg/cfgtypes"

// Config is the operator-tunable surface for the rewrite plugin. The
// constructor in cmd combines these fields with cross-cutting
// values (Listen, Upstream) to produce a rewriter.Options.
type Config struct {
	// ServiceAddr is the gRPC address of the sql-rewriter service.
	ServiceAddr string `json:"service_addr" yaml:"service_addr"`

	// Timeout caps each gRPC call.
	Timeout cfgtypes.Duration `json:"timeout" yaml:"timeout"`

	// PhysicalDatabase is the single physical ClickHouse database that
	// hosts every logical database in this deployment. Used as both
	// the rewriter's database_map target value and the wire-level
	// substitution for hello.Database. Empty disables both.
	PhysicalDatabase string `json:"physical_database" yaml:"physical_database"`

	// Delimiter is the character used to separate logical database names
	// in the rewriter's output. Defaults to ".".
	Delimiter string `json:"delimiter" yaml:"delimiter"`
}
