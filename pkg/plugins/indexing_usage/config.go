package indexingusage

// Config is the operator-tunable surface of the indexing_usage plugin.
type Config struct {
	// Enabled toggles the entire path. When false, the plugin is not
	// wired and the relay never inspects log_comment / Data blocks for
	// billing purposes. Defaults to false to keep the plugin
	// opt-in until a deployment has cross-checked its numbers against
	// the driver's existing AsyncSave path (Shadow mode).
	//
	// There is no flush-interval knob: housegate reports each INSERT
	// directly to the sink rather than buffering a batch locally, so
	// there is nothing to flush on a timer. Batching/aggregation lives
	// in sentio-node's usage.Server accumulator.
	Enabled bool `json:"enabled" yaml:"enabled"`
}
