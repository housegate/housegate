package usage

// Config is the operator-tunable surface for the usage/billing plugin.
type Config struct {
	// Enabled toggles balance check + usage reporting.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// SentioNodeAddr is the gRPC address of the sentio-node service.
	SentioNodeAddr string `json:"sentio_node_addr" yaml:"sentio_node_addr"`

	// RedisAddr is the Redis used for the query-check cache. Empty
	// falls back to the root Config.RedisDefaultAddr.
	RedisAddr string `json:"redis_addr" yaml:"redis_addr"`
}
