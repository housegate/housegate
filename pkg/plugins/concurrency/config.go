package concurrency

import "github.com/housegate/housegate/pkg/cfgtypes"

// Config is the operator-tunable surface for the concurrency-limit
// plugin (per-user dimension; v1).
type Config struct {
	// Enabled toggles the plugin entirely.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// PerUser is the maximum concurrent queries per recovered user
	// identity. Zero disables enforcement on the user dimension while
	// keeping it tracked.
	PerUser int `json:"per_user" yaml:"per_user"`

	// Timeout is the stale-permit reap window. A permit older than this
	// is treated as abandoned the next time anyone acquires under the
	// same user.
	Timeout cfgtypes.Duration `json:"timeout" yaml:"timeout"`

	// FailOpen controls behaviour when the limiter returns an
	// infrastructure error (Redis outage). True allows the query
	// through with a warn log; false rejects it.
	FailOpen bool `json:"fail_open" yaml:"fail_open"`

	// RedisAddr is the Redis used for the limiter sorted-set storage.
	// Empty falls back to the root Config.RedisDefaultAddr.
	RedisAddr string `json:"redis_addr" yaml:"redis_addr"`
}
