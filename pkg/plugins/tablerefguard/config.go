package tablerefguard

import "fmt"

// Config is the operator surface of the table-reference guard (yaml
// `tableref_guard`). The guard itself is wired whenever a SQL rewriter is.
type Config struct {
	// Mode is "enforce" (the default, also when empty) or "observe": observe
	// logs and counts what enforce would refuse, for rollout (spec 2026-09-26
	// §11 step 5).
	Mode string `json:"mode" yaml:"mode"`
}

// Validate rejects an unknown mode.
func (c Config) Validate() error {
	switch Mode(c.Mode) {
	case "", ModeEnforce, ModeObserve:
		return nil
	}
	return fmt.Errorf("tableref_guard.mode %q is invalid (want %q or %q)", c.Mode, ModeEnforce, ModeObserve)
}
