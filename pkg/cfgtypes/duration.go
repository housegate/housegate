// Package cfgtypes holds shared Config-related primitives so that
// plugin packages can declare their own Config structs without taking
// a dependency on pkg/config (which would create an import cycle —
// pkg/config imports plugin packages to compose the root Config).
//
// Currently this package owns only the Duration type used across every
// section's time-valued fields.
package cfgtypes

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/housegate/housegate/pkg/log"
)

// Duration wraps time.Duration to allow human-friendly strings in JSON
// configs (e.g. "5s") in addition to nanosecond integers.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		dur, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		d.Duration = dur
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err == nil {
		d.Duration = time.Duration(n)
		warnIfSuspiciousDuration(n, d.Duration)
		return nil
	}
	return fmt.Errorf("duration must be a string (e.g. \"5s\") or number of nanoseconds")
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

// UnmarshalText implements encoding.TextUnmarshaler, used for CLI flag
// parsing.
func (d *Duration) UnmarshalText(text []byte) error {
	dur, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", string(text), err)
	}
	d.Duration = dur
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err == nil {
		dur, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		d.Duration = dur
		return nil
	}
	var n int64
	if err := unmarshal(&n); err == nil {
		d.Duration = time.Duration(n)
		warnIfSuspiciousDuration(n, d.Duration)
		return nil
	}
	return fmt.Errorf("duration must be a string (e.g. \"5s\") or number of nanoseconds")
}

// warnIfSuspiciousDuration flags a numeric duration that is likely an
// operator mistake: a sub-second value usually means the author thought
// they were writing seconds while the codec interpreted nanoseconds.
func warnIfSuspiciousDuration(raw int64, parsed time.Duration) {
	if parsed <= 0 || parsed >= time.Second {
		return
	}
	log.Warnw("duration value interpreted as nanoseconds; did you mean seconds?",
		"raw", raw,
		"interpreted", parsed,
		"did_you_mean", time.Duration(raw)*time.Second)
}
