package sisnapshotquery

import "testing"

// The production package has no defaults: a disabled deployment cannot create
// a partial snapshot-query writer by accidentally omitting a capability port.
func TestNewFailsClosedWithoutEveryPort(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("New accepted missing default-disabled dependencies")
	}
}
