//go:build !(linux || darwin)

package sistatement

import (
	"errors"
	"testing"
)

func TestLanePoolIsRefusedOnThisPlatform(t *testing.T) {
	if _, err := OpenLanePool(t.TempDir(), LanePoolOptions{}); !errors.Is(err, ErrLanesUnsupported) {
		t.Fatalf("err = %v, want ErrLanesUnsupported", err)
	}
}
