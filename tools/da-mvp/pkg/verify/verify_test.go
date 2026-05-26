package verify

import "testing"

func TestParseMode(t *testing.T) {
	tests := map[string]Mode{"count": ModeCount, "sample": ModeSample, "full": ModeFull}
	for s, want := range tests {
		got, err := ParseMode(s)
		if err != nil || got != want {
			t.Errorf("%s: got %v err=%v", s, got, err)
		}
	}
	if _, err := ParseMode("garbage"); err == nil {
		t.Error("expected error for unknown mode")
	}
}
