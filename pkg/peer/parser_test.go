package peer

import "testing"

func TestParseUser(t *testing.T) {
	tests := []struct {
		name          string
		user          string
		wantAddr      string
		wantForwarded bool
		wantPeer      bool
	}{
		{name: "legacy", user: "__peer__|0x1234567890abcdef1234567890abcdef12345678",
			wantAddr: "0x1234567890abcdef1234567890abcdef12345678", wantForwarded: false, wantPeer: true},
		{name: "forwarded", user: "__peer__|0x1234567890abcdef1234567890abcdef12345678|forwarded",
			wantAddr: "0x1234567890abcdef1234567890abcdef12345678", wantForwarded: true, wantPeer: true},
		{name: "plain user", user: "default", wantPeer: false},
		{name: "empty", user: "", wantPeer: false},
		{name: "prefix only", user: "__peer__", wantPeer: false},
		{name: "prefix delim only", user: "__peer__|", wantPeer: false},
		{name: "wrong prefix", user: "__route__|0xabc", wantPeer: false},
		{name: "unknown trailing token", user: "__peer__|0xabc|garbage", wantPeer: false},
		{name: "forwarded with empty addr", user: "__peer__||forwarded", wantPeer: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, forwarded, ok := ParseUser(tt.user)
			if ok != tt.wantPeer {
				t.Fatalf("isPeer=%v want %v", ok, tt.wantPeer)
			}
			if !tt.wantPeer {
				return
			}
			if addr != tt.wantAddr {
				t.Errorf("addr=%q want %q", addr, tt.wantAddr)
			}
			if forwarded != tt.wantForwarded {
				t.Errorf("forwarded=%v want %v", forwarded, tt.wantForwarded)
			}
		})
	}
}

func TestFormatUser(t *testing.T) {
	got := FormatUser("0xabc")
	want := "__peer__|0xabc"
	if got != want {
		t.Errorf("FormatUser=%q want %q", got, want)
	}
	addr, forwarded, ok := ParseUser(got)
	if !ok || addr != "0xabc" || forwarded {
		t.Errorf("round-trip: addr=%q forwarded=%v ok=%v", addr, forwarded, ok)
	}
}

func TestFormatUserForwarded(t *testing.T) {
	got := FormatUserForwarded("0xabc")
	want := "__peer__|0xabc|forwarded"
	if got != want {
		t.Errorf("FormatUserForwarded=%q want %q", got, want)
	}
	addr, forwarded, ok := ParseUser(got)
	if !ok || addr != "0xabc" || !forwarded {
		t.Errorf("round-trip: addr=%q forwarded=%v ok=%v", addr, forwarded, ok)
	}
}
