package route

import "testing"

func TestParseRouteFromUser(t *testing.T) {
	tests := []struct {
		name       string
		user       string
		wantTarget string
		wantUser   string
		wantRoute  bool
	}{
		{
			name:       "valid route",
			user:       "__route__|10.0.0.8:9001|default",
			wantTarget: "10.0.0.8:9001",
			wantUser:   "default",
			wantRoute:  true,
		},
		{
			name:       "valid route with hostname",
			user:       "__route__|proxy2.example.com:9001|admin",
			wantTarget: "proxy2.example.com:9001",
			wantUser:   "admin",
			wantRoute:  true,
		},
		{
			name:      "no route prefix",
			user:      "default",
			wantRoute: false,
		},
		{
			name:      "empty user",
			user:      "",
			wantRoute: false,
		},
		{
			name:      "prefix only",
			user:      "__route__",
			wantRoute: false,
		},
		{
			name:      "prefix with delim only",
			user:      "__route__|",
			wantRoute: false,
		},
		{
			name:      "prefix with addr but no second separator",
			user:      "__route__|10.0.0.8:9001",
			wantRoute: false,
		},
		{
			name:      "prefix with addr and separator but empty user",
			user:      "__route__|10.0.0.8:9001|",
			wantRoute: false,
		},
		{
			name:      "prefix with separator but empty addr",
			user:      "__route__||default",
			wantRoute: false,
		},
		{
			name:       "user with underscores",
			user:       "__route__|10.0.0.8:9001|my_user_name",
			wantTarget: "10.0.0.8:9001",
			wantUser:   "my_user_name",
			wantRoute:  true,
		},
		{
			name:       "user contains pipe (greedy on right)",
			user:       "__route__|10.0.0.8:9001|user|extra",
			wantTarget: "10.0.0.8:9001",
			wantUser:   "user|extra",
			wantRoute:  true,
		},
		{
			name:      "legacy double-underscore format no longer matches",
			user:      "__route__10.0.0.8:9001__default",
			wantRoute: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, user, isRoute := ParseRouteFromUser(tt.user)
			if isRoute != tt.wantRoute {
				t.Fatalf("isRoute = %v, want %v", isRoute, tt.wantRoute)
			}
			if !tt.wantRoute {
				return
			}
			if target != tt.wantTarget {
				t.Errorf("targetAddr = %q, want %q", target, tt.wantTarget)
			}
			if user != tt.wantUser {
				t.Errorf("realUser = %q, want %q", user, tt.wantUser)
			}
		})
	}
}

func TestFormatRouteUser(t *testing.T) {
	got := FormatRouteUser("10.0.0.8:9001", "default")
	want := "__route__|10.0.0.8:9001|default"
	if got != want {
		t.Errorf("FormatRouteUser = %q, want %q", got, want)
	}

	// Round-trip: format → parse should preserve the inputs.
	target, user, ok := ParseRouteFromUser(got)
	if !ok || target != "10.0.0.8:9001" || user != "default" {
		t.Errorf("round-trip mismatch: target=%q user=%q ok=%v", target, user, ok)
	}
}
