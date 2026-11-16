package chproto

import "testing"

func TestRevisionGates(t *testing.T) {
	cases := []struct {
		name string
		got  bool
		want bool
	}{
		{"interserver roles below", SupportsInterserverExternallyGrantedRoles(54471), false},
		{"interserver roles at", SupportsInterserverExternallyGrantedRoles(54472), true},
		{"query and line numbers below", SupportsQueryAndLineNumbers(54474), false},
		{"query and line numbers at", SupportsQueryAndLineNumbers(54475), true},
		{"chunked packets at", SupportsChunkedPackets(54470), true},
		{"versioned parallel replicas at", SupportsVersionedParallelReplicas(54471), true},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
}
