package network

import "testing"

func TestInMemoryIsIndexerSigner(t *testing.T) {
	const registered = "0x00000000000000000000000000000000000000aa"
	s := NewInMemoryNetworkState()
	s.IndexerInfos[1] = IndexerInfo{IndexerId: 1, Signer: "0x00000000000000000000000000000000000000AA"} // stored mixed-case
	s.IndexerInfos[2] = IndexerInfo{IndexerId: 2}                                                       // no signer registered
	s.IndexerInfos[3] = IndexerInfo{IndexerId: 3, Signer: string(WildcardAddress)}

	for _, tc := range []struct {
		name    string
		address string
		want    bool
	}{
		{"registered signer", registered, true},
		{"registered signer, other case and padding", " 0x00000000000000000000000000000000000000AA ", true},
		{"unregistered address", "0x00000000000000000000000000000000000000bb", false},
		{"empty address does not match an indexer without a signer", "", false},
		{"wildcard address is never a signer", string(WildcardAddress), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := s.IsIndexerSigner(tc.address); got != tc.want {
				t.Fatalf("IsIndexerSigner(%q) = %v, want %v", tc.address, got, tc.want)
			}
		})
	}
}
