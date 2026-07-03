package replay

import "testing"

// CanonicalDigest is the exported single canonicalization profile the
// integrity layer shares (arbiter design §4.3): a second, parallel hash
// profile anywhere in arbiter/verifier code is a correctness regression.

func TestCanonicalDigestMatchesReceiptHash(t *testing.T) {
	r := ExecutionReceipt{
		BlockSeq:           7,
		PrevSafeSnapshotID: "snap-1",
		ComputedStateRoot:  "0xabc",
	}
	want, err := r.Hash()
	if err != nil {
		t.Fatalf("receipt hash: %v", err)
	}
	got, err := CanonicalDigest("replay-execution-receipt", r)
	if err != nil {
		t.Fatalf("CanonicalDigest: %v", err)
	}
	if got != want {
		t.Fatalf("CanonicalDigest diverged from ExecutionReceipt.Hash: got %s want %s", got, want)
	}
}

func TestCanonicalDigestFrozenVector(t *testing.T) {
	// Frozen cross-implementation vector: SHA-256 over
	// "housegate-replay-mvp-v0:" || domain || 0x00 || canonical JSON.
	// If this test breaks, the wire profile changed and every recorded
	// root/receipt in the integrity layer is invalidated — do not "fix" the
	// expected value without a coordinated profile migration.
	got, err := CanonicalDigest("arbiter-p0-vector", struct {
		A string `json:"a"`
		N int    `json:"n"`
	}{A: "x", N: 7})
	if err != nil {
		t.Fatalf("CanonicalDigest: %v", err)
	}
	const want = "0xa6002c60260099a674a29f3af4bf87b440fa9933d7ac4ae4c41603d0099db512"
	if got != want {
		t.Fatalf("frozen vector mismatch: got %s want %s", got, want)
	}
}
