package replay

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const hashPrefix = "0x"

// DigestBytes returns the MVP replay digest for opaque bytes. The concrete
// on-chain hash profile can change later; callers should compare digests
// through this package so test vectors remain centralized.
func DigestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hashPrefix + hex.EncodeToString(sum[:])
}

// DigestString returns the MVP replay digest for UTF-8 text.
func DigestString(s string) string {
	return DigestBytes([]byte(s))
}

// CanonicalDigest returns the digest of v under the integrity layer's single
// canonical hashing profile ("housegate-replay-mvp-v0"): SHA-256 over the
// domain-separated canonical JSON encoding of v, hex-encoded with a 0x
// prefix. Every root/commitment in the replay/arbiter integrity layer must
// go through this profile with a distinct domain tag per commitment kind —
// never a second, parallel hash profile — so independent nodes derive
// identical roots from the same evidence (arbiter design §4.3).
func CanonicalDigest(domain string, v any) (string, error) {
	return canonicalDigest(domain, v)
}

func canonicalDigest(domain string, v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("canonicalize %s: %w", domain, err)
	}
	prefix := []byte("housegate-replay-mvp-v0:")
	payload := make([]byte, 0, len(prefix)+len(domain)+1+len(b))
	payload = append(payload, prefix...)
	payload = append(payload, domain...)
	payload = append(payload, 0)
	payload = append(payload, b...)
	return DigestBytes(payload), nil
}
