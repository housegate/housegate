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
