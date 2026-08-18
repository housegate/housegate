package auth

// jws.go — shared JWS/Keccak helpers used by both EthValidator (for
// verification) and RelaySigner (for signing). These are the low-level
// crypto primitives; algorithmic policy (allowlist, token age) lives in
// eth_validator.go.

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
)

func decodeCanonicalRawURL(segmentName, encoded string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("invalid %s canonical base64url encoding: %w", segmentName, err)
	}
	if canonical := base64.RawURLEncoding.EncodeToString(decoded); encoded != canonical {
		return nil, fmt.Errorf("invalid %s canonical base64url encoding: non-canonical representation", segmentName)
	}
	return decoded, nil
}

// parseJWSCompact parses a JWS compact serialisation token into its
// three components. Each component is returned decoded (header and
// payload as typed structs, signature as raw bytes).
func parseJWSCompact(token string) (JWSHeader, JWSPayload, []byte, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return JWSHeader{}, JWSPayload{}, nil, errors.New("invalid JWS format: expected 3 parts")
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return JWSHeader{}, JWSPayload{}, nil, fmt.Errorf("invalid header encoding: %w", err)
	}
	var header JWSHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return JWSHeader{}, JWSPayload{}, nil, fmt.Errorf("invalid header JSON: %w", err)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return JWSHeader{}, JWSPayload{}, nil, fmt.Errorf("invalid payload encoding: %w", err)
	}
	var payload JWSPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return JWSHeader{}, JWSPayload{}, nil, fmt.Errorf("invalid payload JSON: %w", err)
	}

	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return JWSHeader{}, JWSPayload{}, nil, fmt.Errorf("invalid signature encoding: %w", err)
	}

	return header, payload, signature, nil
}

// keccak256 returns the Keccak-256 hash of data.
func keccak256(data []byte) []byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	return h.Sum(nil)
}

// keccak256Hex returns Keccak-256 hex-encoded with a 0x prefix, the
// form stored in JWSPayload.QueryHash.
func keccak256Hex(data []byte) string {
	return "0x" + hex.EncodeToString(keccak256(data))
}

// recoverAddress recovers the Ethereum address from a 32-byte message
// hash and a 65-byte signature. Accepts both the 0/1 and 27/28 forms of
// the recovery byte (V).
func recoverAddress(messageHash, signature []byte) (string, error) {
	if len(signature) != 65 {
		return "", fmt.Errorf("invalid signature length: %d (expected 65)", len(signature))
	}
	sig := make([]byte, 65)
	copy(sig, signature)
	if sig[64] >= 27 {
		sig[64] -= 27
	}
	pubKey, err := crypto.Ecrecover(messageHash, sig)
	if err != nil {
		return "", err
	}
	addr := keccak256(pubKey[1:])
	return "0x" + hex.EncodeToString(addr[12:]), nil
}
