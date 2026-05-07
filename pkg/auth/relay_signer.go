package auth

// relay_signer.go — RelaySigner produces JWS tokens for proxy-to-proxy
// and sidecar-to-server authentication.
//
// All proxies in a deployment share the same relay private key; the
// corresponding address must be in EthValidator.AllowedAddresses on the
// verifying side.

import (
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

// RelaySigner wraps an Ethereum-style private key and produces JWS
// tokens whose payload binds a query's SQL text.
type RelaySigner struct {
	privateKey *ecdsa.PrivateKey
	address    string // lowercase, 0x-prefixed
}

// NewRelaySigner parses a hex-encoded secp256k1 private key (with or
// without a leading 0x) and returns a ready-to-use signer.
func NewRelaySigner(privateKeyHex string) (*RelaySigner, error) {
	privateKeyHex = strings.TrimPrefix(privateKeyHex, "0x")
	privKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid relay private key: %w", err)
	}
	addr := strings.ToLower(crypto.PubkeyToAddress(privKey.PublicKey).Hex())
	return &RelaySigner{privateKey: privKey, address: addr}, nil
}

// Address returns the Ethereum address (lowercase, 0x-prefixed) of this
// signer. The address must be in the verifier's allowlist.
func (s *RelaySigner) Address() string { return s.address }

// SignToken produces a JWS compact serialisation token whose payload
// binds the given SQL body (via QueryHash = Keccak256(sql)) and carries
// the current time as iat.
func (s *RelaySigner) SignToken(sql string) (string, error) {
	header := JWSHeader{Alg: "ES256K", Typ: "JWT"}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("marshal header: %w", err)
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)

	payload := JWSPayload{
		Iat:       time.Now().Unix(),
		QueryHash: keccak256Hex([]byte(sql)),
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	signingInput := headerB64 + "." + payloadB64
	hash := keccak256([]byte(signingInput))
	sig, err := crypto.Sign(hash, s.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign: %w", err)
	}
	// Adjust V (0/1 → 27/28) for Ethereum convention.
	sig[64] += 27

	signatureB64 := base64.RawURLEncoding.EncodeToString(sig)
	return signingInput + "." + signatureB64, nil
}

// SignPeerLogin produces a JWS compact-serialisation token whose
// payload authenticates this proxy as an upstream housegate peer to
// the proxy identified by audience (typically the receiving proxy's
// indexer-id stringified). The token is bound by audience + exp, NOT
// by SQL hash — the receiving proxy's rewriter will further transform
// the inner statement before it reaches ClickHouse, so a qhash binding
// would always mismatch.
//
// ttl caps the token's validity window relative to now. Callers should
// pick a value short enough to limit replay impact (a few minutes is
// typical) but long enough to absorb clock skew between peers.
func (s *RelaySigner) SignPeerLogin(audience string, ttl time.Duration) (string, error) {
	header := JWSHeader{Alg: "ES256K", Typ: "JWT"}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("marshal header: %w", err)
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)

	now := time.Now().Unix()
	payload := JWSPeerPayload{
		Iat:     now,
		Exp:     now + int64(ttl.Seconds()),
		Aud:     audience,
		Purpose: PeerLoginPurpose,
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal peer payload: %w", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	signingInput := headerB64 + "." + payloadB64
	hash := keccak256([]byte(signingInput))
	sig, err := crypto.Sign(hash, s.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign peer: %w", err)
	}
	sig[64] += 27

	signatureB64 := base64.RawURLEncoding.EncodeToString(sig)
	return signingInput + "." + signatureB64, nil
}
