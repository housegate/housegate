package auth

// relay_signer.go — RelaySigner produces JWS tokens for proxy-to-proxy
// and agent-to-server authentication.
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

const canonicalJWSProtectedHeader = `{"alg":"ES256K","typ":"JWT"}`

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
	return s.signToken(sql, QueryPurpose)
}

// SignTokenWithPurpose is SignToken plus an explicit domain-separation purpose
// claim. Verifiers that require a purpose reject legacy purpose-less tokens.
func (s *RelaySigner) SignTokenWithPurpose(sql, purpose string) (string, error) {
	return s.signToken(sql, purpose)
}

// SignStatementV2 produces the storage-integrity statement token. Purpose is
// forced to StatementPurposeV2 and Iat is filled with the current time when
// zero (tests and vector generation pass an explicit Iat for determinism;
// secp256k1 signing is RFC 6979 deterministic so a fixed payload yields a
// fixed token).
func (s *RelaySigner) SignStatementV2(payload JWSStatementPayloadV2) (string, error) {
	payload.Purpose = StatementPurposeV2
	if payload.Iat == 0 {
		payload.Iat = time.Now().Unix()
	}
	return s.signCompactJWS(payload)
}

// signCompactJWS is the single compact-JWS construction primitive for every
// RelaySigner lane (query, statement, and peer login). Keeping the protected
// header, signing input, Ethereum recovery-byte convention, and encoding in
// one place prevents a new lane from drifting from the canonical profile.
func (s *RelaySigner) signCompactJWS(payload any) (string, error) {
	headerJSON := []byte(canonicalJWSProtectedHeader)
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal JWS payload: %w", err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON)
	sig, err := crypto.Sign(keccak256([]byte(signingInput)), s.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign compact JWS: %w", err)
	}
	// Adjust V (0/1 → 27/28) for Ethereum convention.
	sig[64] += 27
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (s *RelaySigner) signToken(sql, purpose string) (string, error) {
	payload := JWSPayload{
		Iat:       time.Now().Unix(),
		QueryHash: keccak256Hex([]byte(sql)),
		Purpose:   purpose,
	}
	return s.signCompactJWS(payload)
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
	now := time.Now().Unix()
	payload := JWSPeerPayload{
		Iat:     now,
		Exp:     now + int64(ttl.Seconds()),
		Aud:     audience,
		Purpose: PeerLoginPurpose,
	}
	return s.signCompactJWS(payload)
}
