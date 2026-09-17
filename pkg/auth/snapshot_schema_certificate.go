package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
)

const SnapshotSchemaCertificatePurposeV1 = "housegate-snapshot-query-schema-v1"

// SnapshotSchemaCertificatePayloadV1 is the complete timeless payload signed
// by a dedicated schema-authority key. The recovered Ethereum address is the
// only identity supplied by this pure primitive; current authority policy is a
// caller concern.
type SnapshotSchemaCertificatePayloadV1 struct {
	Purpose        string `json:"purpose"`
	Version        uint32 `json:"version"`
	ArtifactDigest string `json:"artifact_digest"`
}

// SnapshotSchemaCertificateSigner is an explicitly constructed schema-key
// credential. Keeping it distinct from RelaySigner prevents a caller from
// silently treating a relay, user, source or generic publisher key as the
// schema authority.
type SnapshotSchemaCertificateSigner struct {
	compact *RelaySigner
}

// NewSnapshotSchemaCertificateSigner constructs only the explicit schema-key
// credential; callers must supply its separately configured private key.
func NewSnapshotSchemaCertificateSigner(privateKeyHex string) (*SnapshotSchemaCertificateSigner, error) {
	signer, err := NewRelaySigner(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid snapshot schema authority private key: %w", err)
	}
	return &SnapshotSchemaCertificateSigner{compact: signer}, nil
}

// Address returns the lowercase recovered Ethereum identity for configuration.
func (s *SnapshotSchemaCertificateSigner) Address() string { return s.compact.Address() }

// SignSnapshotSchemaCertificateV1 signs the inner canonical schema artifact
// digest. It reuses the established compact encoding internally, but no
// relay/user/source/publisher key is selected or defaulted here.
func (s *SnapshotSchemaCertificateSigner) SignSnapshotSchemaCertificateV1(artifactDigest string) (string, error) {
	if !validSnapshotSchemaDigest(artifactDigest) {
		return "", fmt.Errorf("artifact_digest must be a lowercase 0x-prefixed SHA-256 digest")
	}
	return s.compact.signCompactJWS(SnapshotSchemaCertificatePayloadV1{
		Purpose:        SnapshotSchemaCertificatePurposeV1,
		Version:        1,
		ArtifactDigest: artifactDigest,
	})
}

// DecodeSnapshotSchemaCertificatePayloadV1 checks the exact compact JWS
// encoding and canonical payload bytes but does not authenticate the signer.
func DecodeSnapshotSchemaCertificatePayloadV1(token string) (SnapshotSchemaCertificatePayloadV1, error) {
	payload, _, _, err := decodeSnapshotSchemaCertificateV1(token)
	return payload, err
}

func decodeSnapshotSchemaCertificateV1(token string) (SnapshotSchemaCertificatePayloadV1, string, []byte, error) {
	var payload SnapshotSchemaCertificatePayloadV1
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return payload, "", nil, fmt.Errorf("invalid snapshot schema certificate JWS format: expected 3 parts")
	}
	header, err := decodeCanonicalRawURL("snapshot schema certificate header", parts[0])
	if err != nil {
		return payload, "", nil, err
	}
	if string(header) != canonicalJWSProtectedHeader {
		return payload, "", nil, fmt.Errorf("invalid snapshot schema certificate protected header: must be exact canonical %s", canonicalJWSProtectedHeader)
	}
	raw, err := decodeCanonicalRawURL("snapshot schema certificate payload", parts[1])
	if err != nil {
		return payload, "", nil, err
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return SnapshotSchemaCertificatePayloadV1{}, "", nil, fmt.Errorf("invalid snapshot schema certificate payload JSON: %w", err)
	}
	canonical, err := json.Marshal(payload)
	if err != nil {
		return SnapshotSchemaCertificatePayloadV1{}, "", nil, fmt.Errorf("marshal snapshot schema certificate payload: %w", err)
	}
	if !bytes.Equal(raw, canonical) {
		return SnapshotSchemaCertificatePayloadV1{}, "", nil, fmt.Errorf("invalid snapshot schema certificate payload: noncanonical JSON")
	}
	if payload.Purpose != SnapshotSchemaCertificatePurposeV1 || payload.Version != 1 {
		return SnapshotSchemaCertificatePayloadV1{}, "", nil, fmt.Errorf("invalid snapshot schema certificate purpose or version")
	}
	if !validSnapshotSchemaDigest(payload.ArtifactDigest) {
		return SnapshotSchemaCertificatePayloadV1{}, "", nil, fmt.Errorf("invalid snapshot schema certificate artifact_digest")
	}
	signature, err := decodeCanonicalRawURL("snapshot schema certificate signature", parts[2])
	if err != nil {
		return SnapshotSchemaCertificatePayloadV1{}, "", nil, err
	}
	if len(signature) != 65 || (signature[64] != 27 && signature[64] != 28) {
		return SnapshotSchemaCertificatePayloadV1{}, "", nil, fmt.Errorf("invalid snapshot schema certificate signature: expected 65 bytes with V=27/28")
	}
	if !crypto.ValidateSignatureValues(signature[64]-27, new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:64]), true) {
		return SnapshotSchemaCertificatePayloadV1{}, "", nil, fmt.Errorf("invalid snapshot schema certificate signature: noncanonical scalar or high-S")
	}
	return payload, parts[0] + "." + parts[1], signature, nil
}

// VerifySnapshotSchemaCertificateV1 performs timeless signature and inner
// artifact-digest verification and returns the recovered lowercase Ethereum
// address. It intentionally accepts no caller-supplied authority identity,
// current-role allowlist, clock, age, audience, or publication policy.
func VerifySnapshotSchemaCertificateV1(token, artifactDigest string) (string, error) {
	if !validSnapshotSchemaDigest(artifactDigest) {
		return "", fmt.Errorf("expected artifact_digest must be a lowercase 0x-prefixed SHA-256 digest")
	}
	payload, signingInput, signature, err := decodeSnapshotSchemaCertificateV1(token)
	if err != nil {
		return "", err
	}
	if payload.ArtifactDigest != artifactDigest {
		return "", fmt.Errorf("snapshot schema certificate artifact_digest mismatch")
	}
	address, err := recoverAddress(keccak256([]byte(signingInput)), signature)
	if err != nil {
		return "", fmt.Errorf("recover snapshot schema certificate signer: %w", err)
	}
	return strings.ToLower(address), nil
}

func validSnapshotSchemaDigest(value string) bool {
	if len(value) != 66 || value[:2] != "0x" {
		return false
	}
	for _, c := range value[2:] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
