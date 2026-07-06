package storageintegrity

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

const mutationClaimJWSPurpose = "housegate-mutation-claim-v1"

type mutationClaimJWSHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

type mutationClaimJWSPayload struct {
	Iat       int64  `json:"iat"`
	Purpose   string `json:"purpose"`
	WorkerID  string `json:"worker_id,omitempty"`
	ClaimHash string `json:"claim_hash"`
}

type VerifiedMutationClaimSignature struct {
	Signer    string
	WorkerID  string
	ClaimHash string
	Iat       int64
}

type Secp256k1MutationClaimSigner struct {
	privateKeyHex string
	workerID      string
	address       string
}

func NewSecp256k1MutationClaimSigner(privateKeyHex, workerID string) (*Secp256k1MutationClaimSigner, error) {
	privateKeyHex = strings.TrimPrefix(strings.TrimSpace(privateKeyHex), "0x")
	if privateKeyHex == "" {
		return nil, fmt.Errorf("mutation claim private key is required")
	}
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid mutation claim private key: %w", err)
	}
	return &Secp256k1MutationClaimSigner{
		privateKeyHex: privateKeyHex,
		workerID:      workerID,
		address:       strings.ToLower(crypto.PubkeyToAddress(privateKey.PublicKey).Hex()),
	}, nil
}

func (s *Secp256k1MutationClaimSigner) Address() string {
	if s == nil {
		return ""
	}
	return s.address
}

func (s *Secp256k1MutationClaimSigner) SignClaim(claimHash string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("mutation claim signer is nil")
	}
	if claimHash == "" {
		return "", fmt.Errorf("claim hash is required")
	}
	privateKey, err := crypto.HexToECDSA(s.privateKeyHex)
	if err != nil {
		return "", fmt.Errorf("invalid mutation claim private key: %w", err)
	}
	headerJSON, err := json.Marshal(mutationClaimJWSHeader{Alg: "ES256K", Typ: "JWT"})
	if err != nil {
		return "", fmt.Errorf("marshal claim JWS header: %w", err)
	}
	payloadJSON, err := json.Marshal(mutationClaimJWSPayload{
		Iat:       time.Now().Unix(),
		Purpose:   mutationClaimJWSPurpose,
		WorkerID:  s.workerID,
		ClaimHash: claimHash,
	})
	if err != nil {
		return "", fmt.Errorf("marshal claim JWS payload: %w", err)
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)
	signingInput := headerB64 + "." + payloadB64
	sig, err := crypto.Sign(crypto.Keccak256([]byte(signingInput)), privateKey)
	if err != nil {
		return "", fmt.Errorf("sign mutation claim: %w", err)
	}
	sig[64] += 27
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func VerifyMutationClaimSignature(token, expectedClaimHash string) (VerifiedMutationClaimSignature, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("invalid mutation claim JWS: expected 3 parts")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("decode claim JWS header: %w", err)
	}
	var header mutationClaimJWSHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("parse claim JWS header: %w", err)
	}
	if header.Alg != "ES256K" || header.Typ != "JWT" {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("unsupported mutation claim JWS header alg=%q typ=%q", header.Alg, header.Typ)
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("decode claim JWS payload: %w", err)
	}
	var payload mutationClaimJWSPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("parse claim JWS payload: %w", err)
	}
	if payload.Purpose != mutationClaimJWSPurpose {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("invalid mutation claim purpose %q", payload.Purpose)
	}
	if payload.ClaimHash == "" {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("mutation claim hash is empty")
	}
	if expectedClaimHash != "" && payload.ClaimHash != expectedClaimHash {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("mutation claim hash mismatch: got %s want %s", payload.ClaimHash, expectedClaimHash)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("decode claim JWS signature: %w", err)
	}
	if len(sig) != 65 {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("invalid mutation claim signature length %d", len(sig))
	}
	if sig[64] >= 27 {
		sig[64] -= 27
	}
	pub, err := crypto.SigToPub(crypto.Keccak256([]byte(parts[0]+"."+parts[1])), sig)
	if err != nil {
		return VerifiedMutationClaimSignature{}, fmt.Errorf("recover mutation claim signer: %w", err)
	}
	return VerifiedMutationClaimSignature{
		Signer:    strings.ToLower(crypto.PubkeyToAddress(*pub).Hex()),
		WorkerID:  payload.WorkerID,
		ClaimHash: payload.ClaimHash,
		Iat:       payload.Iat,
	}, nil
}

var _ MutationClaimSigner = (*Secp256k1MutationClaimSigner)(nil)
