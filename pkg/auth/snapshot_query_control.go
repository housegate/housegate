package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

// SnapshotQueryControlPurposeV1 domain-separates reservation-control tokens
// from statement, query, and peer-login tokens.
const SnapshotQueryControlPurposeV1 = "housegate-snapshot-query-control-v1"

const (
	SnapshotQueryControlOperationAcquire = "acquire"
	SnapshotQueryControlOperationLookup  = "lookup"
	SnapshotQueryControlOperationRelease = "release"
)

// SnapshotQueryControlBinding is the complete request identity and fence for
// one reservation-control operation. ReservationID and FencingGeneration are
// deliberately allowed to be empty/zero for acquire, lookup-by-request-ID,
// and conditional absent-request release. Their semantic admissibility is an
// Arbiter lifecycle decision; this package only binds the caller-supplied
// values exactly.
//
// The declaration order is the canonical JSON order used in signed tokens.
type SnapshotQueryControlBinding struct {
	Operation         string `json:"operation"`
	NetworkID         string `json:"network_id"`
	KeeperShardID     uint32 `json:"keeper_shard_id"`
	ClientAccount     string `json:"client_account"`
	StatementID       string `json:"statement_id"`
	RequestID         string `json:"request_id"`
	ReservationID     string `json:"reservation_id"`
	FencingGeneration uint64 `json:"fencing_generation"`
}

// SnapshotQueryControlPayloadV1 is the canonical compact-JWS payload. The
// control binding is flattened so its field order remains a frozen wire
// contract after the purpose and issuance time.
type SnapshotQueryControlPayloadV1 struct {
	Purpose string `json:"purpose"`
	Iat     int64  `json:"iat"`
	SnapshotQueryControlBinding
}

// SnapshotQueryControlSigner signs agent-to-Arbiter reservation controls.
// It is intentionally separate from StatementSignerV3: a statement JWS can
// never authorize an acquire, lookup, or release operation.
type SnapshotQueryControlSigner interface {
	Address() string
	SignSnapshotQueryControl(binding SnapshotQueryControlBinding) (string, error)
}

// SnapshotQueryControlValidator performs ingress freshness and allowlist
// checks in addition to the pure signature and binding verification below.
type SnapshotQueryControlValidator interface {
	ValidateSnapshotQueryControl(token string, want SnapshotQueryControlBinding) (string, error)
}

// SignSnapshotQueryControl signs a canonical v1 reservation-control binding.
// It forces the domain purpose and fills Iat when callers do not provide one;
// deterministic fixed vectors can be built with signSnapshotQueryControlAt.
func (s *RelaySigner) SignSnapshotQueryControl(binding SnapshotQueryControlBinding) (string, error) {
	return s.signSnapshotQueryControlAt(binding, time.Now().Unix())
}

func (s *RelaySigner) signSnapshotQueryControlAt(binding SnapshotQueryControlBinding, iat int64) (string, error) {
	payload := SnapshotQueryControlPayloadV1{
		Purpose:                     SnapshotQueryControlPurposeV1,
		Iat:                         iat,
		SnapshotQueryControlBinding: binding,
	}
	return s.signCompactJWS(payload)
}

// DecodeSnapshotQueryControlPayload verifies only compact-JWS canonical
// encoding. Call VerifySnapshotQueryControlSignature or
// ValidateSnapshotQueryControl before trusting the returned fields.
func DecodeSnapshotQueryControlPayload(token string) (SnapshotQueryControlPayloadV1, error) {
	p, _, _, err := decodeSnapshotQueryControl(token)
	return p, err
}

func decodeSnapshotQueryControl(token string) (SnapshotQueryControlPayloadV1, string, []byte, error) {
	var p SnapshotQueryControlPayloadV1
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return p, "", nil, fmt.Errorf("invalid snapshot query control JWS format: expected 3 parts")
	}
	header, err := decodeCanonicalRawURL("snapshot query control header", parts[0])
	if err != nil {
		return p, "", nil, err
	}
	if string(header) != canonicalJWSProtectedHeader {
		return p, "", nil, fmt.Errorf("invalid snapshot query control protected header: must be exact canonical %s", canonicalJWSProtectedHeader)
	}
	raw, err := decodeCanonicalRawURL("snapshot query control payload", parts[1])
	if err != nil {
		return p, "", nil, err
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, "", nil, fmt.Errorf("invalid snapshot query control payload JSON: %w", err)
	}
	canonical, err := json.Marshal(p)
	if err != nil {
		return p, "", nil, fmt.Errorf("marshal snapshot query control payload: %w", err)
	}
	if !bytes.Equal(raw, canonical) {
		return p, "", nil, fmt.Errorf("invalid snapshot query control payload: noncanonical JSON")
	}
	sig, err := decodeCanonicalRawURL("snapshot query control signature", parts[2])
	if err != nil {
		return p, "", nil, err
	}
	if len(sig) != 65 || (sig[64] != 27 && sig[64] != 28) {
		return p, "", nil, fmt.Errorf("invalid snapshot query control signature: expected 65 bytes with V=27/28")
	}
	if !crypto.ValidateSignatureValues(sig[64]-27, new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:64]), true) {
		return p, "", nil, fmt.Errorf("invalid snapshot query control signature: noncanonical scalar or high-S")
	}
	return p, parts[0] + "." + parts[1], sig, nil
}

// VerifySnapshotQueryControlSignature is pure replay-safe verification. It
// checks the fixed domain, operation, every supplied binding field, and that
// the recovered signer is the signed client account. It deliberately does not
// consult a wall clock or an allowlist.
func VerifySnapshotQueryControlSignature(token string, want SnapshotQueryControlBinding) (string, error) {
	p, input, sig, err := decodeSnapshotQueryControl(token)
	if err != nil {
		return "", err
	}
	if p.Purpose != SnapshotQueryControlPurposeV1 {
		return "", fmt.Errorf("snapshot query control token purpose mismatch: expected %q", SnapshotQueryControlPurposeV1)
	}
	if !validSnapshotQueryControlOperation(p.Operation) || !validSnapshotQueryControlOperation(want.Operation) {
		return "", fmt.Errorf("snapshot query control operation is invalid")
	}
	if field := SnapshotQueryControlBindingMismatch(p.SnapshotQueryControlBinding, want); field != "" {
		return "", fmt.Errorf("snapshot query control token binding mismatch on %s", field)
	}
	account, err := recoverAddress(keccak256([]byte(input)), sig)
	if err != nil {
		return "", fmt.Errorf("snapshot query control signature verification failed: %w", err)
	}
	if account != p.ClientAccount {
		return "", fmt.Errorf("snapshot query control client_account does not match signature")
	}
	return account, nil
}

// ValidateSnapshotQueryControl adds the normal ingress freshness and allowlist
// policies to pure verification. This never follows EthValidator.Enabled or
// AllowNoAuth: a reservation-control token is mandatory even while ordinary
// query authentication is disabled.
func (v *EthValidator) ValidateSnapshotQueryControl(token string, want SnapshotQueryControlBinding) (string, error) {
	account, err := VerifySnapshotQueryControlSignature(token, want)
	if err != nil {
		return "", err
	}
	p, err := DecodeSnapshotQueryControlPayload(token)
	if err != nil {
		return "", err
	}
	if err := snapshotQueryControlFreshness(p.Iat, time.Now().Unix(), v.MaxTokenAge); err != nil {
		return "", err
	}
	if len(v.AllowedAddresses) > 0 && !v.AllowedAddresses[account] {
		return "", fmt.Errorf("snapshot query control signer %s not in allowlist", account)
	}
	return account, nil
}

func snapshotQueryControlFreshness(iat, now int64, maxAge time.Duration) error {
	if err := statementV3Freshness(iat, now, maxAge); err != nil {
		return fmt.Errorf("snapshot query control token %w", err)
	}
	return nil
}

func validSnapshotQueryControlOperation(operation string) bool {
	switch operation {
	case SnapshotQueryControlOperationAcquire, SnapshotQueryControlOperationLookup, SnapshotQueryControlOperationRelease:
		return true
	default:
		return false
	}
}

// SnapshotQueryControlBindingMismatch returns the first canonical binding
// field that differs. It deliberately treats empty IDs and zero fences as
// values, never as wildcards.
func SnapshotQueryControlBindingMismatch(got, want SnapshotQueryControlBinding) string {
	switch {
	case got.Operation != want.Operation:
		return "operation"
	case got.NetworkID != want.NetworkID:
		return "network_id"
	case got.KeeperShardID != want.KeeperShardID:
		return "keeper_shard_id"
	case got.ClientAccount != want.ClientAccount:
		return "client_account"
	case got.StatementID != want.StatementID:
		return "statement_id"
	case got.RequestID != want.RequestID:
		return "request_id"
	case got.ReservationID != want.ReservationID:
		return "reservation_id"
	case got.FencingGeneration != want.FencingGeneration:
		return "fencing_generation"
	}
	return ""
}

var (
	_ SnapshotQueryControlSigner    = (*RelaySigner)(nil)
	_ SnapshotQueryControlValidator = (*EthValidator)(nil)
)
