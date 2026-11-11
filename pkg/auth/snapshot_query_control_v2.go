package auth

import (
	"fmt"
	"time"
)

// SnapshotQueryControlPurposeV2 domain-separates the track-aware
// reservation-control tokens. A v2 token additionally binds the canonical
// table id of the query's target, which a track-aware sequencer needs at
// acquire time to scope its admission fence to the target's owner. v1 tokens
// stay valid for sequencers without tracks; a v1 token never verifies as v2
// and a v2 token never verifies as v1.
const SnapshotQueryControlPurposeV2 = "housegate-snapshot-query-control-v2"

// SnapshotQueryControlBindingV2 is the v1 binding plus the target table id.
// Like every v1 field, TargetTableID is bound exactly and an empty string is
// a value, not a wildcard; whether an empty target is admissible is the
// sequencer's lifecycle decision.
//
// The v1 fields keep their order and TargetTableID follows them; together
// with the purpose and issuance time this is the canonical JSON order.
type SnapshotQueryControlBindingV2 struct {
	SnapshotQueryControlBinding
	TargetTableID string `json:"target_table_id"`
}

// SnapshotQueryControlPayloadV2 is the canonical compact-JWS payload of a v2
// control token: purpose, iat, the eight v1 binding fields, target_table_id.
type SnapshotQueryControlPayloadV2 struct {
	Purpose string `json:"purpose"`
	Iat     int64  `json:"iat"`
	SnapshotQueryControlBindingV2
}

// SnapshotQueryControlSignerV2 signs v2 reservation controls.
type SnapshotQueryControlSignerV2 interface {
	Address() string
	SignSnapshotQueryControlV2(binding SnapshotQueryControlBindingV2) (string, error)
}

// SnapshotQueryControlValidatorV2 performs ingress freshness and allowlist
// checks in addition to the pure v2 signature and binding verification.
type SnapshotQueryControlValidatorV2 interface {
	ValidateSnapshotQueryControlV2(token string, want SnapshotQueryControlBindingV2) (string, error)
}

// SignSnapshotQueryControlV2 signs a canonical v2 reservation-control binding.
func (s *RelaySigner) SignSnapshotQueryControlV2(binding SnapshotQueryControlBindingV2) (string, error) {
	return s.signSnapshotQueryControlV2At(binding, time.Now().Unix())
}

func (s *RelaySigner) signSnapshotQueryControlV2At(binding SnapshotQueryControlBindingV2, iat int64) (string, error) {
	return s.signCompactJWS(SnapshotQueryControlPayloadV2{
		Purpose:                       SnapshotQueryControlPurposeV2,
		Iat:                           iat,
		SnapshotQueryControlBindingV2: binding,
	})
}

// DecodeSnapshotQueryControlPayloadV2 verifies only compact-JWS canonical
// encoding of a v2 payload. Call VerifySnapshotQueryControlSignatureV2 or
// ValidateSnapshotQueryControlV2 before trusting the returned fields.
func DecodeSnapshotQueryControlPayloadV2(token string) (SnapshotQueryControlPayloadV2, error) {
	var p SnapshotQueryControlPayloadV2
	_, _, err := decodeSnapshotQueryControlInto(token, &p)
	return p, err
}

// VerifySnapshotQueryControlSignatureV2 is the pure, replay-safe v2
// verification: the fixed v2 domain, the operation, every supplied binding
// field including target_table_id, and that the recovered signer is the
// signed client account. It consults no wall clock and no allowlist.
func VerifySnapshotQueryControlSignatureV2(token string, want SnapshotQueryControlBindingV2) (string, error) {
	var p SnapshotQueryControlPayloadV2
	input, sig, err := decodeSnapshotQueryControlInto(token, &p)
	if err != nil {
		return "", err
	}
	if p.Purpose != SnapshotQueryControlPurposeV2 {
		return "", fmt.Errorf("snapshot query control token purpose mismatch: expected %q", SnapshotQueryControlPurposeV2)
	}
	if !validSnapshotQueryControlOperation(p.Operation) || !validSnapshotQueryControlOperation(want.Operation) {
		return "", fmt.Errorf("snapshot query control operation is invalid")
	}
	if field := SnapshotQueryControlBindingV2Mismatch(p.SnapshotQueryControlBindingV2, want); field != "" {
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

// ValidateSnapshotQueryControlV2 adds the ingress freshness and allowlist
// policies to pure v2 verification. Like v1 it never follows
// EthValidator.Enabled or AllowNoAuth.
func (v *EthValidator) ValidateSnapshotQueryControlV2(token string, want SnapshotQueryControlBindingV2) (string, error) {
	account, err := VerifySnapshotQueryControlSignatureV2(token, want)
	if err != nil {
		return "", err
	}
	p, err := DecodeSnapshotQueryControlPayloadV2(token)
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

// SnapshotQueryControlBindingV2Mismatch returns the first canonical v2
// binding field that differs, in canonical order.
func SnapshotQueryControlBindingV2Mismatch(got, want SnapshotQueryControlBindingV2) string {
	if field := SnapshotQueryControlBindingMismatch(got.SnapshotQueryControlBinding, want.SnapshotQueryControlBinding); field != "" {
		return field
	}
	if got.TargetTableID != want.TargetTableID {
		return "target_table_id"
	}
	return ""
}

var (
	_ SnapshotQueryControlSignerV2    = (*RelaySigner)(nil)
	_ SnapshotQueryControlValidatorV2 = (*EthValidator)(nil)
)
