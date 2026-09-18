package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/housegate/housegate/pkg/replay"
)

// StatementPurposeV3 separates snapshot queries from v2, query and peer tokens.
const StatementPurposeV3 = "housegate-statement-v3"

// JWSStatementPayloadV3 is the ordered, canonical snapshot-query signed payload.
// Iat is signed but excluded from input identity; only admission checks its age.
type JWSStatementPayloadV3 struct {
	Purpose   string                      `json:"purpose"`
	Iat       int64                       `json:"iat"`
	Binding   replay.SnapshotQueryBinding `json:"binding"`
	InputRoot string                      `json:"input_root"`
}

// SignStatementV3 uses the shared deterministic compact ES256K signer. As in
// v2, it forces the lane purpose and supplies the current time for a zero Iat.
// Callers must validate the complete input and derive Binding and InputRoot
// before signing: the payload alone contains neither SQL nor the read set.
func (s *RelaySigner) SignStatementV3(p JWSStatementPayloadV3) (string, error) {
	p.Purpose = StatementPurposeV3
	if p.Iat == 0 {
		p.Iat = time.Now().Unix()
	}
	return s.signCompactJWS(p)
}

// DecodeStatementV3Payload checks the canonical compact serialization, including
// the exact protected header, ordered payload and signature encoding. It does
// NOT authenticate the payload; use VerifyStatementV3Signature to establish
// trust. No whitespace or quoting is stripped from the original token bytes.
func DecodeStatementV3Payload(token string) (JWSStatementPayloadV3, error) {
	p, _, _, err := decodeStatementV3(token)
	return p, err
}

func decodeStatementV3(token string) (JWSStatementPayloadV3, string, []byte, error) {
	var p JWSStatementPayloadV3
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return p, "", nil, fmt.Errorf("invalid statement v3 JWS format: expected 3 parts")
	}
	header, err := decodeCanonicalRawURL("statement v3 header", parts[0])
	if err != nil {
		return p, "", nil, err
	}
	if string(header) != canonicalJWSProtectedHeader {
		return p, "", nil, fmt.Errorf("invalid statement v3 protected header: must be exact canonical %s", canonicalJWSProtectedHeader)
	}
	raw, err := decodeCanonicalRawURL("statement v3 payload", parts[1])
	if err != nil {
		return p, "", nil, err
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return JWSStatementPayloadV3{}, "", nil, fmt.Errorf("invalid statement v3 payload JSON: %w", err)
	}
	canonical, err := json.Marshal(p)
	if err != nil {
		return JWSStatementPayloadV3{}, "", nil, fmt.Errorf("marshal statement v3 payload: %w", err)
	}
	// Every field is non-optional, including zero-valued genesis/shard fields.
	// Exact re-encoding rejects missing/duplicate/unknown/mixed-case keys, null,
	// reordered fields, alternate escapes and any other JSON byte aliases, at
	// every depth. Ordinary Unmarshal alone would silently accept these forms.
	if !bytes.Equal(raw, canonical) {
		return JWSStatementPayloadV3{}, "", nil, fmt.Errorf("invalid statement v3 payload: noncanonical JSON")
	}
	sig, err := decodeCanonicalRawURL("statement v3 signature", parts[2])
	if err != nil {
		return JWSStatementPayloadV3{}, "", nil, err
	}
	// V=27/28 and low-S are the single v3 encoding of a recoverable signature.
	// Do not narrow recoverAddress: legacy lanes also accept V=0/1.
	if len(sig) != 65 || (sig[64] != 27 && sig[64] != 28) {
		return JWSStatementPayloadV3{}, "", nil, fmt.Errorf("invalid statement v3 signature: expected 65 bytes with V=27/28")
	}
	if !crypto.ValidateSignatureValues(sig[64]-27, new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:64]), true) {
		return JWSStatementPayloadV3{}, "", nil, fmt.Errorf("invalid statement v3 signature: noncanonical scalar or high-S")
	}
	return p, parts[0] + "." + parts[1], sig, nil
}

// VerifyStatementV3Signature is pure signature/binding verification for replay.
// It checks the v3 domain, every bound field and the recovered client_account.
// It never consults a clock, allowlist, current profile or reservation policy.
// Callers MUST derive want from a validated full SnapshotQueryInput and its
// recomputed SnapshotQueryInputRoot. This API cannot validate the absent SQL,
// read-set bytes or publication provenance, and does not authorize execution.
func VerifyStatementV3Signature(token string, want JWSStatementPayloadV3) (account string, err error) {
	account, _, err = verifyStatementV3(token, want)
	return account, err
}

func verifyStatementV3(token string, want JWSStatementPayloadV3) (string, JWSStatementPayloadV3, error) {
	p, input, sig, err := decodeStatementV3(token)
	if err != nil {
		return "", p, err
	}
	if p.Purpose != StatementPurposeV3 {
		return "", p, fmt.Errorf("statement v3 token purpose mismatch: expected %q", StatementPurposeV3)
	}
	if field := StatementPayloadV3Mismatch(p, want); field != "" {
		return "", p, fmt.Errorf("statement v3 token binding mismatch on %s", field)
	}
	switch {
	case p.Binding.EnvelopeVersion != replay.SnapshotQueryEnvelopeVersion:
		return "", p, fmt.Errorf("invalid statement v3 envelope_version")
	case p.Binding.InputKind != replay.SnapshotQueryInputKind:
		return "", p, fmt.Errorf("invalid statement v3 input_kind")
	case p.Binding.StatementKind != replay.SnapshotQueryStatementKind:
		return "", p, fmt.Errorf("invalid statement v3 statement_kind")
	}
	account, err := recoverAddress(keccak256([]byte(input)), sig)
	if err != nil {
		return "", p, fmt.Errorf("statement v3 signature verification failed: %w", err)
	}
	if account != p.Binding.ClientAccount {
		return "", p, fmt.Errorf("statement v3 client_account does not match signature")
	}
	return account, p, nil
}

// ValidateStatementV3 adds ingress freshness (including 5s future skew) and the
// existing address allowlist policy to pure verification. Enabled/AllowNoAuth
// do not bypass signed statements, matching ValidateStatementV2. Profile/ACL
// authorization and reservation equality remain separate caller duties.
func (v *EthValidator) ValidateStatementV3(token string, want JWSStatementPayloadV3) (string, error) {
	account, p, err := verifyStatementV3(token, want)
	if err != nil {
		return "", err
	}
	if err := statementV3Freshness(p.Iat, time.Now().Unix(), v.MaxTokenAge); err != nil {
		return "", err
	}
	if len(v.AllowedAddresses) > 0 && !v.AllowedAddresses[account] {
		return "", fmt.Errorf("statement v3 signer %s not in allowlist", account)
	}
	return account, nil
}

func statementV3Freshness(iat, now int64, maxAge time.Duration) error {
	var age uint64
	// Order signed values first, then subtract unsigned values. The mathematical
	// distance fits uint64 even across the full int64 range; signed subtraction
	// or converting untrusted seconds to nanoseconds could overflow and accept.
	if iat > now {
		if uint64(iat)-uint64(now) > 5 {
			return fmt.Errorf("statement v3 token issued in the future")
		}
	} else {
		age = uint64(now) - uint64(iat)
	}
	if maxAge < 0 || age > uint64(maxAge/time.Second) {
		return fmt.Errorf("statement v3 token expired: age %ds exceeds max %s", age, maxAge)
	}
	return nil
}

// StatementPayloadV3Mismatch returns the first differing bound JSON field name.
// Nested snapshot fields use read_snapshot.<field>; binding fields are unprefixed.
// Iat is signed but ignored here so historical identity is independent of age.
func StatementPayloadV3Mismatch(got, want JWSStatementPayloadV3) string {
	g, w := got.Binding, want.Binding
	switch {
	case got.Purpose != want.Purpose:
		return "purpose"
	case g.EnvelopeVersion != w.EnvelopeVersion:
		return "envelope_version"
	case g.InputKind != w.InputKind:
		return "input_kind"
	case g.ClientAccount != w.ClientAccount:
		return "client_account"
	case g.StatementID != w.StatementID:
		return "statement_id"
	case g.StatementKind != w.StatementKind:
		return "statement_kind"
	case g.NetworkID != w.NetworkID:
		return "network_id"
	case g.KeeperShardID != w.KeeperShardID:
		return "keeper_shard_id"
	case g.SQLHash != w.SQLHash:
		return "sql_hash"
	case g.SettingsHash != w.SettingsHash:
		return "settings_hash"
	case g.TargetTableID != w.TargetTableID:
		return "target_table_id"
	case g.SchemaHash != w.SchemaHash:
		return "schema_hash"
	case g.RowIDProfileID != w.RowIDProfileID:
		return "row_id_profile_id"
	case g.ClientRevision != w.ClientRevision:
		return "client_revision"
	case g.ReadSnapshot.NetworkID != w.ReadSnapshot.NetworkID:
		return "read_snapshot.network_id"
	case g.ReadSnapshot.KeeperShardID != w.ReadSnapshot.KeeperShardID:
		return "read_snapshot.keeper_shard_id"
	case g.ReadSnapshot.SnapshotID != w.ReadSnapshot.SnapshotID:
		return "read_snapshot.snapshot_id"
	case g.ReadSnapshot.SafeBlockSeq != w.ReadSnapshot.SafeBlockSeq:
		return "read_snapshot.safe_block_seq"
	case g.ReadSnapshot.ManifestRoot != w.ReadSnapshot.ManifestRoot:
		return "read_snapshot.manifest_root"
	case g.ReadSnapshot.StateRoot != w.ReadSnapshot.StateRoot:
		return "read_snapshot.state_root"
	case g.ReadSnapshot.SchemaSnapshotID != w.ReadSnapshot.SchemaSnapshotID:
		return "read_snapshot.schema_snapshot_id"
	case g.ReadSnapshot.SchemaRoot != w.ReadSnapshot.SchemaRoot:
		return "read_snapshot.schema_root"
	case g.ReadSetRoot != w.ReadSetRoot:
		return "read_set_root"
	case g.SchemaSnapshotID != w.SchemaSnapshotID:
		return "schema_snapshot_id"
	case g.SchemaRoot != w.SchemaRoot:
		return "schema_root"
	case g.LogicalDatabase != w.LogicalDatabase:
		return "logical_database"
	case g.QueryProfileID != w.QueryProfileID:
		return "query_profile_id"
	case g.ExecutorProfileID != w.ExecutorProfileID:
		return "executor_profile_id"
	case g.ReservationID != w.ReservationID:
		return "reservation_id"
	case g.FencingGeneration != w.FencingGeneration:
		return "fencing_generation"
	case got.InputRoot != want.InputRoot:
		return "input_root"
	}
	return ""
}
