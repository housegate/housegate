package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type statementV2Vector struct {
	Name         string                `json:"name"`
	Expect       string                `json:"expect"`                  // "accept" | "reject"
	RejectReason string                `json:"reject_reason,omitempty"` // "binding" | "purpose" | "signature" | "malformed"
	RejectField  string                `json:"reject_field,omitempty"`  // set iff reject_reason == "binding"
	Payload      JWSStatementPayloadV2 `json:"payload"`
	Token        string                `json:"token"`
}

type statementV2VectorFile struct {
	SignerPrivateKeyHex string              `json:"signer_private_key_hex"`
	SignerAddress       string              `json:"signer_address"`
	Vectors             []statementV2Vector `json:"vectors"`
}

const statementV2VectorPath = "testdata/statement_jws_v2.json"

// statementV2LegacyVectorIat freezes the only legacy token in the corpus.
// SignToken injects wall-clock time, which would make regeneration change the
// supposedly shared byte-for-byte fixture on every run.
const statementV2LegacyVectorIat int64 = 1787215550

// TestGenerateStatementV2Vectors rewrites the shared vector file when
// HOUSEGATE_WRITE_VECTORS=1. The file is consumed by this package and copied
// verbatim into arbiter fsm/testdata so both verifiers prove the same set.
func TestGenerateStatementV2Vectors(t *testing.T) {
	if os.Getenv("HOUSEGATE_WRITE_VECTORS") != "1" {
		t.Skip("set HOUSEGATE_WRITE_VECTORS=1 to regenerate testdata/statement_jws_v2.json")
	}
	signer, err := NewRelaySigner(statementV2TestKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	other, err := NewRelaySigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	valid := statementV2Fixture(signer.Address()) // Iat fixed at 1755000000
	valid.Purpose = StatementPurposeV2
	validToken, err := signer.SignStatementV2(valid)
	if err != nil {
		t.Fatalf("SignStatementV2: %v", err)
	}
	wrongSignerToken, err := other.SignStatementV2(valid)
	if err != nil {
		t.Fatalf("SignStatementV2(other): %v", err)
	}
	legacyToken, err := signer.signCompactJWS(JWSPayload{
		Iat:       statementV2LegacyVectorIat,
		QueryHash: keccak256Hex([]byte("INSERT INTO db.table FORMAT Native")),
		Purpose:   QueryPurpose,
	})
	if err != nil {
		t.Fatalf("sign legacy query token: %v", err)
	}
	mutate := func(field string, f func(*JWSStatementPayloadV2)) statementV2Vector {
		p := valid
		f(&p)
		return statementV2Vector{Name: field + "_mismatch", Expect: "reject", RejectReason: "binding", RejectField: field, Payload: p, Token: validToken}
	}
	file := statementV2VectorFile{
		SignerPrivateKeyHex: statementV2TestKey,
		SignerAddress:       signer.Address(),
		Vectors: []statementV2Vector{
			{Name: "valid", Expect: "accept", Payload: valid, Token: validToken},
			mutate("network_id", func(p *JWSStatementPayloadV2) { p.NetworkID = "other-net" }),
			mutate("keeper_shard_id", func(p *JWSStatementPayloadV2) { p.KeeperShardID = 1 }),
			mutate("statement_id", func(p *JWSStatementPayloadV2) { p.StatementID = signer.Address() + ":2:n1" }),
			mutate("sql_hash", func(p *JWSStatementPayloadV2) { p.SQLHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("settings_hash", func(p *JWSStatementPayloadV2) { p.SettingsHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("schema_hash", func(p *JWSStatementPayloadV2) { p.SchemaHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("payload_hash", func(p *JWSStatementPayloadV2) { p.PayloadHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("payload_length", func(p *JWSStatementPayloadV2) { p.PayloadLength = 1 }),
			mutate("payload_format", func(p *JWSStatementPayloadV2) { p.PayloadFormat = "csv-with-names-v1" }),
			mutate("client_revision", func(p *JWSStatementPayloadV2) { p.ClientRevision = 54470 }),
			mutate("target_table_id", func(p *JWSStatementPayloadV2) { p.TargetTableID = "db.other" }),
			mutate("row_id_profile_id", func(p *JWSStatementPayloadV2) { p.RowIDProfileID = "housegate-row-id-v0" }),
			mutate("statement_kind", func(p *JWSStatementPayloadV2) { p.StatementKind = 0 }),
			{Name: "wrong_signer", Expect: "reject", RejectReason: "signature", Payload: valid, Token: wrongSignerToken},
			{Name: "legacy_query_token", Expect: "reject", RejectReason: "purpose", Payload: valid, Token: legacyToken},
			{Name: "garbage_token", Expect: "reject", RejectReason: "malformed", Payload: valid, Token: "not.a.jws"},
		},
	}
	b, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(statementV2VectorPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(statementV2VectorPath, append(b, '\n'), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// TestStatementV2Vectors proves the committed vectors against this package's
// validator. Every reject vector asserts WHY it was rejected — the field name
// for a binding failure, the class otherwise — so a reordered
// StatementPayloadV2Mismatch or a collapsed error taxonomy is a red test
// rather than a still-green "some error happened".
func TestStatementV2Vectors(t *testing.T) {
	raw, err := os.ReadFile(statementV2VectorPath)
	if err != nil {
		t.Fatalf("read vectors (run TestGenerateStatementV2Vectors with HOUSEGATE_WRITE_VECTORS=1 first): %v", err)
	}
	var file statementV2VectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	validator := NewEthValidator([]string{file.SignerAddress}, 100*365*24*time.Hour, true, false, "", nil)
	if len(file.Vectors) != 17 {
		t.Fatalf("expected exactly 17 vectors, got %d", len(file.Vectors))
	}
	seenBindingFields := map[string]bool{}
	for _, vec := range file.Vectors {
		t.Run(vec.Name, func(t *testing.T) {
			addr, err := validator.ValidateStatementV2(vec.Token, vec.Payload)
			switch vec.Expect {
			case "accept":
				if err != nil {
					t.Fatalf("expected accept, got %v", err)
				}
				if addr != file.SignerAddress {
					t.Fatalf("recovered %s, want %s", addr, file.SignerAddress)
				}
				return
			case "reject":
				if err == nil {
					t.Fatal("expected reject, got accept")
				}
			default:
				t.Fatalf("unknown expect %q", vec.Expect)
			}
			switch vec.RejectReason {
			case "binding":
				seenBindingFields[vec.RejectField] = true
				want := "statement token binding mismatch on " + vec.RejectField
				if err.Error() != want {
					t.Fatalf("error = %q, want exactly %q", err.Error(), want)
				}
			case "purpose":
				if !strings.Contains(err.Error(), "statement token purpose mismatch") {
					t.Fatalf("error = %q, want a purpose mismatch", err.Error())
				}
			case "signature":
				if !strings.Contains(err.Error(), "not in allowlist") && !strings.Contains(err.Error(), "signature verification failed") {
					t.Fatalf("error = %q, want a signer failure", err.Error())
				}
			case "malformed":
				if !strings.Contains(err.Error(), "invalid statement") {
					t.Fatalf("error = %q, want a malformed-token failure", err.Error())
				}
			default:
				t.Fatalf("vector %s has no reject_reason", vec.Name)
			}
		})
	}
	// Every bound field must have its own vector: adding a field to
	// JWSStatementPayloadV2 without a vector is what let statement_kind sit
	// unbound in the first place.
	for _, field := range []string{
		"network_id", "keeper_shard_id", "statement_id", "sql_hash", "settings_hash",
		"schema_hash", "payload_hash", "payload_length", "payload_format",
		"client_revision", "target_table_id", "row_id_profile_id", "statement_kind",
	} {
		if !seenBindingFields[field] {
			t.Fatalf("no reject vector covers bound field %q", field)
		}
	}
}

// TestSharedStatementVectorsSHA256 is the cross-repo link: arbiter asserts its
// copy of testdata/statement_jws_v2.json hashes to the same exported constant.
func TestSharedStatementVectorsSHA256(t *testing.T) {
	raw, err := os.ReadFile(statementV2VectorPath)
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != SharedStatementVectorsSHA256 {
		t.Fatalf("statement_jws_v2.json sha256 = %s, SharedStatementVectorsSHA256 = %s\n"+
			"regenerating the vectors is a coordinated wire change: update the constant, copy the file into arbiter fsm/testdata, and cut both releases together", got, SharedStatementVectorsSHA256)
	}
}
