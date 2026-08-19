package auth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type statementV2Vector struct {
	Name    string                `json:"name"`
	Expect  string                `json:"expect"` // "accept" | "reject"
	Payload JWSStatementPayloadV2 `json:"payload"`
	Token   string                `json:"token"`
}

type statementV2VectorFile struct {
	SignerPrivateKeyHex string              `json:"signer_private_key_hex"`
	SignerAddress       string              `json:"signer_address"`
	Vectors             []statementV2Vector `json:"vectors"`
}

const statementV2VectorPath = "testdata/statement_jws_v2.json"

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
	legacyToken, err := signer.SignToken("INSERT INTO db.table FORMAT Native")
	if err != nil {
		t.Fatalf("SignToken: %v", err)
	}
	mutate := func(name string, f func(*JWSStatementPayloadV2)) statementV2Vector {
		p := valid
		f(&p)
		return statementV2Vector{Name: name, Expect: "reject", Payload: p, Token: validToken}
	}
	file := statementV2VectorFile{
		SignerPrivateKeyHex: statementV2TestKey,
		SignerAddress:       signer.Address(),
		Vectors: []statementV2Vector{
			{Name: "valid", Expect: "accept", Payload: valid, Token: validToken},
			mutate("network_id_mismatch", func(p *JWSStatementPayloadV2) { p.NetworkID = "other-net" }),
			mutate("keeper_shard_id_mismatch", func(p *JWSStatementPayloadV2) { p.KeeperShardID = 1 }),
			mutate("statement_id_mismatch", func(p *JWSStatementPayloadV2) { p.StatementID = signer.Address() + ":2:n1" }),
			mutate("sql_hash_mismatch", func(p *JWSStatementPayloadV2) { p.SQLHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("settings_hash_mismatch", func(p *JWSStatementPayloadV2) { p.SettingsHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("schema_hash_mismatch", func(p *JWSStatementPayloadV2) { p.SchemaHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("payload_hash_mismatch", func(p *JWSStatementPayloadV2) { p.PayloadHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("payload_length_mismatch", func(p *JWSStatementPayloadV2) { p.PayloadLength = 1 }),
			mutate("payload_format_mismatch", func(p *JWSStatementPayloadV2) { p.PayloadFormat = "csv-with-names-v1" }),
			mutate("client_revision_mismatch", func(p *JWSStatementPayloadV2) { p.ClientRevision = 54470 }),
			mutate("target_table_id_mismatch", func(p *JWSStatementPayloadV2) { p.TargetTableID = "db.other" }),
			mutate("row_id_profile_id_mismatch", func(p *JWSStatementPayloadV2) { p.RowIDProfileID = "housegate-row-id-v0" }),
			{Name: "wrong_signer", Expect: "reject", Payload: valid, Token: wrongSignerToken},
			{Name: "legacy_query_token", Expect: "reject", Payload: valid, Token: legacyToken},
			{Name: "garbage_token", Expect: "reject", Payload: valid, Token: "not.a.jws"},
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

// TestStatementV2Vectors proves the committed vectors against this
// package's validator. The validator's iat window is bypassed by pinning
// MaxTokenAge very large: the vectors are fixed at iat=1755000000 and the
// FSM-side consumer is wall-clock-free anyway.
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
	if len(file.Vectors) < 16 {
		t.Fatalf("expected at least 16 vectors, got %d", len(file.Vectors))
	}
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
			case "reject":
				if err == nil {
					t.Fatal("expected reject, got accept")
				}
			default:
				t.Fatalf("unknown expect %q", vec.Expect)
			}
		})
	}
}
