package auth

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

func TestRelaySigner_SignAndValidate(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	signer, err := NewRelaySigner(privKeyHex)
	if err != nil {
		t.Fatalf("failed to create relay signer: %v", err)
	}

	validator := NewEthValidator([]string{signer.Address()}, 1*time.Minute, true, false, "", nil)

	sql := "SELECT * FROM default.my_table WHERE id = 1"
	token, err := signer.SignToken(sql)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	meta := QueryMeta{
		SQL:      sql,
		Settings: map[string]string{AuthTokenSettingKey: token},
	}
	if _, err := validator.ValidateQuery(context.Background(), meta); err != nil {
		t.Errorf("relay token validation failed: %v", err)
	}
}

func TestRelaySigner_QHashMatches(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	signer, err := NewRelaySigner(privKeyHex)
	if err != nil {
		t.Fatalf("failed to create relay signer: %v", err)
	}

	sql := "SELECT 1"
	token, err := signer.SignToken(sql)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	_, payload, _, err := parseJWSCompact(token)
	if err != nil {
		t.Fatalf("failed to parse token: %v", err)
	}
	expectedHash := keccak256Hex([]byte(sql))
	if payload.QueryHash != expectedHash {
		t.Errorf("qhash mismatch: got %s, want %s", payload.QueryHash, expectedHash)
	}
}

func TestRelaySigner_SignTokenIncludesQueryPurpose(t *testing.T) {
	signer, err := NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("failed to create relay signer: %v", err)
	}
	token, err := signer.SignToken("SELECT 1")
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	_, payload, _, err := parseJWSCompact(token)
	if err != nil {
		t.Fatalf("failed to parse token: %v", err)
	}
	if payload.Purpose != QueryPurpose {
		t.Fatalf("purpose = %q, want %q", payload.Purpose, QueryPurpose)
	}
}

func TestRelaySigner_AddressInAllowlist(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	signer, err := NewRelaySigner(privKeyHex)
	if err != nil {
		t.Fatalf("failed to create relay signer: %v", err)
	}

	addr := signer.Address()
	if addr == "" {
		t.Fatal("address should not be empty")
	}
	if addr[:2] != "0x" {
		t.Errorf("address should start with 0x, got %s", addr)
	}
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	expectedAddr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()
	if !strings.EqualFold(addr, expectedAddr) {
		t.Errorf("address mismatch: got %s, want %s", addr, expectedAddr)
	}
}

func TestRelaySigner_DifferentSQLDifferentToken(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	signer, err := NewRelaySigner(privKeyHex)
	if err != nil {
		t.Fatalf("failed to create relay signer: %v", err)
	}

	token1, _ := signer.SignToken("SELECT 1")
	token2, _ := signer.SignToken("SELECT 2")
	if token1 == token2 {
		t.Error("tokens for different SQL should be different")
	}
}

func TestRelaySigner_WrongSQLFailsValidation(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	signer, err := NewRelaySigner(privKeyHex)
	if err != nil {
		t.Fatalf("failed to create relay signer: %v", err)
	}

	validator := NewEthValidator([]string{signer.Address()}, 1*time.Minute, true, false, "", nil)

	token, _ := signer.SignToken("SELECT 1")
	meta := QueryMeta{
		SQL:      "SELECT 2",
		Settings: map[string]string{AuthTokenSettingKey: token},
	}
	if _, err := validator.ValidateQuery(context.Background(), meta); err == nil {
		t.Error("expected validation to fail for wrong SQL, got nil")
	}
}

func TestRelaySigner_WithPrefix0x(t *testing.T) {
	signer, err := NewRelaySigner("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("failed to create relay signer with 0x prefix: %v", err)
	}
	if signer.Address() == "" {
		t.Error("address should not be empty")
	}
}
