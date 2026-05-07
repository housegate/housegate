package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

// generateTestToken creates a JWS token for testing using Ethereum-style signing.
func generateTestToken(t *testing.T, privKeyHex string, iat time.Time, qhash string) string {
	t.Helper()

	privateKey, err := crypto.HexToECDSA(privKeyHex)
	if err != nil {
		t.Fatalf("failed to parse private key: %v", err)
	}

	header := JWSHeader{Alg: "ES256K", Typ: "JWT"}
	headerJSON, _ := json.Marshal(header)
	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)

	payload := JWSPayload{Iat: iat.Unix(), QueryHash: qhash}
	payloadJSON, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	signingInput := headerB64 + "." + payloadB64

	hash := keccak256([]byte(signingInput))
	sig, err := crypto.Sign(hash, privateKey)
	if err != nil {
		t.Fatalf("failed to sign: %v", err)
	}
	sig[64] += 27

	signatureB64 := base64.RawURLEncoding.EncodeToString(sig)
	return signingInput + "." + signatureB64
}

func TestEthValidator_ValidToken(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, "", nil)

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL:      sql,
		Settings: map[string]string{AuthTokenSettingKey: token},
	}
	if _, err := validator.ValidateQuery(context.Background(), meta); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEthValidator_MissingToken(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, "", nil)

	meta := QueryMeta{SQL: "SELECT 1", Settings: map[string]string{}}
	if _, err := validator.ValidateQuery(context.Background(), meta); err == nil {
		t.Error("expected error for missing token, got nil")
	}
}

func TestEthValidator_ExpiredToken(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, "", nil)

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now().Add(-2*time.Minute), sqlHash)

	meta := QueryMeta{
		SQL:      sql,
		Settings: map[string]string{AuthTokenSettingKey: token},
	}
	if _, err := validator.ValidateQuery(context.Background(), meta); err == nil {
		t.Error("expected error for expired token, got nil")
	}
}

func TestEthValidator_QueryHashMismatch(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, "", nil)

	sql := "SELECT 1"
	wrongHash := keccak256Hex([]byte("SELECT 2"))
	token := generateTestToken(t, privKeyHex, time.Now(), wrongHash)

	meta := QueryMeta{
		SQL:      sql,
		Settings: map[string]string{AuthTokenSettingKey: token},
	}
	if _, err := validator.ValidateQuery(context.Background(), meta); err == nil {
		t.Error("expected error for query hash mismatch, got nil")
	}
}

func TestEthValidator_UnauthorizedAddress(t *testing.T) {
	validator := NewEthValidator([]string{"0x1234567890123456789012345678901234567890"}, 1*time.Minute, true, false, "", nil)

	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL:      sql,
		Settings: map[string]string{AuthTokenSettingKey: token},
	}
	if _, err := validator.ValidateQuery(context.Background(), meta); err == nil {
		t.Error("expected error for unauthorized address, got nil")
	}
}

func TestEthValidator_Disabled(t *testing.T) {
	validator := NewEthValidator(nil, 1*time.Minute, false, false, "", nil)
	meta := QueryMeta{SQL: "SELECT 1", Settings: map[string]string{}}
	if _, err := validator.ValidateQuery(context.Background(), meta); err != nil {
		t.Errorf("expected no error when validator is disabled, got: %v", err)
	}
}

func TestEthValidator_MaintenanceFromIndexerSigner(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, addr, nil)

	sql := "DROP TABLE IF EXISTS some_proj.some_table"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey:      token,
			"SQL_sentio_maintenance": "1",
		},
	}
	res, err := validator.ValidateQuery(context.Background(), meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Address == "" {
		t.Errorf("expected authenticated address, got empty")
	}
	if !res.Maintenance {
		t.Errorf("expected maintenance=true when SQL_sentio_maintenance=1 and signer matches indexer")
	}
}

// Regression: clickhouse-go's CustomSetting wraps string values in
// single quotes via Field::restoreFromDump, so the wire value of
// SQL_sentio_maintenance arriving from a clickhouse-go client is "'1'"
// rather than "1". isMaintenance must trim those quotes — otherwise
// the GC's DROP TABLE falls through commitgate's PermissionObserver
// and is rejected.
func TestEthValidator_MaintenanceFromQuotedCustomSetting(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, addr, nil)

	sql := "DROP TABLE IF EXISTS some_proj.some_table"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	for _, tc := range []struct {
		name       string
		tokenValue string
		maintValue string
	}{
		{"both raw", token, "1"},
		{"single-quoted (clickhouse-go CustomSetting)", "'" + token + "'", "'1'"},
		{"double-quoted", "\"" + token + "\"", "\"1\""},
		{"maintenance true with quotes", token, "'true'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := QueryMeta{
				SQL: sql,
				Settings: map[string]string{
					AuthTokenSettingKey:      tc.tokenValue,
					"SQL_sentio_maintenance": tc.maintValue,
				},
			}
			res, err := validator.ValidateQuery(context.Background(), meta)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Address == "" {
				t.Error("expected authenticated address, got empty")
			}
			if !res.Maintenance {
				t.Errorf("expected maintenance=true for value %q", tc.maintValue)
			}
		})
	}
}

func TestEthValidator_MaintenanceRejectedFromAllowedNonIndexer(t *testing.T) {
	// A signer that is allow-listed but is NOT this housegate's indexer
	// must NOT be granted the maintenance bypass. We reject the request
	// outright rather than silently downgrade — a misconfigured caller
	// should hear about it.
	signerKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	signerKey, _ := crypto.HexToECDSA(signerKeyHex)
	signerAddr := crypto.PubkeyToAddress(signerKey.PublicKey).Hex()

	indexerKeyHex := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	indexerKey, _ := crypto.HexToECDSA(indexerKeyHex)
	indexerAddr := crypto.PubkeyToAddress(indexerKey.PublicKey).Hex()

	validator := NewEthValidator([]string{signerAddr, indexerAddr}, 1*time.Minute, true, false, indexerAddr, nil)

	sql := "DROP TABLE IF EXISTS some_proj.some_table"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, signerKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey:      token,
			"SQL_sentio_maintenance": "1",
		},
	}
	res, err := validator.ValidateQuery(context.Background(), meta)
	if err == nil {
		t.Fatal("expected error when allow-listed but non-indexer signer requests maintenance, got nil")
	}
	if !strings.Contains(err.Error(), "SQL_sentio_maintenance") {
		t.Errorf("expected error to mention SQL_sentio_maintenance, got: %v", err)
	}
	if res.Maintenance {
		t.Errorf("expected maintenance=false on rejected request, got true")
	}
}

func TestEthValidator_MaintenanceRejectedWhenNoIndexerConfigured(t *testing.T) {
	// indexerAddress is empty (e.g. observer-mode housegate or sidecar).
	// Any maintenance request must be rejected outright.
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, "", nil)

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey:      token,
			"SQL_sentio_maintenance": "1",
		},
	}
	res, err := validator.ValidateQuery(context.Background(), meta)
	if err == nil {
		t.Fatal("expected error when no indexer is configured but maintenance is requested, got nil")
	}
	if res.Maintenance {
		t.Errorf("expected maintenance=false on rejected request, got true")
	}
}

func TestEthValidator_MaintenanceFromDisallowedAddress(t *testing.T) {
	// Build the validator's allowlist with a different address than the signer.
	otherAddr := "0x0000000000000000000000000000000000000099"
	validator := NewEthValidator([]string{otherAddr}, 1*time.Minute, true, false, "", nil)

	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sql := "DROP TABLE IF EXISTS some_proj.some_table"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey:      token,
			"SQL_sentio_maintenance": "1",
		},
	}
	res, err := validator.ValidateQuery(context.Background(), meta)
	if err == nil {
		t.Fatal("expected error for disallowed address, got nil")
	}
	if res.Maintenance {
		t.Errorf("expected maintenance=false on rejected request, got true")
	}
}

func TestEthValidator_MaintenanceFlagWithoutSetting(t *testing.T) {
	// Confirms the success path leaves maintenance=false when the setting
	// is absent — i.e. SetMaintenance is gated on the setting, not on a
	// successful auth alone.
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, "", nil)

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL:      sql,
		Settings: map[string]string{AuthTokenSettingKey: token},
	}
	res, err := validator.ValidateQuery(context.Background(), meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Maintenance {
		t.Errorf("expected maintenance=false when SQL_sentio_maintenance is unset")
	}
}

// TestEthValidator_PlatformOperatorFromAllowlistedSigner verifies the
// success path: SQL_sentio_platform_operator + signer in operator
// allowlist returns platformOperator=true.
func TestEthValidator_PlatformOperatorFromAllowlistedSigner(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, "", []string{addr})

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey:        token,
			PlatformOperatorSettingKey: "1",
		},
	}
	res, err := validator.ValidateQuery(context.Background(), meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Address == "" {
		t.Errorf("expected authenticated address, got empty")
	}
	if res.Maintenance {
		t.Errorf("expected maintenance=false; only platform_operator was set")
	}
	if !res.PlatformOperator {
		t.Errorf("expected platform_operator=true when SQL_sentio_platform_operator=1 and signer is in operator allowlist")
	}
}

// TestEthValidator_PlatformOperatorFromQuotedCustomSetting mirrors the
// maintenance quoting test: clickhouse-go's CustomSetting wraps string
// values in single quotes, so isPlatformOperator must trim them.
func TestEthValidator_PlatformOperatorFromQuotedCustomSetting(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, "", []string{addr})

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	for _, tc := range []struct {
		name     string
		opValue  string
	}{
		{"raw", "1"},
		{"single-quoted", "'1'"},
		{"double-quoted", "\"1\""},
		{"true with quotes", "'true'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := QueryMeta{
				SQL: sql,
				Settings: map[string]string{
					AuthTokenSettingKey:        token,
					PlatformOperatorSettingKey: tc.opValue,
				},
			}
			res, err := validator.ValidateQuery(context.Background(), meta)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !res.PlatformOperator {
				t.Errorf("expected platform_operator=true for value %q", tc.opValue)
			}
		})
	}
}

// TestEthValidator_PlatformOperatorRejectedFromNonOperator verifies a
// signer that is allow-listed for normal queries but NOT in the
// platform-operator allowlist is rejected when it sets the setting.
func TestEthValidator_PlatformOperatorRejectedFromNonOperator(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	otherOperator := "0x000000000000000000000000000000000000beef"
	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, "", []string{otherOperator})

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey:        token,
			PlatformOperatorSettingKey: "1",
		},
	}
	res, err := validator.ValidateQuery(context.Background(), meta)
	if err == nil {
		t.Fatal("expected error when non-operator signer requests platform_operator, got nil")
	}
	if !strings.Contains(err.Error(), "SQL_sentio_platform_operator") {
		t.Errorf("expected error to mention SQL_sentio_platform_operator, got: %v", err)
	}
	if res.PlatformOperator {
		t.Errorf("expected platform_operator=false on rejected request, got true")
	}
}

// TestEthValidator_PlatformOperatorRejectedWhenNoAllowlistConfigured
// verifies that a query setting SQL_sentio_platform_operator is rejected
// outright when no operator allowlist is configured (rather than silently
// downgraded). Loud failure beats silent fallthrough.
func TestEthValidator_PlatformOperatorRejectedWhenNoAllowlistConfigured(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, "", nil)

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey:        token,
			PlatformOperatorSettingKey: "1",
		},
	}
	res, err := validator.ValidateQuery(context.Background(), meta)
	if err == nil {
		t.Fatal("expected error when platform_operator allowlist is unconfigured, got nil")
	}
	if res.PlatformOperator {
		t.Errorf("expected platform_operator=false on rejected request, got true")
	}
}

// TestEthValidator_PlatformOperatorFlagWithoutSetting confirms the
// success path leaves platform_operator=false when the setting is
// absent — the flag must not trigger from a successful auth alone.
func TestEthValidator_PlatformOperatorFlagWithoutSetting(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, "", []string{addr})

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL:      sql,
		Settings: map[string]string{AuthTokenSettingKey: token},
	}
	res, err := validator.ValidateQuery(context.Background(), meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.PlatformOperator {
		t.Errorf("expected platform_operator=false when SQL_sentio_platform_operator is unset")
	}
}

// TestEthValidator_MaintenanceAndPlatformOperatorBothSet verifies the
// two bypass flags compose: a single query may set both, and both
// flags are returned independently when both gates pass.
func TestEthValidator_MaintenanceAndPlatformOperatorBothSet(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true, false, addr, []string{addr})

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))
	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey:        token,
			MaintenanceSettingKey:      "1",
			PlatformOperatorSettingKey: "1",
		},
	}
	res, err := validator.ValidateQuery(context.Background(), meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Maintenance || !res.PlatformOperator {
		t.Errorf("expected both flags true, got maintenance=%v platform_operator=%v",
			res.Maintenance, res.PlatformOperator)
	}
}

func TestParseJWS(t *testing.T) {
	privKeyHex := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))

	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	header, payload, sig, err := parseJWSCompact(token)
	if err != nil {
		t.Fatalf("parseJWSCompact failed: %v", err)
	}
	if header.Alg != "ES256K" {
		t.Errorf("expected alg ES256K, got %s", header.Alg)
	}
	if payload.QueryHash != sqlHash {
		t.Errorf("expected qhash %s, got %s", sqlHash, payload.QueryHash)
	}
	if len(sig) != 65 {
		t.Errorf("expected signature length 65, got %d", len(sig))
	}
}
