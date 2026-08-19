package auth

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

const statementV2TestKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func statementV2Fixture(account string) JWSStatementPayloadV2 {
	return JWSStatementPayloadV2{
		Iat:            1755000000,
		NetworkID:      "testnet-v2",
		KeeperShardID:  0,
		StatementID:    account + ":1:n1",
		SQLHash:        "0x" + strings.Repeat("11", 32),
		SettingsHash:   "0x" + strings.Repeat("22", 32),
		SchemaHash:     "0x" + strings.Repeat("33", 32),
		PayloadHash:    "0x" + strings.Repeat("44", 32),
		PayloadLength:  12345,
		PayloadFormat:  "clickhouse-native-data-v1",
		ClientRevision: 54460,
		TargetTableID:  "db.table",
		RowIDProfileID: "housegate-row-id-v1",
	}
}

func TestStatementV2_SignAndValidateRoundTrip(t *testing.T) {
	signer, err := NewRelaySigner(statementV2TestKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	want := statementV2Fixture(signer.Address())
	want.Iat = time.Now().Unix()
	token, err := signer.SignStatementV2(want)
	if err != nil {
		t.Fatalf("SignStatementV2: %v", err)
	}
	if strings.Count(token, ".") != 2 {
		t.Fatalf("token is not compact JWS: %q", token)
	}
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	got, err := validator.ValidateStatementV2(token, want)
	if err != nil {
		t.Fatalf("ValidateStatementV2: %v", err)
	}
	if got != signer.Address() {
		t.Fatalf("recovered %s, want %s", got, signer.Address())
	}
}

func TestStatementV2_SignerForcesPurposeAndFillsIat(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	p := statementV2Fixture(signer.Address())
	p.Iat = 0
	p.Purpose = "wrong"
	token, err := signer.SignStatementV2(p)
	if err != nil {
		t.Fatalf("SignStatementV2: %v", err)
	}
	decoded, err := DecodeStatementV2Payload(token)
	if err != nil {
		t.Fatalf("DecodeStatementV2Payload: %v", err)
	}
	if decoded.Purpose != StatementPurposeV2 {
		t.Fatalf("purpose = %q, want %q", decoded.Purpose, StatementPurposeV2)
	}
	if decoded.Iat == 0 {
		t.Fatal("iat must be filled when zero")
	}
}

func TestStatementV2_EveryBoundFieldMismatchRejects(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	base := statementV2Fixture(signer.Address())
	base.Iat = time.Now().Unix()
	token, err := signer.SignStatementV2(base)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	mutations := map[string]func(*JWSStatementPayloadV2){
		"network_id":        func(p *JWSStatementPayloadV2) { p.NetworkID = "other" },
		"keeper_shard_id":   func(p *JWSStatementPayloadV2) { p.KeeperShardID = 1 },
		"statement_id":      func(p *JWSStatementPayloadV2) { p.StatementID = signer.Address() + ":2:n1" },
		"sql_hash":          func(p *JWSStatementPayloadV2) { p.SQLHash = "0x" + strings.Repeat("aa", 32) },
		"settings_hash":     func(p *JWSStatementPayloadV2) { p.SettingsHash = "0x" + strings.Repeat("aa", 32) },
		"schema_hash":       func(p *JWSStatementPayloadV2) { p.SchemaHash = "0x" + strings.Repeat("aa", 32) },
		"payload_hash":      func(p *JWSStatementPayloadV2) { p.PayloadHash = "0x" + strings.Repeat("aa", 32) },
		"payload_length":    func(p *JWSStatementPayloadV2) { p.PayloadLength = 1 },
		"payload_format":    func(p *JWSStatementPayloadV2) { p.PayloadFormat = "csv-with-names-v1" },
		"client_revision":   func(p *JWSStatementPayloadV2) { p.ClientRevision = 54470 },
		"target_table_id":   func(p *JWSStatementPayloadV2) { p.TargetTableID = "db.other" },
		"row_id_profile_id": func(p *JWSStatementPayloadV2) { p.RowIDProfileID = "housegate-row-id-v0" },
	}
	for name, mutate := range mutations {
		want := base
		mutate(&want)
		_, err := validator.ValidateStatementV2(token, want)
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("mutation %s: err = %v, want rejection naming the field", name, err)
		}
	}
}

func TestStatementV2_PurposeMismatchRejects(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := statementV2Fixture(signer.Address())
	// A legacy query token in the statement slot must be rejected on purpose.
	legacy, err := signer.SignToken("SELECT 1")
	if err != nil {
		t.Fatalf("SignToken: %v", err)
	}
	if _, err := validator.ValidateStatementV2(legacy, want); err == nil || !strings.Contains(err.Error(), "purpose") {
		t.Fatalf("legacy token accepted as statement token: %v", err)
	}
}

func TestStatementV2_RejectsNonCanonicalProtectedHeader(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := statementV2Fixture(signer.Address())
	want.Iat = time.Now().Unix()
	for name, header := range map[string]JWSHeader{
		"legacy algorithm alias": {Alg: "secp256k1", Typ: "JWT"},
		"missing type":           {Alg: "ES256K"},
		"wrong type":             {Alg: "ES256K", Typ: "JOSE"},
	} {
		t.Run(name, func(t *testing.T) {
			token := signStatementV2WithHeader(t, header, want)
			if _, err := validator.ValidateStatementV2(token, want); err == nil {
				t.Fatalf("accepted non-canonical protected header %+v", header)
			}
		})
	}
}

func TestStatementV2_RejectsCanonicalFieldsInNonCanonicalProtectedJSON(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := statementV2Fixture(signer.Address())
	want.Iat = time.Now().Unix()
	tests := map[string]string{
		"extra field":      `{"alg":"ES256K","typ":"JWT","kid":"key-1"}`,
		"unknown crit":     `{"alg":"ES256K","typ":"JWT","crit":["exp"]}`,
		"reordered keys":   `{"typ":"JWT","alg":"ES256K"}`,
		"whitespace":       `{ "alg":"ES256K","typ":"JWT"}`,
		"duplicate member": `{"alg":"ES256K","alg":"ES256K","typ":"JWT"}`,
	}
	for name, rawHeader := range tests {
		t.Run(name, func(t *testing.T) {
			token := signStatementV2WithRawHeader(t, rawHeader, want)
			if _, err := validator.ValidateStatementV2(token, want); err == nil || !strings.Contains(err.Error(), "canonical") {
				t.Fatalf("ValidateStatementV2 = %v, want canonical-header rejection for %s", err, rawHeader)
			}
		})
	}
}

func TestStatementV2_RejectsNonCanonicalBase64URLSegments(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := statementV2Fixture(signer.Address())
	want.Iat = time.Now().Unix()
	token, err := signer.SignStatementV2(want)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token parts = %d", len(parts))
	}
	signatureAlias, ok := nonCanonicalPadBitAlias(parts[2])
	if !ok {
		t.Fatalf("65-byte signature segment unexpectedly has no unused pad bits: len=%d", len(parts[2]))
	}

	payloadWant, payloadParts := statementTokenWithPadBits(t, signer, want)
	payAlias, ok := nonCanonicalPadBitAlias(payloadParts[1])
	if !ok {
		t.Fatal("failed to construct payload segment with unused pad bits")
	}
	tests := []struct {
		name  string
		parts []string
		want  JWSStatementPayloadV2
	}{
		{"header CRLF", []string{parts[0][:1] + "\r\n" + parts[0][1:], parts[1], parts[2]}, want},
		{"payload CRLF", []string{parts[0], parts[1][:1] + "\n" + parts[1][1:], parts[2]}, want},
		{"signature CRLF", []string{parts[0], parts[1], parts[2][:1] + "\r\n" + parts[2][1:]}, want},
		{"payload unused pad bits", []string{payloadParts[0], payAlias, payloadParts[2]}, payloadWant},
		{"signature unused pad bits", []string{parts[0], parts[1], signatureAlias}, want},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mutated := strings.Join(tc.parts, ".")
			if _, err := validator.ValidateStatementV2(mutated, tc.want); err == nil || !strings.Contains(err.Error(), "canonical base64url") {
				t.Fatalf("ValidateStatementV2 = %v, want canonical base64url rejection", err)
			}
			if strings.Contains(tc.name, "payload") {
				if _, err := DecodeStatementV2Payload(mutated); err == nil || !strings.Contains(err.Error(), "canonical base64url") {
					t.Fatalf("DecodeStatementV2Payload = %v, want canonical base64url rejection", err)
				}
			}
		})
	}
}

func statementTokenWithPadBits(t *testing.T, signer *RelaySigner, base JWSStatementPayloadV2) (JWSStatementPayloadV2, []string) {
	t.Helper()
	for i := 0; i < 4; i++ {
		candidate := base
		candidate.StatementID += strings.Repeat("x", i)
		token, err := signer.SignStatementV2(candidate)
		if err != nil {
			t.Fatal(err)
		}
		parts := strings.Split(token, ".")
		if _, ok := nonCanonicalPadBitAlias(parts[1]); ok {
			return candidate, parts
		}
	}
	t.Fatal("could not construct payload base64url segment with unused pad bits")
	return JWSStatementPayloadV2{}, nil
}

func nonCanonicalPadBitAlias(segment string) (string, bool) {
	remainder := len(segment) % 4
	if remainder != 2 && remainder != 3 {
		return "", false
	}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	idx := strings.IndexByte(alphabet, segment[len(segment)-1])
	if idx < 0 {
		return "", false
	}
	unusedBits := 4
	if remainder == 3 {
		unusedBits = 2
	}
	mask := (1 << unusedBits) - 1
	if idx&mask != 0 {
		return "", false
	}
	alias := idx | 1
	return segment[:len(segment)-1] + string(alphabet[alias]), true
}

func TestRelaySigner_AllCompactJWSLanesUseCanonicalProtectedHeader(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	statement := statementV2Fixture(signer.Address())
	statement.Iat = time.Now().Unix()
	queryToken, err := signer.SignToken("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	statementToken, err := signer.SignStatementV2(statement)
	if err != nil {
		t.Fatal(err)
	}
	peerToken, err := signer.SignPeerLogin("peer-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for lane, token := range map[string]string{
		"query":     queryToken,
		"statement": statementToken,
		"peer":      peerToken,
	} {
		t.Run(lane, func(t *testing.T) {
			parts := strings.Split(token, ".")
			if len(parts) != 3 {
				t.Fatalf("not compact JWS: %q", token)
			}
			raw, err := base64.RawURLEncoding.DecodeString(parts[0])
			if err != nil {
				t.Fatal(err)
			}
			var header JWSHeader
			if err := json.Unmarshal(raw, &header); err != nil {
				t.Fatal(err)
			}
			if header != (JWSHeader{Alg: "ES256K", Typ: "JWT"}) {
				t.Fatalf("protected header = %+v", header)
			}
		})
	}
}

func signStatementV2WithHeader(t *testing.T, header JWSHeader, payload JWSStatementPayloadV2) string {
	t.Helper()
	headerJSON, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	return signStatementV2WithRawHeader(t, string(headerJSON), payload)
}

func signStatementV2WithRawHeader(t *testing.T, rawHeader string, payload JWSStatementPayloadV2) string {
	t.Helper()
	payload.Purpose = StatementPurposeV2
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	input := base64.RawURLEncoding.EncodeToString([]byte(rawHeader)) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON)
	key, err := crypto.HexToECDSA(statementV2TestKey)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := crypto.Sign(keccak256([]byte(input)), key)
	if err != nil {
		t.Fatal(err)
	}
	sig[64] += 27
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestStatementV2_RecoveredAddressNotInAllowlistRejects(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	other, _ := NewRelaySigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	validator := NewEthValidator([]string{other.Address()}, time.Minute, true, false, "", nil)
	want := statementV2Fixture(signer.Address())
	want.Iat = time.Now().Unix()
	token, _ := signer.SignStatementV2(want)
	if _, err := validator.ValidateStatementV2(token, want); err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("expected allowlist rejection, got %v", err)
	}
}

func TestStatementV2_ExpiredIatRejects(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := statementV2Fixture(signer.Address())
	want.Iat = time.Now().Add(-2 * time.Minute).Unix()
	token, _ := signer.SignStatementV2(want)
	if _, err := validator.ValidateStatementV2(token, want); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expiry rejection, got %v", err)
	}
}

func TestStatementPayloadV2Mismatch_IgnoresIat(t *testing.T) {
	a := statementV2Fixture("0xabc")
	b := a
	b.Iat = a.Iat + 100
	if got := StatementPayloadV2Mismatch(a, b); got != "" {
		t.Fatalf("iat must not count as a mismatch, got %q", got)
	}
	b.PayloadLength++
	if got := StatementPayloadV2Mismatch(a, b); got != "payload_length" {
		t.Fatalf("mismatch = %q, want payload_length", got)
	}
}
