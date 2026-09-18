package auth

import (
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
)

const (
	schemaArtifactDigestVector = "0x98823680bbaef323efe77b75a673c0eafed787934877a5622a8556d7c38e35ce"
	schemaAuthorityAddress     = "0x8fd379246834eac74b8419ffda202cf8051f7a03"
	// Filled from the independently checked literal payload and RFC6979
	// secp256k1 signature, then frozen byte-for-byte.
	schemaCertificateHeaderVector    = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ"
	schemaCertificatePayloadVector   = "eyJwdXJwb3NlIjoiaG91c2VnYXRlLXNuYXBzaG90LXF1ZXJ5LXNjaGVtYS12MSIsInZlcnNpb24iOjEsImFydGlmYWN0X2RpZ2VzdCI6IjB4OTg4MjM2ODBiYmFlZjMyM2VmZTc3Yjc1YTY3M2MwZWFmZWQ3ODc5MzQ4NzdhNTYyMmE4NTU2ZDdjMzhlMzVjZSJ9"
	schemaCertificateSignatureVector = "x-DUMn_fv8b_-7zCbzkDCjtVG5XsvWhOJJBUt_AuFJoEXX5llf7gRTXNvgddrUDLzqvEpumQ7V34m-KHrXWCcxw"
)

func schemaCertificateTokenVector() string {
	return schemaCertificateHeaderVector + "." + schemaCertificatePayloadVector + "." + schemaCertificateSignatureVector
}

func TestSnapshotSchemaCertificateFrozenVector(t *testing.T) {
	signer, err := NewSnapshotSchemaCertificateSigner(statementV2TestKey)
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.SignSnapshotSchemaCertificateV1(schemaArtifactDigestVector)
	if err != nil {
		t.Fatal(err)
	}
	if token != schemaCertificateTokenVector() {
		t.Fatalf("certificate token changed:\n%s", token)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatal("literal token is not compact JWS")
	}
	header, _ := base64.RawURLEncoding.DecodeString(parts[0])
	signedPayload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	signature, _ := base64.RawURLEncoding.DecodeString(parts[2])
	if string(header) != `{"alg":"ES256K","typ":"JWT"}` || len(signature) != 65 || signature[64] < 27 || signature[64] > 28 {
		t.Fatal("literal header or signature encoding changed")
	}
	hasher := sha3.NewLegacyKeccak256()
	_, _ = hasher.Write([]byte(parts[0] + "." + parts[1]))
	hash := hasher.Sum(nil)
	recoverySignature := append([]byte{}, signature...)
	recoverySignature[64] -= 27
	pub, err := crypto.SigToPub(hash, recoverySignature)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.ToLower(crypto.PubkeyToAddress(*pub).Hex()); got != schemaAuthorityAddress {
		t.Fatalf("independent recovered address=%s", got)
	}
	payload, err := DecodeSnapshotSchemaCertificatePayloadV1(token)
	if err != nil {
		t.Fatal(err)
	}
	want := SnapshotSchemaCertificatePayloadV1{
		Purpose:        SnapshotSchemaCertificatePurposeV1,
		Version:        1,
		ArtifactDigest: schemaArtifactDigestVector,
	}
	if payload != want {
		t.Fatalf("payload=%+v want=%+v", payload, want)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	const payloadJSON = `{"purpose":"housegate-snapshot-query-schema-v1","version":1,"artifact_digest":"0x98823680bbaef323efe77b75a673c0eafed787934877a5622a8556d7c38e35ce"}`
	if string(raw) != payloadJSON {
		t.Fatalf("payload bytes=%s", raw)
	}
	if string(signedPayload) != payloadJSON {
		t.Fatalf("signed payload bytes=%s", signedPayload)
	}
	address, err := VerifySnapshotSchemaCertificateV1(token, schemaArtifactDigestVector)
	if err != nil || address != schemaAuthorityAddress || address != signer.Address() {
		t.Fatalf("verify address=%s err=%v", address, err)
	}
}

func TestSnapshotSchemaCertificateStrictPayloadAndHeader(t *testing.T) {
	signer, _ := NewSnapshotSchemaCertificateSigner(statementV2TestKey)
	token, err := signer.SignSnapshotSchemaCertificateV1(schemaArtifactDigestVector)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	payloadRaw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	canonicalPayload := string(payloadRaw)
	for name, payload := range map[string]string{
		"wrong_purpose": strings.Replace(canonicalPayload, SnapshotSchemaCertificatePurposeV1, "other", 1),
		"wrong_version": strings.Replace(canonicalPayload, `"version":1`, `"version":2`, 1),
		"bad_digest":    strings.Replace(canonicalPayload, schemaArtifactDigestVector, "0x12", 1),
		"unknown":       strings.Replace(canonicalPayload, `{"purpose":`, `{"iat":1,"purpose":`, 1),
		"duplicate":     strings.Replace(canonicalPayload, `"version":1`, `"version":1,"version":1`, 1),
		"missing":       strings.Replace(canonicalPayload, `"version":1,`, "", 1),
		"null":          strings.Replace(canonicalPayload, `"artifact_digest":"`+schemaArtifactDigestVector+`"`, `"artifact_digest":null`, 1),
		"reordered":     strings.Replace(canonicalPayload, `{"purpose":"`+SnapshotSchemaCertificatePurposeV1+`","version":1`, `{"version":1,"purpose":"`+SnapshotSchemaCertificatePurposeV1+`"`, 1),
		"escaped":       strings.Replace(canonicalPayload, "snapshot-query", `snapshot\u002dquery`, 1),
	} {
		t.Run("payload/"+name, func(t *testing.T) {
			bad := statementV3SignRaw(t, canonicalJWSProtectedHeader, payload)
			if _, err := VerifySnapshotSchemaCertificateV1(bad, schemaArtifactDigestVector); err == nil {
				t.Fatal("malformed payload accepted")
			}
		})
	}
	for name, header := range map[string]string{
		"whitespace": `{ "alg":"ES256K","typ":"JWT"}`,
		"order":      `{"typ":"JWT","alg":"ES256K"}`,
		"algorithm":  `{"alg":"none","typ":"JWT"}`,
		"duplicate":  `{"alg":"ES256K","alg":"ES256K","typ":"JWT"}`,
		"missing":    `{"alg":"ES256K"}`,
		"kid":        `{"alg":"ES256K","typ":"JWT","kid":"authority"}`,
		"jwk":        `{"alg":"ES256K","typ":"JWT","jwk":{}}`,
		"url":        `{"alg":"ES256K","typ":"JWT","x5u":"https://example.test/cert"}`,
	} {
		t.Run("header/"+name, func(t *testing.T) {
			bad := statementV3SignRaw(t, header, canonicalPayload)
			if _, err := VerifySnapshotSchemaCertificateV1(bad, schemaArtifactDigestVector); err == nil {
				t.Fatal("alternate protected header accepted")
			}
		})
	}
	for index, name := range []string{"header", "payload", "signature"} {
		changed := append([]string{}, parts...)
		changed[index] += "="
		if _, err := VerifySnapshotSchemaCertificateV1(strings.Join(changed, "."), schemaArtifactDigestVector); err == nil || !strings.Contains(err.Error(), "base64url") {
			t.Fatalf("%s noncanonical encoding accepted: %v", name, err)
		}
	}
}

func TestSnapshotSchemaCertificateSignatureMalleabilityRefused(t *testing.T) {
	signer, _ := NewSnapshotSchemaCertificateSigner(statementV2TestKey)
	token, _ := signer.SignSnapshotSchemaCertificateV1(schemaArtifactDigestVector)
	parts := strings.Split(token, ".")
	signature, _ := base64.RawURLEncoding.DecodeString(parts[2])

	highS := append([]byte{}, signature...)
	s := new(big.Int).SetBytes(highS[32:64])
	s.Sub(crypto.S256().Params().N, s).FillBytes(highS[32:64])
	if highS[64] == 27 {
		highS[64] = 28
	} else {
		highS[64] = 27
	}
	parts[2] = base64.RawURLEncoding.EncodeToString(highS)
	if _, err := VerifySnapshotSchemaCertificateV1(strings.Join(parts, "."), schemaArtifactDigestVector); err == nil || !strings.Contains(err.Error(), "high-S") {
		t.Fatalf("high-S signature accepted: %v", err)
	}

	for _, v := range []byte{0, 1, 26, 29, 255} {
		bad := append([]byte{}, signature...)
		bad[64] = v
		parts[2] = base64.RawURLEncoding.EncodeToString(bad)
		if _, err := VerifySnapshotSchemaCertificateV1(strings.Join(parts, "."), schemaArtifactDigestVector); err == nil {
			t.Fatalf("bad V=%d accepted", v)
		}
	}
}

func TestSnapshotSchemaCertificateBindsDigestWithoutAgeOrRolePolicy(t *testing.T) {
	signer, _ := NewSnapshotSchemaCertificateSigner(statementV2TestKey)
	token, _ := signer.SignSnapshotSchemaCertificateV1(schemaArtifactDigestVector)
	if _, err := VerifySnapshotSchemaCertificateV1(token, "0x"+strings.Repeat("f", 64)); err == nil || !strings.Contains(err.Error(), "artifact_digest") {
		t.Fatalf("wrong digest accepted: %v", err)
	}
	other, _ := NewSnapshotSchemaCertificateSigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	otherToken, _ := other.SignSnapshotSchemaCertificateV1(schemaArtifactDigestVector)
	address, err := VerifySnapshotSchemaCertificateV1(otherToken, schemaArtifactDigestVector)
	if err != nil || address != other.Address() {
		t.Fatalf("pure historical verification consulted a current role: %s %v", address, err)
	}
	if strings.Contains(token, "iat") || strings.Contains(token, "exp") || strings.Contains(token, "aud") {
		t.Fatal("certificate unexpectedly carries an age or audience claim")
	}
}

func TestSnapshotSchemaCertificateRequiresExplicitDedicatedCredential(t *testing.T) {
	if _, err := NewSnapshotSchemaCertificateSigner("not-a-key"); err == nil {
		t.Fatal("malformed dedicated credential accepted")
	}
	signer, err := NewSnapshotSchemaCertificateSigner(statementV2TestKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, digest := range []string{"", "0x12", strings.Repeat("a", 64), "0x" + strings.Repeat("A", 64)} {
		if _, err := signer.SignSnapshotSchemaCertificateV1(digest); err == nil {
			t.Fatalf("malformed artifact digest %q signed", digest)
		}
	}
}
