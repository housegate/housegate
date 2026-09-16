package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

// Copied verbatim from arbiter-core bc2a08a3a199a12eeb59a0f8185110b5eeab487d,
// conformance/testdata/snapshot_query_identity_v1.json (also mirrored in
// arbiter-proto 5d992114012771284fded2ae6719d09f0b039055). Never regenerate here.
const statementV3IdentityPath = "testdata/snapshot_query_identity_v1.json"

type statementV3IdentityFixture struct {
	Account    string                    `json:"account"`
	Input      replay.SnapshotQueryInput `json:"input"`
	InputRoot  string                    `json:"input_root"`
	Identities []struct {
		Iat         int64  `json:"iat"`
		UserJWS     string `json:"user_jws"`
		UserJWSHash string `json:"user_jws_hash"`
	} `json:"identities"`
}

func statementV3Fixture(t *testing.T) statementV3IdentityFixture {
	t.Helper()
	raw, err := os.ReadFile(statementV3IdentityPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != "fce47e90a772cbe648c657844bb10d04567ba6f9196718d596ffd6f557c7a23f" {
		t.Fatal("immutable A1 identity fixture changed")
	}
	var fixture statementV3IdentityFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func statementV3Payload(t *testing.T) JWSStatementPayloadV3 {
	t.Helper()
	f := statementV3Fixture(t)
	return JWSStatementPayloadV3{Purpose: StatementPurposeV3, Iat: f.Identities[0].Iat, Binding: f.Input.Binding, InputRoot: f.InputRoot}
}

func TestStatementV3FixedIdentityVectors(t *testing.T) {
	f := statementV3Fixture(t)
	// Expected bindings must come from a validated complete input, not the token.
	root, err := replay.SnapshotQueryInputRoot(f.Input)
	if err != nil {
		t.Fatal(err)
	}
	if root != f.InputRoot {
		t.Fatalf("input root = %s, want %s", root, f.InputRoot)
	}
	signer, err := NewRelaySigner(statementV2TestKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Identities) != 2 {
		t.Fatal("expected both fixed signatures")
	}
	want := JWSStatementPayloadV3{Purpose: StatementPurposeV3, Binding: f.Input.Binding, InputRoot: root}
	for _, identity := range f.Identities {
		if replay.DigestString(identity.UserJWS) != identity.UserJWSHash {
			t.Fatal("original token identity changed")
		}
		account, err := VerifyStatementV3Signature(identity.UserJWS, want)
		if err != nil || account != f.Account {
			t.Fatalf("pure historical verification = %s, %v", account, err)
		}
		decoded, err := DecodeStatementV3Payload(identity.UserJWS)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Iat != identity.Iat {
			t.Fatal("iat changed")
		}
		want.Iat = identity.Iat
		token, err := signer.SignStatementV3(want)
		if err != nil || token != identity.UserJWS {
			t.Fatalf("signer must reproduce the unchanged A1 bytes: %v", err)
		}
	}
	if f.Identities[0].UserJWSHash == f.Identities[1].UserJWSHash {
		t.Fatal("distinct transport identities collapsed")
	}
}
