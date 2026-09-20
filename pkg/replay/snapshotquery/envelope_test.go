package snapshotquery

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/replay"
)

// The identity fixture is owned by pkg/auth (see its statement_v3_vectors_test.go)
// and exposed cross-package through the //pkg/auth:snapshot_query_identity_fixture
// filegroup. Never modify it here; it is a frozen, hash-pinned wire vector.
const envelopeIdentityFixturePath = "../../auth/testdata/snapshot_query_identity_v1.json"

type envelopeIdentityFixture struct {
	Account    string                    `json:"account"`
	Input      replay.SnapshotQueryInput `json:"input"`
	InputRoot  string                    `json:"input_root"`
	Identities []struct {
		Iat         int64  `json:"iat"`
		UserJWS     string `json:"user_jws"`
		UserJWSHash string `json:"user_jws_hash"`
	} `json:"identities"`
}

func loadEnvelopeIdentityFixture(t *testing.T) envelopeIdentityFixture {
	t.Helper()
	raw, err := os.ReadFile(envelopeIdentityFixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var f envelopeIdentityFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Identities) < 2 {
		t.Fatal("expected at least two fixed identities in the fixture")
	}
	return f
}

// TestVerifyEnvelope proves the happy path against both frozen signatures in
// the fixture: two different original JWS bytes over the same input/root.
func TestVerifyEnvelope(t *testing.T) {
	f := loadEnvelopeIdentityFixture(t)
	for i, identity := range f.Identities {
		envelope := replay.SnapshotQueryEnvelope{Input: f.Input, InputRoot: f.InputRoot, UserJWS: identity.UserJWS}
		account, err := VerifyEnvelope(envelope)
		if err != nil {
			t.Fatalf("identity %d: unexpected error: %v", i, err)
		}
		if account != f.Account {
			t.Fatalf("identity %d: account = %q, want %q", i, account, f.Account)
		}
	}
}

// tamperedStatementID alters Input.Binding.StatementID and re-derives the
// input root so the tamper is isolated to the signature-verification step:
// the recomputed root still matches the envelope's own InputRoot, but the
// original JWS was signed over the untampered binding.
func tamperedStatementID(t *testing.T, f envelopeIdentityFixture) replay.SnapshotQueryEnvelope {
	t.Helper()
	in := f.Input
	in.Binding.StatementID = in.Binding.StatementID + "-tampered"
	root, err := replay.SnapshotQueryInputRoot(in)
	if err != nil {
		t.Fatal(err)
	}
	return replay.SnapshotQueryEnvelope{Input: in, InputRoot: root, UserJWS: f.Identities[0].UserJWS}
}

// tamperedSQL alters Input.SQL and keeps Binding.SQLHash internally
// consistent with it (so ValidateSnapshotQueryInput's own sql_hash check
// still passes), but leaves the envelope's InputRoot at the fixture's
// original value so the recomputed overall root no longer matches it.
func tamperedSQL(f envelopeIdentityFixture) replay.SnapshotQueryEnvelope {
	in := f.Input
	in.SQL = in.SQL + " -- tampered"
	in.Binding.SQLHash = replay.DigestString(in.SQL)
	return replay.SnapshotQueryEnvelope{Input: in, InputRoot: f.InputRoot, UserJWS: f.Identities[0].UserJWS}
}

// withOtherClientAccount points Binding.ClientAccount at an address that no
// longer agrees with the statement_id's own embedded account prefix.
func withOtherClientAccount(f envelopeIdentityFixture) replay.SnapshotQueryEnvelope {
	in := f.Input
	in.Binding.ClientAccount = "0x1111111111111111111111111111111111111111"
	return replay.SnapshotQueryEnvelope{Input: in, InputRoot: f.InputRoot, UserJWS: f.Identities[0].UserJWS}
}

// flipOneHexDigit returns a copy of digest with its final character changed,
// so the result differs from the input by exactly one hex digit.
func flipOneHexDigit(t *testing.T, digest string) string {
	t.Helper()
	if digest == "" {
		t.Fatal("empty digest")
	}
	b := []byte(digest)
	i := len(b) - 1
	if b[i] == '0' {
		b[i] = '1'
	} else {
		b[i] = '0'
	}
	if string(b) == digest {
		t.Fatal("hex digit did not change")
	}
	return string(b)
}

// TestVerifyEnvelopeRefusals table-tests every refusal path named in
// VerifyEnvelope's contract. Every case but the last is reachable through the
// exported VerifyEnvelope with the real production signature verifier; the
// last case authenticates against a deliberately mismatched injected
// verifier because auth.VerifyStatementV3Signature's own internal binding
// check (see pkg/auth/statement_v3.go's StatementPayloadV3Mismatch) already
// guarantees the recovered account matches envelope.Input.Binding.ClientAccount
// whenever it reports success — so this last defensive step can only be
// exercised directly, not through envelope tampering alone.
func TestVerifyEnvelopeRefusals(t *testing.T) {
	f := loadEnvelopeIdentityFixture(t)
	mismatchedAccount := "0x1111111111111111111111111111111111111111"

	cases := map[string]struct {
		envelope replay.SnapshotQueryEnvelope
		verify   statementV3SignatureVerifier // non-nil overrides the production verifier
		wantStep string
	}{
		"empty user_jws": {
			envelope: replay.SnapshotQueryEnvelope{Input: f.Input, InputRoot: f.InputRoot, UserJWS: ""},
			wantStep: "verify statement signature",
		},
		"input_root altered by one hex digit": {
			envelope: replay.SnapshotQueryEnvelope{Input: f.Input, InputRoot: flipOneHexDigit(t, f.InputRoot), UserJWS: f.Identities[0].UserJWS},
			wantStep: "input_root mismatch",
		},
		"statement_id altered": {
			envelope: tamperedStatementID(t, f),
			wantStep: "verify statement signature",
		},
		"sql altered": {
			envelope: tamperedSQL(f),
			wantStep: "input_root mismatch",
		},
		"client_account set to another address": {
			envelope: withOtherClientAccount(f),
			wantStep: "validate complete input",
		},
		"client_account does not match recovered signer": {
			envelope: replay.SnapshotQueryEnvelope{Input: f.Input, InputRoot: f.InputRoot, UserJWS: f.Identities[0].UserJWS},
			verify: func(string, auth.JWSStatementPayloadV3) (string, error) {
				return mismatchedAccount, nil
			},
			wantStep: "client_account does not match signature",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var account string
			var err error
			if tc.verify == nil {
				account, err = VerifyEnvelope(tc.envelope)
			} else {
				account, _, err = verifyEnvelope(tc.envelope, tc.verify)
			}
			if err == nil {
				t.Fatalf("expected error, got account %q", account)
			}
			if account != "" {
				t.Fatalf("expected empty account on error, got %q", account)
			}
			if !strings.Contains(err.Error(), tc.wantStep) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantStep)
			}
		})
	}
}
