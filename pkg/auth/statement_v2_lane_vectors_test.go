package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"
)

// statementV2LaneVector is one client-lane statement-JWS conformance vector
// (housegate spec 2026-10-09 D10, §7). client_* is the structured statement
// id a verifier rebuilds; payload is what the token signs. A verifier must
// accept exactly when the flat id rendered from client_* equals the signed
// payload.statement_id and the signature recovers signer_address.
type statementV2LaneVector struct {
	Name          string                `json:"name"`
	Expect        string                `json:"expect"`                  // "accept" | "reject"
	RejectReason  string                `json:"reject_reason,omitempty"` // "binding"
	RejectField   string                `json:"reject_field,omitempty"`  // "statement_id"
	ClientAccount string                `json:"client_account"`
	ClientLane    string                `json:"client_lane"`
	ClientSeq     uint64                `json:"client_seq"`
	ClientNonce   string                `json:"client_nonce"`
	Payload       JWSStatementPayloadV2 `json:"payload"`
	Token         string                `json:"token"`
}

type statementV2LaneVectorFile struct {
	SignerPrivateKeyHex string                  `json:"signer_private_key_hex"`
	SignerAddress       string                  `json:"signer_address"`
	Vectors             []statementV2LaneVector `json:"vectors"`
}

const statementV2LaneVectorPath = "testdata/statement_jws_v2_lanes.json"

// laneVectorFlat renders the flat statement id from structured fields; it is
// the rule every verifier must apply (legacy form when the lane is empty).
func laneVectorFlat(account, lane string, seq uint64, nonce string) string {
	if lane == "" {
		return account + ":" + strconv.FormatUint(seq, 10) + ":" + nonce
	}
	return account + ":" + lane + ":" + strconv.FormatUint(seq, 10) + ":" + nonce
}

// TestGenerateStatementV2LaneVectors rewrites the shared lane vector file when
// HOUSEGATE_WRITE_LANE_VECTORS=1. Regenerating it is a coordinated wire change:
// update SharedStatementLaneVectorsSHA256 and the arbiter's verbatim copy.
func TestGenerateStatementV2LaneVectors(t *testing.T) {
	if os.Getenv("HOUSEGATE_WRITE_LANE_VECTORS") != "1" {
		t.Skip("set HOUSEGATE_WRITE_LANE_VECTORS=1 to regenerate testdata/statement_jws_v2_lanes.json")
	}
	signer, err := NewRelaySigner(statementV2TestKey)
	if err != nil {
		t.Fatal(err)
	}
	account := signer.Address()
	sign := func(flat string) (JWSStatementPayloadV2, string) {
		p := statementV2Fixture(account)
		p.Purpose = StatementPurposeV2
		p.StatementID = flat
		token, err := signer.SignStatementV2(p)
		if err != nil {
			t.Fatal(err)
		}
		return p, token
	}
	const laneA, laneB, nonce = "5e1f0a2b7c9d3e4f", "ffffffffffffffff", "9f1c0000000000000000000000000001"
	lanedPayload, lanedToken := sign(laneVectorFlat(account, laneA, 42, nonce))
	otherPayload, otherToken := sign(laneVectorFlat(account, laneB, 1, nonce))
	legacyPayload, legacyToken := sign(laneVectorFlat(account, "", 42, nonce))
	vec := func(name, expect, lane string, seq uint64, p JWSStatementPayloadV2, token string) statementV2LaneVector {
		v := statementV2LaneVector{Name: name, Expect: expect, ClientAccount: account, ClientLane: lane, ClientSeq: seq, ClientNonce: nonce, Payload: p, Token: token}
		if expect == "reject" {
			v.RejectReason, v.RejectField = "binding", "statement_id"
		}
		return v
	}
	file := statementV2LaneVectorFile{SignerPrivateKeyHex: statementV2TestKey, SignerAddress: account, Vectors: []statementV2LaneVector{
		vec("laned_valid", "accept", laneA, 42, lanedPayload, lanedToken),
		vec("laned_valid_other_lane", "accept", laneB, 1, otherPayload, otherToken),
		vec("laned_lane_swapped", "reject", laneB, 42, lanedPayload, lanedToken),
		vec("laned_lane_stripped", "reject", "", 42, lanedPayload, lanedToken),
		vec("legacy_signed_presented_laned", "reject", laneA, 42, legacyPayload, legacyToken),
		vec("laned_seq_changed", "reject", laneA, 43, lanedPayload, lanedToken),
	}}
	b, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statementV2LaneVectorPath, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestStatementV2LaneVectors proves every vector against this package's
// validator with the expectation rebuilt from the structured fields.
func TestStatementV2LaneVectors(t *testing.T) {
	raw, err := os.ReadFile(statementV2LaneVectorPath)
	if err != nil {
		t.Fatalf("read lane vectors: %v", err)
	}
	var file statementV2LaneVectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Vectors) != 6 {
		t.Fatalf("expected exactly 6 lane vectors, got %d", len(file.Vectors))
	}
	validator := NewEthValidator([]string{file.SignerAddress}, 100*365*24*time.Hour, true, false, "", nil)
	for _, vec := range file.Vectors {
		t.Run(vec.Name, func(t *testing.T) {
			want := vec.Payload
			want.StatementID = laneVectorFlat(vec.ClientAccount, vec.ClientLane, vec.ClientSeq, vec.ClientNonce)
			_, err := validator.ValidateStatementV2(vec.Token, want)
			switch vec.Expect {
			case "accept":
				if err != nil || want.StatementID != vec.Payload.StatementID {
					t.Fatalf("expected accept: %v", err)
				}
			case "reject":
				if err == nil || err.Error() != "statement token binding mismatch on "+vec.RejectField {
					t.Fatalf("expected a %s binding reject, got %v", vec.RejectField, err)
				}
			default:
				t.Fatalf("unknown expect %q", vec.Expect)
			}
		})
	}
}

// TestSharedStatementLaneVectorsSHA256 is the cross-repo link for the lane
// vectors: the arbiter asserts its verbatim copy hashes to the same constant.
func TestSharedStatementLaneVectorsSHA256(t *testing.T) {
	raw, err := os.ReadFile(statementV2LaneVectorPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != SharedStatementLaneVectorsSHA256 {
		t.Fatalf("statement_jws_v2_lanes.json sha256 = %s, SharedStatementLaneVectorsSHA256 = %s\n"+
			"regenerating the lane vectors is a coordinated wire change: update the constant, copy the file into arbiter fsm/testdata, and cut both releases together", got, SharedStatementLaneVectorsSHA256)
	}
}
