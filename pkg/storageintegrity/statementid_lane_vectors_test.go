package storageintegrity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestStatementIDGrammarMatchesTheLaneVectors checks the one statement-id
// grammar against the shared lane vectors: every accepted vector's signed flat
// id parses to its structured fields and renders back byte-identically.
func TestStatementIDGrammarMatchesTheLaneVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "auth", "testdata", "statement_jws_v2_lanes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Vectors []struct {
			Name          string `json:"name"`
			Expect        string `json:"expect"`
			ClientAccount string `json:"client_account"`
			ClientLane    string `json:"client_lane"`
			ClientSeq     uint64 `json:"client_seq"`
			ClientNonce   string `json:"client_nonce"`
			Payload       struct {
				StatementID string `json:"statement_id"`
			} `json:"payload"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	for _, v := range file.Vectors {
		structured := StatementID{Account: v.ClientAccount, Lane: v.ClientLane, Seq: v.ClientSeq, Nonce: v.ClientNonce}
		parsed, err := ParseStatementID(v.Payload.StatementID)
		if err != nil {
			t.Fatalf("%s: %v", v.Name, err)
		}
		if parsed.Flat() != v.Payload.StatementID {
			t.Fatalf("%s: Flat(Parse(x)) != x", v.Name)
		}
		if (v.Expect == "accept") != (parsed == structured) {
			t.Fatalf("%s: parsed %+v vs structured %+v disagrees with expect=%s", v.Name, parsed, structured, v.Expect)
		}
	}
}
