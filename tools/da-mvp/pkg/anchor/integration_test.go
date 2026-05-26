//go:build integration

package anchor

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

// Pre-req for these tests:
//   1. anvil running on $DAMVP_ANVIL_RPC (default http://localhost:8545)
//   2. DAAnchor deployed; address in $DAMVP_ANCHOR_ADDR
//   3. private key in $DAMVP_ANCHOR_KEY (anvil deterministic account 0 works)

func envOrSkip(t *testing.T, k, fallback string) string {
	v := os.Getenv(k)
	if v == "" {
		v = fallback
	}
	if v == "" {
		t.Skipf("set %s to run", k)
	}
	return v
}

func TestPublishFirstChunkAndQuery(t *testing.T) {
	rpc := envOrSkip(t, "DAMVP_ANVIL_RPC", "http://localhost:8545")
	addr := envOrSkip(t, "DAMVP_ANCHOR_ADDR", "")
	keyHex := envOrSkip(t, "DAMVP_ANCHOR_KEY", "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")

	key, err := crypto.HexToECDSA(keyHex)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := NewClient(ctx, rpc, addr, key)
	if err != nil {
		t.Fatal(err)
	}

	dbId := [32]byte{1, 2, 3}
	tableId := [32]byte{4, 5, 6}
	params := PublishParams{
		DBID: dbId, TableID: tableId,
		DAHeight: 100, DACommitment: [32]byte{0xaa},
		BlobSize: 999, BlobHash: [32]byte{0xbb},
		SchemaHash: [32]byte{0xcc}, ChunkCount: 1,
	}
	seq1, err := c.PublishFirstChunk(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	seq2, err := c.PublishFirstChunk(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if seq2 != seq1+1 {
		t.Errorf("expected monotonic seq, got %d then %d", seq1, seq2)
	}
	events, err := c.QueryPublished(ctx, dbId, tableId, seq1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 2 {
		t.Errorf("expected ≥ 2 events, got %d", len(events))
	}
}
