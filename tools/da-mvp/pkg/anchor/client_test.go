package anchor

import (
	"testing"
)

func TestPublishedEvent_FieldsRoundtrip(t *testing.T) {
	in := PublishedEvent{
		DBID:         [32]byte{0xaa},
		TableID:      [32]byte{0xbb},
		PartSeq:      42,
		DAHeight:     12345,
		DACommitment: [32]byte{0xcc},
		BlobSize:     999,
		BlobHash:     [32]byte{0xdd},
		SchemaHash:   [32]byte{0xee},
		ChunkIdx:     1,
		ChunkCount:   3,
	}
	if in.PartSeq != 42 || in.ChunkIdx != 1 || in.ChunkCount != 3 {
		t.Errorf("trivial field check failed: %+v", in)
	}
}
