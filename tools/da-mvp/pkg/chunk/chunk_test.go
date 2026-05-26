package chunk

import (
	"bytes"
	"testing"
)

func TestSplit_Empty_OneEmptyChunk(t *testing.T) {
	got := Split(nil)
	if len(got) != 1 || len(got[0]) != 0 {
		t.Errorf("expected [[]], got %d chunks", len(got))
	}
}

func TestSplit_SmallerThanMax_SingleChunk(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 1000)
	got := Split(data)
	if len(got) != 1 || len(got[0]) != 1000 {
		t.Errorf("expected single 1000-byte chunk, got %d chunks", len(got))
	}
}

func TestSplit_ExactlyMaxChunkSize_SingleChunk(t *testing.T) {
	data := bytes.Repeat([]byte("x"), MaxChunkSize)
	got := Split(data)
	if len(got) != 1 || len(got[0]) != MaxChunkSize {
		t.Errorf("expected single MaxChunkSize chunk, got %d chunks (last=%d)", len(got), len(got[len(got)-1]))
	}
}

func TestSplit_OneByteOver_TwoChunks(t *testing.T) {
	data := bytes.Repeat([]byte("x"), MaxChunkSize+1)
	got := Split(data)
	if len(got) != 2 || len(got[0]) != MaxChunkSize || len(got[1]) != 1 {
		t.Errorf("expected [MaxChunkSize, 1], got %d chunks", len(got))
	}
}

func TestRoundtrip(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefgh"), 250_000) // 2 MiB
	got := Assemble(Split(data))
	if !bytes.Equal(got, data) {
		t.Error("roundtrip lost bytes")
	}
}
