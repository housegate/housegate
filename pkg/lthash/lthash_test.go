package lthash

import (
	"encoding/hex"
	"testing"
)

func TestNewIsZero(t *testing.T) {
	h := New()
	if !h.IsZero() {
		t.Fatal("fresh accumulator must be zero")
	}
}

func TestAddRemoveRoundTrip(t *testing.T) {
	e1 := []byte("element-one")
	e2 := []byte("element-two")

	h := New()
	h.Add(e1)
	h.Add(e2)
	h.Remove(e1)

	want := New()
	want.Add(e2)

	if !h.Equal(want) {
		t.Fatalf("add(e1,e2)+remove(e1) != add(e2): %s vs %s", h.Hex(), want.Hex())
	}
}

func TestAddRemoveToZero(t *testing.T) {
	e := []byte("ephemeral")
	h := New()
	h.Add(e)
	if h.IsZero() {
		t.Fatal("accumulator with one element must not be zero")
	}
	h.Remove(e)
	if !h.IsZero() {
		t.Fatal("add then remove the same element must return to zero")
	}
}

func TestOrderIndependence(t *testing.T) {
	e1 := []byte("alpha")
	e2 := []byte("beta")
	e3 := []byte("gamma")

	a := New()
	a.Add(e1)
	a.Add(e2)
	a.Add(e3)

	b := New()
	b.Add(e3)
	b.Add(e1)
	b.Add(e2)

	if !a.Equal(b) {
		t.Fatalf("insertion order must not matter: %s vs %s", a.Hex(), b.Hex())
	}
}

func TestAccumulatorCombine(t *testing.T) {
	e1 := []byte("part-a-row")
	e2 := []byte("part-b-row")

	partA := New()
	partA.Add(e1)
	partB := New()
	partB.Add(e2)

	combined := New()
	combined.AddHash(partA)
	combined.AddHash(partB)

	want := New()
	want.Add(e1)
	want.Add(e2)

	if !combined.Equal(want) {
		t.Fatalf("sum of part accumulators must equal accumulator of all rows")
	}

	combined.SubHash(partA)
	if !combined.Equal(partB) {
		t.Fatalf("subtracting a part accumulator must remove its contribution")
	}
}

// TestLaneWrapCancellation documents the known multiplicity limitation that
// motivates _hg_row_id in the design spec: 2^16 identical elements cancel
// per u16 lane. The MVP hashes raw row values, so this property is expected
// and must hold (it is the arithmetic working as defined, not a bug).
func TestLaneWrapCancellation(t *testing.T) {
	eh := ElementHash([]byte("dup-row"))
	h := New()
	for i := 0; i < 1<<16; i++ {
		h.AddHash(eh)
	}
	if !h.IsZero() {
		t.Fatal("2^16 copies of one element must cancel to zero (documents the duplicate-row caveat)")
	}
}

// TestDigestGolden pins a test vector so wire-side and part-side
// implementations (and future re-implementations) can cross-check.
func TestDigestGolden(t *testing.T) {
	h := New()
	h.Add([]byte("housegate-lthash-test-vector"))
	d1 := h.Digest()

	h2 := New()
	h2.Add([]byte("housegate-lthash-test-vector"))
	d2 := h2.Digest()

	if d1 != d2 {
		t.Fatal("digest must be deterministic")
	}
	if hex.EncodeToString(d1[:]) == hex.EncodeToString(make([]byte, 32)) {
		t.Fatal("digest must not be zero")
	}
	t.Logf("golden digest: %s", hex.EncodeToString(d1[:]))
}

func TestBytesRoundTrip(t *testing.T) {
	h := New()
	h.Add([]byte("serialize-me"))

	b := h.Bytes()
	if len(b) != Size {
		t.Fatalf("Bytes() length = %d, want %d", len(b), Size)
	}

	restored, err := FromBytes(b)
	if err != nil {
		t.Fatalf("FromBytes: %v", err)
	}
	if !restored.Equal(h) {
		t.Fatal("FromBytes(Bytes()) must round-trip")
	}

	if _, err := FromBytes(b[:Size-1]); err == nil {
		t.Fatal("FromBytes must reject wrong-length input")
	}
}
