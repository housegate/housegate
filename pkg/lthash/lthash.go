// Package lthash implements the lattice-hash content commitment used by the
// storage-network data-integrity verification layer (see
// docs/superpowers/specs/2026-06-10-multi-replica-trust-design.md §5).
//
// Configuration follows Solana SIMD-0215: each element is expanded with
// BLAKE3-XOF to 2048 bytes interpreted as 1024 little-endian u16 lanes;
// accumulators combine by lane-wise wrapping addition and remove by
// lane-wise wrapping subtraction. The full 2048-byte accumulator is the
// arithmetic object; Digest() is a display/anchoring digest only.
//
// MVP caveat: callers in this package hash raw elements. Per the design
// spec, identical elements cancel modulo 2^16 per lane — production use
// requires unique row-instance identity (_hg_row_id) inside the element.
package lthash

import (
	"errors"

	"github.com/zeebo/blake3"
)

const (
	// Size is the accumulator size in bytes.
	Size = 2048
	// lanes is the number of u16 lanes.
	lanes = Size / 2

	xofDomain = "housegate-lthash-v1"
)

// AccumulatorProfile is the versioned XOF domain used by ElementHash to expand
// an element into lanes. Exported alongside CanonicalRowProfile so callers that
// cache RowHash results bind their key to both the row-encoding and the
// accumulator profile (a change to either would change the hash bytes).
func AccumulatorProfile() string { return xofDomain }

// RowHashVersion binds both the row-encoding profile (CanonicalRowProfile) and
// the accumulator profile (AccumulatorProfile) into a single version token.
// It is the value a part-LtHash cache must include in its key so that a bump to
// either profile transparently invalidates every cached entry.
func RowHashVersion() string { return canonicalDomain + "\x00" + xofDomain }

// Hash is an LtHash accumulator: 1024 little-endian u16 lanes.
type Hash struct {
	lane [lanes]uint16
}

// New returns a zero accumulator.
func New() *Hash {
	return &Hash{}
}

// ElementHash expands one element to its lane representation. Adding the
// result to an accumulator via AddHash is equivalent to Add(element).
func ElementHash(element []byte) *Hash {
	h := blake3.New()
	_, _ = h.WriteString(xofDomain)
	_, _ = h.Write(element)
	var buf [Size]byte
	d := h.Digest()
	_, _ = d.Read(buf[:])

	out := &Hash{}
	for i := 0; i < lanes; i++ {
		out.lane[i] = uint16(buf[2*i]) | uint16(buf[2*i+1])<<8
	}
	return out
}

// Add mixes one element into the accumulator.
func (h *Hash) Add(element []byte) {
	h.AddHash(ElementHash(element))
}

// Remove subtracts one element from the accumulator.
func (h *Hash) Remove(element []byte) {
	h.SubHash(ElementHash(element))
}

// AddHash adds another accumulator lane-wise (wrapping).
func (h *Hash) AddHash(other *Hash) {
	for i := 0; i < lanes; i++ {
		h.lane[i] += other.lane[i]
	}
}

// SubHash subtracts another accumulator lane-wise (wrapping).
func (h *Hash) SubHash(other *Hash) {
	for i := 0; i < lanes; i++ {
		h.lane[i] -= other.lane[i]
	}
}

// Equal reports whether two accumulators are lane-wise identical.
func (h *Hash) Equal(other *Hash) bool {
	return h.lane == other.lane
}

// IsZero reports whether the accumulator is the zero element.
func (h *Hash) IsZero() bool {
	return h.lane == [lanes]uint16{}
}

// Bytes serializes the accumulator as 2048 little-endian bytes.
func (h *Hash) Bytes() []byte {
	out := make([]byte, Size)
	for i := 0; i < lanes; i++ {
		out[2*i] = byte(h.lane[i])
		out[2*i+1] = byte(h.lane[i] >> 8)
	}
	return out
}

// FromBytes restores an accumulator serialized by Bytes.
func FromBytes(b []byte) (*Hash, error) {
	if len(b) != Size {
		return nil, errors.New("lthash: accumulator must be exactly 2048 bytes")
	}
	h := &Hash{}
	for i := 0; i < lanes; i++ {
		h.lane[i] = uint16(b[2*i]) | uint16(b[2*i+1])<<8
	}
	return h, nil
}

// Digest returns the 32-byte display/anchoring digest of the accumulator.
// It is never an arithmetic object.
func (h *Hash) Digest() [32]byte {
	return blake3.Sum256(h.Bytes())
}

// Hex returns the hex form of Digest for logs.
func (h *Hash) Hex() string {
	d := h.Digest()
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 64)
	for i, b := range d {
		out[2*i] = hexdigits[b>>4]
		out[2*i+1] = hexdigits[b&0x0f]
	}
	return string(out)
}
