package lthash

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// canonicalDomain versions the MVP row-encoding profile. It deliberately
// differs from the spec's production profile ("housegate-row-v1", which
// uses stable column IDs and _hg_row_id): the MVP profile identifies
// columns by name and hashes raw row values.
const canonicalDomain = "housegate-row-mvp-v0"

// Column describes one column of a row to be encoded: the column name and
// the declared ClickHouse type name (e.g. "UInt64", "String", "DateTime").
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// value kind tags keep differently-typed values from colliding byte-wise.
// They are append-only: renumbering any of them silently rewrites every
// historical row hash.
const (
	KindInt byte = iota + 1
	KindUint
	KindFloat
	KindString
	KindBool
	KindTime
	// KindNull is the whole value field of a NULL in a Nullable(T) column. A
	// non-NULL value of that column encodes exactly as T would.
	KindNull
	// KindUUID precedes the 16 bytes of a UUID in RFC 4122 byte order (the
	// order uuid.UUID holds and ClickHouse's toString renders). It is distinct
	// from KindString so a UUID never shares an encoding with a 16-byte string.
	KindUUID
)

// Unexported aliases kept so this file reads as it always has. The exported
// names exist so the column-type authority in pkg/replay/payloadexec can
// declare which tag each admitted type encodes under, and so that claim is
// checkable rather than decorative.
const (
	kindInt    = KindInt
	kindUint   = KindUint
	kindFloat  = KindFloat
	kindString = KindString
	kindBool   = KindBool
	kindTime   = KindTime
	kindNull   = KindNull
	kindUUID   = KindUUID
)

// EncodeRow produces the canonical element bytes for one row under the MVP
// profile: domain ‖ table ‖ sorted-by-name [(name, type, value)], every
// field length-framed. Wire-side and part-side encoders must both go
// through this function for digests to be comparable.
func EncodeRow(table string, cols []Column, values []any) ([]byte, error) {
	if len(cols) != len(values) {
		return nil, fmt.Errorf("lthash: %d columns but %d values", len(cols), len(values))
	}

	idx := make([]int, len(cols))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool { return cols[idx[a]].Name < cols[idx[b]].Name })

	var out []byte
	out = appendFramed(out, []byte(canonicalDomain))
	out = appendFramed(out, []byte(table))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(cols)))

	for _, i := range idx {
		v, err := encodeValue(cols[i], values[i])
		if err != nil {
			return nil, err
		}
		out = appendFramed(out, []byte(cols[i].Name))
		out = appendFramed(out, []byte(cols[i].Type))
		out = appendFramed(out, v)
	}
	return out, nil
}

// RowHash is EncodeRow followed by ElementHash.
func RowHash(table string, cols []Column, values []any) (*Hash, error) {
	b, err := EncodeRow(table, cols, values)
	if err != nil {
		return nil, err
	}
	return ElementHash(b), nil
}

func appendFramed(dst, field []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(field)))
	return append(dst, field...)
}

// nullablePrefix opens a Nullable(T) declaration.
const nullablePrefix = "Nullable("

// valueType is the declared type a value encodes under: T for Nullable(T),
// the declaration itself otherwise. Every type-dependent branch below reads
// it, so a non-NULL value of Nullable(T) encodes exactly as T.
func valueType(declared string) (string, bool) {
	if strings.HasPrefix(declared, nullablePrefix) && strings.HasSuffix(declared, ")") {
		return declared[len(nullablePrefix) : len(declared)-1], true
	}
	return declared, false
}

// encodeValue canonicalizes one value: a 1-byte kind tag followed by a
// fixed-width or framed payload. Integers carry their declared width via
// the byte length; floats canonicalize NaN to one pattern and -0.0 to
// +0.0; times canonicalize to the absolute instant.
func encodeValue(col Column, v any) ([]byte, error) {
	inner, nullable := valueType(col.Type)
	if v == nil {
		// Only a Nullable(T) column may carry NULL; a nil anywhere else is a
		// materializer bug and must not hash as a value.
		if !nullable {
			return nil, fmt.Errorf("lthash: NULL value for non-Nullable column %q (%s)", col.Name, col.Type)
		}
		return []byte{kindNull}, nil
	}
	col = Column{Name: col.Name, Type: inner}
	switch x := v.(type) {
	case int8:
		return appendInt(kindInt, uint64(uint8(x)), 1), nil
	case int16:
		return appendInt(kindInt, uint64(uint16(x)), 2), nil
	case int32:
		return appendInt(kindInt, uint64(uint32(x)), 4), nil
	case int64:
		return appendInt(kindInt, uint64(x), 8), nil
	case uint8:
		return appendInt(kindUint, uint64(x), 1), nil
	case uint16:
		return appendInt(kindUint, uint64(x), 2), nil
	case uint32:
		return appendInt(kindUint, uint64(x), 4), nil
	case uint64:
		return appendInt(kindUint, x, 8), nil
	case float32:
		return encodeFloat(uint64(canonicalFloat32Bits(x)), 4), nil
	case float64:
		return encodeFloat(canonicalFloat64Bits(x), 8), nil
	case string:
		return append([]byte{kindString}, x...), nil
	case []byte:
		return append([]byte{kindString}, x...), nil
	case bool:
		if x {
			return []byte{kindBool, 1}, nil
		}
		return []byte{kindBool, 0}, nil
	case time.Time:
		return encodeTime(col, x), nil
	case uuid.UUID:
		return append([]byte{kindUUID}, x[:]...), nil
	case *big.Int:
		return encodeWideInt(col, x)
	default:
		return nil, fmt.Errorf("lthash: unsupported value type %T for column %q (%s)", v, col.Name, col.Type)
	}
}

// wideIntWidths maps the wide integer declarations to their byte width and
// signedness. They encode under kindInt/kindUint like the narrow integers,
// as fixed-width little-endian two's complement; the 16- and 32-byte widths
// cannot collide with the 1-8 byte ones.
var wideIntWidths = map[string]struct {
	width  int
	signed bool
}{
	"Int128": {16, true}, "Int256": {32, true},
	"UInt128": {16, false}, "UInt256": {32, false},
}

func encodeWideInt(col Column, x *big.Int) ([]byte, error) {
	if x == nil {
		return nil, fmt.Errorf("lthash: nil *big.Int for column %q (%s)", col.Name, col.Type)
	}
	w, ok := wideIntWidths[col.Type]
	if !ok {
		return nil, fmt.Errorf("lthash: *big.Int value for column %q of non-wide type %s", col.Name, col.Type)
	}
	bits := uint(8 * w.width)
	kind := kindUint
	if w.signed {
		kind = kindInt
		limit := new(big.Int).Lsh(big.NewInt(1), bits-1)
		if x.Cmp(new(big.Int).Neg(limit)) < 0 || x.Cmp(limit) >= 0 {
			return nil, fmt.Errorf("lthash: value out of range for column %q (%s)", col.Name, col.Type)
		}
	} else if x.Sign() < 0 || x.BitLen() > int(bits) {
		return nil, fmt.Errorf("lthash: value out of range for column %q (%s)", col.Name, col.Type)
	}
	twos := new(big.Int).Set(x)
	if twos.Sign() < 0 {
		twos.Add(twos, new(big.Int).Lsh(big.NewInt(1), bits))
	}
	be := twos.FillBytes(make([]byte, w.width))
	out := make([]byte, 1, 1+w.width)
	out[0] = kind
	for i := len(be) - 1; i >= 0; i-- {
		out = append(out, be[i])
	}
	return out, nil
}

func appendInt(kind byte, bits uint64, width int) []byte {
	out := make([]byte, 1, 1+width)
	out[0] = kind
	for i := 0; i < width; i++ {
		out = append(out, byte(bits>>(8*i)))
	}
	return out
}

func encodeFloat(bits uint64, width int) []byte {
	return appendInt(kindFloat, bits, width)
}

func canonicalFloat64Bits(f float64) uint64 {
	if math.IsNaN(f) {
		return 0x7ff8000000000000 // single canonical NaN
	}
	if f == 0 {
		return 0 // collapses -0.0 to +0.0
	}
	return math.Float64bits(f)
}

func canonicalFloat32Bits(f float32) uint32 {
	if f != f {
		return 0x7fc00000
	}
	if f == 0 {
		return 0
	}
	return math.Float32bits(f)
}

// encodeTime canonicalizes to the absolute instant. Date types encode days
// since epoch; DateTime64 keeps nanoseconds; DateTime keeps seconds.
func encodeTime(col Column, t time.Time) []byte {
	var payload uint64
	switch {
	case strings.HasPrefix(col.Type, "DateTime64"):
		payload = uint64(t.UnixNano())
	case strings.HasPrefix(col.Type, "Date"):
		if strings.HasPrefix(col.Type, "DateTime") {
			payload = uint64(t.Unix())
		} else {
			payload = uint64(t.Unix() / 86400)
		}
	default:
		payload = uint64(t.Unix())
	}
	return appendInt(kindTime, payload, 8)
}
