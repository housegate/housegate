package lthash

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// canonicalDomain versions the MVP row-encoding profile. It deliberately
// differs from the spec's production profile ("housegate-row-v1", which
// uses stable column IDs and _hg_row_id): the MVP profile identifies
// columns by name and hashes raw row values.
const canonicalDomain = "housegate-row-mvp-v0"

// CanonicalRowProfile is the versioned row-encoding domain used by EncodeRow.
// It is exported so callers that cache the result of RowHash (e.g. the
// storage-integrity part-LtHash cache) can bind their cache key to the exact
// encoding profile and invalidate automatically if the profile is bumped.
func CanonicalRowProfile() string { return canonicalDomain }

// Column describes one column of a row to be encoded: the column name and
// the declared ClickHouse type name (e.g. "UInt64", "String", "DateTime").
type Column struct {
	Name string
	Type string
}

// value kind tags keep differently-typed values from colliding byte-wise.
const (
	kindInt byte = iota + 1
	kindUint
	kindFloat
	kindString
	kindBool
	kindTime
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

// encodeValue canonicalizes one value: a 1-byte kind tag followed by a
// fixed-width or framed payload. Integers carry their declared width via
// the byte length; floats canonicalize NaN to one pattern and -0.0 to
// +0.0; times canonicalize to the absolute instant.
func encodeValue(col Column, v any) ([]byte, error) {
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
	default:
		return nil, fmt.Errorf("lthash: unsupported value type %T for column %q (%s)", v, col.Name, col.Type)
	}
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
