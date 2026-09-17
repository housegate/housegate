package snapshotquery

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// CanonicalOutput owns a complete, fsynced local output and its independent
// readers (at most two open at once; closing one releases its slot). Close
// retires all readers and removes its private directory. Metadata
// methods and Close are concurrent-safe; Next/Close on a reader are serialized.
// This handle is not a restart-discovery or snapshot-retention contract. A durable
// cache owner must copy/fsync and authenticate its own cache before releasing it.
type CanonicalOutput interface {
	RowCount() uint64
	OutputRowsRoot() string
	TouchedPartitionIDs() []string
	OpenRows() (payloadexec.RowSource, error)
	Close() error
}

func checkedAdd(a, b uint64) (uint64, error) {
	if b > math.MaxUint64-a {
		return 0, fmt.Errorf("canonical output counter overflow")
	}
	return a + b, nil
}
func checkedMul(a, b uint64) (uint64, error) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, fmt.Errorf("canonical output counter overflow")
	}
	return a * b, nil
}
func fitInt(n uint64) error {
	if n > uint64(int(^uint(0)>>1)) {
		return fmt.Errorf("canonical output size exceeds int")
	}
	return nil
}

// rowLayout validates types without copying or calling an allocating encoder.
// Sizes include every length frame, and reject EncodeRow's uint32 truncations.
func rowLayout(schema payloadexec.TableSchema, values []any) (keySize, typedSize, partitionBound uint64, err error) {
	if uint64(len(schema.TableID)) > math.MaxUint32 {
		return 0, 0, 0, fmt.Errorf("table ID exceeds uint32 framing")
	}
	if uint64(len(schema.Columns)) > math.MaxUint32 {
		return 0, 0, 0, fmt.Errorf("column count exceeds uint32 framing")
	}
	if len(values) != len(schema.Columns) {
		return 0, 0, 0, fmt.Errorf("row width does not match schema")
	}
	keySize = uint64(4 + len("housegate-row-mvp-v0") + 4 + len(schema.TableID) + 4)
	partitionBound = 64
	for i, c := range schema.Columns {
		p, e := payloadexec.ResolveColumnProfile(c.Type)
		if e != nil {
			return 0, 0, 0, e
		}
		v := values[i]
		if reflect.TypeOf(v) != p.GoType {
			return 0, 0, 0, fmt.Errorf("column %q has %T, want %s", c.Name, v, p.GoType)
		}
		n := uint64(p.GoType.Size())
		switch x := v.(type) {
		case string:
			n = uint64(len(x))
		case []byte:
			n = uint64(len(x))
			if len(x) != p.FixedStringWidth {
				return 0, 0, 0, fmt.Errorf("column %q fixed string width mismatch", c.Name)
			}
		case time.Time:
			n = 8
		}
		if n >= math.MaxUint32 || uint64(len(c.Name)) > math.MaxUint32 || uint64(len(c.Type)) > math.MaxUint32 {
			return 0, 0, 0, fmt.Errorf("canonical row field exceeds uint32 framing")
		}
		keySize, e = checkedAdd(keySize, 13+uint64(len(c.Name))+uint64(len(c.Type))+n)
		if e != nil {
			return 0, 0, 0, e
		}
		if _, ok := v.(time.Time); ok {
			n = 16
		}
		typedSize, e = checkedAdd(typedSize, 8+n)
		if e != nil {
			return 0, 0, 0, e
		}
		if c.Name == schema.PartitionBy {
			partitionBound, e = checkedAdd(partitionBound, n)
			if e != nil {
				return 0, 0, 0, e
			}
		}
	}
	if err = fitInt(keySize); err != nil {
		return
	}
	err = fitInt(typedSize)
	return
}

// NormalizeRow requires exact profile Go types and copies all variable-sized
// values. It does not coerce SQL values or rewrite authenticated type strings.
// Date follows EncodeRow's epoch-day truncation; DateTime keeps seconds and
// DateTime64 keeps its declared precision. Temporal results have no monotonic
// clock or location-dependent representation.
func NormalizeRow(schema payloadexec.TableSchema, values []any) ([]any, error) {
	if _, _, _, err := rowLayout(schema, values); err != nil {
		return nil, err
	}
	out := make([]any, len(values))
	for i, v := range values {
		p, err := payloadexec.ResolveColumnProfile(schema.Columns[i].Type)
		if err != nil {
			return nil, err
		}
		switch x := v.(type) {
		case string:
			out[i] = strings.Clone(x)
		case []byte:
			out[i] = bytes.Clone(x)
		case float32:
			if math.IsNaN(float64(x)) {
				x = math.Float32frombits(0x7fc00000)
			} else if x == 0 {
				x = 0
			}
			out[i] = x
		case float64:
			if math.IsNaN(x) {
				x = math.Float64frombits(0x7ff8000000000000)
			} else if x == 0 {
				x = 0
			}
			out[i] = x
		case time.Time:
			switch p.Family {
			case payloadexec.FamilyDate:
				x = time.Unix((x.Unix()/86400)*86400, 0).UTC()
			case payloadexec.FamilyDateTime:
				x = time.Unix(x.Unix(), 0).UTC()
			case payloadexec.FamilyDateTime64:
				// EncodeRow commits UnixNano; reject instants outside its lossless range.
				if !time.Unix(0, x.UnixNano()).Equal(x) {
					return nil, fmt.Errorf("column %q is outside canonical nanosecond range", schema.Columns[i].Name)
				}
				unit := int64(1)
				for j := p.Precision; j < 9; j++ {
					unit *= 10
				}
				x = time.Unix(x.Unix(), int64(x.Nanosecond())/unit*unit).UTC()
				if !time.Unix(0, x.UnixNano()).Equal(x) {
					return nil, fmt.Errorf("column %q precision truncation exceeds canonical nanosecond range", schema.Columns[i].Name)
				}
			}
			out[i] = x
		default:
			out[i] = v
		}
	}
	return out, nil
}

// Typed records are schema-ordered [uint64 length, bytes] fields. No interface
// codec, dynamic type names, or SQL parser participates in their reconstruction.
func encodeTyped(values []any, size uint64) []byte {
	out := make([]byte, 0, int(size))
	for _, v := range values {
		start := len(out)
		out = append(out, make([]byte, 8)...)
		switch x := v.(type) {
		case string:
			out = append(out, x...)
		case []byte:
			out = append(out, x...)
		case bool:
			if x {
				out = append(out, 1)
			} else {
				out = append(out, 0)
			}
		case float32:
			out = binary.LittleEndian.AppendUint32(out, math.Float32bits(x))
		case float64:
			out = binary.LittleEndian.AppendUint64(out, math.Float64bits(x))
		case time.Time:
			out = binary.LittleEndian.AppendUint64(out, uint64(x.Unix()))
			out = binary.LittleEndian.AppendUint64(out, uint64(x.Nanosecond()))
		default:
			r := reflect.ValueOf(v)
			var n uint64
			if r.Kind() >= reflect.Int && r.Kind() <= reflect.Int64 {
				n = uint64(r.Int())
			} else {
				n = r.Uint()
			}
			for j := 0; j < int(r.Type().Size()); j++ {
				out = append(out, byte(n>>uint(8*j)))
			}
		}
		binary.LittleEndian.PutUint64(out[start:start+8], uint64(len(out)-start-8))
	}
	return out
}
func decodeTyped(schema payloadexec.TableSchema, data []byte) ([]any, error) {
	values := make([]any, len(schema.Columns))
	for i, c := range schema.Columns {
		if len(data) < 8 {
			return nil, io.ErrUnexpectedEOF
		}
		n := binary.LittleEndian.Uint64(data)
		data = data[8:]
		if n > uint64(len(data)) {
			return nil, io.ErrUnexpectedEOF
		}
		b := data[:int(n)]
		data = data[int(n):]
		p, err := payloadexec.ResolveColumnProfile(c.Type)
		if err != nil {
			return nil, err
		}
		switch p.Family {
		case payloadexec.FamilyString:
			values[i] = string(b)
		case payloadexec.FamilyFixedString:
			if len(b) != p.FixedStringWidth {
				return nil, fmt.Errorf("invalid fixed string record")
			}
			values[i] = bytes.Clone(b)
		case payloadexec.FamilyDate, payloadexec.FamilyDateTime, payloadexec.FamilyDateTime64:
			if len(b) != 16 || binary.LittleEndian.Uint64(b[8:]) >= 1e9 {
				return nil, fmt.Errorf("invalid time record")
			}
			values[i] = time.Unix(int64(binary.LittleEndian.Uint64(b)), int64(binary.LittleEndian.Uint64(b[8:]))).UTC()
		default:
			if len(b) != int(p.GoType.Size()) {
				return nil, fmt.Errorf("invalid scalar record width")
			}
			var bits uint64
			for j, x := range b {
				bits |= uint64(x) << uint(8*j)
			}
			v := reflect.New(p.GoType).Elem()
			switch p.Family {
			case payloadexec.FamilyBool:
				if bits > 1 {
					return nil, fmt.Errorf("invalid bool record")
				}
				v.SetBool(bits == 1)
			case payloadexec.FamilyFloat:
				if len(b) == 4 {
					v.SetFloat(float64(math.Float32frombits(uint32(bits))))
				} else {
					v.SetFloat(math.Float64frombits(bits))
				}
			case payloadexec.FamilyInt:
				v.SetInt(int64(bits))
			case payloadexec.FamilyUInt:
				v.SetUint(bits)
			default:
				return nil, fmt.Errorf("unsupported record family %s", p.Family)
			}
			values[i] = v.Interface()
		}
	}
	if len(data) != 0 {
		return nil, fmt.Errorf("trailing typed record data")
	}
	return values, nil
}

// Canonicalize owns input, closing it exactly once even on invalid arguments.
// Next must honor its context. Close and local kernel I/O are synchronous: no
// detached goroutine pretends to forcibly cancel a non-cooperating port/kernel.
func Canonicalize(ctx context.Context, networkID, statementID string, schema payloadexec.TableSchema, input RowStream, limits Limits, tempDir string) (result CanonicalOutput, err error) {
	if input == nil {
		return nil, fmt.Errorf("row stream is required")
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, input.Close())
		}
	}()
	if err = replay.ValidateQueryLimits(limits); err != nil {
		return nil, err
	}
	if limits.MaxExecutionMS > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return nil, fmt.Errorf("execution deadline overflow")
	}
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(limits.MaxExecutionMS)*time.Millisecond)
	defer cancel()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if networkID == "" || statementID == "" || schema.TableID == "" || len(schema.Columns) == 0 {
		return nil, fmt.Errorf("network, statement and nonempty target schema are required")
	}
	if uint64(len(networkID)) > math.MaxUint32 || uint64(len(statementID)) > math.MaxUint32 || uint64(len(schema.TableID)) > math.MaxUint32 {
		return nil, fmt.Errorf("identity exceeds uint32 framing")
	}
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	state, err := newSortState(ctx, networkID, statementID, schema, limits, tempDir)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(tempDir, "snapshot-query-")
	if err != nil {
		return nil, err
	}
	state.dir = dir
	defer func() {
		if result == nil {
			err = errors.Join(err, os.RemoveAll(dir))
		}
	}()
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		values, readErr := input.Next(ctx)
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if readErr == io.EOF {
			if len(values) != 0 {
				return nil, fmt.Errorf("row stream returned values with EOF")
			}
			break
		}
		if readErr != nil {
			return nil, readErr
		}
		if err = state.add(values); err != nil {
			return nil, err
		}
	}
	closed = true
	if err = input.Close(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = state.flush(); err != nil {
		return nil, err
	}
	run, err := state.finishRuns()
	if err != nil {
		return nil, err
	}
	out, err := state.finishOutput(run)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
