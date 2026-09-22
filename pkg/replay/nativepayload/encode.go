package nativepayload

import (
	"fmt"
	"reflect"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// EncodeClientDataPacket encodes one uncompressed client Data packet carrying
// cols: the packet code ClientCodeData, an empty external-table block name,
// BlockInfo{BucketNum: -1} and the block body framed for revision.
//
// It is Decode's inverse over the admitted column set. Every column type must
// resolve through payloadexec.ResolveColumnProfile and equal the profile's
// NativeWireType, so Decode of the returned bytes under a schema declaring the
// same types reproduces the same values. The BlockInfo prefix (revision >=
// 51903) and the per-column custom-serialization flag (revision >= 54454) are
// ch-go's own gates: callers pass the codec's negotiated revision, never a
// constant, because the signed payload is exactly these bytes.
func EncodeClientDataPacket(revision int, cols []proto.InputColumn) ([]byte, error) {
	if revision <= 0 {
		return nil, fmt.Errorf("%w: client protocol revision is required", ErrUnsupported)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("%w: native block requires at least one column", ErrUnsupported)
	}
	if nilColumnInput(cols[0].Data) {
		return nil, fmt.Errorf("%w: native block column %q has no data", ErrUnsupported, cols[0].Name)
	}
	rows := cols[0].Data.Rows()
	if rows <= 0 {
		return nil, fmt.Errorf("%w: native payload contains no rows", ErrUnsupported)
	}
	seen := make(map[string]struct{}, len(cols))
	for _, col := range cols {
		switch {
		case col.Name == "":
			return nil, fmt.Errorf("%w: native block contains an unnamed column", ErrUnsupported)
		case col.Name == "_hg_row_id":
			return nil, fmt.Errorf("%w: native block must not contain reserved _hg_row_id", ErrUnsupported)
		case nilColumnInput(col.Data):
			return nil, fmt.Errorf("%w: native block column %q has no data", ErrUnsupported, col.Name)
		}
		if _, dup := seen[col.Name]; dup {
			return nil, fmt.Errorf("%w: native block contains duplicate column %q", ErrUnsupported, col.Name)
		}
		seen[col.Name] = struct{}{}
		if got := col.Data.Rows(); got != rows {
			return nil, fmt.Errorf("%w: native block column %q has %d rows, first column has %d",
				ErrUnsupported, col.Name, got, rows)
		}
		declared := string(col.Data.Type())
		profile, err := payloadexec.ResolveColumnProfile(declared)
		if err != nil {
			return nil, fmt.Errorf("%w: native block column %q: %w", ErrUnsupported, col.Name, err)
		}
		if declared != profile.NativeWireType {
			return nil, fmt.Errorf("%w: native block column %q type %q is not the admitted wire type %q",
				ErrUnsupported, col.Name, declared, profile.NativeWireType)
		}
	}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	block := proto.Block{Info: proto.BlockInfo{Overflows: false, BucketNum: -1}, Columns: len(cols), Rows: rows}
	if err := block.EncodeBlock(&buf, revision, cols); err != nil {
		return nil, fmt.Errorf("%w: encode native block: %w", ErrUnsupported, err)
	}
	return append([]byte(nil), buf.Buf...), nil
}

// An interface holding a typed nil pointer is not itself nil. Refuse it before
// invoking Rows or Type, which may use value receivers and panic.
func nilColumnInput(col proto.ColInput) bool {
	if col == nil {
		return true
	}
	v := reflect.ValueOf(col)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
