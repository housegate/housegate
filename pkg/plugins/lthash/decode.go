package lthashplugin

import (
	"bytes"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/lthash"
)

// DecodedBlock is the schema-agnostic decode of one client Data packet:
// column names and declared ClickHouse type names exactly as the client
// sent them, plus typed row access.
type DecodedBlock struct {
	Columns []lthash.Column
	Rows    int

	cols []proto.ColResult
}

// DecodeClientData decodes the raw bytes of one ClientData packet (type
// varint + block name + block body) into typed columns, inferring the
// schema from the type names carried inside the block itself. revision is
// the negotiated client protocol revision (SessionState.ClientRevision) —
// it gates BlockInfo and custom-serialization framing.
//
// Compressed blocks are out of MVP scope; callers must gate on the
// query's compression flag before calling (a compressed body fails here
// with a framing error, it is never silently misread).
func DecodeClientData(raw []byte, revision int) (*DecodedBlock, error) {
	r := proto.NewReader(bytes.NewReader(raw))

	code, err := r.UVarInt()
	if err != nil {
		return nil, fmt.Errorf("packet code: %w", err)
	}
	if code != uint64(chproto.ClientDataCode) {
		return nil, fmt.Errorf("packet type %d is not ClientData", code)
	}
	if _, err := r.Str(); err != nil {
		return nil, fmt.Errorf("block name: %w", err)
	}

	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(r, revision, results.Auto()); err != nil {
		return nil, fmt.Errorf("decode block: %w", err)
	}

	out := &DecodedBlock{Rows: block.Rows}
	for _, rc := range results {
		col := rc.Data
		typeName := ""
		if auto, ok := col.(*proto.ColAuto); ok {
			typeName = string(auto.DataType)
			col = auto.Data
		} else if col != nil {
			typeName = string(col.Type())
		}
		out.Columns = append(out.Columns, lthash.Column{Name: rc.Name, Type: typeName})
		out.cols = append(out.cols, col)
	}
	return out, nil
}

// Row materializes row i as Go values aligned with Columns.
func (b *DecodedBlock) Row(i int) ([]any, error) {
	if i < 0 || i >= b.Rows {
		return nil, fmt.Errorf("row %d out of range (rows=%d)", i, b.Rows)
	}
	values := make([]any, len(b.cols))
	for c, col := range b.cols {
		v, err := columnValue(col, i)
		if err != nil {
			return nil, fmt.Errorf("column %q (%s): %w", b.Columns[c].Name, b.Columns[c].Type, err)
		}
		values[c] = v
	}
	return values, nil
}

// columnValue extracts row i from the MVP-supported column types. The
// whitelist mirrors the canonical encoder in pkg/lthash; anything else is
// a hard error so unsupported data can never hash ambiguously.
func columnValue(col proto.ColResult, i int) (any, error) {
	switch c := col.(type) {
	case *proto.ColUInt8:
		return (*c)[i], nil
	case *proto.ColUInt16:
		return (*c)[i], nil
	case *proto.ColUInt32:
		return (*c)[i], nil
	case *proto.ColUInt64:
		return (*c)[i], nil
	case *proto.ColInt8:
		return (*c)[i], nil
	case *proto.ColInt16:
		return (*c)[i], nil
	case *proto.ColInt32:
		return (*c)[i], nil
	case *proto.ColInt64:
		return (*c)[i], nil
	case *proto.ColFloat32:
		return (*c)[i], nil
	case *proto.ColFloat64:
		return (*c)[i], nil
	case *proto.ColStr:
		return c.Row(i), nil
	case *proto.ColBool:
		return (*c)[i], nil
	case *proto.ColDate:
		return c.Row(i), nil
	case *proto.ColDateTime:
		return c.Row(i), nil
	case *proto.ColDateTime64:
		return c.Row(i), nil
	default:
		return nil, fmt.Errorf("unsupported column type %T (MVP whitelist)", col)
	}
}
