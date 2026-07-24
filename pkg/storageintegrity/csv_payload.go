package storageintegrity

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"

	"housegate/housegate/pkg/replay/payloadexec"
)

// PayloadMaterializationInput is the ingress-side bridge from Relay's Native
// ClientData capture to the replay payload encoding selected by the SQL.
type PayloadMaterializationInput struct {
	StatementID     string
	TableID         string
	SQL             string
	PayloadEncoding string
	NativeWire      []byte
	Revision        int
}

// PayloadMaterializationResult is the final replay payload bytes to store in DA
// and submit through Arbiter. The payload may differ from NativeWire when a SQL
// format such as CSVWithNames selects a different replay encoding.
type PayloadMaterializationResult struct {
	Payload  []byte
	Encoding string
}

// PayloadMaterializer converts captured Relay input into the payload bytes
// promised by PayloadEncoding.
type PayloadMaterializer interface {
	MaterializePayload(context.Context, PayloadMaterializationInput) (PayloadMaterializationResult, error)
}

// TableSchemaResolver resolves the pinned replay schema for an admitted table.
type TableSchemaResolver interface {
	StorageIntegrityTableSchema(tableID string) (payloadexec.TableSchema, bool)
}

// TableSchemaResolverFunc adapts a function into TableSchemaResolver.
type TableSchemaResolverFunc func(tableID string) (payloadexec.TableSchema, bool)

func (fn TableSchemaResolverFunc) StorageIntegrityTableSchema(tableID string) (payloadexec.TableSchema, bool) {
	return fn(tableID)
}

// NativeCSVPayloadMaterializer converts captured ClickHouse Native ClientData
// packets into CSVWithNames bytes using the same pinned schema Arbiter replay
// uses for CSV decoding.
type NativeCSVPayloadMaterializer struct {
	SchemaResolver TableSchemaResolver
}

func (m NativeCSVPayloadMaterializer) MaterializePayload(ctx context.Context, in PayloadMaterializationInput) (PayloadMaterializationResult, error) {
	if err := ctx.Err(); err != nil {
		return PayloadMaterializationResult{}, err
	}
	if in.PayloadEncoding != EncodingCSVWithNames {
		return PayloadMaterializationResult{}, fmt.Errorf("CSV materializer only supports %s, got %q", EncodingCSVWithNames, in.PayloadEncoding)
	}
	if m.SchemaResolver == nil {
		return PayloadMaterializationResult{}, fmt.Errorf("CSV materializer schema resolver is required")
	}
	schema, ok := m.SchemaResolver.StorageIntegrityTableSchema(in.TableID)
	if !ok {
		return PayloadMaterializationResult{}, fmt.Errorf("CSV materializer has no pinned schema for table %q", in.TableID)
	}
	if schema.TableID != in.TableID {
		return PayloadMaterializationResult{}, fmt.Errorf("CSV materializer schema table %q does not match admission table %q", schema.TableID, in.TableID)
	}
	payload, err := MaterializeNativePayloadAsCSVWithNames(ctx, schema, in.Revision, in.NativeWire)
	if err != nil {
		return PayloadMaterializationResult{}, err
	}
	return PayloadMaterializationResult{Payload: payload, Encoding: EncodingCSVWithNames}, nil
}

// MaterializeNativePayloadAsCSVWithNames decodes captured Native ClientData and
// emits a deterministic CSVWithNames payload ordered by the pinned schema.
func MaterializeNativePayloadAsCSVWithNames(ctx context.Context, schema payloadexec.TableSchema, revision int, nativeWire []byte) ([]byte, error) {
	rows, err := DecodeNativePayload(schema, revision, nativeWire)
	if err != nil {
		return nil, fmt.Errorf("decode captured Native payload for CSVWithNames: %w", err)
	}
	header := make([]string, len(schema.Columns))
	for i, col := range schema.Columns {
		header[i] = col.Name
	}
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write(header); err != nil {
		return nil, fmt.Errorf("write CSVWithNames header: %w", err)
	}
	for rowIndex, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record := make([]string, len(schema.Columns))
		for colIndex, col := range schema.Columns {
			field, err := csvFieldString(col.Type, row.Values[colIndex])
			if err != nil {
				return nil, fmt.Errorf("row %d column %q: %w", rowIndex, col.Name, err)
			}
			record[colIndex] = field
		}
		if err := w.Write(record); err != nil {
			return nil, fmt.Errorf("write CSVWithNames row %d: %w", rowIndex, err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("write CSVWithNames payload: %w", err)
	}
	return buf.Bytes(), nil
}

func csvFieldString(typeName string, value any) (string, error) {
	switch {
	case typeName == "String":
		v, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("String value has type %T", value)
		}
		return v, nil
	case strings.HasPrefix(typeName, "FixedString("):
		v, ok := value.([]byte)
		if !ok {
			return "", fmt.Errorf("FixedString value has type %T", value)
		}
		return string(v), nil
	case typeName == "Bool":
		v, ok := value.(bool)
		if !ok {
			return "", fmt.Errorf("Bool value has type %T", value)
		}
		return strconv.FormatBool(v), nil
	case typeName == "Float32":
		v, ok := value.(float32)
		if !ok {
			return "", fmt.Errorf("Float32 value has type %T", value)
		}
		if v == 0 {
			return "0", nil
		}
		return strconv.FormatFloat(float64(v), 'g', -1, 32), nil
	case typeName == "Float64":
		v, ok := value.(float64)
		if !ok {
			return "", fmt.Errorf("Float64 value has type %T", value)
		}
		if v == 0 {
			return "0", nil
		}
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case typeName == "UInt8":
		v, ok := value.(uint8)
		if !ok {
			return "", fmt.Errorf("UInt8 value has type %T", value)
		}
		return strconv.FormatUint(uint64(v), 10), nil
	case typeName == "UInt16":
		v, ok := value.(uint16)
		if !ok {
			return "", fmt.Errorf("UInt16 value has type %T", value)
		}
		return strconv.FormatUint(uint64(v), 10), nil
	case typeName == "UInt32":
		v, ok := value.(uint32)
		if !ok {
			return "", fmt.Errorf("UInt32 value has type %T", value)
		}
		return strconv.FormatUint(uint64(v), 10), nil
	case typeName == "UInt64":
		v, ok := value.(uint64)
		if !ok {
			return "", fmt.Errorf("UInt64 value has type %T", value)
		}
		return strconv.FormatUint(v, 10), nil
	case typeName == "Int8":
		v, ok := value.(int8)
		if !ok {
			return "", fmt.Errorf("Int8 value has type %T", value)
		}
		return strconv.FormatInt(int64(v), 10), nil
	case typeName == "Int16":
		v, ok := value.(int16)
		if !ok {
			return "", fmt.Errorf("Int16 value has type %T", value)
		}
		return strconv.FormatInt(int64(v), 10), nil
	case typeName == "Int32":
		v, ok := value.(int32)
		if !ok {
			return "", fmt.Errorf("Int32 value has type %T", value)
		}
		return strconv.FormatInt(int64(v), 10), nil
	case typeName == "Int64":
		v, ok := value.(int64)
		if !ok {
			return "", fmt.Errorf("Int64 value has type %T", value)
		}
		return strconv.FormatInt(v, 10), nil
	default:
		return "", fmt.Errorf("unsupported CSVWithNames materialization type %q", typeName)
	}
}
