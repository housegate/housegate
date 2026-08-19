package storageintegrity

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ErrPartitionFreeze rejects schemas outside the v1 partition-key freeze.
var ErrPartitionFreeze = errors.New("storage_integrity: table violates the P1c partition freeze (partition_by must be a bare String column)")

// PayloadPartitionIDs decodes an admitted payload under its pinned schema and
// returns the sorted, unique logical partition ids it touches.
func PayloadPartitionIDs(schema payloadexec.TableSchema, encoding string, revision int, payload []byte) ([]string, error) {
	if err := validatePartitionFreeze(schema); err != nil {
		return nil, err
	}
	var rows []payloadexec.Row
	var err error
	switch encoding {
	case EncodingCSVWithNames:
		rows, err = payloadexec.DecodeCSV(payload, schema)
	case PayloadEncodingClickHouseNativeData:
		rows, err = DecodeNativePayload(schema, revision, payload)
	default:
		return nil, fmt.Errorf("storage_integrity: cannot derive partitions for payload encoding %q", encoding)
	}
	if err != nil {
		return nil, fmt.Errorf("storage_integrity: decode payload for partition check: %w", err)
	}
	seen := map[string]bool{}
	partitions := make([]string, 0, 1)
	for _, row := range rows {
		if !seen[row.PartitionID] {
			seen[row.PartitionID] = true
			partitions = append(partitions, row.PartitionID)
		}
	}
	sort.Strings(partitions)
	return partitions, nil
}

func validatePartitionFreeze(schema payloadexec.TableSchema) error {
	if schema.PartitionBy == "" {
		return nil
	}
	if strings.ContainsAny(schema.PartitionBy, "()") {
		return fmt.Errorf("%w: table %s partition_by %q is an expression", ErrPartitionFreeze, schema.TableID, schema.PartitionBy)
	}
	for _, column := range schema.Columns {
		if column.Name == schema.PartitionBy {
			if column.Type != "String" {
				return fmt.Errorf("%w: table %s partition column %q has type %s", ErrPartitionFreeze, schema.TableID, column.Name, column.Type)
			}
			return nil
		}
	}
	return fmt.Errorf("%w: table %s partition_by %q names no declared column", ErrPartitionFreeze, schema.TableID, schema.PartitionBy)
}
