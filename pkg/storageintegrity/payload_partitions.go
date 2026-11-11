package storageintegrity

import (
	"errors"
	"fmt"
	"sort"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ErrPartitionFreeze rejects schemas whose partition key is not admitted. The
// admitted set is payloadexec.ParsePartitionKey's (table admission rules v3):
// no key, a bare String column (the P1c freeze), a bare integer, Date, Date32
// or UTC DateTime/DateTime64 column, or an admitted partition expression. It is
// safe to admit them all unconditionally here: the table registry admits no
// such table before the network raises its admission rules version to 3.
var ErrPartitionFreeze = errors.New("storage_integrity: table violates the partition freeze (partition_by must be empty or an admitted partition key)")

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
	if _, err := payloadexec.ParsePartitionKey(schema); err != nil {
		return fmt.Errorf("%w: %v", ErrPartitionFreeze, err)
	}
	return nil
}
