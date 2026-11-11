package storageintegrity

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/housegate/housegate/pkg/replay/nativepayload"
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
// returns the sorted, unique logical partition ids it touches. It is the
// ingress's decode of a new statement, so it also applies the intake-only
// refusals: a DateTime64 value outside the int64-nanosecond range is refused
// with ErrDateTime64OutOfRange on either encoding (#343). Replay never calls
// it.
func PayloadPartitionIDs(schema payloadexec.TableSchema, encoding string, revision int, payload []byte) ([]string, error) {
	if err := validatePartitionFreeze(schema); err != nil {
		return nil, err
	}
	var rows []payloadexec.Row
	var err error
	switch encoding {
	case EncodingCSVWithNames:
		rows, err = payloadexec.DecodeCSV(payload, schema)
		if err == nil {
			err = csvDateTime64sRepresentable(schema, rows)
		}
	case PayloadEncodingClickHouseNativeData:
		rows, err = nativepayload.DecodeForIntake(schema, revision, payload)
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

// csvDateTime64sRepresentable is the CSV lane's DateTime64 intake refusal. The
// CSV decoder parses the instant exactly, so the check is on the instant
// itself: its nanosecond count must fit int64.
func csvDateTime64sRepresentable(schema payloadexec.TableSchema, rows []payloadexec.Row) error {
	var cols []int
	for i, col := range schema.Columns {
		profile, err := payloadexec.ResolveColumnProfile(col.Type)
		if err == nil && profile.Family == payloadexec.FamilyDateTime64 {
			cols = append(cols, i)
		}
	}
	for ordinal, row := range rows {
		for _, i := range cols {
			t, ok := row.Values[i].(time.Time)
			if ok && !time.Unix(0, t.UnixNano()).Equal(t) {
				return fmt.Errorf("csv row %d: column %q (%s): %s: %w",
					ordinal, schema.Columns[i].Name, schema.Columns[i].Type, t.UTC().Format(time.RFC3339Nano), ErrDateTime64OutOfRange)
			}
		}
	}
	return nil
}

func validatePartitionFreeze(schema payloadexec.TableSchema) error {
	if _, err := payloadexec.ParsePartitionKey(schema); err != nil {
		return fmt.Errorf("%w: %v", ErrPartitionFreeze, err)
	}
	return nil
}
