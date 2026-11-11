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
// returns the sorted, unique logical partition ids it touches. It decodes with
// the replay-side decoders, so it answers for a statement an earlier binary
// admitted exactly as that binary did: ingress also runs it on journal
// recovery and on exact retries. Refusals that apply only to a new admission
// are ValidateNewAdmissionPayload's.
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

// ValidateNewAdmissionPayload applies the refusals that guard a statement the
// ingress is about to prepare for the first time. Today that is one: a
// DateTime64 value whose instant lies outside the int64-nanosecond range is
// refused with ErrDateTime64OutOfRange, on Native (nativepayload's
// DecodeForIntake) and CSV payloads alike. ClickHouse would store such a value
// as sent, but every write path re-encodes ch-go's wrapped decode of it, so
// the network would store and commit a different row (#343). It never runs for
// a statement already admitted, whose replay must not change.
func ValidateNewAdmissionPayload(schema payloadexec.TableSchema, encoding string, revision int, payload []byte) error {
	switch encoding {
	case EncodingCSVWithNames:
		rows, err := payloadexec.DecodeCSV(payload, schema)
		if err != nil {
			return fmt.Errorf("storage_integrity: decode payload for intake: %w", err)
		}
		return csvDateTime64sRepresentable(schema, rows)
	case PayloadEncodingClickHouseNativeData:
		if _, err := nativepayload.DecodeForIntake(schema, revision, payload); err != nil {
			return fmt.Errorf("storage_integrity: decode payload for intake: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("storage_integrity: cannot validate payload encoding %q", encoding)
	}
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
