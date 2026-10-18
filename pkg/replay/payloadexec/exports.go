package payloadexec

import (
	"fmt"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
)

// RowIDProfileID names the _hg_row_id derivation profile (§5.2). Envelope v2
// signs it as row_id_profile_id; verifiers reject any other value.
const RowIDProfileID = rowIDDomain

// PayloadFormatCSVWithNames is the legacy/test wire encoding decoded by
// DecodeCSV. Production storage-integrity payloads are Native
// (pkg/replay/nativepayload.PayloadFormat).
const PayloadFormatCSVWithNames = "csv-with-names-v1"

// RowElementHash exposes the executor's canonical per-row LtHash element. Data
// plane scanners call this instead of reimplementing row identity or row
// encoding.
func RowElementHash(sch TableSchema, rowID []byte, values []any) (*lthash.Hash, error) {
	return rowElementHash(sch, rowID, values)
}

// SchemaRoot exposes the deployment schema-root derivation used by replay
// manifests and Arbiter genesis checks.
func SchemaRoot(networkID string, schemas []TableSchema) string {
	return schemaRoot(networkID, schemas)
}

// TableSchemaHash exposes the per-table schema digest used in table manifests.
func TableSchemaHash(networkID string, t TableSchema) string {
	return tableSchemaHash(networkID, t)
}

// SchemaRootFromHashes derives the schema root from table ids and their
// TableSchemaHash values. It is byte-identical to SchemaRoot over the schemas
// those hashes were computed from, so a party that holds only committed hashes
// (the arbiter FSM, a manifest) derives the root a verifier derives from full
// schemas.
func SchemaRootFromHashes(schemaHashes map[string]string) string {
	return schemaRootFromHashes(schemaHashes)
}

// VerifyLedger is the executor's partition-ledger check (§5.4), exported for
// the stage-3 manifest reader and install gate (spec 2026-10-14 §8.4: a
// manifest is accepted only if every partition root equals the sum of its
// listed part row hashes). For every table, the LtHash sum of the
// PartRowLtHash of its active parts in a partition must equal that
// partition's declared root (a declared partition without parts has the zero
// accumulator as its root), and every partition with active parts must
// declare a root. The executor applies the same check to every previous
// snapshot it replays from.
func VerifyLedger(m replay.SafeSnapshotManifest) error {
	for _, t := range m.Tables {
		sums := map[string]*lthash.Hash{}
		for _, p := range t.ActiveParts {
			h, err := lthashFromHex(p.PartRowLtHash)
			if err != nil {
				return fmt.Errorf("table %s part %s: %w", t.TableID, p.PartName, err)
			}
			if sums[p.PartitionID] == nil {
				sums[p.PartitionID] = lthash.New()
			}
			sums[p.PartitionID].AddHash(h)
		}
		declared := make(map[string]struct{}, len(t.PartitionRoots))
		for _, pc := range t.PartitionRoots {
			declared[pc.PartitionID] = struct{}{}
			got := sums[pc.PartitionID]
			if got == nil {
				got = lthash.New()
			}
			if lthashHex(got) != pc.Root {
				return fmt.Errorf("table %s partition %s: parts sum != root", t.TableID, pc.PartitionID)
			}
		}
		// The invariant is bijective: a partition with active parts must also
		// declare a commitment, else an orphan part contributes data summed into
		// no partition root (§5.4 / "Registration arithmetic").
		for partitionID := range sums {
			if _, ok := declared[partitionID]; !ok {
				return fmt.Errorf("table %s partition %s: active parts present with no partition root", t.TableID, partitionID)
			}
		}
	}
	return nil
}

// SumPartRowLtHashes returns the LtHash sum of part row accumulators in the
// executor's form ("0x" + lowercase hex of the 2048-byte accumulator), the
// arithmetic of the install gate (spec 2026-10-14 §8.5 steps 1 and 6: Σ(M) =
// R, Σ(L) + Σ(kept) = R). Each element is a 0x-prefixed hex accumulator as a
// manifest's PartRowLtHash or chexec's PartScanResult.RowLtHash holds it.
// Repeats count, order does not, and the sum of no element is the zero
// accumulator, the root of a partition without parts. An element that is not
// an accumulator fails with its position.
func SumPartRowLtHashes(hashes []string) (string, error) {
	sum := lthash.New()
	for i, s := range hashes {
		h, err := lthashFromHex(s)
		if err != nil {
			return "", fmt.Errorf("part row lthash %d: %w", i, err)
		}
		sum.AddHash(h)
	}
	return lthashHex(sum), nil
}
