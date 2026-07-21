package storageintegrity

import (
	"encoding/hex"
	"fmt"
	"sort"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"
)

// hgCompactShadowDatabase is the shadow database where a compaction worker
// constructs output parts without touching the hg_safe active set (design
// section 6 step 2).
const hgCompactShadowDatabase = "hg_compact"

// compactionManifestDigestDomain versions the new content-addressed manifest id
// derived after a controlled compaction publishes (design section 6 step 5).
const compactionManifestDigestDomain = "controlled-compaction-manifest-v1"

// CompactionPlan is the Arbiter-selected controlled-compaction input (design
// section 6 step 1): the same partition's input safe parts chosen from the
// current manifest, bound to a mutation/publication identity and a base snapshot
// for the signed base-CAS REPLACE.
type CompactionPlan struct {
	CompactionID       string
	PublicationSeq     uint64
	TableID            string
	PartitionID        string
	BaseSafeSnapshotID string
	BasePartitionRoots []replay.PartitionCommitment
	InputParts         []replay.PartManifestEntry
}

// Valid fails closed on a plan that could not drive a controlled compaction: a
// blank id/table/partition, a zero seq, an empty base binding, or an empty input
// set (a compaction with no input parts is meaningless).
func (p CompactionPlan) Valid() error {
	if p.CompactionID == "" || p.TableID == "" || p.PartitionID == "" {
		return fmt.Errorf("compaction plan: blank id/table/partition")
	}
	if p.PublicationSeq == 0 {
		return fmt.Errorf("compaction plan %s: zero publication seq", p.CompactionID)
	}
	if p.BaseSafeSnapshotID == "" || len(p.BasePartitionRoots) == 0 {
		return fmt.Errorf("compaction plan %s: missing base binding", p.CompactionID)
	}
	if len(p.InputParts) == 0 {
		return fmt.Errorf("compaction plan %s: empty input part set", p.CompactionID)
	}
	if err := sameTablePartition(p.TableID, p.PartitionID, p.InputParts); err != nil {
		return fmt.Errorf("compaction plan %s: %w", p.CompactionID, err)
	}
	return nil
}

// CompactionOutput is the hg_compact-shadow output the worker produced for the
// same (table, partition) — the compacted parts that would replace the inputs.
type CompactionOutput struct {
	TableID     string
	PartitionID string
	OutputParts []replay.PartManifestEntry
}

// Valid fails closed on an empty or cross-partition output.
func (o CompactionOutput) Valid() error {
	if o.TableID == "" || o.PartitionID == "" {
		return fmt.Errorf("compaction output: blank table/partition")
	}
	if len(o.OutputParts) == 0 {
		return fmt.Errorf("compaction output %s/%s: empty output part set", o.TableID, o.PartitionID)
	}
	return sameTablePartition(o.TableID, o.PartitionID, o.OutputParts)
}

func sameTablePartition(table, partition string, parts []replay.PartManifestEntry) error {
	for _, p := range parts {
		if p.TableID != table || p.PartitionID != partition {
			return fmt.Errorf("part %s/%s/%s is outside the compaction partition %s/%s", p.TableID, p.PartitionID, p.PartName, table, partition)
		}
		if p.PartRowLtHash == "" {
			return fmt.Errorf("part %s/%s/%s has no row-lthash", p.TableID, p.PartitionID, p.PartName)
		}
	}
	return nil
}

// VerifyCompactionEquation is the pure LtHash ledger equation (design section 6
// step 3): sum(part_row_lthash(input safe parts)) == sum(part_row_lthash(output
// compacted parts)). It folds each side's PartRowLtHash into an lthash
// accumulator and compares the 2048-byte accumulators. A compaction that does not
// preserve the row-content lattice sum is rejected — controlled compaction may
// only re-lay-out rows, never add or drop content.
func VerifyCompactionEquation(input, output []replay.PartManifestEntry) error {
	if len(input) == 0 || len(output) == 0 {
		return fmt.Errorf("compaction equation: empty input or output set")
	}
	inSum, err := sumPartRowLtHash(input)
	if err != nil {
		return fmt.Errorf("compaction equation: input: %w", err)
	}
	outSum, err := sumPartRowLtHash(output)
	if err != nil {
		return fmt.Errorf("compaction equation: output: %w", err)
	}
	if !inSum.Equal(outSum) {
		return fmt.Errorf("compaction equation: sum(input row-lthash) != sum(output row-lthash)")
	}
	return nil
}

// sumPartRowLtHash folds each part's hex-encoded PartRowLtHash into one lthash
// accumulator. The fold is commutative (lattice addition), so the sum is
// order-insensitive.
func sumPartRowLtHash(parts []replay.PartManifestEntry) (*lthash.Hash, error) {
	acc := lthash.New()
	for _, p := range parts {
		raw, err := hex.DecodeString(p.PartRowLtHash)
		if err != nil {
			return nil, fmt.Errorf("decode part %s row-lthash: %w", p.PartName, err)
		}
		h, err := lthash.FromBytes(raw)
		if err != nil {
			return nil, fmt.Errorf("part %s row-lthash: %w", p.PartName, err)
		}
		acc.AddHash(h)
	}
	return acc, nil
}

// BuildCompactionReplacePlan validates the LtHash equation and the output, then
// builds the signed-base-CAS REPLACE plan via the shared BuildReplacePartitionPlan
// (design section 6 step 4): controlled compaction publishes through the same
// REPLACE PARTITION path as a mutation, keyed by (compaction id, publication
// seq). It fails closed unless the equation holds and the output is a non-empty
// same-partition set.
func BuildCompactionReplacePlan(plan CompactionPlan, output CompactionOutput) (ReplacePartitionPlan, error) {
	if err := plan.Valid(); err != nil {
		return ReplacePartitionPlan{}, err
	}
	if err := output.Valid(); err != nil {
		return ReplacePartitionPlan{}, err
	}
	if output.TableID != plan.TableID || output.PartitionID != plan.PartitionID {
		return ReplacePartitionPlan{}, fmt.Errorf("compaction %s: output partition %s/%s != plan %s/%s", plan.CompactionID, output.TableID, output.PartitionID, plan.TableID, plan.PartitionID)
	}
	if err := VerifyCompactionEquation(plan.InputParts, output.OutputParts); err != nil {
		return ReplacePartitionPlan{}, err
	}
	install := PartitionInstallPlan{
		TableID:        plan.TableID,
		PartitionID:    plan.PartitionID,
		Action:         PublicationActionReplacePartition,
		CanonicalParts: output.OutputParts,
	}
	return BuildReplacePartitionPlan(plan.CompactionID, plan.PublicationSeq, install)
}

// BuildCompactionManifestID derives the new content-addressed manifest id for the
// post-compaction publication (design section 6 step 5). It canonicalizes the
// (table, partition, sorted output part names + row-lthashes) so the id is a pure
// function of the new active-part mapping, independently recomputable.
func BuildCompactionManifestID(plan CompactionPlan, output CompactionOutput) (string, error) {
	if err := plan.Valid(); err != nil {
		return "", err
	}
	if err := output.Valid(); err != nil {
		return "", err
	}
	sorted := append([]replay.PartManifestEntry(nil), output.OutputParts...)
	sort.Slice(sorted, func(i, j int) bool { return partManifestLess(sorted[i], sorted[j]) })
	canon := fmt.Sprintf("compaction=%s\nseq=%d\ntable=%s\npartition=%s\nbase=%s\n",
		plan.CompactionID, plan.PublicationSeq, plan.TableID, plan.PartitionID, plan.BaseSafeSnapshotID)
	for _, p := range sorted {
		canon += fmt.Sprintf("out=%s|rows=%d|bytes=%d|lthash=%s\n", p.PartName, p.RowCount, p.Bytes, p.PartRowLtHash)
	}
	return replay.CanonicalDigest(compactionManifestDigestDomain, canon)
}

// ActiveSetMismatch is the evidence of a native un-ledgered merge: the worker's
// observed active-part names diverge from the manifest-declared set (design
// section 6: "发现未经 ledger 的 native safe merge 时，该 worker 立即 active-set mismatch").
type ActiveSetMismatch struct {
	WorkerID            string
	TableID             string
	PartitionID         string
	ExpectedActiveParts []string
	ObservedActiveParts []string
}

// DetectActiveSetMismatch compares the manifest-declared active-part names
// against the worker's observed set. It reports a mismatch (and true) when the
// two sets differ. Both inputs are compared as sets (order-insensitive).
func DetectActiveSetMismatch(workerID, tableID, partitionID string, expected, observed []string) (ActiveSetMismatch, bool) {
	exp := stringSet(expected)
	obs := stringSet(observed)
	mismatch := len(exp) != len(obs)
	if !mismatch {
		for n := range exp {
			if !obs[n] {
				mismatch = true
				break
			}
		}
	}
	if !mismatch {
		return ActiveSetMismatch{}, false
	}
	return ActiveSetMismatch{
		WorkerID:            workerID,
		TableID:             tableID,
		PartitionID:         partitionID,
		ExpectedActiveParts: sortedCopy(expected),
		ObservedActiveParts: sortedCopy(observed),
	}, true
}

// CompactionQuarantineDecision is the fail-closed response to a detected
// active-set mismatch: stop serving, exclude from the read set, and require
// repair (design section 6: the worker "停止服务相关 read set 并进入 repair/quarantine").
type CompactionQuarantineDecision struct {
	StopServing        bool
	ExcludeFromReadSet bool
	RepairRequired     bool
	Reason             string
	Mismatch           ActiveSetMismatch
}

// DecideCompactionQuarantine maps a detected mismatch to the fail-closed
// stop-serving + exclude + repair decision.
func DecideCompactionQuarantine(m ActiveSetMismatch) CompactionQuarantineDecision {
	return CompactionQuarantineDecision{
		StopServing:        true,
		ExcludeFromReadSet: true,
		RepairRequired:     true,
		Reason:             fmt.Sprintf("native un-ledgered merge: worker %s active-set mismatch on %s/%s", m.WorkerID, m.TableID, m.PartitionID),
		Mismatch:           m,
	}
}

func sortedCopy(ss []string) []string {
	out := append([]string(nil), ss...)
	sort.Strings(out)
	return out
}
