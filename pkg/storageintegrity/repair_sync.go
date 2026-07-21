package storageintegrity

import (
	"fmt"

	"housegate/housegate/pkg/replay"
)

// RepairSource is the recovery source a worker synced from. A worker may recover
// ONLY from the authoritative manifest or a canonical peer (design section 5.3:
// "worker 只从权威 manifest/canonical peer 恢复"). Any other source is illegal and
// keeps the worker out of the read set.
type RepairSource int

const (
	RepairSourceUnknown RepairSource = iota
	RepairSourceAuthoritativeManifest
	RepairSourceCanonicalPeer
)

func (s RepairSource) String() string {
	switch s {
	case RepairSourceAuthoritativeManifest:
		return "AuthoritativeManifest"
	case RepairSourceCanonicalPeer:
		return "CanonicalPeer"
	default:
		return "Unknown"
	}
}

func legalRepairSource(s RepairSource) bool {
	return s == RepairSourceAuthoritativeManifest || s == RepairSourceCanonicalPeer
}

// RepairStage is where a worker stands in the repair → verify → audit → re-entry
// progression (design section 5.2/5.3). A worker joins the read set only at the
// terminal Reentered stage, and only when every gate below it passed.
type RepairStage int

const (
	RepairStageExcluded RepairStage = iota
	RepairStageSyncing
	RepairStageSynced
	RepairStageVerified
	RepairStageAuditPending
	RepairStageAuditPassed
	RepairStageReentered
)

func (s RepairStage) String() string {
	switch s {
	case RepairStageExcluded:
		return "Excluded"
	case RepairStageSyncing:
		return "Syncing"
	case RepairStageSynced:
		return "Synced"
	case RepairStageVerified:
		return "Verified"
	case RepairStageAuditPending:
		return "AuditPending"
	case RepairStageAuditPassed:
		return "AuditPassed"
	case RepairStageReentered:
		return "Reentered"
	default:
		return "Unknown"
	}
}

// ReentryDenyReason is the typed reason a worker is not (yet) eligible to re-enter
// the read set.
type ReentryDenyReason int

const (
	ReentryAllowed ReentryDenyReason = iota
	ReentryDenyStillQuarantined
	ReentryDenyIllegalRepairSource
	ReentryDenyExactVerificationFailed
	ReentryDenyAuditNotPassed
)

func (r ReentryDenyReason) String() string {
	switch r {
	case ReentryAllowed:
		return "Allowed"
	case ReentryDenyStillQuarantined:
		return "StillQuarantined"
	case ReentryDenyIllegalRepairSource:
		return "IllegalRepairSource"
	case ReentryDenyExactVerificationFailed:
		return "ExactVerificationFailed"
	case ReentryDenyAuditNotPassed:
		return "AuditNotPassed"
	default:
		return "Unknown"
	}
}

// ReadSetReentryInput is the pure HouseGate-local evidence a re-entry decision
// reads: the target manifest the worker synced to, the manifest's active parts,
// the worker's exact readback of those parts, the repair source it used, and the
// still-quarantined / audit-passed bits (populated truthfully only once the
// companion seam lands; green-today tests construct these directly).
type ReadSetReentryInput struct {
	WorkerID            string
	TargetManifestID    string
	ManifestActiveParts []replay.PartManifestEntry
	WorkerReadbackParts []CandidatePart
	RepairSource        RepairSource
	StillQuarantined    bool
	ServingAuditPassed  bool
}

// ReadSetReentryDecision is the routed outcome. EligibleForReadSet is true ONLY
// when the worker is not quarantined AND repaired from a legal source AND its
// readback exactly matches the target manifest AND its serving audit passed
// (design section 5.2/5.3: a lagging worker repairs/syncs and re-passes the
// serving audit before it may re-join the read set).
type ReadSetReentryDecision struct {
	WorkerID           string
	Stage              RepairStage
	EligibleForReadSet bool
	Reason             ReentryDenyReason
	Detail             string
}

// VerifyReadbackAgainstManifest is the pure exact-parts equality check: the
// worker's readback must exactly equal the target manifest's active parts by the
// CandidatePart fields (table/partition/part name/row-lthash/rows/bytes),
// order-insensitively, both directions. Any drift fails closed.
func VerifyReadbackAgainstManifest(manifestParts []replay.PartManifestEntry, readback []CandidatePart) error {
	if len(manifestParts) != len(readback) {
		return fmt.Errorf("readback has %d parts, manifest has %d", len(readback), len(manifestParts))
	}
	expected := map[string]CandidatePart{}
	for _, p := range manifestParts {
		k := auditPartKey(p.TableID, p.PartitionID, p.PartName)
		if _, dup := expected[k]; dup {
			return fmt.Errorf("manifest has a duplicate part %s/%s/%s", p.TableID, p.PartitionID, p.PartName)
		}
		expected[k] = CandidatePart{
			TableID: p.TableID, PartitionID: p.PartitionID, PartName: p.PartName,
			PartRowLtHash: p.PartRowLtHash, RowCount: p.RowCount, Bytes: p.Bytes,
		}
	}
	// Walk the readback tracking seen keys so a duplicate readback part cannot
	// "cover" for a missing manifest part when the slice lengths happen to match
	// (e.g. manifest {A,B} vs readback [A,A]). Rejecting duplicates plus the
	// equal-length precondition means every distinct expected key must be matched
	// exactly once, so the two sets are equal.
	seen := map[string]bool{}
	for _, got := range readback {
		k := auditPartKey(got.TableID, got.PartitionID, got.PartName)
		if seen[k] {
			return fmt.Errorf("readback has a duplicate part %s/%s/%s", got.TableID, got.PartitionID, got.PartName)
		}
		seen[k] = true
		exp, ok := expected[k]
		if !ok {
			return fmt.Errorf("readback part %s/%s/%s is not in the manifest", got.TableID, got.PartitionID, got.PartName)
		}
		if got != exp {
			return fmt.Errorf("readback part %s/%s/%s does not match the manifest", got.TableID, got.PartitionID, got.PartName)
		}
	}
	return nil
}

// DecideReadSetReentry is total and fail-closed. It walks the gates in order and
// returns the stage the worker reached plus whether it is eligible to re-enter
// the read set. EligibleForReadSet is true only at RepairStageReentered, which
// requires: not quarantined, a legal repair source, an exact readback match, and
// a passed serving audit. A failure at any gate stops the progression with a
// typed reason.
func DecideReadSetReentry(in ReadSetReentryInput) ReadSetReentryDecision {
	d := ReadSetReentryDecision{WorkerID: in.WorkerID}

	if in.StillQuarantined {
		d.Stage = RepairStageExcluded
		d.Reason = ReentryDenyStillQuarantined
		d.Detail = "worker is still quarantined; cannot re-enter the read set"
		return d
	}
	if !legalRepairSource(in.RepairSource) {
		d.Stage = RepairStageSyncing
		d.Reason = ReentryDenyIllegalRepairSource
		d.Detail = fmt.Sprintf("repair source %s is not authoritative-manifest or canonical-peer", in.RepairSource)
		return d
	}
	// Synced from a legal source; now the exact verification gate.
	if err := VerifyReadbackAgainstManifest(in.ManifestActiveParts, in.WorkerReadbackParts); err != nil {
		d.Stage = RepairStageSynced
		d.Reason = ReentryDenyExactVerificationFailed
		d.Detail = fmt.Sprintf("exact verification failed: %v", err)
		return d
	}
	// Exact verification passed; the audit gate is last.
	if !in.ServingAuditPassed {
		d.Stage = RepairStageAuditPending
		d.Reason = ReentryDenyAuditNotPassed
		d.Detail = "serving audit has not passed; worker stays out of the read set"
		return d
	}
	d.Stage = RepairStageReentered
	d.EligibleForReadSet = true
	d.Reason = ReentryAllowed
	return d
}
