package storageintegrity

import "context"

// RollbackState classifies where a mutation stands on one worker when a rollback
// or recovery decision is needed (design sections 4.5 and 5.3). The four states
// are ordered by how far the mutation progressed on that worker.
type RollbackState int

const (
	// RollbackNotApplied: no command issued, or a command issued but provably not
	// locally applied. This is the ONLY state where treating the worker as
	// un-executed (rebind/retry) is legal (design section 5.3 rule 1).
	RollbackNotApplied RollbackState = iota
	// RollbackCommandIssued: the command demonstrably reached the worker but the
	// manifest is not committed and the worker is not provably locally applied —
	// either a durable ack is present (the command reached the worker, even if the
	// command-issued bookkeeping was lost) or a command was issued with an unknown
	// local-apply status. Authority/watermark/CAS still gate a stale action; query
	// the durable ack, do not rebind while unknown (design section 5.3 rule 2,
	// section 4.5 PublicationBlocked "unknown ack").
	RollbackCommandIssued
	// RollbackPartialLocalApply: locally applied but the manifest is not committed
	// — exclude the worker from the read set and repair; never treat as
	// un-executed (design section 5.3 rule 3, section 4.5 PublicationBlocked
	// "partial apply", section 4.5 line: a worker that applied cannot be
	// un-executed).
	RollbackPartialLocalApply
	// RollbackManifestCommitted: the manifest committed — done; a reversing
	// statement through the normal mutation path is the only undo (design section
	// 5.3 rule 4, section 4.5 Safe).
	RollbackManifestCommitted
)

func (s RollbackState) String() string {
	switch s {
	case RollbackNotApplied:
		return "NotApplied"
	case RollbackCommandIssued:
		return "CommandIssued"
	case RollbackPartialLocalApply:
		return "PartialLocalApply"
	case RollbackManifestCommitted:
		return "ManifestCommitted"
	default:
		return "Unknown"
	}
}

// RollbackAction is what HouseGate does for a rollback state.
type RollbackAction int

const (
	RollbackActionRebindRetry RollbackAction = iota
	RollbackActionQueryHold
	RollbackActionExcludeAndRepair
	RollbackActionDone
)

func (a RollbackAction) String() string {
	switch a {
	case RollbackActionRebindRetry:
		return "RebindRetry"
	case RollbackActionQueryHold:
		return "QueryHold"
	case RollbackActionExcludeAndRepair:
		return "ExcludeAndRepair"
	case RollbackActionDone:
		return "Done"
	default:
		return "Unknown"
	}
}

// RollbackEvidence is the pure HouseGate-local evidence a rollback decision reads.
// ApplyStatusKnown distinguishes "provably not applied" (known false) from
// "unknown" (a command issued whose local effect HouseGate cannot yet determine).
// The bits DurableAckPresent / ManifestCommitted are truthfully populated only
// once the companion seam lands; green-today tests construct evidence directly.
type RollbackEvidence struct {
	MutationID         string
	WorkerID           string
	PublicationSeq     uint64
	CommandIssued      bool
	ApplyStatusKnown   bool
	LocallyApplied     bool
	DurableAckPresent  bool
	ManifestCommitted  bool
	AffectedPartitions []AffectedPartition
}

// RollbackDecision is the routed outcome. MayRebindAsUnexecuted is true ONLY for
// RollbackNotApplied; a partial local apply always excludes and repairs and may
// never be rebound as un-executed (design section 4.5).
type RollbackDecision struct {
	MutationID            string
	WorkerID              string
	State                 RollbackState
	Action                RollbackAction
	Reason                string
	ExcludeFromReadSet    bool
	RepairRequired        bool
	MayRebindAsUnexecuted bool
	RepairScope           []AffectedPartition
}

// ClassifyRollback maps evidence to a state. It is total and fail-closed toward
// the most-committed consistent state, so HouseGate never under-classifies a
// worker that touched storage: manifest-committed dominates, then partial local
// apply, then command-issued-unknown, else not-applied.
func ClassifyRollback(ev RollbackEvidence) RollbackState {
	if ev.ManifestCommitted {
		return RollbackManifestCommitted
	}
	if ev.LocallyApplied {
		return RollbackPartialLocalApply
	}
	// Not (provably) locally applied. A durable ack is itself proof that the command
	// reached this worker: it holds at CommandIssued (query/hold) even if the
	// command-issued bookkeeping was lost — never fall through to NotApplied and
	// enable rebind/retry against a worker that durably acked. Otherwise, a command
	// issued whose apply status is unknown is also command-issued.
	if ev.DurableAckPresent {
		return RollbackCommandIssued
	}
	if ev.CommandIssued && !ev.ApplyStatusKnown {
		return RollbackCommandIssued
	}
	return RollbackNotApplied
}

// DecideRollback classifies the evidence and maps the state to an action,
// enforcing the invariant matrix. RepairScope is a defensive copy.
func DecideRollback(ev RollbackEvidence) RollbackDecision {
	state := ClassifyRollback(ev)
	d := RollbackDecision{
		MutationID:  ev.MutationID,
		WorkerID:    ev.WorkerID,
		State:       state,
		RepairScope: append([]AffectedPartition(nil), ev.AffectedPartitions...),
	}
	switch state {
	case RollbackNotApplied:
		d.Action = RollbackActionRebindRetry
		d.MayRebindAsUnexecuted = true
		d.Reason = "not applied: safe to cancel/rebind/retry"
	case RollbackCommandIssued:
		d.Action = RollbackActionQueryHold
		d.Reason = "command issued, local apply unknown: query the durable ack, hold"
	case RollbackPartialLocalApply:
		d.Action = RollbackActionExcludeAndRepair
		d.ExcludeFromReadSet = true
		d.RepairRequired = true
		d.Reason = "partial local apply: exclude from read set and repair; never rebind as un-executed"
	case RollbackManifestCommitted:
		d.Action = RollbackActionDone
		d.Reason = "manifest committed: undo only via a reversing statement through the normal mutation path"
	}
	return d
}

// MutationRepairDriver is the gated port that excludes a worker from the read set
// and repairs it to the current manifest (design section 5.3). No implementation
// exists; see CompanionMutationConsensusAvailable. HouseGate drives it; it never
// implements the Arbiter read-set cut.
type MutationRepairDriver interface {
	ExcludeAndRepair(ctx context.Context, workerID string, scope []AffectedPartition, targetManifestID string) error
}
