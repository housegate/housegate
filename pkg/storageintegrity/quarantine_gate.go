package storageintegrity

import "fmt"

// QuarantineRole is one evidence-submission / serving role a quarantine can
// block (design section 5.3: a quarantined worker cannot submit the affected
// roles' replay, byte-side, mutation, promotion, or SafeAudit evidence, nor
// serve the corresponding safe watermark).
type QuarantineRole string

const (
	RoleReplay       QuarantineRole = "replay"
	RoleByteSide     QuarantineRole = "byte_side"
	RoleMutation     QuarantineRole = "mutation"
	RolePromotion    QuarantineRole = "promotion"
	RoleServingAudit QuarantineRole = "serving_audit"
	// RoleServing is the safe-SELECT serving eligibility axis. It is distinct from
	// the evidence roles above and, per design section 5.2, distinct from P1a
	// MarkActive (source/Verifier selection membership) — quarantine here controls
	// serving eligibility, not active membership.
	RoleServing QuarantineRole = "serving"
)

// knownQuarantineRoles is the closed set a quarantine may name; an unknown role
// is a fail-closed wiring error.
var knownQuarantineRoles = map[QuarantineRole]bool{
	RoleReplay: true, RoleByteSide: true, RoleMutation: true,
	RolePromotion: true, RoleServingAudit: true, RoleServing: true,
}

// WorkerQuarantine is the design section 5.3 record. A quarantine blocks the
// named affected roles from the since_block block onward; repair_required marks
// a worker that must repair/sync before it can re-enter (PR21).
type WorkerQuarantine struct {
	WorkerID       string
	Reason         string
	EvidenceRef    string
	AffectedRoles  []QuarantineRole
	SinceBlock     uint64
	RepairRequired bool
}

// NewWorkerQuarantine constructs a quarantine, defensively copying the role set.
func NewWorkerQuarantine(workerID, reason, evidenceRef string, roles []QuarantineRole, sinceBlock uint64, repairRequired bool) WorkerQuarantine {
	return WorkerQuarantine{
		WorkerID:       workerID,
		Reason:         reason,
		EvidenceRef:    evidenceRef,
		AffectedRoles:  append([]QuarantineRole(nil), roles...),
		SinceBlock:     sinceBlock,
		RepairRequired: repairRequired,
	}
}

// Valid fails closed on a quarantine that blocks nothing or is under-specified:
// a blank worker id or reason, a zero since_block, an empty affected-role set, or
// an unknown role. A quarantine that names no role would silently let a
// quarantined worker act, so it is rejected.
func (q WorkerQuarantine) Valid() error {
	if q.WorkerID == "" {
		return fmt.Errorf("quarantine: blank worker id")
	}
	if q.Reason == "" {
		return fmt.Errorf("quarantine %s: blank reason", q.WorkerID)
	}
	if q.SinceBlock == 0 {
		return fmt.Errorf("quarantine %s: zero since_block", q.WorkerID)
	}
	if len(q.AffectedRoles) == 0 {
		return fmt.Errorf("quarantine %s: empty affected-role set (a quarantine that blocks nothing is invalid)", q.WorkerID)
	}
	for _, r := range q.AffectedRoles {
		if !knownQuarantineRoles[r] {
			return fmt.Errorf("quarantine %s: unknown role %q", q.WorkerID, r)
		}
	}
	return nil
}

// Blocks reports whether this quarantine blocks the given role.
func (q WorkerQuarantine) Blocks(role QuarantineRole) bool {
	for _, r := range q.AffectedRoles {
		if r == role {
			return true
		}
	}
	return false
}

// Clone deep-copies the role set.
func (q WorkerQuarantine) Clone() WorkerQuarantine {
	q.AffectedRoles = append([]QuarantineRole(nil), q.AffectedRoles...)
	return q
}

// QuarantineDenyReason is the typed reason the quarantine gate refuses an action.
type QuarantineDenyReason int

const (
	QuarantineAllowed QuarantineDenyReason = iota
	QuarantineDenyEvidenceRole
	QuarantineDenyTaskClaim
	QuarantineDenyServing
)

func (r QuarantineDenyReason) String() string {
	switch r {
	case QuarantineAllowed:
		return "Allowed"
	case QuarantineDenyEvidenceRole:
		return "EvidenceRoleQuarantined"
	case QuarantineDenyTaskClaim:
		return "TaskClaimQuarantined"
	case QuarantineDenyServing:
		return "ServingQuarantined"
	default:
		return "Unknown"
	}
}

// QuarantineDecision is the typed answer, mirroring GateDecision's shape.
type QuarantineDecision struct {
	Allowed bool
	Reason  QuarantineDenyReason
	Detail  string
}

// QuarantineGate answers the unified quarantine question across every affected
// role: a quarantined worker uniformly cannot submit the affected roles'
// evidence, claim a task in a quarantined role, or serve the corresponding read
// set (design section 5.3). It does NOT rewrite P1a active membership.
type QuarantineGate struct {
	quarantines map[string]WorkerQuarantine
}

// NewQuarantineGate validates every entry and wraps a defensive copy. Fail-closed
// on any invalid entry (an invalid quarantine could let a worker act unchecked).
func NewQuarantineGate(quarantines map[string]WorkerQuarantine) (QuarantineGate, error) {
	copied := make(map[string]WorkerQuarantine, len(quarantines))
	for id, q := range quarantines {
		if q.WorkerID != id {
			return QuarantineGate{}, fmt.Errorf("quarantine gate: key %q != worker id %q", id, q.WorkerID)
		}
		if err := q.Valid(); err != nil {
			return QuarantineGate{}, err
		}
		copied[id] = q.Clone()
	}
	return QuarantineGate{quarantines: copied}, nil
}

// IsQuarantined reports whether the worker has any quarantine.
func (g QuarantineGate) IsQuarantined(workerID string) (WorkerQuarantine, bool) {
	q, ok := g.quarantines[workerID]
	return q, ok
}

// MaySubmitEvidence answers whether the worker may submit evidence for a role. A
// quarantine that blocks the role denies it; an evidence submission from a
// quarantined role must be rejected before it reaches the Arbiter FSM.
func (g QuarantineGate) MaySubmitEvidence(workerID string, role QuarantineRole) QuarantineDecision {
	if q, ok := g.quarantines[workerID]; ok && q.Blocks(role) {
		return QuarantineDecision{Reason: QuarantineDenyEvidenceRole, Detail: fmt.Sprintf("worker %s quarantined for role %s: %s", workerID, role, q.Reason)}
	}
	return QuarantineDecision{Allowed: true, Reason: QuarantineAllowed}
}

// MayClaimTask answers whether the worker may claim a task in a role.
func (g QuarantineGate) MayClaimTask(workerID string, role QuarantineRole) QuarantineDecision {
	if q, ok := g.quarantines[workerID]; ok && q.Blocks(role) {
		return QuarantineDecision{Reason: QuarantineDenyTaskClaim, Detail: fmt.Sprintf("worker %s quarantined for role %s: %s", workerID, role, q.Reason)}
	}
	return QuarantineDecision{Allowed: true, Reason: QuarantineAllowed}
}

// MayServe answers the read-serving half by composing this gate's serving-role
// quarantine with the existing SafeReadGate: a worker blocked for RoleServing is
// denied here even if the safe cut's own QuarantinedWorkers set has not yet been
// updated (the two are independent fail-closed checks). Otherwise it delegates to
// the passed SafeReadGate.MayServe and maps a gate-quarantine deny to this gate's
// typed reason. This makes quarantine a single unified decision even before the
// next safe cut installs the quarantine into its QuarantinedWorkers set.
func (g QuarantineGate) MayServe(workerID string, requestedSnapshot uint64, gate SafeReadGate) QuarantineDecision {
	if q, ok := g.quarantines[workerID]; ok && q.Blocks(RoleServing) {
		return QuarantineDecision{Reason: QuarantineDenyServing, Detail: fmt.Sprintf("worker %s quarantined for serving: %s", workerID, q.Reason)}
	}
	d := gate.MayServe(workerID, requestedSnapshot)
	if d.Allowed {
		return QuarantineDecision{Allowed: true, Reason: QuarantineAllowed}
	}
	if d.Reason == GateDenyQuarantined {
		return QuarantineDecision{Reason: QuarantineDenyServing, Detail: d.Detail}
	}
	// A non-quarantine gate denial (read-set / watermark) is not this gate's
	// concern; surface it as not-allowed with the gate's detail so the caller sees
	// the real reason.
	return QuarantineDecision{Allowed: false, Reason: QuarantineAllowed, Detail: d.Detail}
}
