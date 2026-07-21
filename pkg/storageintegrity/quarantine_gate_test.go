package storageintegrity

import "testing"

func quarantineFixture() WorkerQuarantine {
	return NewWorkerQuarantine("w-3", "audit minority", "evidence://vote/snap-1",
		[]QuarantineRole{RoleServingAudit, RoleServing, RoleMutation}, 42, true)
}

func TestWorkerQuarantine_Valid(t *testing.T) {
	if err := quarantineFixture().Valid(); err != nil {
		t.Fatalf("well-formed quarantine must validate: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*WorkerQuarantine)
	}{
		{"blank worker", func(q *WorkerQuarantine) { q.WorkerID = "" }},
		{"blank reason", func(q *WorkerQuarantine) { q.Reason = "" }},
		{"zero since_block", func(q *WorkerQuarantine) { q.SinceBlock = 0 }},
		{"empty roles", func(q *WorkerQuarantine) { q.AffectedRoles = nil }},
		{"unknown role", func(q *WorkerQuarantine) { q.AffectedRoles = []QuarantineRole{"bogus"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := quarantineFixture()
			tc.mutate(&q)
			if err := q.Valid(); err == nil {
				t.Fatal("invalid quarantine must fail closed")
			}
		})
	}
}

func TestWorkerQuarantine_BlocksAndClone(t *testing.T) {
	q := quarantineFixture()
	if !q.Blocks(RoleServingAudit) || !q.Blocks(RoleServing) || !q.Blocks(RoleMutation) {
		t.Fatal("named roles must be blocked")
	}
	if q.Blocks(RoleReplay) || q.Blocks(RoleByteSide) || q.Blocks(RolePromotion) {
		t.Fatal("unnamed roles must not be blocked")
	}
	c := q.Clone()
	c.AffectedRoles[0] = "mutated"
	if q.AffectedRoles[0] == "mutated" {
		t.Fatal("Clone must deep-copy the role set")
	}
}

func TestNewQuarantineGate_ValidatesEntries(t *testing.T) {
	if _, err := NewQuarantineGate(map[string]WorkerQuarantine{"w-3": quarantineFixture()}); err != nil {
		t.Fatalf("valid gate: %v", err)
	}
	// Key/worker mismatch.
	if _, err := NewQuarantineGate(map[string]WorkerQuarantine{"other": quarantineFixture()}); err == nil {
		t.Fatal("key != worker id must fail closed")
	}
	// Invalid entry.
	bad := quarantineFixture()
	bad.AffectedRoles = nil
	if _, err := NewQuarantineGate(map[string]WorkerQuarantine{"w-3": bad}); err == nil {
		t.Fatal("invalid entry must fail closed")
	}
}

func TestQuarantineGate_UnifiedRoleBlocking(t *testing.T) {
	g, err := NewQuarantineGate(map[string]WorkerQuarantine{"w-3": quarantineFixture()})
	if err != nil {
		t.Fatalf("gate: %v", err)
	}

	// A quarantined role is blocked for both evidence submission and task claim.
	if d := g.MaySubmitEvidence("w-3", RoleMutation); d.Allowed {
		t.Fatal("quarantined worker must not submit mutation evidence")
	}
	if d := g.MayClaimTask("w-3", RoleServingAudit); d.Allowed {
		t.Fatal("quarantined worker must not claim a serving-audit task")
	}
	// A role NOT named in the quarantine is allowed.
	if d := g.MaySubmitEvidence("w-3", RoleReplay); !d.Allowed {
		t.Fatalf("un-quarantined role must be allowed: %s", d.Detail)
	}
	// A worker with no quarantine is allowed everywhere.
	if d := g.MayClaimTask("w-1", RoleMutation); !d.Allowed {
		t.Fatal("un-quarantined worker must be allowed")
	}
	if d := g.MaySubmitEvidence("w-1", RoleServingAudit); !d.Allowed {
		t.Fatal("un-quarantined worker must be allowed")
	}
}

func TestQuarantineGate_MayServe_ComposesWithSafeReadGate(t *testing.T) {
	// A safe cut that WOULD allow w-3 to serve (in read set, watermark covers).
	cut := NewSafeCutView("m-1", 10, map[string]uint64{"w-1": 10, "w-3": 10}, map[string]bool{"w-1": true, "w-3": true}, 3, nil)
	gate, err := NewSafeReadGate(cut)
	if err != nil {
		t.Fatalf("read gate: %v", err)
	}

	// With the quarantine gate, w-3 is denied serving even though the cut's own
	// QuarantinedWorkers set is empty — the quarantine is a unified decision that
	// fails closed before the next cut installs it.
	g, _ := NewQuarantineGate(map[string]WorkerQuarantine{"w-3": quarantineFixture()})
	if d := g.MayServe("w-3", 5, gate); d.Allowed || d.Reason != QuarantineDenyServing {
		t.Fatalf("quarantined-for-serving worker must be denied: %+v", d)
	}
	// w-1 (no quarantine) still serves via the delegated SafeReadGate.
	if d := g.MayServe("w-1", 5, gate); !d.Allowed {
		t.Fatalf("un-quarantined worker must serve: %+v", d)
	}
	// A worker not in the read set surfaces the gate's real (non-quarantine) reason.
	if d := g.MayServe("w-9", 5, gate); d.Allowed {
		t.Fatal("worker outside the read set must not serve")
	}
}

func TestQuarantineGate_MayServe_RespectsCutQuarantine(t *testing.T) {
	// Even with an empty quarantine gate, a worker the safe cut already quarantined
	// is denied and mapped to the serving reason.
	cut := NewSafeCutView("m-1", 10, map[string]uint64{"w-1": 10}, map[string]bool{"w-1": true}, 3, map[string]bool{"w-1": true})
	gate, _ := NewSafeReadGate(cut)
	g, _ := NewQuarantineGate(nil)
	if d := g.MayServe("w-1", 5, gate); d.Allowed || d.Reason != QuarantineDenyServing {
		t.Fatalf("cut-quarantined worker must map to serving deny: %+v", d)
	}
}

func TestQuarantineDenyReason_StableStrings(t *testing.T) {
	for _, r := range []QuarantineDenyReason{QuarantineAllowed, QuarantineDenyEvidenceRole, QuarantineDenyTaskClaim, QuarantineDenyServing} {
		if r.String() == "" || r.String() == "Unknown" {
			t.Fatalf("reason %d needs a stable string", r)
		}
	}
}

// --- Gated: the runtime that installs quarantine into the live evidence /
// claim / cut paths needs the absent companion C3 seam. ---

func TestQuarantineGate_EnforcedAcrossLiveEvidenceAndClaimPaths(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion C3 evidence-submission / task-claim seam lands")
}
