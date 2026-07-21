package storageintegrity

import "testing"

func rollbackEvidenceFixture() RollbackEvidence {
	return RollbackEvidence{
		MutationID:         "m-1",
		WorkerID:           "w-1",
		PublicationSeq:     2,
		AffectedPartitions: []AffectedPartition{{TableID: "net1.events", PartitionID: "p1"}},
	}
}

func TestClassifyRollback_States(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*RollbackEvidence)
		want   RollbackState
	}{
		{"no command", func(e *RollbackEvidence) { e.ApplyStatusKnown = true }, RollbackNotApplied},
		{"command issued not applied (known)", func(e *RollbackEvidence) {
			e.CommandIssued = true
			e.ApplyStatusKnown = true
			e.LocallyApplied = false
		}, RollbackNotApplied},
		{"command issued unknown apply", func(e *RollbackEvidence) {
			e.CommandIssued = true
			e.ApplyStatusKnown = false
		}, RollbackCommandIssued},
		{"command issued durable ack unknown apply", func(e *RollbackEvidence) {
			e.CommandIssued = true
			e.ApplyStatusKnown = true
			e.DurableAckPresent = true
			e.LocallyApplied = false
		}, RollbackCommandIssued},
		{"durable ack with command bookkeeping lost", func(e *RollbackEvidence) {
			// The command-issued bit was lost, but a durable ack proves the command
			// reached this worker. A durable ack alone must hold at CommandIssued —
			// never fall through to NotApplied and enable rebind/retry.
			e.CommandIssued = false
			e.ApplyStatusKnown = true
			e.DurableAckPresent = true
			e.LocallyApplied = false
		}, RollbackCommandIssued},
		{"partial local apply", func(e *RollbackEvidence) {
			e.CommandIssued = true
			e.LocallyApplied = true
			e.ManifestCommitted = false
		}, RollbackPartialLocalApply},
		{"manifest committed", func(e *RollbackEvidence) { e.ManifestCommitted = true }, RollbackManifestCommitted},
		{"manifest dominates partial", func(e *RollbackEvidence) {
			e.LocallyApplied = true
			e.ManifestCommitted = true
		}, RollbackManifestCommitted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := rollbackEvidenceFixture()
			tc.mutate(&ev)
			if got := ClassifyRollback(ev); got != tc.want {
				t.Fatalf("ClassifyRollback = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestDecideRollback_NotApplied_AllowsRebind(t *testing.T) {
	ev := rollbackEvidenceFixture()
	ev.ApplyStatusKnown = true
	d := DecideRollback(ev)
	if d.Action != RollbackActionRebindRetry || !d.MayRebindAsUnexecuted || d.ExcludeFromReadSet {
		t.Fatalf("not-applied must allow rebind: %+v", d)
	}
}

func TestDecideRollback_CommandIssued_QueryHold(t *testing.T) {
	ev := rollbackEvidenceFixture()
	ev.CommandIssued = true
	d := DecideRollback(ev)
	if d.Action != RollbackActionQueryHold || d.MayRebindAsUnexecuted {
		t.Fatalf("command-issued must query/hold, never rebind: %+v", d)
	}
}

// TestDecideRollback_PartialApply_ExcludesNeverRebinds is the headline invariant:
// a partial local apply excludes + repairs and must never be rebound as
// un-executed (design section 4.5).
func TestDecideRollback_PartialApply_ExcludesNeverRebinds(t *testing.T) {
	ev := rollbackEvidenceFixture()
	ev.CommandIssued = true
	ev.LocallyApplied = true
	d := DecideRollback(ev)
	if d.Action != RollbackActionExcludeAndRepair || !d.ExcludeFromReadSet || !d.RepairRequired || d.MayRebindAsUnexecuted {
		t.Fatalf("partial apply must exclude+repair and never rebind: %+v", d)
	}
}

func TestDecideRollback_ManifestCommitted_Done(t *testing.T) {
	ev := rollbackEvidenceFixture()
	ev.ManifestCommitted = true
	d := DecideRollback(ev)
	if d.Action != RollbackActionDone || d.RepairRequired || d.MayRebindAsUnexecuted {
		t.Fatalf("committed must be Done, no repair/rebind: %+v", d)
	}
}

func TestDecideRollback_RepairScopeIsDefensiveCopy(t *testing.T) {
	ev := rollbackEvidenceFixture()
	ev.CommandIssued = true
	ev.LocallyApplied = true
	d := DecideRollback(ev)
	if len(d.RepairScope) != 1 {
		t.Fatalf("repair scope: %+v", d.RepairScope)
	}
	d.RepairScope[0].PartitionID = "mutated"
	if ev.AffectedPartitions[0].PartitionID != "p1" {
		t.Fatal("RepairScope must be a defensive copy of the evidence partitions")
	}
}

// TestDecideRollback_MayRebindOnlyForNotApplied locks the invariant matrix: only
// RollbackNotApplied permits rebind-as-un-executed.
func TestDecideRollback_MayRebindOnlyForNotApplied(t *testing.T) {
	build := func(f func(*RollbackEvidence)) RollbackDecision {
		ev := rollbackEvidenceFixture()
		f(&ev)
		return DecideRollback(ev)
	}
	states := []struct {
		name   string
		mutate func(*RollbackEvidence)
		state  RollbackState
		rebind bool
	}{
		{"not applied", func(e *RollbackEvidence) { e.ApplyStatusKnown = true }, RollbackNotApplied, true},
		{"command issued", func(e *RollbackEvidence) { e.CommandIssued = true }, RollbackCommandIssued, false},
		{"durable ack, command bit lost", func(e *RollbackEvidence) { e.DurableAckPresent = true; e.ApplyStatusKnown = true }, RollbackCommandIssued, false},
		{"partial apply", func(e *RollbackEvidence) { e.CommandIssued = true; e.LocallyApplied = true }, RollbackPartialLocalApply, false},
		{"committed", func(e *RollbackEvidence) { e.ManifestCommitted = true }, RollbackManifestCommitted, false},
	}
	for _, s := range states {
		t.Run(s.name, func(t *testing.T) {
			d := build(s.mutate)
			if d.State != s.state {
				t.Fatalf("state = %s, want %s", d.State, s.state)
			}
			if d.MayRebindAsUnexecuted != s.rebind {
				t.Fatalf("%s: MayRebindAsUnexecuted = %v, want %v", s.state, d.MayRebindAsUnexecuted, s.rebind)
			}
		})
	}
}

func TestRollbackState_String(t *testing.T) {
	for _, s := range []RollbackState{RollbackNotApplied, RollbackCommandIssued, RollbackPartialLocalApply, RollbackManifestCommitted} {
		if s.String() == "" || s.String() == "Unknown" {
			t.Fatalf("state %d needs a stable string", s)
		}
	}
	if RollbackState(99).String() != "Unknown" {
		t.Fatal("out-of-range state must be Unknown")
	}
}

func TestRollbackAction_String(t *testing.T) {
	for _, a := range []RollbackAction{RollbackActionRebindRetry, RollbackActionQueryHold, RollbackActionExcludeAndRepair, RollbackActionDone} {
		if a.String() == "" || a.String() == "Unknown" {
			t.Fatalf("action %d needs a stable string", a)
		}
	}
}

// --- Gated: driving the real exclude+repair needs the absent C2 seam. ---

func TestMutationRepairDriver_DriveExcludeAndRepair(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion read-set exclude+repair seam lands")
}
