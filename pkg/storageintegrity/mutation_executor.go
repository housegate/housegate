package storageintegrity

import (
	"context"
	"fmt"
)

// ScratchReplayResult is what a MutationScratchExecutor returns after cloning
// the frozen manifest base into a per-worker scratch, executing the mutation,
// and reading back the post-state (design section 4.6 steps 4-6).
// ScratchInitialBaseRoots is the recomputed commitment of the freshly attached
// scratch, which the executor verifies equals the task's manifest base roots
// (step 4) before trusting the post-state.
type ScratchReplayResult struct {
	MutationID               string
	WorkerID                 string
	ScratchInitialBaseRoots  []PartitionCommitment
	PostStateRoot            string
	PostPartitionCommitments []PartitionCommitment
	PartitionDeltas          []PartitionDelta
	RowsBefore               uint64
	RowsAfter                uint64
}

// MutationScratchExecutor is the gated port that performs the real per-worker
// local replay: it clones the frozen manifest base into
// hg_mutation.<table>__<worker> via ATTACH PARTITION FROM hg_safe, executes the
// mutation, waits for system.mutations completion, and recomputes the post
// commitments (design section 4.6). No real implementation exists — it needs a
// live ClickHouse and the versioned P2 executor profile; see
// CompanionMutationConsensusAvailable.
type MutationScratchExecutor interface {
	CloneExecuteAndReadback(ctx context.Context, task MutationTask, workerID string) (ScratchReplayResult, error)
}

// MutationExecutor drives the section-4.6 per-worker replay for one worker over
// the scratch-executor and signer ports. It holds no ClickHouse or Arbiter state
// of its own and produces only a signed per-worker claim; it does not group
// claims, decide a quorum, or publish a manifest (those are Arbiter FSM work).
type MutationExecutor struct {
	scratch  MutationScratchExecutor
	signer   MutationClaimSigner
	workerID string
}

// NewMutationExecutor constructs the executor for one worker.
func NewMutationExecutor(scratch MutationScratchExecutor, signer MutationClaimSigner, workerID string) *MutationExecutor {
	return &MutationExecutor{scratch: scratch, signer: signer, workerID: workerID}
}

// Execute runs the clone/execute/readback via the scratch port, verifies the
// scratch initial commitment equals the frozen manifest base root (fail closed
// on mismatch), assembles the complete equality-key claim, and signs it. It
// returns an error — and no claim — on any local coordination or verification
// failure; it never fabricates a claim.
func (e *MutationExecutor) Execute(ctx context.Context, task MutationTask) (SignedMutationClaim, error) {
	if e.scratch == nil {
		return SignedMutationClaim{}, fmt.Errorf("mutation executor: no scratch executor wired")
	}
	res, err := e.scratch.CloneExecuteAndReadback(ctx, task, e.workerID)
	if err != nil {
		return SignedMutationClaim{}, fmt.Errorf("mutation executor: scratch replay for %s: %w", task.MutationID, err)
	}
	if err := verifyScratchBaseRoots(task, res); err != nil {
		return SignedMutationClaim{}, fmt.Errorf("mutation executor: %w", err)
	}
	claim, err := AssembleMutationClaim(task, res, e.workerID)
	if err != nil {
		return SignedMutationClaim{}, err
	}
	return SignAssembledClaim(ctx, e.signer, claim)
}

// verifyScratchBaseRoots checks the scratch's recomputed initial base roots
// match the task's frozen manifest base roots exactly — every affected partition
// present, equal, and no extra (design section 4.6 step 4: the scratch initial
// commitment must equal the manifest base root before the post-state is
// trusted). It is pure and fails closed on any mismatch.
func verifyScratchBaseRoots(task MutationTask, res ScratchReplayResult) error {
	want := map[string]string{}
	for _, c := range task.BasePartitionRoots {
		want[c.TableID+"/"+c.PartitionID] = c.Commitment
	}
	got := map[string]string{}
	for _, c := range res.ScratchInitialBaseRoots {
		got[c.TableID+"/"+c.PartitionID] = c.Commitment
	}
	if len(want) != len(got) {
		return fmt.Errorf("scratch base-root set size mismatch: manifest has %d, scratch has %d", len(want), len(got))
	}
	for key, w := range want {
		g, ok := got[key]
		if !ok {
			return fmt.Errorf("scratch missing base root for %s", key)
		}
		if g != w {
			return fmt.Errorf("scratch base root for %s = %q, manifest = %q", key, g, w)
		}
	}
	return nil
}
