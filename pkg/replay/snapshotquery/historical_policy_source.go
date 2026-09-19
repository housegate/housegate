package snapshotquery

import (
	"context"
	"fmt"

	"github.com/housegate/housegate/pkg/replay"
)

// historicalDecisionSource is the deliberately narrow C1 injection seam. A
// production implementation owns its endpoint authentication, role check,
// leader barrier and proof verification. It is intentionally package-private:
// this package does not provide a transport, a protocol, or runtime wiring.
type historicalDecisionSource interface {
	// loadAuthenticatedHistoricalDecision returns records only after the source
	// itself has authenticated its endpoint, accepted its authorized historical
	// role, crossed its leader/read barrier, and verified the committed proof.
	// Missing history and every failed gate are errors; there is no successful
	// "not found" or caller-provided readiness/policy result.
	loadAuthenticatedHistoricalDecision(context.Context, historicalDecisionRequest) (HistoricalDecisionRecords, error)
}

// historicalDecisionRequest gives a source an immutable copy of the exact job
// identity and the expected policy scope. The accessors copy the job again so a
// source cannot retain or mutate verifier-owned state.
type historicalDecisionRequest struct {
	job               replay.SnapshotQueryJob
	networkID         string
	keeperShardID     uint32
	executorProfileID string
	queryProfileID    string
}

func (r historicalDecisionRequest) Job() replay.SnapshotQueryJob { return cloneJob(r.job) }
func (r historicalDecisionRequest) NetworkID() string            { return r.networkID }
func (r historicalDecisionRequest) KeeperShardID() uint32        { return r.keeperShardID }
func (r historicalDecisionRequest) ExecutorProfileID() string    { return r.executorProfileID }
func (r historicalDecisionRequest) QueryProfileID() string       { return r.queryProfileID }

// authenticatedHistoricalPolicy turns one fully authenticated C1 source into
// the existing HistoricalPolicy decision boundary. It has no current-use or
// readiness authority and is not installed by any default construction path.
type authenticatedHistoricalPolicy struct{ source historicalDecisionSource }

func newAuthenticatedHistoricalPolicy(source historicalDecisionSource) (HistoricalPolicy, error) {
	if isNil(source) {
		return nil, fmt.Errorf("historical decision source is required")
	}
	return &authenticatedHistoricalPolicy{source: source}, nil
}

func (p *authenticatedHistoricalPolicy) VerifyReservation(ctx context.Context, job replay.SnapshotQueryJob) (HistoricalDecision, error) {
	if p == nil || isNil(p.source) {
		return HistoricalDecision{}, fmt.Errorf("historical decision source is required")
	}
	if ctx == nil {
		return HistoricalDecision{}, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return HistoricalDecision{}, err
	}

	// Keep one private verifier copy. The source receives a separate copy below.
	// In particular a source cannot make an unverified response match by
	// mutating the caller's job through a retained slice or source claim.
	verifiedJob := cloneJob(job)
	if err := validateJob(verifiedJob); err != nil {
		return HistoricalDecision{}, err
	}
	binding := verifiedJob.Statement.Envelope.Input.Binding
	request := historicalDecisionRequest{
		job:               cloneJob(verifiedJob),
		networkID:         binding.NetworkID,
		keeperShardID:     binding.KeeperShardID,
		executorProfileID: binding.ExecutorProfileID,
		queryProfileID:    binding.QueryProfileID,
	}
	records, err := p.source.loadAuthenticatedHistoricalDecision(ctx, request)
	if err != nil {
		return HistoricalDecision{}, fmt.Errorf("load authenticated historical decision: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return HistoricalDecision{}, err
	}

	// HistoricalDecision remains the sole record-to-decision authority. Copy the
	// result at this boundary even though its constructor currently retains no
	// slices, so future record fields cannot accidentally share source storage.
	records = cloneHistoricalDecisionRecords(records)
	decision, err := NewHistoricalDecisionFromVerifiedRecords(verifiedJob, records)
	if err != nil {
		return HistoricalDecision{}, fmt.Errorf("authenticated historical records: %w", err)
	}
	return decision, nil
}

func cloneHistoricalDecisionRecords(records HistoricalDecisionRecords) HistoricalDecisionRecords {
	records.Artifacts.Parts = copySlice(records.Artifacts.Parts)
	return records
}
