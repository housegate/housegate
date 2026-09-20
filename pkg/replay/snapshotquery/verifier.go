package snapshotquery

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
)

// VerifyRequest is in-process only. The trusted lifecycle owner supplies its
// registered/admitted/retained reference for THIS invocation. Neither reference
// spelling nor historical facts confer use authority; B4 independently checks
// the live registry before Open. The outer owner retains CloseUse/Release duties.
type VerifyRequest struct {
	Job         replay.SnapshotQueryJob `json:"-"`
	ReferenceID string                  `json:"-"`
}

// VerifierOptions borrows fixed trusted dependencies; it transfers no lifecycle
// ownership. Real authenticated history, current use and committed source-claim
// provenance remain obligations of the configured C1/C5 and B4 adapters.
type VerifierOptions struct {
	Dispatcher       *CompositeExecutor
	Signer           replay.Signer
	HistoricalPolicy HistoricalPolicy
}

// Verifier signs applied evidence only. It never retries, creates references,
// authorizes release or converts execution failures into abort evidence.
type Verifier struct {
	dispatcher      *CompositeExecutor
	signer          replay.Signer
	policy          HistoricalPolicy
	verifySignature statementV3SignatureVerifier
}
type statementV3SignatureVerifier func(string, auth.JWSStatementPayloadV3) (string, error)

func NewVerifier(opts VerifierOptions) (*Verifier, error) {
	return newVerifier(opts, auth.VerifyStatementV3Signature)
}
func newVerifier(opts VerifierOptions, verify statementV3SignatureVerifier) (*Verifier, error) {
	if opts.Dispatcher == nil || len(opts.Dispatcher.queryByProfile) == 0 || isNil(opts.Signer) || isNil(opts.HistoricalPolicy) || verify == nil {
		return nil, fmt.Errorf("query routes, signer and historical policy are required")
	}
	// Freeze the dispatcher value as well: a caller may replace its exported
	// struct wholesale. Its private map was already copied and has no mutator.
	dispatcher := *opts.Dispatcher
	return &Verifier{dispatcher: &dispatcher, signer: opts.Signer, policy: opts.HistoricalPolicy, verifySignature: verify}, nil
}

func (v *Verifier) Verify(ctx context.Context, req VerifyRequest) (replay.SnapshotQueryAttestation, error) {
	if v == nil || v.dispatcher == nil || len(v.dispatcher.queryByProfile) == 0 || isNil(v.signer) || isNil(v.policy) || v.verifySignature == nil {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("uninitialized snapshot query verifier")
	}
	if ctx == nil {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return replay.SnapshotQueryAttestation{}, err
	}
	if strings.TrimSpace(req.ReferenceID) == "" {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("query invocation reference is required")
	}
	job := cloneJob(req.Job)
	if err := validateJob(job); err != nil {
		return replay.SnapshotQueryAttestation{}, err
	}
	in := job.Statement.Envelope.Input
	// validateJob recomputes the complete read/input/statement bindings. Retain
	// independently computed roots, never token-decoded or caller-asserted roots.
	// verifyEnvelope threads v.verifySignature through so the injectable test
	// seam (see newVerifier) still observes every signature-verification call.
	_, inputRoot, err := verifyEnvelope(job.Statement.Envelope, v.verifySignature)
	if err != nil {
		return replay.SnapshotQueryAttestation{}, err
	}
	statementRoot, err := replay.SnapshotQueryStatementRoot(job.Statement)
	if err != nil {
		return replay.SnapshotQueryAttestation{}, err
	}
	if err = ctx.Err(); err != nil {
		return replay.SnapshotQueryAttestation{}, err
	}
	decision, err := v.policy.VerifyReservation(ctx, cloneJob(job))
	if err != nil {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("historical query policy: %w", err)
	}
	if err = decision.CheckJob(job); err != nil {
		return replay.SnapshotQueryAttestation{}, err
	}
	claimPartitions, err := validateConsumerClaim(job)
	if err != nil {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("query claim: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return replay.SnapshotQueryAttestation{}, err
	}
	execJob := cloneJob(job)
	result, err := v.dispatcher.Replay(ctx, replay.ExecutionRequest{SnapshotQuery: &execJob, SnapshotQueryReferenceID: req.ReferenceID})
	// Error is not quiescence. B4/outer owners retain any uncertain handles; in
	// particular an independent output failure never resurrects a closed S lease.
	if err != nil {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("execute snapshot query: %w", err)
	}
	result = cloneExecutionResult(result)
	resultPartitions, err := validateConsumerResult(job, result)
	if err != nil {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("query result: %w", err)
	}
	evidence, claim := result.SnapshotQuery, job.SourceClaim
	match := result.ComputedStateRoot == claim.ComputedStateRoot && evidence.OutputRowCount == claim.OutputRowCount && evidence.OutputRowsRoot == claim.OutputRowsRoot && reflect.DeepEqual(resultPartitions, claimPartitions)
	b := in.Binding
	receipt := replay.SnapshotQueryReceipt{
		BlockSeq:                  job.BlockSeq,
		StatementRoot:             statementRoot,
		InputRoot:                 inputRoot,
		ReadSetRoot:               b.ReadSetRoot,
		ReadSnapshot:              b.ReadSnapshot,
		SchemaSnapshotID:          job.SchemaSnapshotID,
		ExecutorProfileID:         job.ExecutorProfileID,
		QueryProfileID:            job.QueryProfileID,
		ReservationID:             b.ReservationID,
		FencingGeneration:         b.FencingGeneration,
		ExecutionOutcome:          "applied",
		OutputRowCount:            evidence.OutputRowCount,
		OutputRowsRoot:            evidence.OutputRowsRoot,
		SourceClaimRoot:           job.SourceClaimRoot,
		ComputedStateRoot:         result.ComputedStateRoot,
		MatchSourceRoot:           match,
		PartitionCommitmentsAfter: resultPartitions,
		AffectedParts:             result.AffectedParts,
		ReplayLogHash:             result.ReplayLogHash,
	}
	hash, err := receipt.Hash()
	if err != nil {
		return replay.SnapshotQueryAttestation{}, err
	}
	if err = ctx.Err(); err != nil {
		return replay.SnapshotQueryAttestation{}, err
	}
	replica, sig, err := v.signer.SignReplayReceipt(ctx, hash)
	if err != nil {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("sign query receipt: %w", err)
	}
	if strings.TrimSpace(replica) == "" || strings.TrimSpace(sig) == "" {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("signer returned blank replica id or signature")
	}
	if err = ctx.Err(); err != nil {
		return replay.SnapshotQueryAttestation{}, err
	}
	return replay.SnapshotQueryAttestation{ReplicaID: replica, Receipt: receipt, ReceiptHash: hash, Signature: sig}, nil
}

func validateConsumerClaim(job replay.SnapshotQueryJob) ([]replay.PartitionCommitment, error) {
	c := job.SourceClaim
	if c == nil || !canonicalDigest(job.SourceClaimRoot) {
		return nil, fmt.Errorf("complete source claim is required")
	}
	b := job.Statement.Envelope.Input.Binding
	if strings.TrimSpace(c.SourceNode) == "" || c.StatementID != b.StatementID || c.StatementSeq != job.Statement.StatementSeq || c.BlockSeq != job.BlockSeq || c.InputRoot != job.Statement.Envelope.InputRoot || c.ReservationID != b.ReservationID || c.FencingGeneration != b.FencingGeneration || c.ExecutionOutcome != "applied" {
		return nil, fmt.Errorf("applied claim identity mismatch")
	}
	if !canonicalDigest(c.ComputedStateRoot) || !canonicalDigest(c.OutputRowsRoot) {
		return nil, fmt.Errorf("invalid claim state or output root")
	}
	if _, err := consumerPartitions(c.PartitionDeltas); err != nil {
		return nil, err
	}
	partitions, err := consumerPartitions(c.PartitionCommitmentsAfter)
	if err != nil {
		return nil, err
	}
	for _, p := range c.CandidateParts {
		if !canonicalDigest(p.PartPhysHash) || !canonicalLtHash(p.PartRowLtHash) {
			return nil, fmt.Errorf("invalid candidate part commitment")
		}
	}
	// Hash validates all required part identities and duplicate commitments. It
	// binds the complete claim; C1/current-use must authenticate its commitment.
	hash, err := c.Hash()
	if err != nil {
		return nil, err
	}
	if hash != job.SourceClaimRoot {
		return nil, fmt.Errorf("source claim root mismatch")
	}
	return partitions, nil
}

func validateConsumerResult(job replay.SnapshotQueryJob, r replay.ExecutionResult) ([]replay.PartitionCommitment, error) {
	if r.BlockSeq != job.BlockSeq || r.PrevSafeSnapshotID != job.PrevSafeSnapshotID || r.PrevStateRoot != job.PrevStateRoot || r.SchemaSnapshotID != job.SchemaSnapshotID || r.ExecutorProfileID != job.ExecutorProfileID {
		return nil, fmt.Errorf("execution identity mismatch")
	}
	if r.SnapshotQuery == nil || r.SnapshotQuery.ExecutionOutcome != "applied" || !canonicalDigest(r.SnapshotQuery.OutputRowsRoot) || !canonicalDigest(r.ComputedStateRoot) || !canonicalDigest(r.ReplayLogHash) {
		return nil, fmt.Errorf("invalid applied execution evidence")
	}
	partitions, err := consumerPartitions(r.PartitionCommitmentsAfter)
	if err != nil {
		return nil, err
	}
	parts := make([]replay.SnapshotReadPart, 0, len(r.AffectedParts))
	for _, p := range r.AffectedParts {
		if !canonicalDigest(p.PartPhysHash) || !canonicalLtHash(p.PartRowLtHash) {
			return nil, fmt.Errorf("invalid affected part commitment")
		}
		parts = append(parts, replay.SnapshotReadPart{TableID: p.TableID, PartitionID: p.PartitionID, PartName: p.PartName, PartPhysHash: p.PartPhysHash, PartRowLtHash: p.PartRowLtHash, RowCount: p.RowCount, Bytes: p.Bytes})
	}
	// Reuse the existing canonical identity/duplicate validator, without a new
	// digest/domain or comparing independently produced physical part names.
	if _, err = (replay.SnapshotQueryClaim{CandidateParts: parts}).Hash(); err != nil {
		return nil, err
	}
	return partitions, nil
}

func consumerPartitions(in []replay.PartitionCommitment) ([]replay.PartitionCommitment, error) {
	out := append([]replay.PartitionCommitment{}, in...)
	seen := make(map[[2]string]bool, len(out))
	for _, p := range out {
		key := [2]string{p.TableID, p.PartitionID}
		if strings.TrimSpace(p.TableID) == "" || strings.TrimSpace(p.PartitionID) == "" || !canonicalLtHash(p.Root) || seen[key] {
			return nil, fmt.Errorf("invalid or duplicate partition commitment")
		}
		seen[key] = true
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TableID != out[j].TableID {
			return out[i].TableID < out[j].TableID
		}
		return out[i].PartitionID < out[j].PartitionID
	})
	return out, nil
}
func canonicalLtHash(s string) bool {
	if len(s) != 2+2*lthash.Size || !strings.HasPrefix(s, "0x") {
		return false
	}
	for _, c := range s[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
