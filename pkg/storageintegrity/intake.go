package storageintegrity

import (
	"bytes"
	"context"
	"fmt"
	"sync"
)

// CompanionStagedIntakeAvailable reports whether the Sentio companion topology
// exposes the staged-prepare seam this orchestrator drives: the SNode-side
// PrepareLocalStatement / RegisterPreparedClaim / AbortPreparedStatement split
// described in design section 3.2.
//
// It is false today. The companion repos expose only ArbiterIngress
// .SubmitStatement and SourceClaims.RegisterResultClaim, and SNode local intake
// is the single-shot in-process snode.Role.SubmitLocalStatement, which performs
// the unsafe write and registers the result claim in one call and cannot be
// split into prepare / register-later / abort. Until the companion seam lands,
// no real StatementSubmitter/SourcePreparer implementation exists, and the
// end-to-end intake path is not wired. This constant is the single, honest gate
// that flips to true when a real adapter is implemented against the companion
// seam; the orchestration contract tests read it so a red run is never mistaken
// for green.
const CompanionStagedIntakeAvailable = false

// Kind mirrors the storage-integrity statement kind admitted by the ingress
// plugin. It intentionally uses the same string values as
// pkg/plugins/storageintegrity so the two packages agree without the core
// package importing the plugin (which would be an import cycle).
type Kind string

const (
	KindInsert Kind = "INSERT"
	KindUpdate Kind = "UPDATE"
	KindDelete Kind = "DELETE"
)

// AdmissionRecord is the completed, input-bound admission the intake
// orchestrator consumes. It is the HouseGate-core projection of the ingress
// plugin's Admission: statement identity, admitted kind, logical target table,
// signed SQL, signer, and the exact captured payload with its content-addressed
// hash, length, pinned client revision, and encoding. The plugin maps its
// Admission into this record in a later wiring PR; the core package stays free
// of any plugin dependency.
type AdmissionRecord struct {
	StatementID     string
	Kind            Kind
	TableID         string
	SQL             string
	SQLHash         string
	Signer          string
	UserJWS         string
	Payload         []byte
	PayloadLength   uint64
	PayloadHash     string
	PayloadEncoding string
	Revision        int
}

// StatementEnvelope is the HouseGate-core mirror of the frozen source statement
// envelope. The same envelope value feeds both PrepareLocalStatement and
// SubmitStatement, so the payload identity a source claim binds is byte
// identical to the identity the Arbiter sequences. It is a comparable value
// type on purpose: EnvelopeFromAdmission is deterministic for a given
// admission, and the orchestrator relies on that.
type StatementEnvelope struct {
	StatementID   string
	StatementKind Kind
	SQL           string
	SQLHash       string
	TargetTableID string
	PayloadRef    string
	PayloadHash   string
	PayloadLength uint64
	// PayloadEncoding and Revision together pin how the payload is decoded. The
	// captured client protocol Revision is authenticated per statement and is
	// required to decode the Native block (see DecodeNativePayload, which rejects
	// revision 0); a source preparer decodes with exactly this revision rather
	// than guessing or configuring one, so a client that negotiated a different
	// revision cannot silently produce a different materialization.
	PayloadEncoding string
	Revision        int
	Signer          string
	UserJWS         string
}

// EnvelopeFromAdmission builds the source statement envelope from a completed
// admission. It is a pure HouseGate-local derivation and does not depend on the
// companion seam. An INSERT admission without a captured payload, or an
// admission without a statement id or target table, is rejected fail closed.
func EnvelopeFromAdmission(adm AdmissionRecord) (StatementEnvelope, error) {
	if adm.StatementID == "" {
		return StatementEnvelope{}, fmt.Errorf("intake: admission has no statement id")
	}
	if adm.TableID == "" {
		return StatementEnvelope{}, fmt.Errorf("intake: admission %s has no target table id", adm.StatementID)
	}
	switch adm.Kind {
	case KindInsert, KindUpdate, KindDelete:
	default:
		return StatementEnvelope{}, fmt.Errorf("intake: admission %s has unsupported kind %q", adm.StatementID, adm.Kind)
	}
	if adm.Kind == KindInsert {
		if len(adm.Payload) == 0 || adm.PayloadLength == 0 {
			return StatementEnvelope{}, fmt.Errorf("intake: INSERT admission %s has no captured payload", adm.StatementID)
		}
		if adm.PayloadHash == "" {
			return StatementEnvelope{}, fmt.Errorf("intake: INSERT admission %s has no payload hash", adm.StatementID)
		}
		// A payload-bearing admission must pin the client protocol revision used
		// to decode it; without it a source preparer would have to guess a
		// revision, breaking the exact-payload contract. This matches
		// DecodeNativePayload, which rejects revision 0.
		if adm.Revision == 0 {
			return StatementEnvelope{}, fmt.Errorf("intake: INSERT admission %s has no client protocol revision", adm.StatementID)
		}
	}
	return StatementEnvelope{
		StatementID:     adm.StatementID,
		StatementKind:   adm.Kind,
		SQL:             adm.SQL,
		SQLHash:         adm.SQLHash,
		TargetTableID:   adm.TableID,
		PayloadRef:      adm.PayloadHash, // content-addressed: the ref is the payload hash
		PayloadHash:     adm.PayloadHash,
		PayloadLength:   adm.PayloadLength,
		PayloadEncoding: adm.PayloadEncoding,
		Revision:        adm.Revision,
		Signer:          adm.Signer,
		UserJWS:         adm.UserJWS,
	}, nil
}

// sameStatement reports whether a later call for an already-open statement id
// presents the identical statement. The envelope is a comparable value type, so
// == covers every signed/routed field (SQL, target, kind, payload identity,
// signer, JWS). The raw payload bytes are compared too, so a caller cannot keep
// the envelope's payload hash while swapping the underlying bytes.
func sameStatement(a StatementEnvelope, aAdm AdmissionRecord, b StatementEnvelope, bAdm AdmissionRecord) bool {
	return a == b && bytes.Equal(aAdm.Payload, bAdm.Payload)
}

// OutcomeCategory classifies a SubmitStatement or RegisterResultClaim result
// per design section 3.4. Only the success/exact-idempotent categories may
// reach ACK2; only a terminal reject may abort candidate parts.
type OutcomeCategory int

const (
	OutcomeUnspecified OutcomeCategory = iota
	// OutcomeAccepted is a fresh accept (statement sequenced / claim bound).
	OutcomeAccepted
	// OutcomeExactIdempotent is a byte-identical re-ack / re-bind of the same
	// envelope or RC.
	OutcomeExactIdempotent
	// OutcomeRetryable is NotLeader, transient unavailability, or an explicit
	// retryable error: no ACK2, no cleanup, reuse the record and retry.
	OutcomeRetryable
	// OutcomeUnknown is a timeout or broken connection with indeterminate
	// server state: no ACK2, no cleanup, converge by query/idempotent re-send.
	OutcomeUnknown
	// OutcomeTerminalReject is a conflicting duplicate, source mismatch,
	// malformed statement, gap-budget exceeded, etc.: no ACK2, exact cleanup.
	OutcomeTerminalReject
)

// PermitsAck2 reports whether this category, on its own, keeps ACK2 reachable.
// It does not itself grant ACK2 — both gates plus the RCBound lifecycle are
// required (see Orchestrator.Orchestrate).
func (c OutcomeCategory) PermitsAck2() bool {
	return c == OutcomeAccepted || c == OutcomeExactIdempotent
}

// RequiresAbort reports whether this category requires exact-candidate cleanup.
// Only a terminal reject does; retryable and unknown outcomes must never clean.
func (c OutcomeCategory) RequiresAbort() bool {
	return c == OutcomeTerminalReject
}

// SubmitOutcome is the Arbiter SubmitStatement result.
type SubmitOutcome struct {
	Category OutcomeCategory
	Reason   string
}

// ClaimOutcome is the SNode RegisterResultClaim (RC-binding) result. BoundSource
// is the deterministic source_node the FSM recorded for the statement; it must
// match the source the prepare produced.
type ClaimOutcome struct {
	Category    OutcomeCategory
	BoundSource string
	Reason      string
}

// Lifecycle is the crash-safe intake journal lifecycle (design section 3.4).
type Lifecycle string

const (
	LifecyclePreparing      Lifecycle = "Preparing"
	LifecycleUnsafeWritten  Lifecycle = "UnsafeWritten"
	LifecycleSubmitAccepted Lifecycle = "SubmitAccepted"
	LifecycleRCBound        Lifecycle = "RCBound"
	LifecycleAck2           Lifecycle = "Ack2"
	LifecycleAbortPending   Lifecycle = "AbortPending"
	LifecycleCleaned        Lifecycle = "Cleaned"
)

// PreparedLocalResult is what a durable PrepareLocalStatement returns: the exact
// candidate parts and per-partition new-part sums that the result claim binds,
// the frozen source-claim root, and the payload identity the source persisted.
// The orchestrator checks this payload identity against the admission and the
// source_node against the expected deterministic source before it will ACK2.
type PreparedLocalResult struct {
	StatementID          string
	SourceNode           string
	PayloadRef           string
	PayloadHash          string
	PayloadLength        uint64
	PayloadEncoding      string
	Revision             int // the client protocol revision the source decoded the payload with
	CandidateParts       []CandidatePart
	PartitionNewPartSums []PartitionLtHashSum
	SourceClaimRoot      string
	Lifecycle            Lifecycle
}

// CandidatePart mirrors the frozen source candidate-part shape. It is the exact
// unit of terminal-reject cleanup: abort drops these part names and nothing
// else.
type CandidatePart struct {
	TableID       string
	PartitionID   string
	PartName      string
	PartRowLtHash string
	RowCount      uint64
	Bytes         uint64
}

// PartitionLtHashSum mirrors the frozen per-partition new-parts LtHash sum.
type PartitionLtHashSum struct {
	TableID           string
	PartitionID       string
	NewPartsLtHashSum string
}

// StatementSubmitter is the Arbiter route-A sequencing port. The real
// implementation calls ArbiterIngress.SubmitStatement. No such implementation
// exists in HouseGate today; see CompanionStagedIntakeAvailable.
type StatementSubmitter interface {
	SubmitStatement(ctx context.Context, env StatementEnvelope) (SubmitOutcome, error)
}

// SourcePreparer is the selected-SNode staged-prepare port (design section 3.2).
// The real implementation drives the companion PrepareLocalStatement /
// RegisterPreparedClaim / AbortPreparedStatement seam, which does not exist yet;
// see CompanionStagedIntakeAvailable. HouseGate must never fabricate this seam
// with a local mock shape.
type SourcePreparer interface {
	// PrepareLocalStatement durably stages the local unsafe write on the
	// selected source and returns the exact candidate inventory and RC inputs.
	// The source performs payload hash/length verification and the durable
	// PayloadStore.Put before the unsafe INSERT, so a failed Put never produces
	// an hg_unsafe part.
	PrepareLocalStatement(ctx context.Context, env StatementEnvelope, payload []byte) (PreparedLocalResult, error)
	// RegisterPreparedClaim late-binds the prepared result claim by statement
	// id, triggering the source's RegisterResultClaim to the Arbiter. The
	// orchestrator calls it only after SubmitStatement is accepted.
	RegisterPreparedClaim(ctx context.Context, statementID string) (ClaimOutcome, error)
	// AbortPreparedStatement performs exact-candidate cleanup for a terminally
	// rejected statement: it persists AbortPending, excludes the local sums, and
	// idempotently drops the exact candidate part names. It never drops a whole
	// partition. HouseGate hands it the exact frozen candidate parts (from the
	// journal's PreparedLocalResult), so the cleanup surface is bounded by
	// HouseGate's record, not inferred source-side by statement_id — a part not
	// present is treated as already cleaned, and an empty set is a no-op cleanup.
	AbortPreparedStatement(ctx context.Context, statementID string, parts []CandidatePart, reason string) error
}

// OrchestratorConfig pins the deterministic source the FSM is expected to record
// for this HouseGate's statements. A committed-source mismatch fails closed.
type OrchestratorConfig struct {
	ExpectedSource string
}

// IntakeResult is the orchestration outcome for one admission.
type IntakeResult struct {
	StatementID string
	Ack2        bool
	Lifecycle   Lifecycle
	Submit      SubmitOutcome
	Claim       ClaimOutcome
	Prepared    PreparedLocalResult

	// Reason, when non-empty on a non-ACK2 result, names why ACK2 was withheld
	// at the dual gate. It is diagnostic only; the protocol outcome is carried by
	// Submit/Claim and Ack2.
	Reason string

	// terminal marks an outcome that will not change on retry (ACK2, or a
	// successfully cleaned abort). Only terminal outcomes are recorded for
	// idempotent replay; retryable/unknown outcomes and failed aborts are not,
	// so they reconverge on a later call.
	terminal bool
}

// Orchestrator runs the staged intake for one admission: it starts
// PrepareLocalStatement and SubmitStatement in parallel, and registers the
// result claim only after SubmitStatement is accepted (the RC gate). It is pure
// HouseGate-local coordination over the two ports; it holds no ClickHouse,
// payload-store, journal, or Arbiter state of its own. Persistent intake
// journaling, ACK2 delivery wiring, retryable/unknown convergence, and crash
// recovery arrive in later PRs.
type Orchestrator struct {
	submitter StatementSubmitter
	preparer  SourcePreparer
	cfg       OrchestratorConfig

	// querier is the optional status-query port (PR05). When non-nil, an
	// indeterminate (Unknown) submit/RC outcome is converged by querying the
	// server by statement_id before any idempotent re-send (design section 3.4's
	// "先按同一 statement_id 查询" branch). When nil, an Unknown outcome falls
	// back to the design's equally-permitted "或幂等重试" branch: an idempotent
	// re-send that never repeats the unsafe write. See intake_retry.go.
	querier IntakeStatusQuerier

	mu        sync.Mutex
	records   map[string]*intakeRecord   // statement_id -> cross-call intake state
	frontiers map[string]*sourceFrontier // frontier key (source) -> serial gate
}

// intakeRecord is the cross-call coordination state for one statement id. It
// deduplicates concurrent attempts (active + done), caches a successful prepare
// so a retry reuses it instead of re-running the unsafe write, records the
// lifecycle stage that reached durability so a retry resumes from the right
// point, and caches a terminal outcome for idempotent replay. All fields are
// guarded by Orchestrator.mu except res/err, which follow the usual
// write-before-close(done) / read-after-<-done happens-before.
type intakeRecord struct {
	statementID string
	source      string // frontier key this intake queues on

	// env/adm are the ORIGINAL statement this id was first opened with. Every
	// later call for the same statement id must present a byte-identical envelope;
	// a retry always resumes against these stored values, never the newly supplied
	// ones, so a caller cannot reuse a prepared unsafe write under a different
	// SQL/target/kind/signer/JWS. adm is retained because a resume that still
	// needs to prepare replays the original payload bytes.
	env StatementEnvelope
	adm AdmissionRecord

	// Concurrent-attempt dedup: one attempt runs at a time; others wait on done.
	active bool
	done   chan struct{}
	res    IntakeResult
	err    error

	// Cached prepare: set once PrepareLocalStatement succeeds; reused by every
	// later attempt so the unsafe write is never repeated (design section 3.2).
	prepared    PreparedLocalResult
	hasPrepared bool

	// Cached accepted submit outcome: set once SubmitStatement returns an
	// ACK2-permitting outcome ({Accepted, ExactIdempotent}). Retained verbatim so
	// a resume from SubmitAccepted reports the authoritative submit category — an
	// exact re-ack must stay distinguishable from a fresh acceptance — instead of
	// synthesizing one. Non-accepting submit outcomes are never cached here (they
	// do not advance to SubmitAccepted).
	submit    SubmitOutcome
	hasSubmit bool

	// submitUnknown / claimUnknown record that the last submit / RC outcome was
	// indeterminate (OutcomeUnknown), as opposed to a plain retryable outcome
	// (PR05). A resume consults these to decide whether to query the server by
	// statement_id before re-sending (deterministic convergence, design section
	// 3.4). They are distinct from the stage: an unknown submit leaves the stage
	// below SubmitAccepted, an unknown RC leaves it at SubmitAccepted; the flag
	// says "the reason it did not advance was indeterminate, not merely
	// transient". Cleared once the outcome converges.
	submitUnknown bool
	claimUnknown  bool

	// stage is the highest durable lifecycle point this intake has reached; a
	// resuming attempt uses it to skip already-completed stages.
	stage       Lifecycle
	abortReason string // set when stage == AbortPending, so a retry re-aborts with the original reason

	// Terminal cache: an Ack2 or a successfully Cleaned abort. A repeated call
	// returns terminalRes without re-running anything.
	terminalRes IntakeResult
	isTerminal  bool
}

// sourceFrontier serializes source writes on one source: design section 3.2's
// v1 serial constraint requires the next source write to wait until the prior
// intake on the same source reaches RCBound/Ack2 or Cleaned. It is a
// holder-identified gate (not a plain mutex) so a statement can re-enter the
// frontier it already holds during its own retry without deadlocking, with a
// FIFO waiter queue so distinct statements do not starve.
type sourceFrontier struct {
	holder  string // statement_id currently holding the frontier ("" = free)
	waiters []*frontierWaiter
}

type frontierWaiter struct {
	statementID string
	ready       chan struct{} // closed when this waiter is handed the frontier
}

// defaultFrontierKey is the frontier key used when no deterministic source is
// configured: all statements serialize on one gate, which is conservatively
// correct (never less strict than the design's serial constraint).
const defaultFrontierKey = "__default_source__"

// NewOrchestrator constructs an Orchestrator over the two staged-intake ports.
func NewOrchestrator(submitter StatementSubmitter, preparer SourcePreparer, cfg OrchestratorConfig) *Orchestrator {
	return &Orchestrator{
		submitter: submitter,
		preparer:  preparer,
		cfg:       cfg,
		records:   map[string]*intakeRecord{},
		frontiers: map[string]*sourceFrontier{},
	}
}

// frontierKey is the source a statement serializes on. Prepare runs before the
// RC binds a concrete source_node, so the key is the configured deterministic
// source (design's "deterministic source"); with none configured it degrades to
// a single global gate.
func (o *Orchestrator) frontierKey() string {
	if o.cfg.ExpectedSource != "" {
		return o.cfg.ExpectedSource
	}
	return defaultFrontierKey
}

// Orchestrate drives one admission through the staged intake. Prepare runs at
// most once per statement id — even under concurrency and across retries: a
// call for a statement with a recorded terminal outcome returns it without
// re-running anything; a call racing an in-flight attempt for the same id waits
// and observes the same result; and a retry after a non-terminal outcome reuses
// the cached prepare and resumes the unresolved submit/claim/abort stage rather
// than re-executing the unsafe write. Distinct statement ids on the same source
// are serialized on the source frontier: the next source write waits until the
// prior intake is terminal (RCBound/Ack2 or Cleaned). It returns an error only
// for a local coordination failure (bad admission, transport error, or a failed
// abort that must be retried); protocol outcomes (retryable/unknown/terminal)
// are carried in the IntakeResult with Ack2 == false.
func (o *Orchestrator) Orchestrate(ctx context.Context, adm AdmissionRecord) (IntakeResult, error) {
	if err := ctx.Err(); err != nil {
		return IntakeResult{}, err
	}
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		return IntakeResult{}, err
	}

	// Find or create the cross-call record, bind it to the first envelope, and
	// become this statement's attempt owner (or wait for the in-flight attempt /
	// return the terminal outcome).
	o.mu.Lock()
	rec := o.records[env.StatementID]
	if rec == nil {
		rec = &intakeRecord{statementID: env.StatementID, source: o.frontierKey(), env: env, adm: adm}
		o.records[env.StatementID] = rec
	} else if !sameStatement(rec.env, rec.adm, env, adm) {
		// The statement id is already bound to a different envelope. A retry may
		// not swap the SQL/target/kind/signer/JWS/payload under a reused id and
		// ride the originally prepared unsafe write; fail closed.
		o.mu.Unlock()
		return IntakeResult{StatementID: env.StatementID}, fmt.Errorf("intake: statement id %s reused with a different envelope", env.StatementID)
	}
	if rec.isTerminal {
		res := rec.terminalRes
		o.mu.Unlock()
		return res, nil
	}
	if rec.active {
		done := rec.done
		o.mu.Unlock()
		select {
		case <-done:
			o.mu.Lock()
			res, rerr := rec.res, rec.err
			o.mu.Unlock()
			return res, rerr
		case <-ctx.Done():
			return IntakeResult{StatementID: env.StatementID}, ctx.Err()
		}
	}
	rec.active = true
	rec.done = make(chan struct{})
	o.mu.Unlock()

	// Acquire the source frontier before any source write. This is re-entrant
	// for the statement that already holds it (its own retry passes through) and
	// blocks a different statement until the holder reaches a terminal stage.
	if err := o.acquireFrontier(ctx, rec.source, env.StatementID); err != nil {
		o.finishAttempt(rec, IntakeResult{StatementID: env.StatementID}, err, false)
		return IntakeResult{StatementID: env.StatementID}, err
	}

	// Always run against the ORIGINAL bound envelope, never the newly supplied
	// one, so a resume can only ever advance the statement it first prepared.
	res, runErr := o.run(ctx, rec.env, rec.adm, rec)

	// Release the frontier only when this intake reaches a terminal stage
	// (RCBound/Ack2 success, or a completed Cleaned abort). A retryable/unknown
	// outcome or a still-pending abort keeps the frontier held so no later
	// source write jumps ahead of an undecided one; the holder stays this
	// statement, so its own next retry re-enters.
	releaseFrontier := runErr == nil && (res.terminal || res.Lifecycle == LifecycleRCBound)
	o.finishAttempt(rec, res, runErr, releaseFrontier)
	return res, runErr
}

// finishAttempt publishes the attempt outcome, updates the durable record,
// records terminal outcomes for idempotent replay, and releases the frontier
// when the intake is terminal.
func (o *Orchestrator) finishAttempt(rec *intakeRecord, res IntakeResult, err error, releaseFrontier bool) {
	o.mu.Lock()
	rec.res, rec.err = res, err
	rec.active = false
	if err == nil && res.terminal {
		rec.isTerminal = true
		rec.terminalRes = res
	}
	close(rec.done)
	o.mu.Unlock()
	if releaseFrontier {
		o.releaseFrontier(rec.source, rec.statementID)
	}
}

// run executes one intake attempt, resuming from the record's durable stage: it
// reuses a cached prepare, skips an already-accepted submit, and re-aborts a
// pending abort — so the unsafe write is never repeated. It updates rec.stage /
// rec.prepared under the lock as each stage completes; Orchestrate owns the
// active/done/terminal bookkeeping.
func (o *Orchestrator) run(ctx context.Context, env StatementEnvelope, adm AdmissionRecord, rec *intakeRecord) (IntakeResult, error) {
	prepared, hasPrepared := o.cachedPrepared(rec)

	switch rec.stage {
	case LifecycleAbortPending:
		// A prior attempt judged this statement terminal but its abort did not
		// complete. Reuse the cached candidate; re-run only the abort.
		return o.abort(ctx, o.resultFor(rec), rec, rec.abortReason)

	case LifecycleSubmitAccepted:
		// Prepare is cached and submit was already accepted; a prior RC call
		// returned retryable/unknown. If it was indeterminate and a querier is
		// wired, converge the RC deterministically by querying claim status
		// before re-registering (PR05); the query result is finalized directly, so
		// a queried-Bound claim never triggers a second RegisterPreparedClaim.
		if res, err, done := o.convergeUnknownClaim(ctx, prepared, rec); done {
			return res, err
		}
		return o.registerAndFinish(ctx, env, prepared, rec)

	default:
		// Stages "" / Preparing / UnsafeWritten: submit is not yet accepted, so
		// it must run this attempt. Prepare runs only if not already cached.
		var submit SubmitOutcome
		if hasPrepared {
			// A prior submit was retryable/unknown or errored: converge only the
			// submit; never re-run the unsafe write. If the prior outcome was
			// indeterminate and a querier is wired, query submit status first so an
			// already-sequenced statement is not blindly re-submitted (PR05).
			resolved, queried, qerr := o.convergeUnknownSubmit(ctx, env, rec)
			if qerr != nil {
				return IntakeResult{StatementID: env.StatementID, Prepared: prepared}, qerr
			}
			if resolved {
				submit = queried
			} else {
				var submitErr error
				submit, submitErr = o.submitter.SubmitStatement(ctx, env)
				if submitErr != nil {
					return IntakeResult{StatementID: env.StatementID, Prepared: prepared}, fmt.Errorf("intake: submit failed for %s: %w", env.StatementID, submitErr)
				}
			}
		} else {
			p, s, prepareErr, submitErr := o.prepareAndSubmit(ctx, env, adm)
			if prepareErr != nil {
				// A failed prepare produced no unsafe part, so it is safe to retry
				// prepare later; nothing is cached.
				return IntakeResult{StatementID: env.StatementID, Submit: s}, fmt.Errorf("intake: prepare failed for %s: %w", env.StatementID, prepareErr)
			}
			// Prepare succeeded: cache it so any retry never re-runs the unsafe
			// write, even if submit errored or is non-terminal below.
			o.cachePrepared(rec, p)
			prepared = p
			if submitErr != nil {
				return IntakeResult{StatementID: env.StatementID, Prepared: p}, fmt.Errorf("intake: submit failed for %s: %w", env.StatementID, submitErr)
			}
			submit = s
		}

		if done, res, err := o.afterSubmit(ctx, env, prepared, submit, rec); done {
			return res, err
		}
		// Submit accepted and consistency-checked: proceed to the RC gate.
		return o.registerAndFinish(ctx, env, prepared, rec)
	}
}

// prepareAndSubmit runs PrepareLocalStatement and SubmitStatement concurrently
// (design section 3.2 par block). Both goroutines write only local variables;
// the record is updated by the caller under the lock.
func (o *Orchestrator) prepareAndSubmit(ctx context.Context, env StatementEnvelope, adm AdmissionRecord) (PreparedLocalResult, SubmitOutcome, error, error) {
	var (
		prepared   PreparedLocalResult
		prepareErr error
		submit     SubmitOutcome
		submitErr  error
		wg         sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		prepared, prepareErr = o.preparer.PrepareLocalStatement(ctx, env, adm.Payload)
	}()
	go func() {
		defer wg.Done()
		submit, submitErr = o.submitter.SubmitStatement(ctx, env)
	}()
	wg.Wait()
	return prepared, submit, prepareErr, submitErr
}

// afterSubmit classifies a fresh submit outcome and, on acceptance, runs the
// pre-RC consistency checks. It returns done == true when the intake resolves
// here (terminal reject/abort, or a non-terminal retryable/unknown submit); it
// returns done == false to fall through to the RC gate.
func (o *Orchestrator) afterSubmit(ctx context.Context, env StatementEnvelope, prepared PreparedLocalResult, submit SubmitOutcome, rec *intakeRecord) (bool, IntakeResult, error) {
	res := o.resultFor(rec)
	res.Submit = submit

	// Terminal submit reject: abort the exact prepared candidate, no ACK2.
	if submit.Category.RequiresAbort() {
		r, err := o.abort(ctx, res, rec, submit.Reason)
		return true, r, err
	}
	// Retryable / unknown submit: hold the frontier, no claim, no abort, no ACK2.
	// Record whether it was indeterminate so a resume knows to query submit
	// status before re-sending (PR05); a plain retryable is safe to re-send.
	if !submit.Category.PermitsAck2() {
		o.setSubmitUnknown(rec, submit.Category == OutcomeUnknown)
		return true, res, nil
	}

	// Submit accepted. Before opening the RC gate, the prepared result must
	// completely and exactly bind THIS statement. The prepared result carries
	// its own statement id; a stale/buggy preparer answering for another
	// statement with the same payload/source must not bind the wrong candidate.
	if prepared.StatementID == "" || prepared.StatementID != env.StatementID {
		r, err := o.abort(ctx, res, rec, "prepared statement id mismatch")
		return true, r, err
	}
	if reason := o.preparedConsistencyReject(env, prepared); reason != "" {
		r, err := o.abort(ctx, res, rec, reason)
		return true, r, err
	}

	// Cache the accepted submit outcome verbatim before advancing the stage, so a
	// resume from SubmitAccepted (which does not re-run SubmitStatement) still
	// reports whether the submit was a fresh accept or an exact re-ack.
	o.cacheSubmit(rec, submit)
	o.setStage(rec, LifecycleSubmitAccepted)
	return false, res, nil
}

// registerAndFinish runs the RC gate for an accepted, consistency-checked
// statement and finishes the intake on the claim outcome.
func (o *Orchestrator) registerAndFinish(ctx context.Context, env StatementEnvelope, prepared PreparedLocalResult, rec *intakeRecord) (IntakeResult, error) {
	// resultFor restores res.Submit from the record's cached accepted submit
	// outcome. registerAndFinish is only ever reached after the submit was
	// accepted and cached — on the fresh path afterSubmit calls cacheSubmit before
	// returning done == false, and on the resume path rec.stage is already
	// SubmitAccepted with the outcome cached. So res.Submit carries the true
	// category (Accepted vs ExactIdempotent), not a synthesized one, and the ACK2
	// gate reads the same authoritative value on both paths.
	res := o.resultFor(rec)

	claim, claimErr := o.preparer.RegisterPreparedClaim(ctx, env.StatementID)
	if claimErr != nil {
		return res, fmt.Errorf("intake: register claim failed for %s: %w", env.StatementID, claimErr)
	}
	return o.finalizeClaim(ctx, prepared, claim, rec)
}

// finalizeClaim classifies an RC outcome (freshly registered, or resolved by a
// status query) and drives it to a terminal result or a non-terminal hold. It
// is shared by registerAndFinish and the unknown-RC query-convergence path so a
// queried-Bound claim finishes without a second RegisterPreparedClaim.
func (o *Orchestrator) finalizeClaim(ctx context.Context, prepared PreparedLocalResult, claim ClaimOutcome, rec *intakeRecord) (IntakeResult, error) {
	res := o.resultFor(rec)
	res.Claim = claim

	// Classify the RC by category BEFORE inspecting its bound source_node. A
	// retryable/unknown claim (e.g. NotLeader) carries an empty BoundSource
	// because the FSM has not bound a source yet; per design 3.4 it must never be
	// cleaned. Testing the source first would misread that empty BoundSource as a
	// terminal "source mismatch" and wrongly abort a retryable RC. The
	// source-agreement check is only meaningful once a claim has actually BOUND
	// (Accepted/ExactIdempotent), so it moves onto the accept path below.

	// Terminal RC reject: abort the exact candidate, no ACK2, regardless of source.
	if claim.Category.RequiresAbort() {
		return o.abort(ctx, res, rec, claim.Reason)
	}
	// Retryable / unknown RC: no ACK2, no abort; converge on retry from the
	// SubmitAccepted stage (submit stays accepted; prepare stays cached). Record
	// whether it was indeterminate so a resume queries claim status before
	// re-registering (PR05); a plain retryable is safe to re-register.
	if !claim.Category.PermitsAck2() {
		o.setClaimUnknown(rec, claim.Category == OutcomeUnknown)
		return res, nil
	}

	// Accept path (Accepted / ExactIdempotent): the RC has bound a source, so its
	// bound source_node must be present and equal to the source that prepared the
	// write. An accepted claim with a blank or wrong bound source is a genuine
	// terminal inconsistency and must abort (exact-candidate cleanup), not merely
	// withhold ACK2.
	if prepared.SourceNode == "" || claim.BoundSource == "" || claim.BoundSource != prepared.SourceNode {
		return o.abort(ctx, res, rec, "RC source_node mismatch")
	}

	// The RC is bound: advance the journal to RCBound, then consult the single
	// ACK2 dual gate. The gate re-checks all five design-section-3.4 conditions
	// together (payload durable, unsafe write complete, submit accepted, claim
	// bound, lifecycle == RCBound) plus source agreement, so ACK2 is granted in
	// exactly one place and can never be reported from a partially satisfied
	// state. Reaching here means the local write is complete and durable (the
	// prepared candidate was cached), so those inputs are set true.
	res.Lifecycle = LifecycleRCBound
	o.setStage(rec, LifecycleRCBound)
	ack, reason := Ack2Ready(Ack2Inputs{
		PayloadDurable:   true,
		UnsafeWriteDone:  true,
		Submit:           res.Submit,
		Claim:            claim,
		JournalLifecycle: LifecycleRCBound,
		PreparedSource:   prepared.SourceNode,
	})
	if !ack {
		// Defensive: the branch checks above already exclude every non-ack path,
		// so this cannot fire today. Keep it fail-closed rather than acking on an
		// unexpected input, and do not mark the outcome terminal so a retry can
		// still converge.
		res.Reason = reason
		return res, nil
	}
	res.Ack2 = true
	res.terminal = true
	return res, nil
}

// abort performs exact-candidate cleanup for a terminally rejected statement. A
// successful abort is a terminal outcome (recorded for idempotent replay). A
// failed abort keeps the record at AbortPending (with the reason) and returns an
// error, so a later attempt re-runs only the abort — never prepare — instead of
// silently recording the cleanup as done.
//
// The cleanup surface is HouseGate's frozen candidate inventory: abort hands the
// exact CandidateParts from the record's prepared result to the seam (PR06), so
// the drop is bounded to those exact part names and can never widen to a whole
// partition. Both the failed attempt and any retry hand the identical frozen
// set. An empty candidate set is a legitimate no-op cleanup that still reaches
// Cleaned (a part not present is already clean, design rule 4).
func (o *Orchestrator) abort(ctx context.Context, res IntakeResult, rec *intakeRecord, reason string) (IntakeResult, error) {
	res.Ack2 = false
	res.Lifecycle = LifecycleAbortPending
	o.setAbortPending(rec, reason)
	parts := o.abortParts(rec)
	if err := o.preparer.AbortPreparedStatement(ctx, res.StatementID, parts, reason); err != nil {
		return res, fmt.Errorf("intake: abort failed for %s (%s): %w", res.StatementID, reason, err)
	}
	res.Lifecycle = LifecycleCleaned
	res.terminal = true
	return res, nil
}

// resultFor seeds an IntakeResult from the record's cached prepare and accepted
// submit outcome so every exit path reports the prepared candidate and the
// authoritative submit category consistently — including a resume that did not
// re-run SubmitStatement.
func (o *Orchestrator) resultFor(rec *intakeRecord) IntakeResult {
	o.mu.Lock()
	defer o.mu.Unlock()
	res := IntakeResult{StatementID: rec.statementID}
	if rec.hasPrepared {
		res.Prepared = rec.prepared
		res.Lifecycle = rec.prepared.Lifecycle
	}
	if rec.hasSubmit {
		res.Submit = rec.submit
	}
	return res
}

// cacheSubmit stores the accepted submit outcome verbatim so a later resume
// reports the true submit category rather than synthesizing one. It is called
// only for an ACK2-permitting outcome, as the record advances to SubmitAccepted.
func (o *Orchestrator) cacheSubmit(rec *intakeRecord, submit SubmitOutcome) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rec.submit = submit
	rec.hasSubmit = true
}

func (o *Orchestrator) cachedPrepared(rec *intakeRecord) (PreparedLocalResult, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return rec.prepared, rec.hasPrepared
}

func (o *Orchestrator) cachePrepared(rec *intakeRecord, prepared PreparedLocalResult) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rec.prepared = prepared
	rec.hasPrepared = true
	if rankLifecycle(rec.stage) < rankLifecycle(LifecycleUnsafeWritten) {
		rec.stage = LifecycleUnsafeWritten
	}
}

func (o *Orchestrator) setStage(rec *intakeRecord, stage Lifecycle) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if rankLifecycle(stage) > rankLifecycle(rec.stage) {
		rec.stage = stage
	}
}

func (o *Orchestrator) setAbortPending(rec *intakeRecord, reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rec.stage = LifecycleAbortPending
	rec.abortReason = reason
}

// rankLifecycle orders the linear success stages so setStage never regresses.
// The abort branch (AbortPending/Cleaned) is handled by explicit checks, not by
// this rank, so it is deliberately not comparable against the success path.
func rankLifecycle(l Lifecycle) int {
	switch l {
	case LifecyclePreparing:
		return 1
	case LifecycleUnsafeWritten:
		return 2
	case LifecycleSubmitAccepted:
		return 3
	case LifecycleRCBound:
		return 4
	case LifecycleAck2:
		return 5
	default:
		return 0
	}
}

// acquireFrontier blocks until this statement holds the source frontier. It is
// re-entrant: a statement that already holds the frontier (its own retry)
// passes through immediately. A different statement queues FIFO and is handed
// the frontier when the holder reaches a terminal stage. ctx cancellation
// dequeues the waiter (and re-releases if it was concurrently handed the
// frontier) so no later waiter is stranded.
func (o *Orchestrator) acquireFrontier(ctx context.Context, source, statementID string) error {
	o.mu.Lock()
	f := o.frontiers[source]
	if f == nil {
		f = &sourceFrontier{}
		o.frontiers[source] = f
	}
	if f.holder == "" || f.holder == statementID {
		f.holder = statementID
		o.mu.Unlock()
		return nil
	}
	w := &frontierWaiter{statementID: statementID, ready: make(chan struct{})}
	f.waiters = append(f.waiters, w)
	o.mu.Unlock()

	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		o.mu.Lock()
		if removeWaiter(f, w) {
			// Still queued: safely abandoned.
			o.mu.Unlock()
			return ctx.Err()
		}
		// Raced with a hand-off: we were made holder. Release to the next waiter
		// so the frontier is not stranded.
		o.mu.Unlock()
		o.releaseFrontier(source, statementID)
		return ctx.Err()
	}
}

// releaseFrontier hands the frontier to the FIFO-next waiter, or frees it. It is
// a no-op if this statement no longer holds the frontier (idempotent release).
func (o *Orchestrator) releaseFrontier(source, statementID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f := o.frontiers[source]
	if f == nil || f.holder != statementID {
		return
	}
	if len(f.waiters) > 0 {
		next := f.waiters[0]
		f.waiters = f.waiters[1:]
		f.holder = next.statementID
		close(next.ready)
		return
	}
	f.holder = ""
}

func removeWaiter(f *sourceFrontier, w *frontierWaiter) bool {
	for i, cur := range f.waiters {
		if cur == w {
			f.waiters = append(f.waiters[:i], f.waiters[i+1:]...)
			return true
		}
	}
	return false
}

// preparedConsistencyReject returns a non-empty abort reason if the prepared
// result does not completely and exactly bind the submitted statement. It reads
// only fields available before RC registration, so a claim is never registered
// for a statement that would then be aborted. Every binding field is required
// to be present and equal — a missing (blank/zero) field is a mismatch, never a
// tolerated wildcard — so a preparer returning empty binding fields fails
// closed. The statement id is checked by the caller before this. An empty
// return means the prepared result is a complete, exact match.
func (o *Orchestrator) preparedConsistencyReject(env StatementEnvelope, prepared PreparedLocalResult) string {
	if prepared.SourceNode == "" {
		return "prepared result has no source_node"
	}
	if o.cfg.ExpectedSource != "" && prepared.SourceNode != o.cfg.ExpectedSource {
		return "committed source mismatch"
	}
	// Statements that carry a payload (INSERT) must have the full payload
	// identity tuple bound exactly. UPDATE/DELETE carry no payload in this stage,
	// so an empty envelope payload identity is legitimate and skipped.
	if env.PayloadHash != "" || env.PayloadRef != "" || env.PayloadLength != 0 {
		if prepared.PayloadRef == "" || prepared.PayloadRef != env.PayloadRef {
			return "payload ref mismatch"
		}
		if prepared.PayloadHash == "" || prepared.PayloadHash != env.PayloadHash {
			return "payload hash mismatch"
		}
		if prepared.PayloadLength == 0 || prepared.PayloadLength != env.PayloadLength {
			return "payload length mismatch"
		}
		if prepared.PayloadEncoding == "" || prepared.PayloadEncoding != env.PayloadEncoding {
			return "payload encoding mismatch"
		}
		// The source must have decoded the payload with exactly the pinned client
		// revision; a different revision can yield a different materialization.
		if prepared.Revision == 0 || prepared.Revision != env.Revision {
			return "payload revision mismatch"
		}
	}
	return ""
}
