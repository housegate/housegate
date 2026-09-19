package storageintegrity

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/replay"
)

type snapshotQueryMemoryJournal struct {
	records   map[string]SnapshotQueryJournalRecord
	events    *[]string
	failStage SnapshotQueryJournalStage
}

func (j *snapshotQueryMemoryJournal) Load(_ context.Context, id string) (SnapshotQueryJournalRecord, bool, error) {
	r, ok := j.records[id]
	return r, ok, nil
}
func (j *snapshotQueryMemoryJournal) List(_ context.Context) ([]SnapshotQueryJournalRecord, error) {
	out := make([]SnapshotQueryJournalRecord, 0, len(j.records))
	for _, r := range j.records {
		out = append(out, r)
	}
	return out, nil
}
func (j *snapshotQueryMemoryJournal) Save(_ context.Context, r SnapshotQueryJournalRecord) error {
	if j.events != nil {
		*j.events = append(*j.events, "persist_"+string(r.Stage))
	}
	if r.Stage == j.failStage {
		return errors.New("injected fsync failure")
	}
	j.records[r.StatementID] = r
	return nil
}

type snapshotQueryFakeGate struct {
	events *[]string
	win    bool
}

func (g snapshotQueryFakeGate) TryStart() bool {
	if g.events != nil {
		*g.events = append(*g.events, "TryStart")
	}
	return g.win
}

type snapshotQueryFakeValidator struct {
	events *[]string
	reject bool
}

func (v snapshotQueryFakeValidator) ValidateStatementV3(_ string, want auth.JWSStatementPayloadV3) (string, error) {
	if v.events != nil {
		*v.events = append(*v.events, "validate")
	}
	if want.Purpose != auth.StatementPurposeV3 {
		return "", errors.New("missing snapshot query statement purpose")
	}
	if v.reject {
		return "", errors.New("invalid signature")
	}
	return "0xabc", nil
}

type snapshotQueryFakeSequencer struct {
	events    *[]string
	submit    int
	lookup    int
	status    replay.SnapshotQueryStatus
	submitErr error
	result    *replay.SnapshotQuerySubmitResult
}

func (s *snapshotQueryFakeSequencer) SubmitSnapshotQuery(_ context.Context, env replay.SnapshotQueryEnvelope) (replay.SnapshotQuerySubmitResult, error) {
	if s.events != nil {
		*s.events = append(*s.events, "submit")
	}
	s.submit++
	if s.submitErr != nil {
		return replay.SnapshotQuerySubmitResult{}, s.submitErr
	}
	if s.result != nil {
		return *s.result, nil
	}
	return snapshotQueryAcceptedResult(env), nil
}
func (s *snapshotQueryFakeSequencer) LookupSnapshotQuery(_ context.Context, _, _, _ string) (replay.SnapshotQueryStatus, error) {
	if s.events != nil {
		*s.events = append(*s.events, "lookup_submit")
	}
	s.lookup++
	return s.status, nil
}

type snapshotQueryFakeReconciler struct {
	events *[]string
	calls  int
}

func (r *snapshotQueryFakeReconciler) ReconcileSnapshotQueryIntent(_ context.Context, _ SnapshotQueryJournalRecord, _ replay.SnapshotQueryStatus) error {
	if r.events != nil {
		*r.events = append(*r.events, "reconcile")
	}
	r.calls++
	return nil
}

func newSnapshotQueryIntakeFixture(t *testing.T) (*SnapshotQueryIntake, replay.SnapshotQueryEnvelope, *[]string, *snapshotQueryMemoryJournal, *snapshotQueryFakeSequencer, *snapshotQueryFakeReconciler) {
	t.Helper()
	events := []string{}
	journal := &snapshotQueryMemoryJournal{records: map[string]SnapshotQueryJournalRecord{}, events: &events}
	sequencer := &snapshotQueryFakeSequencer{events: &events}
	reconciler := &snapshotQueryFakeReconciler{events: &events}
	env := snapshotQueryEnvelopeFixture(t)
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	env.Input.Binding.ClientAccount = signer.Address()
	env.Input.Binding.StatementID = signer.Address() + ":1:nonce"
	root, err := replay.SnapshotQueryInputRoot(env.Input)
	if err != nil {
		t.Fatal(err)
	}
	env.InputRoot = root
	env.UserJWS, err = signer.SignStatementV3(auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding: env.Input.Binding, InputRoot: env.InputRoot})
	if err != nil {
		t.Fatal(err)
	}
	intake, err := NewSnapshotQueryIntake(SnapshotQueryIntakeOptions{Journal: journal, Sequencer: sequencer, Validator: snapshotQueryFakeValidator{events: &events}, Reconciler: reconciler})
	if err != nil {
		t.Fatal(err)
	}
	return intake, env, &events, journal, sequencer, reconciler
}

func snapshotQueryAcceptedResult(env replay.SnapshotQueryEnvelope) replay.SnapshotQuerySubmitResult {
	b := env.Input.Binding
	return replay.SnapshotQuerySubmitResult{
		AdmissionCode: snapshotQueryAdmissionAccepted,
		StatementSeq:  1,
		BlockSeq:      7,
		SourceNode:    "source",
		InputRoot:     env.InputRoot,
		Reservation: replay.SnapshotQueryReservation{
			ReservationID: b.ReservationID, FencingGeneration: b.FencingGeneration,
			ClientAccount: b.ClientAccount, StatementID: b.StatementID,
			ReadSnapshot: b.ReadSnapshot, ExecutorProfileID: b.ExecutorProfileID,
			QueryProfileID: b.QueryProfileID,
		},
	}
}

type snapshotQueryBlockingAuthorizedJournal struct {
	*snapshotQueryMemoryJournal
	entered chan struct{}
	release <-chan struct{}
}

func (j *snapshotQueryBlockingAuthorizedJournal) Save(ctx context.Context, rec SnapshotQueryJournalRecord) error {
	if rec.Stage == SnapshotQueryStageSubmitAuthorized {
		select {
		case j.entered <- struct{}{}:
		default:
		}
		<-j.release
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return j.snapshotQueryMemoryJournal.Save(ctx, rec)
}

func TestSnapshotQueryIntakeSerializesSubmitAndCleansStatementLock(t *testing.T) {
	intake, env, _, journal, sequencer, reconciler := newSnapshotQueryIntakeFixture(t)
	intake.opts.Validator = snapshotQueryFakeValidator{}
	journal.events = nil
	sequencer.events = nil
	reconciler.events = nil
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := intake.Submit(context.Background(), env, snapshotQueryFakeGate{win: true})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if sequencer.submit != 1 {
		t.Fatalf("submit=%d, want one serialized submission", sequencer.submit)
	}
	intake.statementLocksMu.Lock()
	locks := len(intake.statementLocks)
	intake.statementLocksMu.Unlock()
	if locks != 0 {
		t.Fatalf("statement locks retained after calls: %d", locks)
	}
}

func TestSnapshotQueryIntakeGateWinUsesIntakeOwnedContext(t *testing.T) {
	intake, env, events, baseJournal, sequencer, reconciler := newSnapshotQueryIntakeFixture(t)
	release := make(chan struct{})
	journal := &snapshotQueryBlockingAuthorizedJournal{snapshotQueryMemoryJournal: baseJournal, entered: make(chan struct{}, 1), release: release}
	intake.opts.Journal = journal
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := intake.Submit(ctx, env, snapshotQueryFakeGate{events: events, win: true})
		done <- err
	}()
	select {
	case <-journal.entered:
	case <-time.After(time.Second):
		t.Fatal("Submit did not reach durable authorization")
	}
	cancel()
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("caller cancellation reversed gate winner: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Submit did not complete")
	}
	if sequencer.submit != 1 || reconciler.calls != 0 {
		t.Fatalf("submit=%d reconcile=%d", sequencer.submit, reconciler.calls)
	}
}

func TestSnapshotQueryIntakeRecoverAcceptsExpiredHistoricalJWS(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	env := snapshotQueryEnvelopeForAccount(t, signer.Address())
	env.UserJWS, err = signer.SignStatementV3(auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Iat: 1, Binding: env.Input.Binding, InputRoot: env.InputRoot})
	if err != nil {
		t.Fatal(err)
	}
	events := []string{}
	journal := &snapshotQueryMemoryJournal{records: map[string]SnapshotQueryJournalRecord{}, events: &events}
	rec := newSnapshotQueryRecord(env)
	rec.Stage = SnapshotQueryStageSubmitUnknown
	rec.LaunchAuthorization = snapshotQueryLaunchAuthorization(env)
	journal.records[rec.StatementID] = rec
	sequencer := &snapshotQueryFakeSequencer{events: &events, status: replay.SnapshotQueryStatus{Found: true, Accepted: snapshotQueryAcceptedResult(env)}}
	// This validator would reject the old token for both freshness and policy;
	// recovery intentionally uses only the pure historical verifier.
	validator := auth.NewEthValidator([]string{"0x0000000000000000000000000000000000000001"}, time.Second, true, false, "", nil)
	intake, err := NewSnapshotQueryIntake(SnapshotQueryIntakeOptions{Journal: journal, Sequencer: sequencer, Validator: validator, Reconciler: &snapshotQueryFakeReconciler{events: &events}})
	if err != nil {
		t.Fatal(err)
	}
	if err := intake.Recover(context.Background()); err != nil {
		t.Fatalf("historical recovery rejected cryptographically valid JWS: %v", err)
	}
	if got := journal.records[rec.StatementID].Stage; got != SnapshotQueryStageSequenced {
		t.Fatalf("stage=%q, want Sequenced", got)
	}
}

func TestSnapshotQueryIntakeRecoverFoundRequiresAcceptedIdentity(t *testing.T) {
	intake, env, _, journal, sequencer, _ := newSnapshotQueryIntakeFixture(t)
	rec := newSnapshotQueryRecord(env)
	rec.Stage = SnapshotQueryStageSubmitUnknown
	rec.LaunchAuthorization = snapshotQueryLaunchAuthorization(env)
	journal.records[rec.StatementID] = rec
	bad := snapshotQueryAcceptedResult(env)
	bad.Reservation.FencingGeneration++
	sequencer.status = replay.SnapshotQueryStatus{Found: true, Accepted: bad}
	if err := intake.Recover(context.Background()); err == nil {
		t.Fatal("mismatched Found accepted identity was sequenced")
	}
	if got := journal.records[rec.StatementID].Stage; got != SnapshotQueryStageSubmitUnknown {
		t.Fatalf("stage=%q, want unchanged SubmitUnknown", got)
	}
}

func TestSnapshotQueryIntakeNeverSequencesRejectedOrUnknownAdmission(t *testing.T) {
	for _, code := range []uint32{2, 99} {
		t.Run(string(rune(code)), func(t *testing.T) {
			intake, env, _, journal, sequencer, _ := newSnapshotQueryIntakeFixture(t)
			result := snapshotQueryAcceptedResult(env)
			result.AdmissionCode = code
			sequencer.result = &result
			if _, err := intake.Submit(context.Background(), env, snapshotQueryFakeGate{events: &[]string{}, win: true}); err == nil {
				t.Fatalf("admission code %d accepted", code)
			}
			got := journal.records[env.Input.Binding.StatementID]
			if got.Stage == SnapshotQueryStageSequenced {
				t.Fatalf("admission code %d persisted as Sequenced", code)
			}
			if code == 2 && got.Stage != SnapshotQueryStageRejected {
				t.Fatalf("rejection stage=%q, want Rejected", got.Stage)
			}
			if code == 99 && got.Stage != SnapshotQueryStageSubmitUnknown {
				t.Fatalf("unknown stage=%q, want SubmitUnknown", got.Stage)
			}
		})
	}
}

func TestSnapshotQueryIntakeSubmitPersistsAuthorizationBeforeSubmit(t *testing.T) {
	intake, env, events, _, sequencer, _ := newSnapshotQueryIntakeFixture(t)
	got, err := intake.Submit(context.Background(), env, snapshotQueryFakeGate{events: events, win: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.BlockSeq != 7 || got.AckLevel != "sequenced" {
		t.Fatalf("result = %+v", got)
	}
	want := []string{"validate", "persist_Signed", "persist_SubmitIntent", "TryStart", "persist_SubmitAuthorized", "submit", "persist_Sequenced"}
	if !reflect.DeepEqual(*events, want) {
		t.Fatalf("events = %v, want %v", *events, want)
	}
	if sequencer.submit != 1 {
		t.Fatalf("submit count = %d, want 1", sequencer.submit)
	}
}

func TestSnapshotQueryIntakeAuthorizationFailureNeverSubmits(t *testing.T) {
	intake, env, events, journal, sequencer, reconciler := newSnapshotQueryIntakeFixture(t)
	journal.failStage = SnapshotQueryStageSubmitAuthorized
	if _, err := intake.Submit(context.Background(), env, snapshotQueryFakeGate{events: events, win: true}); err == nil {
		t.Fatal("authorization persistence failure must surface to caller")
	}
	if sequencer.submit != 0 || reconciler.calls != 1 {
		t.Fatalf("submit=%d reconcile=%d, want 0/1", sequencer.submit, reconciler.calls)
	}
	if got := (*events)[len(*events)-2:]; !reflect.DeepEqual(got, []string{"lookup_submit", "reconcile"}) {
		t.Fatalf("tail = %v", got)
	}
}

func TestSnapshotQueryIntakeIntentOnlyRecoveryDoesNotSubmitAfterNotFound(t *testing.T) {
	intake, env, events, journal, sequencer, reconciler := newSnapshotQueryIntakeFixture(t)
	rec := newSnapshotQueryRecordAtIntent(env)
	journal.records[rec.StatementID] = rec
	if err := intake.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sequencer.submit != 0 || sequencer.lookup != 1 || reconciler.calls != 1 {
		t.Fatalf("submit=%d lookup=%d reconcile=%d, want 0/1/1", sequencer.submit, sequencer.lookup, reconciler.calls)
	}
	if !reflect.DeepEqual(*events, []string{"lookup_submit", "reconcile"}) {
		t.Fatalf("events = %v", *events)
	}
}

func TestSnapshotQueryIntakeRejectsChangedIdentityBeforeEffects(t *testing.T) {
	intake, env, events, _, sequencer, _ := newSnapshotQueryIntakeFixture(t)
	env.InputRoot = "0xchanged"
	if _, err := intake.Submit(context.Background(), env, snapshotQueryFakeGate{events: events, win: true}); err == nil {
		t.Fatal("changed input root accepted")
	}
	if sequencer.submit != 0 || len(*events) != 0 {
		t.Fatalf("side effect after invalid identity: events=%v submit=%d", *events, sequencer.submit)
	}
}

func TestSnapshotQueryIntakeRejectsInvalidJWSBeforeEffects(t *testing.T) {
	intake, env, events, _, sequencer, _ := newSnapshotQueryIntakeFixture(t)
	intake.opts.Validator = snapshotQueryFakeValidator{events: events, reject: true}
	if _, err := intake.Submit(context.Background(), env, snapshotQueryFakeGate{events: events, win: true}); err == nil {
		t.Fatal("invalid JWS accepted")
	}
	if sequencer.submit != 0 || !reflect.DeepEqual(*events, []string{"validate"}) {
		t.Fatalf("events=%v submit=%d", *events, sequencer.submit)
	}
}

func TestSnapshotQueryIntakeUsesPurposeWithRealValidator(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	env := snapshotQueryEnvelopeForAccount(t, signer.Address())
	env.UserJWS, err = signer.SignStatementV3(auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding: env.Input.Binding, InputRoot: env.InputRoot})
	if err != nil {
		t.Fatal(err)
	}
	events := []string{}
	journal := &snapshotQueryMemoryJournal{records: map[string]SnapshotQueryJournalRecord{}, events: &events}
	sequencer := &snapshotQueryFakeSequencer{events: &events}
	reconciler := &snapshotQueryFakeReconciler{events: &events}
	validator := auth.NewEthValidator([]string{signer.Address()}, 0, true, false, "", nil)
	intake, err := NewSnapshotQueryIntake(SnapshotQueryIntakeOptions{Journal: journal, Sequencer: sequencer, Validator: validator, Reconciler: reconciler})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := intake.Submit(context.Background(), env, snapshotQueryFakeGate{events: &events, win: true}); err != nil {
		t.Fatalf("real v3 validator rejected purpose-bound envelope: %v", err)
	}
}

func TestSnapshotQueryIntakePhasePortMapsAgentPrepareCallbacks(t *testing.T) {
	intake, env, events, journal, sequencer, _ := newSnapshotQueryIntakeFixture(t)
	port, err := intake.NewSnapshotQueryIntakePhasePort(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	callbacks := port.AgentPrepareCallbacks()
	if err := callbacks.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := callbacks.PersistForwardIntent(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := callbacks.AuthorizeForward(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := callbacks.SubmitAfterAuthorization(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.BlockSeq != 7 || sequencer.submit != 1 {
		t.Fatalf("result=%+v submit=%d", got, sequencer.submit)
	}
	if rec := journal.records[env.Input.Binding.StatementID]; rec.Stage != SnapshotQueryStageSequenced {
		t.Fatalf("stage = %q, want Sequenced", rec.Stage)
	}
	want := []string{"validate", "persist_Signed", "persist_SubmitIntent", "persist_SubmitAuthorized", "submit", "persist_Sequenced"}
	if !reflect.DeepEqual(*events, want) {
		t.Fatalf("events = %v, want %v", *events, want)
	}
}

func TestSnapshotQueryIntakePhasePortGateLostPersistsCancelBeforeReconcile(t *testing.T) {
	intake, env, events, journal, sequencer, reconciler := newSnapshotQueryIntakeFixture(t)
	port, err := intake.NewSnapshotQueryIntakePhasePort(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	callbacks := port.AgentPrepareCallbacks()
	if err := callbacks.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := callbacks.PersistForwardIntent(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := callbacks.ReconcileCancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sequencer.submit != 0 || reconciler.calls != 1 {
		t.Fatalf("submit=%d reconcile=%d, want 0/1", sequencer.submit, reconciler.calls)
	}
	if rec := journal.records[env.Input.Binding.StatementID]; rec.Stage != SnapshotQueryStageCancelPending || !rec.PreSubmitCancelIntent || !rec.ReleaseReconciliationDebt {
		t.Fatalf("cancel record = %+v", rec)
	}
	want := []string{"validate", "persist_Signed", "persist_SubmitIntent", "persist_CancelPending", "lookup_submit", "reconcile"}
	if !reflect.DeepEqual(*events, want) {
		t.Fatalf("events = %v, want %v", *events, want)
	}
}

func TestSnapshotQueryIntakePhasePortPreparedCancelBeforeIntentIsDurableAndRecoverable(t *testing.T) {
	events := []string{}
	journal, err := NewFileSnapshotQueryJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sequencer := &snapshotQueryFakeSequencer{events: &events}
	reconciler := &snapshotQueryFakeReconciler{events: &events}
	intake, err := NewSnapshotQueryIntake(SnapshotQueryIntakeOptions{
		Journal: journal, Sequencer: sequencer, Validator: snapshotQueryFakeValidator{events: &events}, Reconciler: reconciler,
	})
	if err != nil {
		t.Fatal(err)
	}
	env := snapshotQueryEnvelopeFixture(t)
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	env.Input.Binding.ClientAccount = signer.Address()
	env.Input.Binding.StatementID = signer.Address() + ":1:prepared-cancel"
	env.InputRoot, err = replay.SnapshotQueryInputRoot(env.Input)
	if err != nil {
		t.Fatal(err)
	}
	env.UserJWS, err = signer.SignStatementV3(auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding: env.Input.Binding, InputRoot: env.InputRoot})
	if err != nil {
		t.Fatal(err)
	}
	port, err := intake.NewSnapshotQueryIntakePhasePort(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	if err := port.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The D1 bridge can publish C4 ownership after Prepare but before the
	// relay invokes PersistForwardIntent.  This must durably cancel the Signed
	// record; it must not invent submit intent or issue a submit.
	if err := port.CancelAndReconcile(context.Background()); err != nil {
		t.Fatalf("cancel prepared record: %v", err)
	}
	if sequencer.submit != 0 || sequencer.lookup != 1 || reconciler.calls != 1 {
		t.Fatalf("after cancel submit=%d lookup=%d reconcile=%d, want 0/1/1", sequencer.submit, sequencer.lookup, reconciler.calls)
	}
	rec, found, err := journal.Load(context.Background(), env.Input.Binding.StatementID)
	if err != nil || !found {
		t.Fatalf("load durable cancellation record found=%t err=%v", found, err)
	}
	if rec.Stage != SnapshotQueryStageCancelPending || !rec.PreSubmitCancelIntent || !rec.ReleaseReconciliationDebt {
		t.Fatalf("prepared cancel record = %+v, want durable CancelPending reconciliation debt", rec)
	}
	if rec.LaunchAuthorization != nil || rec.SubmitUnknown || rec.HasSubmit {
		t.Fatalf("prepared cancel fabricated submit state: %+v", rec)
	}

	// A new intake observes the durable cancellation boundary and can only
	// repeat authoritative reconciliation.  It cannot turn Signed work into an
	// intent or a submission, so there is no orphaned pre-intent C4 record.
	restarted, err := NewSnapshotQueryIntake(intake.opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Recover(context.Background()); err != nil {
		t.Fatalf("recover prepared cancellation: %v", err)
	}
	if sequencer.submit != 0 || sequencer.lookup != 2 || reconciler.calls != 2 {
		t.Fatalf("after restart submit=%d lookup=%d reconcile=%d, want 0/2/2", sequencer.submit, sequencer.lookup, reconciler.calls)
	}
	if rec, found, err = journal.Load(context.Background(), env.Input.Binding.StatementID); err != nil || !found {
		t.Fatalf("reload cancellation record after restart found=%t err=%v", found, err)
	}
	if rec.Stage != SnapshotQueryStageCancelPending || !rec.PreSubmitCancelIntent || !rec.ReleaseReconciliationDebt {
		t.Fatalf("restart changed cancellation boundary: %+v", rec)
	}
	// FileSnapshotQueryJournal deliberately has no test event stream; the Load
	// assertions above prove its Signed-to-CancelPending writes were durable.
	want := []string{"validate", "lookup_submit", "reconcile", "lookup_submit", "reconcile"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestSnapshotQueryIntakePhasePortAuthorizationPersistenceFailureRecoversWithoutSubmit(t *testing.T) {
	intake, env, events, journal, sequencer, reconciler := newSnapshotQueryIntakeFixture(t)
	journal.failStage = SnapshotQueryStageSubmitAuthorized
	port, err := intake.NewSnapshotQueryIntakePhasePort(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	if err := port.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := port.PersistSubmitIntent(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := port.AuthorizeSubmit(context.Background()); err == nil {
		t.Fatal("authorization persistence failure accepted")
	}
	if sequencer.submit != 0 || reconciler.calls != 1 {
		t.Fatalf("submit=%d reconcile=%d, want 0/1", sequencer.submit, reconciler.calls)
	}
	if rec := journal.records[env.Input.Binding.StatementID]; rec.Stage != SnapshotQueryStageSubmitAuthorizationUnknown || rec.SubmitUnknown {
		t.Fatalf("record = %+v, want durable submit-authorization unknown", rec)
	}

	// A process restart with an authoritative NotFound must retain the
	// ambiguity for reconciliation and must never turn it into a Submit retry.
	restarted, err := NewSnapshotQueryIntake(intake.opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sequencer.submit != 0 || sequencer.lookup != 2 || reconciler.calls != 2 {
		t.Fatalf("after restart submit=%d lookup=%d reconcile=%d, want 0/2/2", sequencer.submit, sequencer.lookup, reconciler.calls)
	}
	want := []string{"validate", "persist_Signed", "persist_SubmitIntent", "persist_SubmitAuthorized", "persist_SubmitAuthorizationUnknown", "lookup_submit", "reconcile", "lookup_submit", "reconcile"}
	if !reflect.DeepEqual(*events, want) {
		t.Fatalf("events = %v, want %v", *events, want)
	}
}

func TestSnapshotQueryIntakePhasePortUnknownAuthorizationNeverSubmits(t *testing.T) {
	intake, env, _, journal, sequencer, reconciler := newSnapshotQueryIntakeFixture(t)
	port, err := intake.NewSnapshotQueryIntakePhasePort(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	if err := port.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := port.PersistSubmitIntent(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := port.AuthorizeSubmit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := port.PersistAuthorizationUnknownAndReconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sequencer.submit != 0 || reconciler.calls != 1 {
		t.Fatalf("submit=%d reconcile=%d, want 0/1", sequencer.submit, reconciler.calls)
	}
	if rec := journal.records[env.Input.Binding.StatementID]; rec.Stage != SnapshotQueryStageSubmitUnknown || !rec.SubmitUnknown {
		t.Fatalf("record = %+v, want durable SubmitUnknown", rec)
	}
	if _, err := port.SubmitAfterAuthorization(context.Background()); err == nil {
		t.Fatal("unknown authorization allowed submit")
	}
}

func TestSnapshotQueryIntakePhasePortPhaseCallsAreIdempotent(t *testing.T) {
	intake, env, events, _, sequencer, _ := newSnapshotQueryIntakeFixture(t)
	port, err := intake.NewSnapshotQueryIntakePhasePort(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []func(context.Context) error{port.Prepare, port.Prepare, port.PersistSubmitIntent, port.PersistSubmitIntent, port.AuthorizeSubmit, port.AuthorizeSubmit} {
		if err := phase(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := port.SubmitAfterAuthorization(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := port.SubmitAfterAuthorization(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sequencer.submit != 1 {
		t.Fatalf("submit = %d, want 1", sequencer.submit)
	}
	want := []string{"validate", "persist_Signed", "persist_SubmitIntent", "persist_SubmitAuthorized", "submit", "persist_Sequenced"}
	if !reflect.DeepEqual(*events, want) {
		t.Fatalf("events = %v, want %v", *events, want)
	}
}

func TestSnapshotQueryIntakePhasePortRecoveredIntentNeverSubmits(t *testing.T) {
	intake, env, events, journal, sequencer, reconciler := newSnapshotQueryIntakeFixture(t)
	rec := newSnapshotQueryRecordAtIntent(env)
	journal.records[rec.StatementID] = rec
	port, err := intake.NewSnapshotQueryIntakePhasePort(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	if err := port.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := port.SubmitAfterAuthorization(context.Background()); err == nil {
		t.Fatal("recovered intent unexpectedly allowed submit")
	}
	if err := intake.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sequencer.submit != 0 || sequencer.lookup != 1 || reconciler.calls != 1 {
		t.Fatalf("submit=%d lookup=%d reconcile=%d, want 0/1/1", sequencer.submit, sequencer.lookup, reconciler.calls)
	}
	want := []string{"validate", "lookup_submit", "reconcile"}
	if !reflect.DeepEqual(*events, want) {
		t.Fatalf("events = %v, want %v", *events, want)
	}
}

func TestSnapshotQueryIntakePhasePortValidatesBeforeEffects(t *testing.T) {
	intake, env, events, journal, sequencer, _ := newSnapshotQueryIntakeFixture(t)
	env.InputRoot = "0xchanged"
	if _, err := intake.NewSnapshotQueryIntakePhasePort(context.Background(), env); err == nil {
		t.Fatal("invalid phase input accepted")
	}
	if sequencer.submit != 0 || len(journal.records) != 0 || len(*events) != 0 {
		t.Fatalf("effects after invalid phase input: events=%v records=%v submit=%d", *events, journal.records, sequencer.submit)
	}
}

func snapshotQueryEnvelopeFixture(t *testing.T) replay.SnapshotQueryEnvelope {
	return snapshotQueryEnvelopeForAccount(t, "0xabc")
}

func snapshotQueryEnvelopeForAccount(t *testing.T, account string) replay.SnapshotQueryEnvelope {
	t.Helper()
	pin := replay.SnapshotPin{NetworkID: "net", KeeperShardID: 1, SnapshotID: "snapshot", SafeBlockSeq: 1, ManifestRoot: "manifest", StateRoot: "state", SchemaSnapshotID: "schema-snapshot", SchemaRoot: "schema-root"}
	reads := replay.SnapshotReadSet{ReadSnapshot: pin, Tables: []replay.SnapshotReadTable{{Database: "db", Table: "target", TableID: "target-id", SchemaHash: "schema"}}}
	readRoot, err := replay.SnapshotQueryReadSetRoot(reads)
	if err != nil {
		t.Fatal(err)
	}
	binding := replay.SnapshotQueryBinding{EnvelopeVersion: replay.SnapshotQueryEnvelopeVersion, InputKind: replay.SnapshotQueryInputKind, ClientAccount: account, StatementID: account + ":1:nonce", StatementKind: replay.SnapshotQueryStatementKind, NetworkID: pin.NetworkID, KeeperShardID: pin.KeeperShardID, SQLHash: replay.DigestString("INSERT INTO db.target SELECT 1"), SettingsHash: "settings", TargetTableID: "target-id", SchemaHash: "schema", RowIDProfileID: "row", ClientRevision: 1, ReadSnapshot: pin, ReadSetRoot: readRoot, SchemaSnapshotID: pin.SchemaSnapshotID, SchemaRoot: pin.SchemaRoot, LogicalDatabase: "db", QueryProfileID: "query-profile", ExecutorProfileID: "executor-profile", ReservationID: "reservation", FencingGeneration: 1}
	input := replay.SnapshotQueryInput{Binding: binding, SQL: "INSERT INTO db.target SELECT 1", ReadSet: reads}
	root, err := replay.SnapshotQueryInputRoot(input)
	if err != nil {
		t.Fatal(err)
	}
	return replay.SnapshotQueryEnvelope{Input: input, InputRoot: root, UserJWS: "original-compact-jws"}
}
