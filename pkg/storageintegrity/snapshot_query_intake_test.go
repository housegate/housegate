package storageintegrity

import (
	"context"
	"errors"
	"reflect"
	"testing"

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
	*j.events = append(*j.events, "persist_"+string(r.Stage))
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
	*g.events = append(*g.events, "TryStart")
	return g.win
}

type snapshotQueryFakeValidator struct {
	events *[]string
	reject bool
}

func (v snapshotQueryFakeValidator) ValidateStatementV3(_ string, want auth.JWSStatementPayloadV3) (string, error) {
	*v.events = append(*v.events, "validate")
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
}

func (s *snapshotQueryFakeSequencer) SubmitSnapshotQuery(_ context.Context, env replay.SnapshotQueryEnvelope) (replay.SnapshotQuerySubmitResult, error) {
	*s.events = append(*s.events, "submit")
	s.submit++
	if s.submitErr != nil {
		return replay.SnapshotQuerySubmitResult{}, s.submitErr
	}
	return replay.SnapshotQuerySubmitResult{InputRoot: env.InputRoot, BlockSeq: 7}, nil
}
func (s *snapshotQueryFakeSequencer) LookupSnapshotQuery(_ context.Context, _, _, _ string) (replay.SnapshotQueryStatus, error) {
	*s.events = append(*s.events, "lookup_submit")
	s.lookup++
	return s.status, nil
}

type snapshotQueryFakeReconciler struct {
	events *[]string
	calls  int
}

func (r *snapshotQueryFakeReconciler) ReconcileSnapshotQueryIntent(_ context.Context, _ SnapshotQueryJournalRecord, _ replay.SnapshotQueryStatus) error {
	*r.events = append(*r.events, "reconcile")
	r.calls++
	return nil
}

func newSnapshotQueryIntakeFixture(t *testing.T) (*SnapshotQueryIntake, replay.SnapshotQueryEnvelope, *[]string, *snapshotQueryMemoryJournal, *snapshotQueryFakeSequencer, *snapshotQueryFakeReconciler) {
	t.Helper()
	events := []string{}
	journal := &snapshotQueryMemoryJournal{records: map[string]SnapshotQueryJournalRecord{}, events: &events}
	sequencer := &snapshotQueryFakeSequencer{events: &events}
	reconciler := &snapshotQueryFakeReconciler{events: &events}
	intake, err := NewSnapshotQueryIntake(SnapshotQueryIntakeOptions{Journal: journal, Sequencer: sequencer, Validator: snapshotQueryFakeValidator{events: &events}, Reconciler: reconciler})
	if err != nil {
		t.Fatal(err)
	}
	return intake, snapshotQueryEnvelopeFixture(t), &events, journal, sequencer, reconciler
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
	if !reflect.DeepEqual(*events, []string{"validate", "lookup_submit", "reconcile"}) {
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
