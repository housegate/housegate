package storageintegrity

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"
)

func TestPreparedOutputProjectionCannotBeClaim(t *testing.T) {
	env := snapshotQueryEnvelopeFixture(t)
	accepted := replay.SnapshotQuerySubmitResult{AdmissionCode: 0, StatementSeq: 1, BlockSeq: 9, SourceNode: "source", InputRoot: env.InputRoot, Reservation: replay.SnapshotQueryReservation{ReservationID: env.Input.Binding.ReservationID, FencingGeneration: env.Input.Binding.FencingGeneration, ClientAccount: env.Input.Binding.ClientAccount, StatementID: env.Input.Binding.StatementID, ReadSnapshot: env.Input.Binding.ReadSnapshot, ExecutorProfileID: env.Input.Binding.ExecutorProfileID, QueryProfileID: env.Input.Binding.QueryProfileID, ActivationID: "active"}}
	base := SnapshotQueryPrepared{StatementID: env.Input.Binding.StatementID, InputRoot: env.InputRoot, BlockSeq: 9, FencingGeneration: env.Input.Binding.FencingGeneration, OutputRowsRoot: replay.DigestString("output"), ComputedStateRoot: replay.DigestString("state"), Status: "PendingUnsubmitted", Candidates: []replay.SnapshotReadPart{}, Stage: string(SnapshotQueryStagePreparedOutput), CachePath: "cache", CacheDigest: replay.DigestString("cache")}
	if err := validateSnapshotQueryPrepared(env, accepted, base); err != nil {
		t.Fatalf("valid pending projection: %v", err)
	}
	for name, alter := range map[string]func(*SnapshotQueryPrepared){
		"claim root": func(p *SnapshotQueryPrepared) { p.SourceClaimRoot = replay.DigestString("claim") },
		"candidate":  func(p *SnapshotQueryPrepared) { p.Candidates = []replay.SnapshotReadPart{{TableID: "t"}} },
		"capacity":   func(p *SnapshotQueryPrepared) { p.CapacityReservationID = "capacity" },
		"status":     func(p *SnapshotQueryPrepared) { p.Status = "Committed" },
	} {
		t.Run(name, func(t *testing.T) {
			p := base
			alter(&p)
			if err := validateSnapshotQueryPrepared(env, accepted, p); err == nil {
				t.Fatal("claim-like prepared projection was accepted")
			}
		})
	}
}

type preparedOutputFake struct {
	job    replay.SnapshotQueryJob
	result replay.ExecutionResult
	out    *canonicalOutputFake
	closed int
	events *[]string
}

func (p *preparedOutputFake) Job() replay.SnapshotQueryJob                   { return p.job }
func (p *preparedOutputFake) PreparedResult() replay.ExecutionResult         { return p.result }
func (p *preparedOutputFake) CanonicalOutput() snapshotquery.CanonicalOutput { return p.out }
func (p *preparedOutputFake) Close() error {
	p.closed++
	*p.events = append(*p.events, "close")
	return nil
}

type canonicalOutputFake struct {
	opens    int
	failOpen error
	failNext error
	events   *[]string
}

func (o *canonicalOutputFake) RowCount() uint64              { return 0 }
func (o *canonicalOutputFake) OutputRowsRoot() string        { return replay.DigestString("output") }
func (o *canonicalOutputFake) TouchedPartitionIDs() []string { return nil }
func (o *canonicalOutputFake) OpenRows() (payloadexec.RowSource, error) {
	o.opens++
	if o.failOpen != nil {
		return nil, o.failOpen
	}
	return &rowSourceFake{err: o.failNext}, nil
}
func (o *canonicalOutputFake) Close() error { return nil }

type rowSourceFake struct{ err error }

func (r *rowSourceFake) Next(context.Context) (payloadexec.Row, error) {
	if r.err != nil {
		return payloadexec.Row{}, r.err
	}
	return payloadexec.Row{}, io.EOF
}
func (r *rowSourceFake) Close() error { return nil }

type stageJournalFake struct {
	rec     SnapshotQueryJournalRecord
	saveErr error
	saved   int
	events  *[]string
}

func (j *stageJournalFake) Load(context.Context, string) (SnapshotQueryJournalRecord, bool, error) {
	return j.rec, true, nil
}
func (j *stageJournalFake) List(context.Context) ([]SnapshotQueryJournalRecord, error) {
	return nil, nil
}
func (j *stageJournalFake) Save(_ context.Context, r SnapshotQueryJournalRecord) error {
	j.saved++
	*j.events = append(*j.events, "save")
	if j.saveErr != nil {
		return j.saveErr
	}
	j.rec = r
	return nil
}

func TestPreparedOutputStagerStageClosesBeforePersistenceAndOnFailures(t *testing.T) {
	env := snapshotQueryEnvelopeFixture(t)
	accepted := snapshotQueryAcceptedResult(env)
	baseJob := replay.SnapshotQueryJob{BlockSeq: accepted.BlockSeq, Reservation: accepted.Reservation, Statement: replay.SnapshotQueryStatement{StatementSeq: accepted.StatementSeq, Envelope: env}}
	for name, tc := range map[string]struct {
		cancel                    bool
		openErr, nextErr, saveErr error
		wantSave                  int
	}{
		"success":          {wantSave: 1},
		"context canceled": {cancel: true},
		"open rows":        {openErr: errors.New("open")},
		"row read":         {nextErr: errors.New("read")},
		"save":             {saveErr: errors.New("save"), wantSave: 1},
	} {
		t.Run(name, func(t *testing.T) {
			events := []string{}
			ctx := context.Background()
			if tc.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			out := &canonicalOutputFake{failOpen: tc.openErr, failNext: tc.nextErr, events: &events}
			p := &preparedOutputFake{job: baseJob, result: replay.ExecutionResult{SnapshotQuery: &replay.SnapshotQueryEvidence{ExecutionOutcome: "applied", OutputRowsRoot: out.OutputRowsRoot()}, ComputedStateRoot: replay.DigestString("state")}, out: out, events: &events}
			j := &stageJournalFake{rec: SnapshotQueryJournalRecord{Version: SnapshotQueryJournalVersion, StatementID: env.Input.Binding.StatementID, Envelope: env, Stage: SnapshotQueryStageSequenced, Submit: accepted, HasSubmit: true}, saveErr: tc.saveErr, events: &events}
			s, err := NewPreparedOutputStager(j, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.stage(ctx, SnapshotQueryPrepareRequest{Envelope: env, Accepted: accepted}, p)
			if tc.saveErr == nil && !tc.cancel && tc.openErr == nil && tc.nextErr == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("Stage unexpectedly succeeded")
			}
			if p.closed != 1 {
				t.Fatalf("close=%d, want 1", p.closed)
			}
			if out.opens != 1 && !tc.cancel {
				t.Fatalf("OpenRows=%d, want 1", out.opens)
			}
			if j.saved != tc.wantSave {
				t.Fatalf("saves=%d, want %d", j.saved, tc.wantSave)
			}
			if tc.wantSave == 1 && events[0] != "close" {
				t.Fatalf("events=%v, close must precede Save", events)
			}
		})
	}
}

func TestSnapshotQueryJournalPreparedOutputStageRequiresProjection(t *testing.T) {
	env := snapshotQueryEnvelopeFixture(t)
	rec := SnapshotQueryJournalRecord{Version: SnapshotQueryJournalVersion, StatementID: env.Input.Binding.StatementID, Envelope: env, Stage: SnapshotQueryStagePreparedOutput}
	if err := validateSnapshotQueryJournalRecord(rec); err == nil {
		t.Fatal("PreparedOutput stage without projection was accepted")
	}
}
