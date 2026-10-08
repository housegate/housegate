package housegate

import (
	"context"
	"testing"
	"time"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type recordingMeter struct{ events chan sicore.SIWriteEvent }

func (m recordingMeter) OnStatementSequenced(_ context.Context, ev sicore.SIWriteEvent) {
	m.events <- ev
}

type blockingMeter struct{ release chan struct{} }

func (m blockingMeter) OnStatementSequenced(context.Context, sicore.SIWriteEvent) { <-m.release }

func TestWriteMeter_SequencedEventCarriesTheOwner(t *testing.T) {
	ingress, _, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeAccepted, StatementSeq: 9}
	meter := recordingMeter{events: make(chan sicore.SIWriteEvent, 1)}
	ingress.SetWriteMeter(meter)
	adm := bpAdmission()
	adm.Owner, adm.Principal = "0x00000000000000000000000000000000000000c2", "0x00000000000000000000000000000000000000c2"
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	select {
	case ev := <-meter.events:
		want := sicore.SIWriteEvent{
			StatementID: adm.StatementID, Signer: adm.Signer, Owner: adm.Owner, Principal: adm.Principal,
			TableID: adm.TableID, PayloadBytes: uint64(len(adm.Payload.Bytes)), StatementSeq: 9,
		}
		if ev != want {
			t.Fatalf("event = %+v, want %+v", ev, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no OnStatementSequenced event")
	}
}

func TestWriteMeter_NotCalledWithoutAcceptedSubmission(t *testing.T) {
	ingress, _, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, AdmissionCode: "ADMISSION_CODE_MALFORMED"}
	meter := recordingMeter{events: make(chan sicore.SIWriteEvent, 1)}
	ingress.SetWriteMeter(meter)
	_ = ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	select {
	case ev := <-meter.events:
		t.Fatalf("unexpected event %+v for a rejected statement", ev)
	case <-time.After(200 * time.Millisecond):
	}
}

// The meter is best-effort and asynchronous: a stuck meter never blocks or
// fails the write (spec 2026-10-09 §6.9).
func TestWriteMeter_NeverBlocksTheWrite(t *testing.T) {
	ingress, _, _, _ := newBackpressureIngress(t, &fakePartsPressure{})
	meter := blockingMeter{release: make(chan struct{})}
	defer close(meter.release)
	ingress.SetWriteMeter(meter)
	done := make(chan error, 1)
	go func() { done <- ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a blocked meter blocked the write")
	}
}
