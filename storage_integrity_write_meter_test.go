package housegate

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

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

type panickingMeter struct {
	calls chan string
}

func (m panickingMeter) OnStatementSequenced(_ context.Context, ev sicore.SIWriteEvent) {
	m.calls <- ev.StatementID
	panic("meter exploded")
}

// A panicking meter never fails the write or kills the process, and later
// events are still delivered.
func TestWriteMeter_PanicIsRecovered(t *testing.T) {
	ingress, _, _, _ := newBackpressureIngress(t, &fakePartsPressure{})
	meter := panickingMeter{calls: make(chan string, 2)}
	ingress.SetWriteMeter(meter)
	for n := 1; n <= 2; n++ {
		adm := bpAdmission()
		adm.StatementID = fmt.Sprintf("0xabc:%d:n1", n)
		if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm); err != nil {
			t.Fatalf("Consume %d: %v", n, err)
		}
		select {
		case id := <-meter.calls:
			if id != adm.StatementID {
				t.Fatalf("event %d statement id = %q, want %q", n, id, adm.StatementID)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("meter call %d never ran", n)
		}
	}
	ingress.Close()
}

// The meter is bounded: with a stuck meter exactly writeMeterMaxInFlight
// events are in flight and the overflow is dropped and counted, never blocking
// a write.
func TestWriteMeter_BoundedInFlightDropsOverflow(t *testing.T) {
	ingress, _, _, _ := newBackpressureIngress(t, &fakePartsPressure{})
	started := make(chan struct{}, writeMeterMaxInFlight+1)
	release := make(chan struct{})
	ingress.SetWriteMeter(funcMeter(func(context.Context, sicore.SIWriteEvent) {
		started <- struct{}{}
		<-release
	}))
	before := testutil.ToFloat64(storageIntegrityWriteMeterDroppedTotal)
	const total = writeMeterMaxInFlight + 1
	for n := 1; n <= total; n++ {
		adm := bpAdmission()
		adm.StatementID = fmt.Sprintf("0xabc:%d:n1", n)
		done := make(chan error, 1)
		go func() { done <- ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Consume %d: %v", n, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("write %d blocked on a stuck meter", n)
		}
	}
	for n := 0; n < writeMeterMaxInFlight; n++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d meter calls started, want %d", n, writeMeterMaxInFlight)
		}
	}
	if got := testutil.ToFloat64(storageIntegrityWriteMeterDroppedTotal) - before; got != 1 {
		t.Fatalf("dropped = %v, want exactly the overflow (1)", got)
	}
	close(release)
	ingress.Close()
	select {
	case <-started:
		t.Fatal("a dropped event reached the meter")
	default:
	}
}

type funcMeter func(context.Context, sicore.SIWriteEvent)

func (f funcMeter) OnStatementSequenced(ctx context.Context, ev sicore.SIWriteEvent) { f(ctx, ev) }

// Close joins in-flight meter calls (cancelling their context), and a write
// that completes after Close starts no meter call.
func TestWriteMeter_CloseJoinsInFlightAndStopsNewCalls(t *testing.T) {
	ingress, _, _, _ := newBackpressureIngress(t, &fakePartsPressure{})
	started := make(chan struct{})
	finished := make(chan struct{})
	var calls atomic.Int32
	ingress.SetWriteMeter(funcMeter(func(ctx context.Context, _ sicore.SIWriteEvent) {
		calls.Add(1)
		close(started)
		<-ctx.Done() // only Close (or the 10s timeout) ends it
		close(finished)
	}))
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("meter call never started")
	}
	closed := make(chan struct{})
	go func() { ingress.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after cancelling the in-flight meter call")
	}
	select {
	case <-finished:
	default:
		t.Fatal("Close returned before the in-flight meter call finished")
	}

	before := testutil.ToFloat64(storageIntegrityWriteMeterDroppedTotal)
	adm := bpAdmission()
	adm.StatementID = "0xabc:2:n1"
	_ = ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
	time.Sleep(100 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("meter calls = %d, want 1: no call may start after Close", got)
	}
	if got := testutil.ToFloat64(storageIntegrityWriteMeterDroppedTotal) - before; got != 1 {
		t.Fatalf("post-Close event dropped count = %v, want 1", got)
	}
}
