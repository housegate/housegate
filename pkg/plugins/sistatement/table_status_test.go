package sistatement

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// scriptedStatuses answers every lookup with one status (or error) and
// records the (database, table) pairs asked.
type scriptedStatuses struct {
	mu     sync.Mutex
	status registry.TableStatus
	err    error
	asked  []string
}

func (s *scriptedStatuses) StorageIntegrityTableStatus(_ context.Context, database, table string) (registry.TableStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked = append(s.asked, database+"."+table)
	return s.status, s.err
}

func activeStatus(t *testing.T, schema payloadexec.TableSchema, hash string) registry.TableStatus {
	t.Helper()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	return registry.TableStatus{Status: registry.TableStatusActive, SchemaJSON: string(js), SchemaHash: hash, RegistryVersion: 9}
}

type statusMetrics struct {
	inlineMetrics
	failed int
}

func (m *statusMetrics) TableStatusLookupFailed() { m.failed++ }

func newStatusPlugin(t *testing.T, statuses registry.TableStatuses, inline bool, ev ValuesEvaluator) (*Plugin, *SeqCounter, *statusMetrics) {
	t.Helper()
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := OpenSeqCounter(t.TempDir(), signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	metrics := &statusMetrics{}
	opts := Options{Signer: signer, Statuses: statuses, NetworkID: testNetworkID, Seq: seq, MaxPayloadBytes: 1 << 20, Observer: metrics}
	if inline {
		opts.Evaluator = ev
		opts.InlineValues = InlineValuesOptions{Enabled: true, EvaluationTimeout: 5 * time.Second, MaxRows: 1000}
	}
	p, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p, seq, metrics
}

// TestPlugin_SignsActiveTablesOnly is spec 2026-09-24 §10.1: Active signs,
// every other status passes through unchanged and unsigned.
func TestPlugin_SignsActiveTablesOnly(t *testing.T) {
	for _, status := range []string{registry.TableStatusOrdinary, registry.TableStatusPending, registry.TableStatusRefused, registry.TableStatusGone} {
		statuses := &scriptedStatuses{status: registry.TableStatus{Status: status, RefusedCode: "column_type"}}
		p, seq, _ := newStatusPlugin(t, statuses, false, nil)
		q := insertQctx(newSession(1, ""), "INSERT INTO shop.orders FORMAT Native")
		q.Query.Compression = proto.CompressionEnabled // a pass-through is not subject to the lane's rules
		if err := p.OnQuery(context.Background(), q); err != nil {
			t.Fatalf("%s: err = %v, want pass-through", status, err)
		}
		if q.DeferredInsert != nil || q.Query.ID != "client-uuid-1" || seq.Last() != 0 {
			t.Fatalf("%s: the statement was claimed (deferred=%v id=%q seq=%d)", status, q.DeferredInsert, q.Query.ID, seq.Last())
		}
		if strings.Join(statuses.asked, ",") != "shop.orders" {
			t.Fatalf("%s: asked %v", status, statuses.asked)
		}
	}
	statuses := &scriptedStatuses{status: activeStatus(t, testSchema(), payloadexec.TableSchemaHash(testNetworkID, testSchema()))}
	p, seq, _ := newStatusPlugin(t, statuses, false, nil)
	q := insertQctx(newSession(2, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if q.DeferredInsert == nil || seq.Last() != 1 {
		t.Fatalf("an Active table must be claimed for signing (deferred=%v seq=%d)", q.DeferredInsert, seq.Last())
	}
}

func TestPlugin_StatusFailurePassesThroughAndCounts(t *testing.T) {
	statuses := &scriptedStatuses{err: errors.New("rpc: connection refused")}
	p, seq, metrics := newStatusPlugin(t, statuses, false, nil)
	q := insertQctx(newSession(3, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("a status failure must pass through, got %v", err)
	}
	if q.DeferredInsert != nil || seq.Last() != 0 || metrics.failed != 1 {
		t.Fatalf("deferred=%v seq=%d failures=%d, want unsigned pass-through counted once", q.DeferredInsert, seq.Last(), metrics.failed)
	}
}

func TestPlugin_HashMismatchRefusesUnsigned(t *testing.T) {
	statuses := &scriptedStatuses{status: activeStatus(t, testSchema(), "0xdeadbeef")}
	p, seq, _ := newStatusPlugin(t, statuses, false, nil)
	q := insertQctx(newSession(4, ""), "INSERT INTO shop.orders FORMAT Native")
	err := p.OnQuery(context.Background(), q)
	if err == nil || !strings.Contains(err.Error(), "does not match the recomputed") {
		t.Fatalf("err = %v, want a hash-mismatch refusal", err)
	}
	if q.DeferredInsert != nil || seq.Last() != 0 {
		t.Fatal("a refused statement must not be claimed")
	}
}

func TestPlugin_ActiveSchemaForAnotherTableIsRefused(t *testing.T) {
	other := testSchema()
	other.TableID = "shop.other"
	statuses := &scriptedStatuses{status: activeStatus(t, other, payloadexec.TableSchemaHash(testNetworkID, other))}
	p, _, _ := newStatusPlugin(t, statuses, false, nil)
	err := p.OnQuery(context.Background(), insertQctx(newSession(5, ""), "INSERT INTO shop.orders FORMAT Native"))
	if err == nil || !strings.Contains(err.Error(), `carries a schema for "shop.other"`) {
		t.Fatalf("err = %v, want an identity refusal", err)
	}
}

// TestPlugin_InlineValuesEvaluatesOnlyActiveTables pins that the inline lane
// neither evaluates nor refuses a statement into a non-Active table.
func TestPlugin_InlineValuesEvaluatesOnlyActiveTables(t *testing.T) {
	ev := &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}}
	statuses := &scriptedStatuses{status: registry.TableStatus{Status: registry.TableStatusPending}}
	p, seq, _ := newStatusPlugin(t, statuses, true, ev)
	q := inlineQctx(newSession(6, ""), "INSERT INTO shop.orders VALUES (rand(), 'eu', 1.5)")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("a Pending inline INSERT must pass through, even one the closure gate would refuse: %v", err)
	}
	if ev.calls != 0 || q.SynthesizedInsert != nil || seq.Last() != 0 {
		t.Fatalf("evaluations=%d synthesized=%v seq=%d, want none", ev.calls, q.SynthesizedInsert, seq.Last())
	}
}
