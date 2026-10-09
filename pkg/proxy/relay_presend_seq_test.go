package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/sistatement"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Spec 2026-10-09 §6.5: a client_seq reserved by the real sistatement plugin
// in OnQueryInputCompleteStrict is released when Relay terminates the signed
// INSERT before WriteQuery begins, and stays burned once the write started.
// These tests drive the real plugin through Relay on both signed lanes.

const (
	presendKey       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	presendNetworkID = "testnet-v2"
	presendDeferred  = "INSERT INTO shop.orders FORMAT Native"
	presendInline    = "INSERT INTO shop.orders (id, region, amount) VALUES (1, 'eu', 1.5)"
)

func presendSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "shop.orders",
		PartitionBy: "region",
		Columns: []lthash.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "region", Type: "String"},
			{Name: "amount", Type: "Float64"},
		},
	}
}

type presendSeqMetrics struct {
	mu       sync.Mutex
	recycled int
	burned   map[string]int
}

func (*presendSeqMetrics) InlineValuesSynthesized()      {}
func (*presendSeqMetrics) InlineValuesEvaluationFailed() {}
func (*presendSeqMetrics) InlineValuesClosureRefused()   {}
func (m *presendSeqMetrics) SeqRecycled()                { m.mu.Lock(); m.recycled++; m.mu.Unlock() }
func (m *presendSeqMetrics) SeqBurned(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.burned == nil {
		m.burned = map[string]int{}
	}
	m.burned[reason]++
}

func (m *presendSeqMetrics) snapshot() (int, map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]int{}
	for k, v := range m.burned {
		out[k] = v
	}
	return m.recycled, out
}

type presendEvaluator struct{}

func (presendEvaluator) Evaluate(context.Context, sistatement.ValuesEvaluation) ([][]proto.InputColumn, error) {
	id, region, amount := &proto.ColUInt64{}, &proto.ColStr{}, &proto.ColFloat64{}
	id.Append(1)
	region.Append("eu")
	amount.Append(1.5)
	return [][]proto.InputColumn{{{Name: "id", Data: id}, {Name: "region", Data: region}, {Name: "amount", Data: amount}}}, nil
}

// materializedMarker stands in for the agent's materialize plugin, which the
// inline lane requires to have run.
type materializedMarker struct{}

func (materializedMarker) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	qctx.Values[plugin.ValuesKeyMaterialized] = plugin.MaterializeOutcomeNoop
	return nil
}

// presendSession hides its upstream once drop is set, the shape of a session
// whose upstream went away between the strict hook and the forward.
type presendSession struct {
	chsession.Session
	drop atomic.Bool
}

func (s *presendSession) Upstream() *chproto.Codec {
	if s.drop.Load() {
		return nil
	}
	return s.Session.Upstream()
}

type presendSite int

const (
	siteStrictHookClose presendSite = iota
	siteStrictHookKeep
	siteNoUpstream
	siteActiveQueryRace
	siteWriteQueryFailed
)

// presendSiteHook runs after sistatement's strict hook, i.e. after the seq was
// reserved and the statement signed, and induces one termination site.
type presendSiteHook struct {
	site     presendSite
	sess     *presendSession
	relay    atomic.Pointer[Relay]
	upstream io.Closer // the peer end of the upstream connection
}

func (h *presendSiteHook) OnQueryInputCompleteStrict(_ context.Context, qctx *plugin.QueryContext) error {
	if qctx.DeferredInsert == nil && qctx.SynthesizedInsert == nil {
		return nil
	}
	switch h.site {
	case siteStrictHookClose:
		return errors.New("agent-sign: key unavailable")
	case siteStrictHookKeep:
		return &chproto.ClientError{Code: 252, Message: "retry later", KeepSession: true}
	case siteNoUpstream:
		h.sess.drop.Store(true)
	case siteActiveQueryRace:
		r := h.relay.Load()
		r.queryMu.Lock()
		r.activeQuery, r.activeQueryID = true, "someone-else"
		r.queryMu.Unlock()
	case siteWriteQueryFailed:
		_ = h.upstream.Close()
	}
	return nil
}

type presendFixture struct {
	si      *sistatement.Plugin
	seq     *sistatement.SeqCounter
	metrics *presendSeqMetrics
	h       *deferredHarness
}

func newPresendFixture(t *testing.T, site presendSite, inline bool) *presendFixture {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	schema := presendSchema()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	ns.TableSchemas["shop/orders@1"] = network.TableSchemaInfo{
		DatabaseId: "shop", TableId: "orders", Version: 1,
		SchemaHash: payloadexec.TableSchemaHash(presendNetworkID, schema), SchemaJson: string(js),
	}
	signer, err := auth.NewRelaySigner(presendKey)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := sistatement.OpenSeqCounter(t.TempDir(), signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = seq.Close() })
	metrics := &presendSeqMetrics{}
	opts := sistatement.Options{Signer: signer, Schemas: ns, NetworkID: presendNetworkID, Seq: seq, MaxPayloadBytes: 1 << 20, Observer: metrics}
	if inline {
		opts.Evaluator = presendEvaluator{}
		opts.InlineValues = sistatement.InlineValuesOptions{Enabled: true, EvaluationTimeout: 5 * time.Second, MaxRows: 1000}
	}
	si, err := sistatement.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	siteHook := &presendSiteHook{site: site}
	chain := &plugin.PluginChain{
		QueryPlugins:                    []plugin.QueryPlugin{materializedMarker{}, si},
		StrictDataPlugins:               []plugin.StrictDataPlugin{si},
		ExceptionPlugins:                []plugin.ExceptionPlugin{si},
		QueryInputCompleteStrictPlugins: []plugin.QueryInputCompleteStrictPlugin{si, siteHook},
		QueryAbortPlugins:               []plugin.QueryAbortPlugin{si},
		QuerySuccessPlugins:             []plugin.QuerySuccessPlugin{si},
		QueryCompletePlugins:            []plugin.QueryCompletePlugin{si},
		ClosePlugins:                    []plugin.ClosePlugin{si},
	}
	h := newPresendHarness(t, chain, siteHook)
	return &presendFixture{si: si, seq: seq, metrics: metrics, h: h}
}

// newPresendHarness is newDeferredHarness with the session wrapped so a site
// can hide the upstream, and the client leg drained in the background.
func newPresendHarness(t *testing.T, hooks plugin.Hooks, site *presendSiteHook) *deferredHarness {
	t.Helper()
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	inner := chsession.New(1, proxyClient)
	inner.Client().SetRevision(deferredTestRev)
	inner.State().ClientRevision = deferredTestRev
	inner.State().SetUpstreamHello(&chproto.ClientHello{ProtocolVersion: deferredTestRev, User: "writer", Database: "shop"})
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(deferredTestRev)
	if err := inner.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	site.sess = &presendSession{Session: inner}
	site.upstream = upstreamProxy
	r := &Relay{sess: site.sess, hooks: hooks}
	site.relay.Store(r)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	h := &deferredHarness{clientProxy: clientProxy, proxyClient: proxyClient, upstreamProxy: upstreamProxy, proxyUpstream: proxyUpstream, relay: r, loopErr: make(chan error, 2), cancel: cancel}
	go func() { h.loopErr <- r.clientToUpstream(ctx) }()
	go func() { h.loopErr <- r.upstreamToClient(ctx) }()
	go func() { _, _ = io.Copy(io.Discard, clientProxy) }()
	t.Cleanup(func() {
		cancel()
		_ = clientProxy.Close()
		_ = upstreamProxy.Close()
	})
	return h
}

func (f *presendFixture) run(t *testing.T, inline bool) {
	t.Helper()
	if inline {
		writeAllConn(t, f.h.clientProxy, encodeInsertQuery(t, "client-id", presendInline))
		writeAllConn(t, f.h.clientProxy, encodeEmptyClientData(t))
	} else {
		writeAllConn(t, f.h.clientProxy, encodeInsertQuery(t, "client-id", presendDeferred))
		writeAllConn(t, f.h.clientProxy, encodeEmptyClientData(t))
		writeAllConn(t, f.h.clientProxy, encodeNonEmptyClientDataPacket(t, deferredTestRev))
		writeAllConn(t, f.h.clientProxy, encodeEmptyClientData(t))
	}
	// Every site ends the query through OnQueryComplete, which settles the
	// reservation one way or the other; only then are the loops stopped.
	deadline := time.Now().Add(3 * time.Second)
	for {
		recycled, burned := f.metrics.snapshot()
		if recycled+len(burned) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the reservation was never settled (client_seq high watermark %d)", f.seq.Last())
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.h.close(t)
	if f.seq.Last() != 1 {
		t.Fatalf("client_seq high watermark = %d, want exactly one reservation", f.seq.Last())
	}
}

func TestRelay_SignedInsertPreSendTerminationReleasesClientSeq(t *testing.T) {
	sites := map[string]presendSite{
		"later strict hook refuses (close)": siteStrictHookClose,
		"later strict hook refuses (keep)":  siteStrictHookKeep,
		"no upstream after strict hook":     siteNoUpstream,
		"active query race":                 siteActiveQueryRace,
	}
	for _, lane := range []struct {
		name   string
		inline bool
	}{{"deferred", false}, {"synthesized", true}} {
		for name, site := range sites {
			t.Run(lane.name+"/"+name, func(t *testing.T) {
				f := newPresendFixture(t, site, lane.inline)
				f.run(t, lane.inline)
				recycled, burned := f.metrics.snapshot()
				if recycled != 1 || len(burned) != 0 {
					t.Fatalf("recycled=%d burned=%v; want the pre-send seq released", recycled, burned)
				}
				if got, err := f.seq.Reserve(); err != nil || got != 1 {
					t.Fatalf("next Reserve = %d (err %v), want the released seq 1", got, err)
				}
			})
		}
	}
}

func TestRelay_SignedInsertFailedQueryWriteBurnsClientSeq(t *testing.T) {
	for _, lane := range []struct {
		name   string
		inline bool
	}{{"deferred", false}, {"synthesized", true}} {
		t.Run(lane.name, func(t *testing.T) {
			f := newPresendFixture(t, siteWriteQueryFailed, lane.inline)
			f.run(t, lane.inline)
			recycled, burned := f.metrics.snapshot()
			if recycled != 0 || burned["unknown_outcome"] != 1 {
				t.Fatalf("recycled=%d burned=%v; want the seq burned once WriteQuery began", recycled, burned)
			}
			if got, err := f.seq.Reserve(); err != nil || got != 2 {
				t.Fatalf("next Reserve = %d (err %v), want a fresh seq 2", got, err)
			}
		})
	}
}
