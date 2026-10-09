package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

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
	siteNone // the strict hook succeeds; the outcome is decided upstream
)

// presendSiteHook runs after sistatement's strict hook, i.e. after the seq was
// reserved and the statement signed, and induces one termination site.
type presendSiteHook struct {
	site     presendSite
	signed   atomic.Pointer[weak.Pointer[plugin.QueryContext]] // never a strong reference
	input    chan struct{}                                     // OnQueryInputComplete fired
	sess     *presendSession
	relay    atomic.Pointer[Relay]
	upstream io.Closer // the peer end of the upstream connection
}

func (h *presendSiteHook) OnQueryInputCompleteStrict(_ context.Context, qctx *plugin.QueryContext) error {
	if qctx.DeferredInsert == nil && qctx.SynthesizedInsert == nil {
		return nil
	}
	wp := weak.Make(qctx)
	h.signed.Store(&wp)
	switch h.site {
	case siteStrictHookClose:
		return errors.New("agent-sign: key unavailable")
	case siteStrictHookKeep:
		return &chproto.ClientError{Code: 252, Message: "retry later", KeepSession: true}
	case siteNoUpstream:
		h.sess.drop.Store(true)
	case siteActiveQueryRace:
		// Unreachable in production: clientToUpstream refuses a new Query while
		// currentActiveQuery reports one in flight (relay.go "client sent query
		// ... before upstream completed query"), so nothing else can hold the
		// active slot when forwardSignedInsert calls beginActiveQuery. The test
		// forges the slot to cover that defensive branch.
		r := h.relay.Load()
		r.queryMu.Lock()
		r.activeQuery, r.activeQueryID = true, "someone-else"
		r.queryMu.Unlock()
	case siteWriteQueryFailed:
		_ = h.upstream.Close()
	}
	return nil
}

// OnQueryInputComplete lets an upstream script answer EndOfStream only after
// Relay finished writing the input, as a real server does.
func (h *presendSiteHook) OnQueryInputComplete(context.Context, *plugin.QueryContext) {
	select {
	case h.input <- struct{}{}:
	default:
	}
}

type presendFixture struct {
	si      *sistatement.Plugin
	seq     *sistatement.SeqCounter
	metrics *presendSeqMetrics
	h       *deferredHarness
	site    *presendSiteHook
	laneDir string // non-empty: the plugin reserves on a client lane here
}

func newPresendFixture(t *testing.T, site presendSite, inline bool) *presendFixture {
	t.Helper()
	return newPresendFixtureWith(t, site, inline, false)
}

// newPresendFixtureWith builds the fixture; laned makes the network report
// client lanes enabled, so every statement reserves on a client lane.
func newPresendFixtureWith(t *testing.T, site presendSite, inline, laned bool) *presendFixture {
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
	var laneDir string
	if laned {
		laneDir = t.TempDir()
		opts.Lanes = sistatement.LaneModeAuto
		opts.ClientLanesEnabled = func() bool { return true }
		opts.LaneDir = func(string) (string, error) { return laneDir, nil }
	}
	if inline {
		opts.Evaluator = presendEvaluator{}
		opts.InlineValues = sistatement.InlineValuesOptions{Enabled: true, EvaluationTimeout: 5 * time.Second, MaxRows: 1000}
	}
	si, err := sistatement.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = si.Close() })
	siteHook := &presendSiteHook{site: site, input: make(chan struct{}, 1)}
	chain := &plugin.PluginChain{
		QueryPlugins:                    []plugin.QueryPlugin{materializedMarker{}, si},
		StrictDataPlugins:               []plugin.StrictDataPlugin{si},
		ExceptionPlugins:                []plugin.ExceptionPlugin{si},
		QueryInputCompleteStrictPlugins: []plugin.QueryInputCompleteStrictPlugin{si, siteHook},
		QueryInputCompletePlugins:       []plugin.QueryInputCompletePlugin{siteHook},
		QueryAbortPlugins:               []plugin.QueryAbortPlugin{si},
		QuerySuccessPlugins:             []plugin.QuerySuccessPlugin{si},
		QueryCompletePlugins:            []plugin.QueryCompletePlugin{si},
		ClosePlugins:                    []plugin.ClosePlugin{si},
	}
	h := newPresendHarness(t, chain, siteHook)
	return &presendFixture{si: si, seq: seq, metrics: metrics, h: h, site: siteHook, laneDir: laneDir}
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
	if f.laneDir != "" {
		if lane := f.onlyLane(t); f.seq.Last() != 0 || lane.Next != 2 {
			t.Fatalf("legacy high watermark = %d, lane %s next = %d; want exactly one reservation, on the client lane", f.seq.Last(), lane.Lane, lane.Next)
		}
		return
	}
	if f.seq.Last() != 1 {
		t.Fatalf("client_seq high watermark = %d, want exactly one reservation", f.seq.Last())
	}
}

// presendLaneFile is the on-disk state of a client lane.
type presendLaneFile struct {
	Lane      string   `json:"lane"`
	Next      uint64   `json:"next"`
	Free      []uint64 `json:"free"`
	Abandoned bool     `json:"abandoned"`
}

// onlyLane reads the single client lane the laned fixture's plugin acquired.
func (f *presendFixture) onlyLane(t *testing.T) presendLaneFile {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(f.laneDir, "lanes", "*.json"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("lane files = %v (%v), want exactly one", paths, err)
	}
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	var lane presendLaneFile
	if err := json.Unmarshal(raw, &lane); err != nil {
		t.Fatal(err)
	}
	return lane
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

// Spec 2026-10-09 §6.5: once WriteQuery succeeded, an abort before the
// terminal (here the upstream refusing the INSERT where the lane expects the
// sample block, without the unspent marker) carries no unsent proof, so the
// seq stays burned.
func TestRelay_SignedInsertAbortAfterQueryWriteBurnsClientSeq(t *testing.T) {
	for _, lane := range []struct {
		name   string
		inline bool
	}{{"deferred", false}, {"synthesized", true}} {
		t.Run(lane.name, func(t *testing.T) {
			f := newPresendFixture(t, siteNone, lane.inline)
			upDone := make(chan error, 1)
			go func() {
				up := chproto.NewCodec(f.h.upstreamProxy, chproto.DirFromClient)
				up.SetRevision(deferredTestRev)
				up.SetCompression(proto.CompressionDisabled)
				upDone <- func() error {
					pkt, err := up.ReadPacket(uint64(chproto.ClientQueryCode))
					if err != nil {
						return err
					}
					if q, ok := pkt.Decoded.(*chproto.Query); !ok || !strings.Contains(q.ID, ":1:") {
						return errors.New("upstream did not receive the signed statement")
					}
					if _, err := up.ReadPacket(); err != nil { // external-tables marker
						return err
					}
					_, err = f.h.upstreamProxy.Write(encodeServerExceptionPacket(deferredTestRev, 60, "Table shop.orders does not exist"))
					return err
				}()
			}()
			f.run(t, lane.inline)
			if err := <-upDone; err != nil {
				t.Fatalf("upstream: %v", err)
			}
			recycled, burned := f.metrics.snapshot()
			if recycled != 0 || burned["unknown_outcome"] != 1 {
				t.Fatalf("recycled=%d burned=%v; want the seq burned after the Query was sent", recycled, burned)
			}
			if got, err := f.seq.Reserve(); err != nil || got != 2 {
				t.Fatalf("next Reserve = %d (err %v), want a fresh seq 2", got, err)
			}
		})
	}
}

// abortUnsentQuery enforces its pre-send-only contract: called for a query
// whose upstream WriteQuery has begun (a reject closure reached after the
// write), it must not vouch for the query, so the reserved seq is burned.
func TestRelay_AbortUnsentAfterQueryWriteBeganBurnsClientSeq(t *testing.T) {
	for _, written := range []bool{false, true} {
		name := map[bool]string{false: "before write (released)", true: "after write began (burned)"}[written]
		t.Run(name, func(t *testing.T) {
			f := newPresendFixture(t, siteNone, false)
			f.h.close(t) // drive the hooks directly on the stopped relay
			r := f.h.relay
			sess := r.sess
			sess.State().ClientRevision = deferredTestRev
			ctx := context.Background()
			qctx := &plugin.QueryContext{
				Session: sess, OriginalSQL: presendDeferred, Values: map[string]any{},
				Query: &chproto.Query{ID: "client-id", Body: presendDeferred, Compression: proto.CompressionDisabled},
			}
			if err := r.hooks.OnQuery(ctx, qctx); err != nil || qctx.DeferredInsert == nil {
				t.Fatalf("OnQuery: %v (deferred=%v)", err, qctx.DeferredInsert)
			}
			if err := r.hooks.OnClientDataStrict(ctx, qctx, encodeNonEmptyClientDataPacket(t, deferredTestRev)); err != nil {
				t.Fatal(err)
			}
			if err := r.hooks.OnQueryInputCompleteStrict(ctx, qctx); err != nil {
				t.Fatal(err)
			}
			if written {
				defer r.markSignedQueryWriteBegun(qctx.Query.ID)()
			}
			r.abortUnsentQuery(ctx, qctx)
			recycled, burned := f.metrics.snapshot()
			next, err := f.seq.Reserve()
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case written && (qctx.UpstreamQueryUnsent || recycled != 0 || burned["unknown_outcome"] != 1 || next != 2):
				t.Fatalf("after write: unsent=%v recycled=%d burned=%v next=%d; want no proof, seq 1 burned", qctx.UpstreamQueryUnsent, recycled, burned, next)
			case !written && (!qctx.UpstreamQueryUnsent || recycled != 1 || len(burned) != 0 || next != 1):
				t.Fatalf("before write: unsent=%v recycled=%d burned=%v next=%d; want seq 1 released", qctx.UpstreamQueryUnsent, recycled, burned, next)
			}
		})
	}
}

// unsentRecordingPrepareHooks records the unsent proof every abort carries.
type unsentRecordingPrepareHooks struct {
	relayPrepareHooks
	mu     sync.Mutex
	aborts []bool
}

func (h *unsentRecordingPrepareHooks) OnQueryAbort(_ context.Context, qctx *plugin.QueryContext) {
	h.mu.Lock()
	h.aborts = append(h.aborts, qctx.UpstreamQueryUnsent)
	h.mu.Unlock()
}

// A preparation continuation can install a DeferredInsert plan after the
// AgentPrepare lane already began the active query and may have sequenced the
// statement. Relay refuses that ownership conflict like the synthesized lane
// does: unmarked, and without the unsent proof.
func TestRelay_DeferredInsertAfterAgentPrepareIsAnOwnershipConflict(t *testing.T) {
	signedPrepared := plugin.PreparedAgentQuery{Query: &chproto.Query{ID: "prepared", Body: "INSERT INTO db.t FORMAT Native", Settings: signedSettings()}}
	hooks := &unsentRecordingPrepareHooks{relayPrepareHooks: relayPrepareHooks{
		prepare:   func(context.Context) (plugin.PreparedAgentQuery, error) { return signedPrepared, nil },
		intent:    func(context.Context, plugin.PreparedAgentQuery) error { return nil },
		authorize: func(context.Context, plugin.PreparedAgentQuery) error { return nil },
		unknown:   func(context.Context, plugin.PreparedAgentQuery) error { return nil },
		onResume: func(qctx *plugin.QueryContext) error {
			qctx.DeferredInsert = &plugin.DeferredInsertPlan{SampleColumns: []chproto.SampleColumn{{Name: "v", Type: "UInt64"}}, MaxPayloadBytes: 1 << 20}
			return nil
		},
	}}
	clientPeer, clientProxy := net.Pipe()
	upstreamPeer, upstreamProxy := net.Pipe()
	defer clientPeer.Close()
	defer upstreamPeer.Close()
	const rev = chproto.MaxSupportedRevision
	sess := chsession.New(1, clientProxy)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(upstreamProxy, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	r := NewRelay(sess, hooks, nil, nil)
	client := chproto.NewCodec(clientPeer, chproto.DirToUpstream)
	client.SetRevision(rev)
	run := make(chan error, 1)
	go func() { run <- r.clientToUpstream(context.Background()) }()
	if err := client.WriteQuery(&chproto.Query{ID: "signed", Body: "INSERT INTO db.t FORMAT Native", Settings: signedSettings()}); err != nil {
		t.Fatalf("write query: %v", err)
	}
	_ = clientPeer.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := client.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("read exception: %v", err)
	}
	exc, ok := pkt.Decoded.(*chproto.Exception)
	if !ok || !strings.Contains(exc.Message, "DeferredInsert conflicts with another ownership plan") {
		t.Fatalf("client got packet %d %#v, want the ownership-conflict Exception", pkt.Type, pkt.Decoded)
	}
	if chproto.HasSeqUnspentSuffix(exc.Message) {
		t.Fatalf("post-preparation conflict carries the unspent marker: %q", exc.Message)
	}
	_ = clientPeer.Close()
	select {
	case <-run:
	case <-time.After(2 * time.Second):
		t.Fatal("relay did not finish")
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if len(hooks.aborts) != 1 || hooks.aborts[0] {
		t.Fatalf("aborts (unsent proof) = %v, want exactly one abort without the proof", hooks.aborts)
	}
	if _, active := r.currentActiveQuery(); active {
		t.Fatal("the prepared query's active slot was not released")
	}
}

// presendSample is the 0-row sample block ClickHouse answers for the signed
// INSERT into presendSchema.
func presendSample(t *testing.T) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeData))
	buf.PutString("")
	input := proto.Input{{Name: "id", Data: &proto.ColUInt64{}}, {Name: "region", Data: &proto.ColStr{}}, {Name: "amount", Data: &proto.ColFloat64{}}}
	if err := (proto.Block{Rows: 0, Columns: len(input)}).EncodeBlock(&buf, deferredTestRev, input); err != nil {
		t.Fatalf("encode sample: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

// The write-boundary guard must not keep a completed signed INSERT's
// QueryContext reachable: on the synthesized lane it owns the typed blocks and
// the encoded payload, which would otherwise stay pinned on every idle agent
// session for the connection's lifetime.
func TestRelay_SignedInsertQueryContextIsNotRetainedAfterCompletion(t *testing.T) {
	for _, lane := range []struct {
		name   string
		inline bool
	}{{"deferred", false}, {"synthesized", true}} {
		t.Run(lane.name, func(t *testing.T) {
			f := newPresendFixture(t, siteNone, lane.inline)
			sample := presendSample(t)
			upDone := make(chan error, 1)
			go func() {
				up := chproto.NewCodec(f.h.upstreamProxy, chproto.DirFromClient)
				up.SetRevision(deferredTestRev)
				up.SetCompression(proto.CompressionDisabled)
				upDone <- func() error {
					if _, err := up.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
						return err
					}
					if _, err := up.ReadPacket(); err != nil { // external-tables marker
						return err
					}
					if _, err := f.h.upstreamProxy.Write(sample); err != nil {
						return err
					}
					for i := 0; i < 2; i++ { // payload + terminator
						if _, err := up.ReadPacket(); err != nil {
							return err
						}
					}
					select {
					case <-f.site.input:
					case <-time.After(2 * time.Second):
						return errors.New("relay never completed the input")
					}
					_, err := f.h.upstreamProxy.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
					return err
				}()
			}()
			if lane.inline {
				writeAllConn(t, f.h.clientProxy, encodeInsertQuery(t, "client-id", presendInline))
				writeAllConn(t, f.h.clientProxy, encodeEmptyClientData(t))
			} else {
				writeAllConn(t, f.h.clientProxy, encodeInsertQuery(t, "client-id", presendDeferred))
				writeAllConn(t, f.h.clientProxy, encodeEmptyClientData(t))
				writeAllConn(t, f.h.clientProxy, encodeNonEmptyClientDataPacket(t, deferredTestRev))
				writeAllConn(t, f.h.clientProxy, encodeEmptyClientData(t))
			}
			if err := <-upDone; err != nil {
				t.Fatalf("upstream: %v", err)
			}
			// The relay loops exit only after the success and completion hooks.
			deadline := time.Now().Add(3 * time.Second)
			for {
				if _, active := f.h.relay.currentActiveQuery(); !active {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the signed INSERT never completed")
				}
				time.Sleep(5 * time.Millisecond)
			}
			f.h.close(t)
			if recycled, burned := f.metrics.snapshot(); recycled != 0 || len(burned) != 0 {
				t.Fatalf("recycled=%d burned=%v; want a sequenced (spent) seq", recycled, burned)
			}
			// The guard keeps only a statement ID, and only until the terminal.
			var record *string = f.h.relay.signedQueryWriteID.Load()
			if record != nil {
				t.Fatalf("write-boundary record %q survived the completed query", *record)
			}
			wp := f.site.signed.Load()
			if wp == nil {
				t.Fatal("the strict hook never saw the signed query")
			}
			for i := 0; i < 5 && wp.Value() != nil; i++ {
				runtime.GC()
			}
			if wp.Value() != nil {
				t.Fatal("the completed signed INSERT's QueryContext is still reachable from the relay")
			}
			runtime.KeepAlive(f.h.relay)
		})
	}
}
