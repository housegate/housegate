package sistatement

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

type fakeDiscovery struct {
	info      registry.StorageIntegrityInfo
	infoErr   error
	writer    bool
	writerErr error
	mu        sync.Mutex
	infoCalls int
	checked   []string
}

func (f *fakeDiscovery) StorageIntegrityInfo(context.Context, string) (registry.StorageIntegrityInfo, error) {
	f.mu.Lock()
	f.infoCalls++
	f.mu.Unlock()
	return f.info, f.infoErr
}

func (f *fakeDiscovery) StorageIntegrityWriterCheck(_ context.Context, database, account string) (bool, error) {
	f.mu.Lock()
	f.checked = append(f.checked, database+"/"+account)
	f.mu.Unlock()
	return f.writer, f.writerErr
}

type discoveryMetrics struct {
	seqMetrics
	failed map[string]int
}

func (m *discoveryMetrics) SIDiscoveryFailed(step string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failed == nil {
		m.failed = map[string]int{}
	}
	m.failed[step]++
}

func discoveryPlugin(t *testing.T, d *fakeDiscovery, configured string, precheck bool) (*Plugin, *discoveryMetrics, *[]string) {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	var opened []string
	metrics := &discoveryMetrics{}
	p, err := New(Options{
		Signer: signer, Schemas: ns, NetworkID: configured, MaxPayloadBytes: 1 << 20,
		Discovery: d, WriterPrecheck: precheck, Observer: metrics,
		Now: func() time.Time { return time.Unix(1_760_000_000, 0) },
		OpenSeq: func(networkID string) (*SeqCounter, error) {
			opened = append(opened, networkID)
			return OpenSeqCounter(dir+"/"+networkID, signer.Address())
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, metrics, &opened
}

func goodInfo() registry.StorageIntegrityInfo {
	return registry.StorageIntegrityInfo{Enabled: true, NetworkID: testNetworkID, ServerUnixTime: 1_760_000_000}
}

func TestDiscovery_NetworkIDComesFromTheHostingIndexer(t *testing.T) {
	d := &fakeDiscovery{info: goodInfo(), writer: true}
	p, _, opened := discoveryPlugin(t, d, "", true)
	q := insertQctx(newSession(1, ""), lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	id := signDeferred(t, p, q)
	if seqOf(t, id) != 1 || len(*opened) != 1 || (*opened)[0] != testNetworkID {
		t.Fatalf("id=%q opened=%v; want the counter for the discovered network", id, *opened)
	}
	q2 := insertQctx(newSession(2, ""), lateSQL)
	_ = p.OnQuery(context.Background(), q2)
	signDeferred(t, p, q2)
	if d.infoCalls != 1 || len(*opened) != 1 {
		t.Fatalf("info calls=%d opened=%v; info and counter are cached for the process", d.infoCalls, *opened)
	}
}

func TestDiscovery_InfoFailureRefusesWithoutAConfiguredID(t *testing.T) {
	d := &fakeDiscovery{infoErr: errors.New("method not found"), writer: true}
	p, metrics, _ := discoveryPlugin(t, d, "", true)
	err := p.OnQuery(context.Background(), insertQctx(newSession(1, ""), lateSQL))
	if err == nil || !strings.Contains(err.Error(), "storage_integrity agent: cannot discover network id") {
		t.Fatalf("err = %v", err)
	}
	if metrics.failed["info"] != 1 {
		t.Fatalf("failures = %v", metrics.failed)
	}
}

func TestDiscovery_InfoFailureFallsBackToTheConfiguredID(t *testing.T) {
	d := &fakeDiscovery{infoErr: errors.New("method not found"), writer: true}
	p, metrics, opened := discoveryPlugin(t, d, testNetworkID, true)
	q := insertQctx(newSession(1, ""), lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("configured network id must keep signing (plan P7): %v", err)
	}
	signDeferred(t, p, q)
	if (*opened)[0] != testNetworkID || metrics.failed["info"] != 1 {
		t.Fatalf("opened=%v failures=%v", *opened, metrics.failed)
	}
}

func TestDiscovery_EmptyNetworkIDIsAFailure(t *testing.T) {
	info := goodInfo()
	info.NetworkID = " "
	p, metrics, _ := discoveryPlugin(t, &fakeDiscovery{info: info, writer: true}, "", true)
	err := p.OnQuery(context.Background(), insertQctx(newSession(1, ""), lateSQL))
	if err == nil || !strings.Contains(err.Error(), "cannot discover network id") || metrics.failed["network_id"] != 1 {
		t.Fatalf("err = %v failures = %v", err, metrics.failed)
	}
}

func TestDiscovery_MismatchedNetworkRefuses(t *testing.T) {
	d := &fakeDiscovery{info: goodInfo(), writer: true}
	p, _, _ := discoveryPlugin(t, d, "some-other-net", true)
	err := p.OnQuery(context.Background(), insertQctx(newSession(1, ""), lateSQL))
	if err == nil || !strings.Contains(err.Error(), "refusing to sign for the wrong network") {
		t.Fatalf("err = %v", err)
	}
}

func TestDiscovery_ClockSkewIsReportedOnce(t *testing.T) {
	info := goodInfo()
	info.ServerUnixTime = 1_760_000_000 + 30
	p, _, _ := discoveryPlugin(t, &fakeDiscovery{info: info, writer: true}, "", true)
	if !p.checkSkew("shop", info) || p.checkSkew("shop", info) {
		t.Fatal("a 30 s skew must be reported once per database")
	}
	ok := goodInfo()
	if p.checkSkew("other", ok) {
		t.Fatal("no skew must not be reported")
	}
}

func TestDiscovery_WriterPrecheck(t *testing.T) {
	refuse := &fakeDiscovery{info: goodInfo(), writer: false}
	p, _, opened := discoveryPlugin(t, refuse, "", true)
	q := insertQctx(newSession(1, ""), lateSQL)
	err := p.OnQuery(context.Background(), q)
	if err == nil || !strings.Contains(err.Error(), "is not a writer of database shop") || q.DeferredInsert != nil {
		t.Fatalf("err = %v deferred=%v; want a local refusal before the claim", err, q.DeferredInsert)
	}
	if refuse.checked[0] != "shop/"+p.account {
		t.Fatalf("checked %v, want the signer as principal", refuse.checked)
	}
	// The refusal is local, session-preserving and ACCESS_DENIED like the
	// server's (spec 2026-10-09 R6), and it consumed no seq.
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeAccessDenied || !chproto.KeepsSession(err) {
		t.Fatalf("err = %#v; want a session-preserving 497 ClientError", err)
	}
	if len(*opened) != 0 {
		t.Fatalf("opened %v; a refused INSERT must not open the seq store", *opened)
	}

	unknown := &fakeDiscovery{info: goodInfo(), writerErr: errors.New("method not found")}
	p2, metrics, _ := discoveryPlugin(t, unknown, "", true)
	q2 := insertQctx(newSession(1, ""), lateSQL)
	if err := p2.OnQuery(context.Background(), q2); err != nil || q2.DeferredInsert == nil {
		t.Fatalf("an unknown pre-check must fall through to the server: err=%v", err)
	}
	if metrics.failed["precheck"] != 1 {
		t.Fatalf("failures = %v", metrics.failed)
	}

	driver, _, _ := discoveryPlugin(t, &fakeDiscovery{info: goodInfo(), writer: false}, "", false)
	q3 := insertQctx(newSession(1, ""), lateSQL)
	if err := driver.OnQuery(context.Background(), q3); err != nil {
		t.Fatalf("pre-check disabled (driver) must not refuse: %v", err)
	}
}

// A database no indexer hosts makes the routed client answer from the
// bootstrap, which cannot know the database's writers: that answer is
// "unknown", not a refusal, and the server decides (controller ruling on
// Task 13). It is not a discovery failure either.
func TestDiscovery_WriterPrecheckUnhostedDatabaseIsUnknown(t *testing.T) {
	d := &fakeDiscovery{info: goodInfo(), writerErr: fmt.Errorf("rpc: %w", registry.ErrDatabaseNotHosted)}
	p, metrics, _ := discoveryPlugin(t, d, "", true)
	q := insertQctx(newSession(1, ""), lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil || q.DeferredInsert == nil {
		t.Fatalf("an unhosted database must fall through to the server: err=%v", err)
	}
	if metrics.failed["precheck"] != 0 {
		t.Fatalf("failures = %v; an unhosted database is not a discovery failure", metrics.failed)
	}
}

func TestDiscovery_OwnerIsThePrecheckPrincipal(t *testing.T) {
	d := &fakeDiscovery{info: goodInfo(), writer: true}
	p, _, _ := discoveryPlugin(t, d, "", true)
	p.owner = "0x00000000000000000000000000000000000000c2"
	_ = p.OnQuery(context.Background(), insertQctx(newSession(1, ""), lateSQL))
	if d.checked[0] != "shop/0x00000000000000000000000000000000000000c2" {
		t.Fatalf("checked %v, want the owner", d.checked)
	}
}

func TestDiscovery_NewRequiresNetworkIDOnlyWithoutDiscovery(t *testing.T) {
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	ns := network.NewInMemoryNetworkState()
	open := func(string) (*SeqCounter, error) { return nil, errors.New("unused") }
	if _, err := New(Options{Signer: signer, Schemas: ns, MaxPayloadBytes: 1, OpenSeq: open}); err == nil || !strings.Contains(err.Error(), "network id is required") {
		t.Fatalf("without discovery: err = %v", err)
	}
	if _, err := New(Options{Signer: signer, Schemas: ns, MaxPayloadBytes: 1, OpenSeq: open, Discovery: &fakeDiscovery{}}); err != nil {
		t.Fatalf("with discovery: %v", err)
	}
	if _, err := New(Options{Signer: signer, Schemas: ns, MaxPayloadBytes: 1, NetworkID: testNetworkID}); err == nil || !strings.Contains(err.Error(), "seq counter or opener is required") {
		t.Fatalf("without a counter: err = %v", err)
	}
}

// A failed lazy open leaves no lock behind and is retried by the next INSERT;
// Close releases every counter it opened, and refuses to open new ones
// (controller ruling on Task 11).
func TestDiscovery_LazyOpenFailureAndCloseReleaseTheLock(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), testNetworkID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	freeList := filepath.Join(dir, strings.ToLower(signer.Address())+".seq.free")
	if err := os.WriteFile(freeList, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	opens := 0
	p, err := New(Options{
		Signer: signer, Schemas: ns, MaxPayloadBytes: 1 << 20,
		Discovery: &fakeDiscovery{info: goodInfo(), writer: true},
		OpenSeq: func(networkID string) (*SeqCounter, error) {
			opens++
			return OpenSeqCounter(filepath.Join(filepath.Dir(dir), networkID), signer.Address())
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	failed := insertQctx(newSession(1, ""), lateSQL)
	if err := p.OnQuery(context.Background(), failed); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), failed, encodeRows(t)); err != nil {
		t.Fatal(err)
	}
	err = p.OnQueryInputCompleteStrict(context.Background(), failed)
	if err == nil || !strings.Contains(err.Error(), "open client_seq store for network "+testNetworkID) || hasStatementToken(failed) {
		t.Fatalf("err = %v; want an unsigned refusal naming the store", err)
	}
	// The failed open holds no lock: the same process can open the store.
	if err := os.Remove(freeList); err != nil {
		t.Fatal(err)
	}
	probe, err := OpenSeqCounter(dir, signer.Address())
	if err != nil {
		t.Fatalf("a failed lazy open left the lock held: %v", err)
	}
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}

	// The next INSERT retries the open.
	q := insertQctx(newSession(2, ""), lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if id := signDeferred(t, p, q); seqOf(t, id) != 1 || opens != 2 {
		t.Fatalf("id=%q opens=%d; want the retried open to sign seq 1", id, opens)
	}
	if _, err := OpenSeqCounter(dir, signer.Address()); !errors.Is(err, ErrSeqLocked) {
		t.Fatalf("open while the plugin holds the store: err = %v, want ErrSeqLocked", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := OpenSeqCounter(dir, signer.Address())
	if err != nil {
		t.Fatalf("Close did not release the lock: %v", err)
	}
	if reopened.Last() != 1 {
		t.Fatalf("last = %d, want 1", reopened.Last())
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	// A closed plugin opens nothing more.
	late := insertQctx(newSession(3, ""), lateSQL)
	if err := p.OnQuery(context.Background(), late); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), late, encodeRows(t)); err != nil {
		t.Fatal(err)
	}
	if err := p.OnQueryInputCompleteStrict(context.Background(), late); !errors.Is(err, ErrSeqClosed) || opens != 2 {
		t.Fatalf("err = %v opens = %d; a closed plugin must refuse with ErrSeqClosed without opening", err, opens)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// Close also releases a counter supplied through Options.Seq.
func TestDiscovery_CloseReleasesASuppliedCounter(t *testing.T) {
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	seq, err := OpenSeqCounter(dir, signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(Options{Signer: signer, Schemas: network.NewInMemoryNetworkState(), NetworkID: testNetworkID, Seq: seq, MaxPayloadBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := OpenSeqCounter(dir, signer.Address())
	if err != nil {
		t.Fatalf("Close did not release the supplied counter: %v", err)
	}
	_ = again.Close()
}
