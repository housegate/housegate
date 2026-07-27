# Arbiter DA Payload-Store Client Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the three da.proto client legs in the arbiter repo — verifier fetch, snode put-spool, and arbiter custody-chain pins — per [the approved spec](../specs/2026-07-27-arbiter-da-client-design.md).

**Architecture:** One shared client package `dataplane/dastore` (the repo's only da.proto pb⇄Go boundary) consumed by four injection points: the verifier binary (via housegate's `replay.PayloadStore` interface), the snode intake (via a new narrow `PayloadSpool` interface), the ingress ack barrier (SEQUENCED pin before every ack), and the leader orchestrator (idempotent REPLAY/AUDIT custody rescan with leader-term stage memory — no new Raft command).

**Tech Stack:** Go 1.26, arbiter-proto v0.3.0 (`gen/pb` da types), grpc-go, `net.Listen("tcp", "127.0.0.1:0")`-based fake store for tests, `da-store --dev` for integration.

**Repo:** All tasks run in `/Users/uranuswch/Dev/sentio_xyz/arbiter` unless a path says otherwise. Run tests with plain `go test`.

## Global Constraints

- arbiter-proto pin: exactly `v0.3.0` (da.proto tagged there; v0.2.0 lacks it).
- `PinKey.holder_id` is the constant `"arbiter"` — cluster-stable, never a per-node/leader id.
- Scope keys are decimal strings: `strconv.FormatUint(seq, 10)` for both `statement_seq` (SEQUENCED) and `block_seq` (REPLAY/AUDIT).
- `ReleasePins.authority_jws` stays empty (store v1 is channel-trust-only).
- The client is hash-silent: it never verifies payload bytes against a hash — that stays in `replay.Verifier`.
- Custody ordering: successor pin durable **before** predecessor release, per pass and per block (REPLAY before SEQUENCED-release; AUDIT before REPLAY-release). AUDIT pins are never released in v1.
- Custody progression state is leader-term memory only — no new Raft command, no FSM Apply/snapshot/wire change. The only FSM change in this plan is a read-facade view.
- No pin/fetch/put behavior when the feature is unconfigured: nil `Custody` and the `fs` backend must behave byte-identically to today.
- FSM red lines still apply (CI-enforced): `fsm/` never imports `gen/pb` and never reads the wall clock — which is why `CustodyWork` uses only Go types and why custody stage memory lives in the orchestrator.
- English comments/logs; `fmt.Errorf("context: %w", err)` wrapping; structured slog logging (no metrics this phase).

---

### Task 1: Bump arbiter-proto to v0.3.0

**Files:**
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Consumes: nothing prior.
- Produces: `pb "github.com/sentioxyz/arbiter-proto/gen/pb"` now exposes `PayloadStoreClient`, `PayloadLifecycleClient`, `RegisterPayloadStoreServer`, `RegisterPayloadLifecycleServer`, `StoreLimits`, `PutPayloadHeader`, `PutPayloadInlineRequest`, `PutPayloadFrame{Frame: *PutPayloadFrame_Header | *PutPayloadFrame_Chunk}`, `PutPayloadResult`, `PutCode_*`, `FetchPayloadsRequest`, `FetchSpec`, `FetchFrame{Frame: *FetchFrame_Begin | *FetchFrame_Data | *FetchFrame_End}`, `FetchCode_*`, `PinKey`, `PinPurpose_*`, `PinPayloadsRequest/Result`, `PinCode_*`, `ReleasePinsRequest/Result`, `ReleaseCode_*` — used by every later task.

- [ ] **Step 1: Bump the module**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
GOPRIVATE=github.com/sentioxyz,github.com/housegate go get github.com/sentioxyz/arbiter-proto@v0.3.0
go mod tidy
```

- [ ] **Step 2: Verify the da types are importable and the tree still builds/tests green**

```bash
go build ./...
go vet ./...
go test ./... -count=1
grep 'arbiter-proto v0.3.0' go.mod
```

Expected: build/vet/tests pass (docker-gated tests skip as usual), and go.mod pins v0.3.0. The conformance tests in `conformance/` must still pass — v0.3.0 is additive over v0.2.0.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "build(deps): bump arbiter-proto to v0.3.0 (da.proto)"
```

---

### Task 2: `dastoretest` fake store

A real gRPC store serving both da.proto services on a loopback TCP listener, with content-addressed dedupe, pin bookkeeping, a call recorder, and per-RPC fault injection. Every later task's unit tests dial it exactly like production dials the real store.

**Files:**
- Create: `dataplane/dastore/dastoretest/store.go`
- Test: `dataplane/dastore/dastoretest/store_test.go`

**Interfaces:**
- Consumes: Task 1's pb types.
- Produces (used by Tasks 3–5, 9, 10):

```go
package dastoretest

// New starts both services on 127.0.0.1:0 and returns the running fake.
// Callers must Close it. All knobs are safe to mutate only before first use.
func New(t *testing.T) *Store

func (s *Store) Addr() string  // "127.0.0.1:<port>" serving BOTH services (dev-mode shape)
func (s *Store) Close()

// State inspection (mutex-guarded copies):
func (s *Store) Payload(ref string) ([]byte, bool)
func (s *Store) RefFor(payloadHash string, length uint64) (string, bool)
func (s *Store) Pins() map[string][]string  // "holder|purpose|scope" -> sorted refs
func (s *Store) Calls() []Call              // ordered RPC names: "GetStoreLimits", "PutPayloadInline", "PutPayload", "FetchPayloads", "StatPayloads", "PinPayloads", "ReleasePins"

type Call struct {
    Method string
    Key    string   // "holder|purpose|scope" for pin/release, "" otherwise
    Refs   []string // pin refs / fetch refs, nil otherwise
}

// Fault injection knobs (zero values = honest behavior):
type Store struct {
    Limits         *pb.StoreLimits // defaults: inline 2MiB, chunk 256KiB, batch 1024, payload 256MiB, lease 600000ms
    PutCode        pb.PutCode      // forced non-OK put outcome when != 0
    MintRefSuffix  string          // appended to minted refs — forces ref-mismatch in Put tests
    FetchCodeFor   map[string]pb.FetchCode // per-ref forced FetchEnd code (no frames served when set)
    PendingOnce    map[string]int  // per-ref countdown: serve FETCH_CODE_PENDING n times, then honest
    PinCodeFor     map[string]pb.PinCode   // per-ref forced pin outcome
    ReleaseCode    pb.ReleaseCode  // forced release outcome when != 0
    FailPins       int             // fail the next n PinPayloads calls with gRPC Unavailable
    // ... unexported: listener, grpc.Server, payloads, refs, pins, calls, mu
}
```

Honest semantics to implement: `GetStoreLimits` returns `Limits`; puts verify declared `(payload_hash, payload_length)` against received bytes (mismatch ⇒ `PUT_CODE_COMMITMENT_MISMATCH`, nothing stored), mint `ref = "fake://" + payload_hash + MintRefSuffix`, dedupe on content identity, record `deduplicated`; chunked `PutPayload` requires exactly one leading header frame then non-empty chunks (violations ⇒ `PUT_CODE_MALFORMED`); `FetchPayloads` serves each spec as `Begin(ref, spec_index) → Data(offset-contiguous chunks of ≤ max_chunk_bytes) → End(OK, served_length)`, unknown ref ⇒ `End(NOT_FOUND)` without Begin; `PinPayloads` unions refs into `pins[key]` (unknown ref ⇒ per-ref `PIN_CODE_NOT_FOUND`); `ReleasePins` deletes the key and returns OK even when the key is unknown.

- [ ] **Step 1: Write the self-test first**

```go
package dastoretest

import (
    "context"
    "testing"

    pb "github.com/sentioxyz/arbiter-proto/gen/pb"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func dial(t *testing.T, addr string) *grpc.ClientConn {
    t.Helper()
    conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        t.Fatalf("dial: %v", err)
    }
    t.Cleanup(func() { _ = conn.Close() })
    return conn
}

func TestFake_PutFetchPinReleaseRoundTrip(t *testing.T) {
    s := New(t)
    conn := dial(t, s.Addr())
    store := pb.NewPayloadStoreClient(conn)
    lifecycle := pb.NewPayloadLifecycleClient(conn)
    ctx := context.Background()

    lim, err := store.GetStoreLimits(ctx, &pb.GetStoreLimitsRequest{})
    if err != nil || lim.GetMaxInlineBytes() == 0 {
        t.Fatalf("limits: %v %v", lim, err)
    }

    payload := []byte("a,b\n1,2\n")
    put, err := store.PutPayloadInline(ctx, &pb.PutPayloadInlineRequest{
        Header:  &pb.PutPayloadHeader{PayloadHash: digest(payload), PayloadLength: uint64(len(payload))},
        Payload: payload,
    })
    if err != nil || put.GetCode() != pb.PutCode_PUT_CODE_OK {
        t.Fatalf("put: %v %v", put, err)
    }
    ref := put.GetPayloadRef()

    // duplicate put dedupes to the same ref
    put2, err := store.PutPayloadInline(ctx, &pb.PutPayloadInlineRequest{
        Header:  &pb.PutPayloadHeader{PayloadHash: digest(payload), PayloadLength: uint64(len(payload))},
        Payload: payload,
    })
    if err != nil || put2.GetPayloadRef() != ref || !put2.GetDeduplicated() {
        t.Fatalf("dedupe: %v %v", put2, err)
    }

    // fetch round-trips the bytes
    fs, err := store.FetchPayloads(ctx, &pb.FetchPayloadsRequest{Specs: []*pb.FetchSpec{{PayloadRef: ref}}})
    if err != nil {
        t.Fatalf("fetch open: %v", err)
    }
    var got []byte
    for {
        fr, err := fs.Recv()
        if err != nil {
            t.Fatalf("fetch recv: %v", err)
        }
        if d := fr.GetData(); d != nil {
            got = append(got, d.GetChunk()...)
        }
        if e := fr.GetEnd(); e != nil {
            if e.GetCode() != pb.FetchCode_FETCH_CODE_OK {
                t.Fatalf("fetch end: %v", e)
            }
            break
        }
    }
    if string(got) != string(payload) {
        t.Fatalf("fetch bytes: %q", got)
    }

    // pin then release
    pin, err := lifecycle.PinPayloads(ctx, &pb.PinPayloadsRequest{
        Key:         &pb.PinKey{HolderId: "arbiter", Purpose: pb.PinPurpose_PIN_PURPOSE_SEQUENCED, ScopeKey: "1"},
        PayloadRefs: []string{ref},
    })
    if err != nil || pin.GetResults()[0].GetCode() != pb.PinCode_PIN_CODE_OK {
        t.Fatalf("pin: %v %v", pin, err)
    }
    if got := s.Pins()["arbiter|PIN_PURPOSE_SEQUENCED|1"]; len(got) != 1 || got[0] != ref {
        t.Fatalf("pin bookkeeping: %v", s.Pins())
    }
    rel, err := lifecycle.ReleasePins(ctx, &pb.ReleasePinsRequest{
        Key: &pb.PinKey{HolderId: "arbiter", Purpose: pb.PinPurpose_PIN_PURPOSE_SEQUENCED, ScopeKey: "1"},
    })
    if err != nil || rel.GetCode() != pb.ReleaseCode_RELEASE_CODE_OK {
        t.Fatalf("release: %v %v", rel, err)
    }
    // idempotent unknown release
    rel2, err := lifecycle.ReleasePins(ctx, &pb.ReleasePinsRequest{
        Key: &pb.PinKey{HolderId: "arbiter", Purpose: pb.PinPurpose_PIN_PURPOSE_SEQUENCED, ScopeKey: "999"},
    })
    if err != nil || rel2.GetCode() != pb.ReleaseCode_RELEASE_CODE_OK {
        t.Fatalf("idempotent release: %v %v", rel2, err)
    }
}

func TestFake_PutCommitmentMismatch(t *testing.T) {
    s := New(t)
    conn := dial(t, s.Addr())
    store := pb.NewPayloadStoreClient(conn)
    put, err := store.PutPayloadInline(context.Background(), &pb.PutPayloadInlineRequest{
        Header:  &pb.PutPayloadHeader{PayloadHash: digest([]byte("other")), PayloadLength: 5},
        Payload: []byte("bytes"),
    })
    if err != nil || put.GetCode() != pb.PutCode_PUT_CODE_COMMITMENT_MISMATCH {
        t.Fatalf("want COMMITMENT_MISMATCH, got %v %v", put, err)
    }
    if _, ok := s.RefFor(digest([]byte("other")), 5); ok {
        t.Fatal("mismatched put must store nothing")
    }
}
```

`digest` is a tiny test helper in `store.go` (exported as `Digest`) implementing `"0x" + hex(SHA-256)` — identical to housegate's `replay.DigestBytes`, duplicated here so the testutil package does not import housegate.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./dataplane/dastore/dastoretest/ -run TestFake -v`
Expected: FAIL — package does not exist / `New` undefined.

- [ ] **Step 3: Implement `store.go`**

Single file: `Store` struct with the exported knobs above; `New(t)` builds a `grpc.NewServer()`, registers both services on it (`pb.RegisterPayloadStoreServer(g, s)` + `pb.RegisterPayloadLifecycleServer(g, s)` — `Store` embeds both `pb.UnimplementedPayloadStoreServer` and `pb.UnimplementedPayloadLifecycleServer`), listens on `net.Listen("tcp", "127.0.0.1:0")`, serves in a goroutine, and `t.Cleanup(s.Close)`. Implement the honest semantics and fault knobs listed in the Interfaces block. Chunked serving uses `Limits.MaxChunkBytes` slices. Record every RPC in `calls` under the mutex before handling it.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./dataplane/dastore/dastoretest/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dataplane/dastore/dastoretest/
git commit -m "test(dastore): loopback fake payload store with fault injection"
```

---

### Task 3: `dastore.Client` — construction, limits cache, `Put`

**Files:**
- Create: `dataplane/dastore/client.go`, `dataplane/dastore/put.go`
- Test: `dataplane/dastore/put_test.go`

**Interfaces:**
- Consumes: Task 2's `dastoretest.New/Addr/Calls/RefFor/Payload`, Task 1's pb types, housegate `replay.DigestBytes`.
- Produces (used by Tasks 4–11):

```go
package dastore

type Config struct {
    DataAddr    string        // PayloadStore endpoint; required for Put/GetPayload and limits
    ControlAddr string        // PayloadLifecycle endpoint; required for Pin/Release
    DialTimeout time.Duration // default 5s
    CallTimeout time.Duration // per unary RPC, default 10s; streams use the caller ctx
}

func New(cfg Config) (*Client, error) // lazy: no I/O, no dial; errors only on both addrs empty
func (c *Client) Close() error
func (c *Client) Put(ctx context.Context, expectedRef string, payload []byte) error
```

- [ ] **Step 1: Write the failing tests**

```go
package dastore

import (
    "context"
    "strings"
    "testing"

    pb "github.com/sentioxyz/arbiter-proto/gen/pb"

    "housegate/housegate/pkg/replay"

    "github.com/sentioxyz/arbiter/dataplane/dastore/dastoretest"
)

func newClient(t *testing.T, s *dastoretest.Store) *Client {
    t.Helper()
    c, err := New(Config{DataAddr: s.Addr(), ControlAddr: s.Addr()})
    if err != nil {
        t.Fatalf("new client: %v", err)
    }
    t.Cleanup(func() { _ = c.Close() })
    return c
}

func TestPut_InlineSmallPayload(t *testing.T) {
    s := dastoretest.New(t)
    c := newClient(t, s)
    payload := []byte("a,b\n1,2\n")
    ref := "fake://" + replay.DigestBytes(payload)
    if err := c.Put(context.Background(), ref, payload); err != nil {
        t.Fatalf("put: %v", err)
    }
    got, ok := s.Payload(ref)
    if !ok || string(got) != string(payload) {
        t.Fatalf("stored payload: %q ok=%v", got, ok)
    }
    for _, call := range s.Calls() {
        if call.Method == "PutPayload" {
            t.Fatal("small payload must use PutPayloadInline, not the stream")
        }
    }
}

func TestPut_ChunkedLargePayload(t *testing.T) {
    s := dastoretest.New(t)
    s.Limits = &pb.StoreLimits{MaxInlineBytes: 8, MaxChunkBytes: 4, MaxBatchRefs: 1024, MaxPayloadBytes: 1 << 20, IngestLeaseMs: 60000}
    c := newClient(t, s)
    payload := []byte("0123456789abcdef") // 16 bytes > inline 8, chunks of 4
    ref := "fake://" + replay.DigestBytes(payload)
    if err := c.Put(context.Background(), ref, payload); err != nil {
        t.Fatalf("put: %v", err)
    }
    got, ok := s.Payload(ref)
    if !ok || string(got) != string(payload) {
        t.Fatalf("stored payload: %q ok=%v", got, ok)
    }
    var sawStream bool
    for _, call := range s.Calls() {
        sawStream = sawStream || call.Method == "PutPayload"
    }
    if !sawStream {
        t.Fatal("large payload must use the PutPayload stream")
    }
}

func TestPut_BoundaryExactlyInline(t *testing.T) {
    s := dastoretest.New(t)
    s.Limits = &pb.StoreLimits{MaxInlineBytes: 8, MaxChunkBytes: 4, MaxBatchRefs: 1024, MaxPayloadBytes: 1 << 20, IngestLeaseMs: 60000}
    c := newClient(t, s)
    payload := []byte("01234567") // exactly max_inline_bytes ⇒ inline
    if err := c.Put(context.Background(), "fake://"+replay.DigestBytes(payload), payload); err != nil {
        t.Fatalf("put: %v", err)
    }
    for _, call := range s.Calls() {
        if call.Method == "PutPayload" {
            t.Fatal("payload at the inline boundary must go inline")
        }
    }
}

func TestPut_RefMismatchFailsClosed(t *testing.T) {
    s := dastoretest.New(t)
    s.MintRefSuffix = "-divergent"
    c := newClient(t, s)
    payload := []byte("x")
    err := c.Put(context.Background(), "fake://"+replay.DigestBytes(payload), payload)
    if err == nil || !strings.Contains(err.Error(), "divergence") {
        t.Fatalf("want ref-minting divergence error, got %v", err)
    }
}

func TestPut_TerminalCodeSurfaces(t *testing.T) {
    s := dastoretest.New(t)
    s.PutCode = pb.PutCode_PUT_CODE_TOO_LARGE
    c := newClient(t, s)
    err := c.Put(context.Background(), "fake://whatever", []byte("x"))
    if err == nil || !strings.Contains(err.Error(), "PUT_CODE_TOO_LARGE") {
        t.Fatalf("want TOO_LARGE error, got %v", err)
    }
}

func TestNew_RequiresAnAddr(t *testing.T) {
    if _, err := New(Config{}); err == nil {
        t.Fatal("want error when both addrs are empty")
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./dataplane/dastore/ -run 'TestPut|TestNew' -v`
Expected: FAIL — package/types undefined.

- [ ] **Step 3: Implement `client.go` + `put.go`**

`client.go`:

```go
// Package dastore is the arbiter repo's client for the da.proto payload
// store (arbiter-proto v0.3.0): snode put-spool, verifier fetch, and the
// custody-chain pin/release lifecycle. It is deliberately hash-silent —
// content verification against the sequenced envelope stays in
// housegate's replay.Verifier.
package dastore

const holderID = "arbiter" // cluster-stable PinKey.holder_id (contract requirement)

type Client struct {
    cfg Config

    mu       sync.Mutex
    dataConn *grpc.ClientConn
    ctlConn  *grpc.ClientConn
    limits   *pb.StoreLimits
}
```

`New` applies defaults (`DialTimeout` 5s, `CallTimeout` 10s), errors if both addrs are empty. `dataConn()`/`ctlConn()` lazily `grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))` under the mutex (same idiom as `dataplane.Client.conn`), erroring with a clear message when the needed addr is unconfigured. `storeLimits(ctx)` returns the cached value or calls `GetStoreLimits` on the data conn with a `CallTimeout` child context, caching only on success. `Close` closes both conns.

`put.go`:

```go
func (c *Client) Put(ctx context.Context, expectedRef string, payload []byte) error {
    lim, err := c.storeLimits(ctx)
    if err != nil {
        return fmt.Errorf("dastore: store limits: %w", err)
    }
    header := &pb.PutPayloadHeader{
        PayloadHash:   replay.DigestBytes(payload),
        PayloadLength: uint64(len(payload)),
    }
    var res *pb.PutPayloadResult
    if uint64(len(payload)) <= lim.GetMaxInlineBytes() {
        res, err = c.putInline(ctx, header, payload)
    } else {
        res, err = c.putStream(ctx, lim, header, payload)
    }
    if err != nil {
        return fmt.Errorf("dastore: put %s: %w", expectedRef, err)
    }
    if res.GetCode() != pb.PutCode_PUT_CODE_OK {
        return fmt.Errorf("dastore: put %s: %s: %s", expectedRef, res.GetCode(), res.GetMessage())
    }
    if res.GetPayloadRef() != expectedRef {
        return fmt.Errorf("dastore: put ref-minting divergence: store minted %q, envelope carries %q", res.GetPayloadRef(), expectedRef)
    }
    return nil
}
```

`putInline` is a unary call under a `CallTimeout` child ctx. `putStream` opens `PutPayload(ctx)` (caller ctx, no CallTimeout — large payloads), sends the header frame, then sequential `Chunk` frames of `lim.GetMaxChunkBytes()`; a `Send` returning `io.EOF` breaks to `CloseAndRecv()` (gRPC client-stream idiom: the real error surfaces there); otherwise `CloseAndRecv()` after the last chunk.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./dataplane/dastore/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dataplane/dastore/client.go dataplane/dastore/put.go dataplane/dastore/put_test.go
git commit -m "feat(dastore): client construction, limits cache, inline/chunked put"
```

---

### Task 4: `dastore.Client.GetPayload` + hash-silence conformance test

**Files:**
- Create: `dataplane/dastore/fetch.go`
- Test: `dataplane/dastore/fetch_test.go`, `dataplane/dastore/hash_silent_test.go`

**Interfaces:**
- Consumes: Tasks 2–3.
- Produces: `func (c *Client) GetPayload(ctx context.Context, payloadRef string) ([]byte, error)` — satisfies housegate `replay.PayloadStore`; used by Tasks 7 and 11.

- [ ] **Step 1: Write the failing tests**

```go
package dastore

import (
    "context"
    "strings"
    "testing"

    pb "github.com/sentioxyz/arbiter-proto/gen/pb"

    "housegate/housegate/pkg/replay"

    "github.com/sentioxyz/arbiter/dataplane/dastore/dastoretest"
)

var _ replay.PayloadStore = (*Client)(nil)

func putOne(t *testing.T, c *Client, payload []byte) string {
    t.Helper()
    ref := "fake://" + replay.DigestBytes(payload)
    if err := c.Put(context.Background(), ref, payload); err != nil {
        t.Fatalf("seed put: %v", err)
    }
    return ref
}

func TestGetPayload_RoundTrip(t *testing.T) {
    s := dastoretest.New(t)
    c := newClient(t, s)
    payload := []byte("a,b\n1,2\n3,4\n")
    ref := putOne(t, c, payload)
    got, err := c.GetPayload(context.Background(), ref)
    if err != nil || string(got) != string(payload) {
        t.Fatalf("get: %q %v", got, err)
    }
}

func TestGetPayload_MultiChunkReassembly(t *testing.T) {
    s := dastoretest.New(t)
    s.Limits = &pb.StoreLimits{MaxInlineBytes: 1 << 20, MaxChunkBytes: 3, MaxBatchRefs: 1024, MaxPayloadBytes: 1 << 20, IngestLeaseMs: 60000}
    c := newClient(t, s)
    payload := []byte("0123456789") // served in ceil(10/3) = 4 chunks
    ref := putOne(t, c, payload)
    got, err := c.GetPayload(context.Background(), ref)
    if err != nil || string(got) != string(payload) {
        t.Fatalf("get: %q %v", got, err)
    }
}

func TestGetPayload_NotFoundIsAvailabilityIncident(t *testing.T) {
    s := dastoretest.New(t)
    c := newClient(t, s)
    _, err := c.GetPayload(context.Background(), "fake://missing")
    if err == nil || !strings.Contains(err.Error(), "FETCH_CODE_NOT_FOUND") {
        t.Fatalf("want NOT_FOUND error, got %v", err)
    }
}

func TestGetPayload_PendingRetriesThenSucceeds(t *testing.T) {
    s := dastoretest.New(t)
    c := newClient(t, s)
    payload := []byte("late")
    ref := putOne(t, c, payload)
    s.PendingOnce = map[string]int{ref: 2} // two PENDING answers, then honest
    got, err := c.GetPayload(context.Background(), ref)
    if err != nil || string(got) != string(payload) {
        t.Fatalf("get after pending: %q %v", got, err)
    }
}

func TestGetPayload_PendingExhaustsRetries(t *testing.T) {
    s := dastoretest.New(t)
    c := newClient(t, s)
    payload := []byte("never")
    ref := putOne(t, c, payload)
    s.PendingOnce = map[string]int{ref: 99}
    _, err := c.GetPayload(context.Background(), ref)
    if err == nil || !strings.Contains(err.Error(), "FETCH_CODE_PENDING") {
        t.Fatalf("want PENDING exhaustion error, got %v", err)
    }
}
```

`hash_silent_test.go` — pins the structural hash-silence assumption against future proto drift:

```go
package dastore

import (
    "strings"
    "testing"

    pb "github.com/sentioxyz/arbiter-proto/gen/pb"
    "google.golang.org/protobuf/reflect/protoreflect"
)

// The read path is hash-silent by contract: no fetch/stat response may
// carry a server-asserted content hash or length the client could be
// tempted to trust. Pin that at the descriptor level.
func TestReadPathMessagesCarryNoHashAssertions(t *testing.T) {
    for _, m := range []protoreflect.Message{
        (&pb.FetchBegin{}).ProtoReflect(),
        (&pb.FetchData{}).ProtoReflect(),
        (&pb.FetchEnd{}).ProtoReflect(),
        (&pb.PayloadStat{}).ProtoReflect(),
    } {
        fields := m.Descriptor().Fields()
        for i := 0; i < fields.Len(); i++ {
            name := string(fields.Get(i).Name())
            if strings.Contains(name, "hash") || name == "payload_length" {
                t.Errorf("%s.%s breaks the hash-silent read path", m.Descriptor().Name(), name)
            }
        }
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./dataplane/dastore/ -run 'TestGetPayload|TestReadPath' -v`
Expected: FAIL — `GetPayload` undefined (the conformance test alone would pass; that is fine).

- [ ] **Step 3: Implement `fetch.go`**

```go
const (
    pendingAttempts = 3
    pendingBackoff  = 200 * time.Millisecond
)

func (c *Client) GetPayload(ctx context.Context, payloadRef string) ([]byte, error) {
    backoff := pendingBackoff
    for attempt := 1; ; attempt++ {
        buf, pending, err := c.fetchOnce(ctx, payloadRef)
        if err == nil {
            return buf, nil
        }
        if !pending || attempt >= pendingAttempts {
            return nil, err
        }
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(backoff):
        }
        backoff *= 2
    }
}
```

`fetchOnce` dials the data conn, opens `FetchPayloads` with one whole-payload spec, and consumes frames: `Begin` must carry `spec_index == 0`; `Data` must be contiguous (`offset == len(buf)` else a grammar error mentioning "non-contiguous"); `End` switches on the code — `OK` returns the buffer, `PENDING` returns `(nil, true, err)`, `NOT_FOUND`/`RELEASED` return an error string containing the code name and "availability incident", others return code+message. `io.EOF` before `End` is a "stream ended without FetchEnd" error. The whole call uses the caller ctx (streams are not CallTimeout-bounded).

- [ ] **Step 4: Run to verify pass**

Run: `go test ./dataplane/dastore/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dataplane/dastore/fetch.go dataplane/dastore/fetch_test.go dataplane/dastore/hash_silent_test.go
git commit -m "feat(dastore): GetPayload fetch reassembly + hash-silence conformance"
```

---

### Task 5: `dastore.Client.Pin` / `Release`

**Files:**
- Create: `dataplane/dastore/lifecycle.go`
- Test: `dataplane/dastore/lifecycle_test.go`

**Interfaces:**
- Consumes: Tasks 2–3.
- Produces (used by Tasks 9–11):

```go
func (c *Client) Pin(ctx context.Context, purpose pb.PinPurpose, scopeKey string, refs []string) error
func (c *Client) Release(ctx context.Context, purpose pb.PinPurpose, scopeKey string) error
```

- [ ] **Step 1: Write the failing tests**

```go
package dastore

import (
    "context"
    "fmt"
    "strings"
    "testing"

    pb "github.com/sentioxyz/arbiter-proto/gen/pb"

    "housegate/housegate/pkg/replay"

    "github.com/sentioxyz/arbiter/dataplane/dastore/dastoretest"
)

func TestPin_UnionsAndRelease(t *testing.T) {
    s := dastoretest.New(t)
    c := newClient(t, s)
    ref := putOne(t, c, []byte("pinme"))
    if err := c.Pin(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "7", []string{ref}); err != nil {
        t.Fatalf("pin: %v", err)
    }
    key := "arbiter|PIN_PURPOSE_REPLAY|7"
    if got := s.Pins()[key]; len(got) != 1 || got[0] != ref {
        t.Fatalf("pin bookkeeping: %v", s.Pins())
    }
    if err := c.Release(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "7"); err != nil {
        t.Fatalf("release: %v", err)
    }
    if _, ok := s.Pins()[key]; ok {
        t.Fatal("release must drop the whole pin")
    }
    // releasing an unknown key is OK
    if err := c.Release(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "404"); err != nil {
        t.Fatalf("idempotent release: %v", err)
    }
}

func TestPin_EmptyRefsIsNoOp(t *testing.T) {
    s := dastoretest.New(t)
    c := newClient(t, s)
    if err := c.Pin(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "7", nil); err != nil {
        t.Fatalf("empty pin: %v", err)
    }
    for _, call := range s.Calls() {
        if call.Method == "PinPayloads" {
            t.Fatal("empty refs must not issue an RPC")
        }
    }
}

func TestPin_SplitsAtMaxBatchRefs(t *testing.T) {
    s := dastoretest.New(t)
    s.Limits = &pb.StoreLimits{MaxInlineBytes: 1 << 20, MaxChunkBytes: 1 << 18, MaxBatchRefs: 2, MaxPayloadBytes: 1 << 20, IngestLeaseMs: 60000}
    c := newClient(t, s)
    var refs []string
    for i := 0; i < 5; i++ {
        refs = append(refs, putOne(t, c, []byte(fmt.Sprintf("payload-%d", i))))
    }
    if err := c.Pin(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "9", refs); err != nil {
        t.Fatalf("pin: %v", err)
    }
    var pinCalls int
    for _, call := range s.Calls() {
        if call.Method == "PinPayloads" {
            pinCalls++
            if len(call.Refs) > 2 {
                t.Fatalf("batch over max_batch_refs: %v", call.Refs)
            }
        }
    }
    if pinCalls != 3 { // ceil(5/2)
        t.Fatalf("want 3 pin batches, got %d", pinCalls)
    }
    if got := s.Pins()["arbiter|PIN_PURPOSE_REPLAY|9"]; len(got) != 5 {
        t.Fatalf("union across batches: %v", got)
    }
}

func TestPin_NotFoundIsCustodyBroken(t *testing.T) {
    s := dastoretest.New(t)
    c := newClient(t, s)
    err := c.Pin(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "7", []string{"fake://ghost"})
    if err == nil || !strings.Contains(err.Error(), "PIN_CODE_NOT_FOUND") {
        t.Fatalf("want NOT_FOUND custody error, got %v", err)
    }
}

func TestRelease_LeavesAuthorityJWSEmpty(t *testing.T) {
    s := dastoretest.New(t)
    c := newClient(t, s)
    if err := c.Release(context.Background(), pb.PinPurpose_PIN_PURPOSE_AUDIT, "1"); err != nil {
        t.Fatalf("release: %v", err)
    }
    // the fake records the raw request; assert the JWS slot stayed empty
    if jws := s.LastReleaseJWS(); jws != "" {
        t.Fatalf("authority_jws must stay empty in v1, got %q", jws)
    }
}
```

Add `func (s *Store) LastReleaseJWS() string` to `dastoretest` in this task (records `ReleasePinsRequest.AuthorityJws` of the most recent call).

- [ ] **Step 2: Run to verify failure**

Run: `go test ./dataplane/dastore/ -run 'TestPin|TestRelease' -v`
Expected: FAIL — `Pin`/`Release` undefined.

- [ ] **Step 3: Implement `lifecycle.go`**

```go
func (c *Client) Pin(ctx context.Context, purpose pb.PinPurpose, scopeKey string, refs []string) error {
    if len(refs) == 0 {
        return nil
    }
    lim, err := c.storeLimits(ctx)
    if err != nil {
        return fmt.Errorf("dastore: store limits: %w", err)
    }
    batch := int(lim.GetMaxBatchRefs())
    if batch <= 0 {
        batch = len(refs)
    }
    conn, err := c.controlConn()
    if err != nil {
        return err
    }
    client := pb.NewPayloadLifecycleClient(conn)
    for start := 0; start < len(refs); start += batch {
        end := min(start+batch, len(refs))
        callCtx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
        res, err := client.PinPayloads(callCtx, &pb.PinPayloadsRequest{
            Key:         &pb.PinKey{HolderId: holderID, Purpose: purpose, ScopeKey: scopeKey},
            PayloadRefs: refs[start:end],
        })
        cancel()
        if err != nil {
            return fmt.Errorf("dastore: pin %s/%s: %w", purpose, scopeKey, err)
        }
        for _, r := range res.GetResults() {
            if r.GetCode() != pb.PinCode_PIN_CODE_OK {
                return fmt.Errorf("dastore: pin %s/%s ref %s: custody chain broken: %s: %s",
                    purpose, scopeKey, r.GetPayloadRef(), r.GetCode(), r.GetMessage())
            }
        }
    }
    return nil
}

func (c *Client) Release(ctx context.Context, purpose pb.PinPurpose, scopeKey string) error {
    conn, err := c.controlConn()
    if err != nil {
        return err
    }
    callCtx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
    defer cancel()
    // authority_jws deliberately empty: the v1 store is channel-trust-only.
    res, err := pb.NewPayloadLifecycleClient(conn).ReleasePins(callCtx, &pb.ReleasePinsRequest{
        Key: &pb.PinKey{HolderId: holderID, Purpose: purpose, ScopeKey: scopeKey},
    })
    if err != nil {
        return fmt.Errorf("dastore: release %s/%s: %w", purpose, scopeKey, err)
    }
    if res.GetCode() != pb.ReleaseCode_RELEASE_CODE_OK {
        return fmt.Errorf("dastore: release %s/%s: %s: %s", purpose, scopeKey, res.GetCode(), res.GetMessage())
    }
    return nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./dataplane/dastore/... -v`
Expected: PASS (including the extended fake self-test).

- [ ] **Step 5: Commit**

```bash
git add dataplane/dastore/lifecycle.go dataplane/dastore/lifecycle_test.go dataplane/dastore/dastoretest/
git commit -m "feat(dastore): pin/release lifecycle with batch splitting"
```

---

### Task 6: SNode `PayloadSpool` seam + grpc backend in `cmd/arbiter-snode`

**Files:**
- Modify: `snode/snode.go` (Deps type), `snode/intake.go:37` (Put call), `dataplane/fspayload/store.go:33` (Put signature)
- Modify: `cmd/arbiter-snode/config.go`, `cmd/arbiter-snode/main.go`
- Test: `snode/snode_test.go` (new interface test), `cmd/arbiter-snode/main_test.go` (config validation), plus mechanical fixes to existing `fspayload.Put` call sites (`snode/intake_test.go`, `dataplane/fspayload/store_test.go`, `integration/chpipeline/*`)

**Interfaces:**
- Consumes: `dastore.Client.Put` (Task 3).
- Produces (used by Tasks 7 and 11's config/wiring conventions):

```go
// snode package
type PayloadSpool interface {
    Put(ctx context.Context, ref string, payload []byte) error
}
// Deps.Payloads becomes PayloadSpool (was *fspayload.Store)

// fspayload package — signature change
func (s *Store) Put(ctx context.Context, ref string, payload []byte) error // ctx ignored

// cmd/arbiter-snode config
type PayloadStoreConfig struct {
    Backend  string `yaml:"backend"`   // "" | "fs" | "grpc"; "" means fs
    DataAddr string `yaml:"data_addr"` // required when backend == "grpc"
}
// Config gains: PayloadStore PayloadStoreConfig `yaml:"payload_store"`
```

- [ ] **Step 1: Write the failing tests**

In `snode/snode_test.go` add:

```go
// PayloadSpool is the intake's put seam: fspayload and dastore both satisfy it.
func TestPayloadSpoolImplementations(t *testing.T) {
    var _ PayloadSpool = (*fspayload.Store)(nil)
    var _ PayloadSpool = (*dastore.Client)(nil)
}
```

In `cmd/arbiter-snode/main_test.go` add config-validation cases following the file's existing test style:

```go
func TestConfigValidate_PayloadStoreBackends(t *testing.T) {
    base := validConfig() // reuse/extract the existing valid-config fixture in this file
    cfg := base
    cfg.PayloadStore = PayloadStoreConfig{Backend: "grpc", DataAddr: "127.0.0.1:9001"}
    if err := cfg.validate(); err != nil {
        t.Fatalf("grpc backend with addr must validate: %v", err)
    }
    cfg = base
    cfg.PayloadStore = PayloadStoreConfig{Backend: "grpc"}
    if err := cfg.validate(); err == nil {
        t.Fatal("grpc backend without data_addr must fail")
    }
    cfg = base
    cfg.PayloadStore = PayloadStoreConfig{Backend: "s3"}
    if err := cfg.validate(); err == nil {
        t.Fatal("unknown backend must fail")
    }
    cfg = base
    cfg.PayloadStore = PayloadStoreConfig{Backend: "grpc", DataAddr: "127.0.0.1:9001"}
    cfg.PayloadDir = ""
    if err := cfg.validate(); err != nil {
        t.Fatalf("grpc backend must not require payload_dir: %v", err)
    }
    cfg = base
    cfg.PayloadDir = ""
    if err := cfg.validate(); err == nil {
        t.Fatal("fs backend (default) must still require payload_dir")
    }
}
```

(If `main_test.go` has no reusable valid-config fixture, extract one `validConfig() Config` helper from its existing cases as part of this step.)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./snode/ ./cmd/arbiter-snode/ -run 'TestPayloadSpool|TestConfigValidate_PayloadStore' -v`
Expected: FAIL — `PayloadSpool` and `PayloadStoreConfig` undefined.

- [ ] **Step 3: Implement**

1. `snode/snode.go`: declare `PayloadSpool` (with doc comment: "the intake's payload-before-write seam"), change `Deps.Payloads` to it, drop the now-unused `fspayload` import if any.
2. `snode/intake.go:37`: `r.d.Payloads.Put(ctx, env.PayloadRef, payload)`.
3. `dataplane/fspayload/store.go:33`: add the `ctx context.Context` first parameter (ignored — local filesystem); update every call site the compiler flags (`fspayload/store_test.go`, `snode` tests, `integration/chpipeline` harness).
4. `cmd/arbiter-snode/config.go`: add `PayloadStoreConfig` + the `payload_store` field; in `validate()`, allowlist `backend ∈ {"", "fs", "grpc"}`, require `data_addr` for `grpc`, and make the existing `req(c.PayloadDir, "payload_dir")` conditional on the fs/default backend.
5. `cmd/arbiter-snode/main.go`: replace the unconditional `fspayload.New(cfg.PayloadDir)` with:

```go
payloads, cleanup, err := buildPayloadSpool(cfg)
if err != nil {
    return err
}
defer cleanup()
```

```go
func buildPayloadSpool(cfg Config) (snode.PayloadSpool, func(), error) {
    if cfg.PayloadStore.Backend == "grpc" {
        c, err := dastore.New(dastore.Config{DataAddr: cfg.PayloadStore.DataAddr})
        if err != nil {
            return nil, nil, fmt.Errorf("payload store client: %w", err)
        }
        return c, func() { _ = c.Close() }, nil
    }
    st, err := fspayload.New(cfg.PayloadDir)
    if err != nil {
        return nil, nil, err
    }
    return st, func() {}, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go build ./... && go test ./snode/ ./cmd/arbiter-snode/ ./dataplane/... -count=1`
Expected: PASS (ClickHouse-gated tests skip).

- [ ] **Step 5: Commit**

```bash
git add snode/ dataplane/fspayload/ cmd/arbiter-snode/ integration/
git commit -m "feat(snode): PayloadSpool seam with fs/grpc payload-store backends"
```

---

### Task 7: grpc backend in `cmd/arbiter-verifier`

**Files:**
- Modify: `cmd/arbiter-verifier/config.go`, `cmd/arbiter-verifier/main.go:83-88`
- Test: `cmd/arbiter-verifier/main_test.go`

**Interfaces:**
- Consumes: `dastore.Client.GetPayload` (Task 4, satisfies `replay.PayloadStore`), Task 6's `PayloadStoreConfig` shape (duplicated per binary — the two config packages are separate mains and stay self-contained, matching the existing per-binary Config duplication).
- Produces: nothing new for later tasks (Task 11 wires the same pattern in tests).

- [ ] **Step 1: Write the failing test**

Mirror Task 6's config-validation test in `cmd/arbiter-verifier/main_test.go` (same five cases: grpc+addr ok, grpc without addr fails, unknown backend fails, grpc without payload_dir ok, default without payload_dir fails), against this binary's `Config`/`validate`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/arbiter-verifier/ -run TestConfigValidate_PayloadStore -v`
Expected: FAIL — `PayloadStoreConfig` undefined.

- [ ] **Step 3: Implement**

Add the same `PayloadStoreConfig` type + `payload_store` field + validation rules to `cmd/arbiter-verifier/config.go`. In `main.go`, replace `fspayload.New(cfg.PayloadDir)` with a local `buildPayloadStore(cfg) (replay.PayloadStore, func(), error)` returning `*dastore.Client` for `grpc` and `*fspayload.Store` otherwise; pass the result to `verifier.NewReplayCore(roleCfg, conn, dataplane.NewManifestStore(client), payloads)` unchanged.

- [ ] **Step 4: Run to verify pass**

Run: `go build ./... && go test ./cmd/arbiter-verifier/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/arbiter-verifier/
git commit -m "feat(verifier): grpc payload-store backend behind replay.PayloadStore"
```

---

### Task 8: FSM `CustodyWorkSet` read view

**Files:**
- Create: `fsm/reads_custody.go`
- Test: `fsm/reads_custody_test.go`

**Interfaces:**
- Consumes: existing FSM internals only (`f.st.Blocks`, `f.blockStatements`, `f.st.Verifications`, `f.st.SafeWatermark`, `StatementState{Seq, Env, Status}`, `StatusRejected`).
- Produces (used by Task 10):

```go
// fsm package — read facade only; no Apply/snapshot/wire change. No pb
// imports (CI-enforced red line), which is why PinPurpose does not appear.
type CustodyWork struct {
    BlockSeq         uint64
    Refs             []string // deduplicated, first-seen order; may be empty
    StatementSeqs    []uint64 // ascending
    EvidenceComplete bool     // every VerifierSet member has attestation AND scan
    Safe             bool     // SafeWatermark.SafeBlockSeq >= BlockSeq
    Rejected         bool     // any statement in the block is StatusRejected
}
func (f *FSM) CustodyWorkSet() []CustodyWork // ascending BlockSeq, sealed blocks only
```

- [ ] **Step 1: Write the failing test**

Build FSM state the same way the existing `fsm/reads_work_test.go` does — through `Apply` of wire commands (find the file's existing helpers for submitting statements, registering RCs, sealing, attesting, and publishing manifests, and reuse them; do not hand-poke `f.st`). Cover:

```go
func TestCustodyWorkSet(t *testing.T) {
    // Arrange, via the reads_work_test.go helper conventions:
    //   block 1: sealed, 2 statements sharing one payload ref + 1 distinct ref,
    //            full evidence (3 verifiers × attestation+scan), safe watermark past it
    //   block 2: sealed, 1 statement, evidence incomplete (1 of 3 attested), not safe
    //   block 3: sealed, 1 statement, challenge resolved Rejected
    //   plus an open (unsealed) block that must NOT appear
    ws := f.CustodyWorkSet()
    if len(ws) != 3 {
        t.Fatalf("want 3 sealed blocks, got %d", len(ws))
    }
    b1, b2, b3 := ws[0], ws[1], ws[2]
    if b1.BlockSeq != 1 || !b1.Safe || !b1.EvidenceComplete || b1.Rejected {
        t.Fatalf("block1 flags: %+v", b1)
    }
    if len(b1.Refs) != 2 { // 3 statements' refs dedupe to 2
        t.Fatalf("block1 refs must dedupe: %v", b1.Refs)
    }
    if len(b1.StatementSeqs) != 3 {
        t.Fatalf("block1 seqs: %v", b1.StatementSeqs)
    }
    if b2.Safe || b2.EvidenceComplete {
        t.Fatalf("block2 must be unsafe+incomplete: %+v", b2)
    }
    if !b3.Rejected {
        t.Fatalf("block3 must be rejected: %+v", b3)
    }
}
```

(Adjust the arrange section to the actual helper names found in `fsm/reads_work_test.go` / `fsm/promotion_test.go`; the assertions above are the contract.)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./fsm/ -run TestCustodyWorkSet -v`
Expected: FAIL — `CustodyWorkSet` undefined.

- [ ] **Step 3: Implement `fsm/reads_custody.go`**

```go
package fsm

// CustodyWork is one detached point-in-time WorkSet row for the
// orchestrator's payload custody-chain progression (dastore pins). Derived
// entirely from replicated state; the FSM knows nothing about pin
// durability — that is external store state re-asserted idempotently.
type CustodyWork struct {
    BlockSeq         uint64
    Refs             []string
    StatementSeqs    []uint64
    EvidenceComplete bool
    Safe             bool
    Rejected         bool
}

// CustodyWorkSet returns one row per sealed block, ascending.
func (f *FSM) CustodyWorkSet() []CustodyWork {
    f.mu.RLock()
    defer f.mu.RUnlock()

    out := make([]CustodyWork, 0, len(f.st.Blocks))
    for i := range f.st.Blocks {
        blockSeq := uint64(i + 1)
        cw := CustodyWork{
            BlockSeq: blockSeq,
            Safe:     f.st.SafeWatermark.SafeBlockSeq >= blockSeq,
        }
        seen := map[string]bool{}
        for _, ss := range f.blockStatements(blockSeq) {
            cw.StatementSeqs = append(cw.StatementSeqs, ss.Seq)
            if ref := ss.Env.PayloadRef; ref != "" && !seen[ref] {
                seen[ref] = true
                cw.Refs = append(cw.Refs, ref)
            }
            if ss.Status == StatusRejected {
                cw.Rejected = true
            }
        }
        if bv := f.st.Verifications[blockSeq]; bv != nil && len(bv.VerifierSet) > 0 {
            cw.EvidenceComplete = true
            for _, rid := range bv.VerifierSet {
                if bv.Attestations[rid] == nil || bv.ByteScans[rid] == nil {
                    cw.EvidenceComplete = false
                    break
                }
            }
        }
        out = append(out, cw)
    }
    return out
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./fsm/ -count=1`
Expected: PASS, including the CI red-line greps (`no pb import`, `no time.Now`) which this file must survive:

```bash
grep -n 'gen/pb\|time\.Now' fsm/reads_custody.go && echo VIOLATION || echo clean
```

- [ ] **Step 5: Commit**

```bash
git add fsm/reads_custody.go fsm/reads_custody_test.go
git commit -m "feat(fsm): CustodyWorkSet read view for payload custody progression"
```

---

### Task 9: Ingress SEQUENCED pin (ack barrier) + `cmd/arbiter` config/wiring

**Files:**
- Modify: `server/server.go` (Deps + helper), `server/ingress.go:27-61`
- Modify: `config/config.go` (PayloadStore block), `cmd/arbiter/services.go` (wiring)
- Test: `server/ingress_test.go`, `config/config_test.go`

**Interfaces:**
- Consumes: `dastore.Client.Pin` (Task 5).
- Produces (Task 10 reuses the config/wiring):

```go
// server package
type CustodyPinner interface {
    Pin(ctx context.Context, purpose pb.PinPurpose, scopeKey string, refs []string) error
}
// Deps gains: Custody CustodyPinner // optional; nil ⇒ no pins

// config package
type PayloadStoreConfig struct {
    DataAddr    string `yaml:"data_addr"`
    ControlAddr string `yaml:"control_addr"`
}
func (p PayloadStoreConfig) Enabled() bool // any field set
// Config gains: PayloadStore PayloadStoreConfig `yaml:"payload_store"`
// Validate: when Enabled, both addrs required
```

- [ ] **Step 1: Write the failing tests**

In `server/ingress_test.go`, following the file's existing fixture style (find how it builds a `Server` with a fake `raftnode.ConsensusNode`/FSM and reuse that), add a recording pinner:

```go
type recordingPinner struct {
    mu    sync.Mutex
    calls []string // "purpose|scope|ref0,ref1"
    err   error
}

func (p *recordingPinner) Pin(_ context.Context, purpose pb.PinPurpose, scope string, refs []string) error {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.calls = append(p.calls, fmt.Sprintf("%s|%s|%s", purpose, scope, strings.Join(refs, ",")))
    return p.err
}
```

Cases (each drives `SubmitStatement` through the existing test harness with an envelope whose `PayloadRef = "fake://p1"`):

1. **Accepted pins before ack:** successful submit ⇒ ack returned AND exactly one call `"PIN_PURPOSE_SEQUENCED|<seq>|fake://p1"`.
2. **Pin failure blocks the ack:** `recordingPinner{err: errors.New("store down")}` ⇒ `SubmitStatement` returns gRPC `codes.Unavailable`, no `SequencedAck`.
3. **Duplicate re-ack re-pins:** submit once (pin call 1), submit the identical envelope again ⇒ second ack with the same seq AND a second identical pin call.
4. **Rejected admission never pins:** an envelope the FSM rejects (reuse an existing rejection fixture, e.g. non-INSERT kind) ⇒ error/na ack and zero pin calls.
5. **Nil custody bypass:** `Deps.Custody == nil` ⇒ submit succeeds, no panic.
6. **Empty payload ref never pins:** envelope with `PayloadRef == ""` ⇒ ack succeeds, zero pin calls.

In `config/config_test.go`: `payload_store` absent ⇒ valid; only `control_addr` set ⇒ invalid (message mentions `data_addr`); only `data_addr` ⇒ invalid; both ⇒ valid.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./server/ -run TestSubmitStatement -v; go test ./config/ -run PayloadStore -v`
Expected: FAIL — `CustodyPinner`/`Custody`/`PayloadStoreConfig` undefined.

- [ ] **Step 3: Implement**

`server/server.go`: add the interface, the Deps field, and:

```go
// pinSequenced makes the per-statement SEQUENCED pin durable. Called on
// every ack path — first send and idempotent duplicate replay — BEFORE the
// ack is returned: the ack is the custody barrier that ends the writer's
// lease duty (da.proto custody-chain rule), so it must never outrun the pin.
func (s *Server) pinSequenced(ctx context.Context, statementSeq uint64, payloadRef string) error {
    if s.d.Custody == nil || payloadRef == "" {
        return nil
    }
    scope := strconv.FormatUint(statementSeq, 10)
    if err := s.d.Custody.Pin(ctx, pb.PinPurpose_PIN_PURPOSE_SEQUENCED, scope, []string{payloadRef}); err != nil {
        s.d.Logger.Warn("sequenced custody pin failed", "statement_seq", statementSeq, "err", err)
        return status.Error(codes.Unavailable, "payload custody pin unavailable; retry")
    }
    return nil
}
```

`server/ingress.go`: after the `submit` type assertion —

```go
	if submit.Code == arbiter.AdmissionCodeAccepted {
		if err := svc.s.pinSequenced(ctx, submit.StatementSeq, env.PayloadRef); err != nil {
			return nil, err
		}
	}
	if submit.Code == arbiter.AdmissionCodeDuplicateClientSeq {
		if ack, ok := svc.reackDuplicate(env); ok {
			if err := svc.s.pinSequenced(ctx, ack.StatementSeq, env.PayloadRef); err != nil {
				return nil, err
			}
			return ack, nil
		}
	}
	return submitResultToPB(submit), nil
```

`config/config.go`: add the type, field, and `Validate` rule ("payload_store: data_addr and control_addr are both required when the block is set" via the existing `errors.Join` pattern).

`cmd/arbiter/services.go` in `newServerAndLoop`:

```go
	var custody *dastore.Client
	if rt.cfg.PayloadStore.Enabled() {
		custody, err = dastore.New(dastore.Config{
			DataAddr:    rt.cfg.PayloadStore.DataAddr,
			ControlAddr: rt.cfg.PayloadStore.ControlAddr,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("payload store client: %w", err)
		}
	}
```

Pass `Custody: custodyOrNilForServer(custody)` into `server.Deps` — Go nil-interface footgun: a nil `*dastore.Client` stored in a non-nil interface would defeat the `== nil` check, so wire it as:

```go
	srvDeps := server.Deps{ /* existing fields */ }
	if custody != nil {
		srvDeps.Custody = custody
	}
```

(Task 10 threads the same `custody` into orchestrator Deps; leave a `_ = custody` free — it is used in this task's server wiring already.) Close it on shutdown alongside the node's other owned resources (follow where `anchorClient` teardown lives in `cmd/arbiter`; if anchor has no explicit close, register `custody.Close` on the app's shutdown path in `app.go`).

- [ ] **Step 4: Run to verify pass**

Run: `go build ./... && go test ./server/ ./config/ ./cmd/arbiter/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/ config/ cmd/arbiter/
git commit -m "feat(server): SEQUENCED custody pin as the SubmitStatement ack barrier"
```

---

### Task 10: Orchestrator custody progression (REPLAY/AUDIT)

**Files:**
- Create: `orchestrator/custody.go`
- Modify: `orchestrator/deps.go` (Custody interface + Deps field), `orchestrator/loop.go` (stage map reset in `Run`, `advanceCustody` call in `rescan`), `cmd/arbiter/services.go` (thread `custody` into orchestrator Deps)
- Test: `orchestrator/custody_test.go`

**Interfaces:**
- Consumes: Task 8's `fsm.CustodyWorkSet`, Task 5's `Pin`/`Release` signatures, Task 9's `custody` wiring in services.go.
- Produces:

```go
// orchestrator package
type Custody interface {
    Pin(ctx context.Context, purpose pb.PinPurpose, scopeKey string, refs []string) error
    Release(ctx context.Context, purpose pb.PinPurpose, scopeKey string) error
}
// Deps gains: Custody Custody // optional; nil ⇒ custody step skipped
```

- [ ] **Step 1: Write the failing tests**

`orchestrator/custody_test.go` drives `advanceCustody` directly against a stub FSM view — follow how `orchestrator/loop_test.go` / `orchestrator_fakes_test.go` fake FSM reads today. If the existing fakes wrap `*fsm.FSM` concretely, introduce the minimal seam this task needs: `advanceCustody` reads rows via `o.custodyRows()`, a one-line method `func (o *Loop) custodyRows() []fsm.CustodyWork { return o.d.FSM.CustodyWorkSet() }` overridable in tests through a `rowsForTest func() []fsm.CustodyWork` field on `Loop` (nil in production). A fake custody records ordered calls and can inject per-call errors:

```go
type fakeCustody struct {
    mu    sync.Mutex
    calls []string // e.g. "pin|PIN_PURPOSE_REPLAY|3|refA,refB" / "release|PIN_PURPOSE_SEQUENCED|7|"
    fail  map[string]error // keyed by the same string; one-shot
}
```

Cases:

1. **Replay step order:** one sealed row (`BlockSeq: 3`, refs `[a b]`, seqs `[7 8]`, not safe) ⇒ calls exactly `pin|REPLAY|3|a,b`, `release|SEQUENCED|7`, `release|SEQUENCED|8`, in that order; stage advances (second `advanceCustody` makes zero new calls).
2. **Release failure re-runs the whole step:** same row, `fail["release|SEQUENCED|8"]` once ⇒ first pass three calls with one failure and NO stage advance; second pass repeats `pin|REPLAY|3|a,b` (idempotent no-op at the store) and both releases; third pass silent.
3. **Audit gating:** row safe+complete ⇒ after the replay step, same pass continues `pin|AUDIT|3|a,b`, `release|REPLAY|3`; a row safe but evidence-incomplete stops after the replay step; a row complete but not safe likewise.
4. **Rejected stops at REPLAY:** row with `Rejected: true`, safe+complete ⇒ replay step runs, audit step never runs.
5. **Empty refs:** row with no refs ⇒ no pin RPCs, releases still issued (harmless idempotent), stages advance.
6. **Nil custody:** `Deps.Custody == nil` ⇒ `advanceCustody` returns nil, zero calls.
7. **Term reset:** after stages complete, simulate a new term by calling the same reset `Run` performs (`o.resetCustodyStages()`) ⇒ next pass re-asserts everything (all idempotent calls repeat once, then silent).

- [ ] **Step 2: Run to verify failure**

Run: `go test ./orchestrator/ -run TestCustody -v`
Expected: FAIL — `advanceCustody`/`Custody` undefined.

- [ ] **Step 3: Implement**

`orchestrator/deps.go`: the `Custody` interface + `Deps.Custody` field (doc comment: satisfied by `*dastore.Client`; nil disables custody progression).

`orchestrator/custody.go`:

```go
package orchestrator

// Custody-chain progression (da.proto lifecycle): pure idempotent
// side-effect rescan over fsm.CustodyWorkSet. Stage memory is
// leader-term-local; a new term re-asserts everything and the store's
// idempotent pin/release semantics absorb the repeats. Successor pin is
// always made durable before the predecessor is released.

type custodyStage uint8

const (
    custodyNone custodyStage = iota
    custodyReplayPinned // REPLAY pinned, SEQUENCED released
    custodyAuditDone    // AUDIT pinned, REPLAY released; AUDIT is held forever in v1
)

func (o *Loop) resetCustodyStages() {
    o.custodyStage = make(map[uint64]custodyStage)
}

func (o *Loop) advanceCustody(ctx context.Context) error {
    if o.d.Custody == nil {
        return nil
    }
    for _, cw := range o.custodyRows() {
        if err := ctx.Err(); err != nil {
            return err
        }
        stage := o.custodyStage[cw.BlockSeq]
        if stage < custodyReplayPinned {
            if !o.custodyReplayStep(ctx, cw) {
                continue
            }
            o.custodyStage[cw.BlockSeq] = custodyReplayPinned
            stage = custodyReplayPinned
        }
        if stage < custodyAuditDone && cw.Safe && cw.EvidenceComplete && !cw.Rejected {
            if !o.custodyAuditStep(ctx, cw) {
                continue
            }
            o.custodyStage[cw.BlockSeq] = custodyAuditDone
        }
    }
    return nil
}

func (o *Loop) custodyReplayStep(ctx context.Context, cw fsm.CustodyWork) bool {
    scope := strconv.FormatUint(cw.BlockSeq, 10)
    if len(cw.Refs) > 0 {
        if err := o.d.Custody.Pin(ctx, pb.PinPurpose_PIN_PURPOSE_REPLAY, scope, cw.Refs); err != nil {
            o.d.Logger.Warn("replay custody pin failed", "block", cw.BlockSeq, "err", err)
            return false
        }
    }
    ok := true
    for _, seq := range cw.StatementSeqs {
        if err := o.d.Custody.Release(ctx, pb.PinPurpose_PIN_PURPOSE_SEQUENCED, strconv.FormatUint(seq, 10)); err != nil {
            o.d.Logger.Warn("sequenced custody release failed", "statement_seq", seq, "err", err)
            ok = false
        }
    }
    return ok
}

func (o *Loop) custodyAuditStep(ctx context.Context, cw fsm.CustodyWork) bool {
    scope := strconv.FormatUint(cw.BlockSeq, 10)
    if len(cw.Refs) > 0 {
        if err := o.d.Custody.Pin(ctx, pb.PinPurpose_PIN_PURPOSE_AUDIT, scope, cw.Refs); err != nil {
            o.d.Logger.Warn("audit custody pin failed", "block", cw.BlockSeq, "err", err)
            return false
        }
    }
    if err := o.d.Custody.Release(ctx, pb.PinPurpose_PIN_PURPOSE_REPLAY, scope); err != nil {
        o.d.Logger.Warn("replay custody release failed", "block", cw.BlockSeq, "err", err)
        return false
    }
    return true
}

func (o *Loop) custodyRows() []fsm.CustodyWork {
    if o.rowsForTest != nil {
        return o.rowsForTest()
    }
    return o.d.FSM.CustodyWorkSet()
}
```

`orchestrator/loop.go`: add `custodyStage map[uint64]custodyStage` + `rowsForTest func() []fsm.CustodyWork` fields to `Loop`; call `o.resetCustodyStages()` at the top of `Run` (leader-term reset); in `rescan`, after `handleWorkSet`:

```go
	if err := o.advanceCustody(ctx); err != nil {
		return err
	}
```

(`advanceCustody` returns only ctx errors; per-block failures are warn-and-continue, matching `handleLoopError`'s philosophy without routing through it.)

`cmd/arbiter/services.go`: add `Custody: ...` to `orchestrator.Deps` with the same nil-safe pattern as Task 9 (`if custody != nil { loopDeps.Custody = custody }` — restructure the literal into a variable first).

- [ ] **Step 4: Run to verify pass**

Run: `go build ./... && go test ./orchestrator/ ./cmd/arbiter/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/ cmd/arbiter/
git commit -m "feat(orchestrator): idempotent REPLAY/AUDIT custody-chain progression"
```

---

### Task 11: chpipeline integration against real `da-store --dev` + CI

**Files:**
- Modify: `integration/chpipeline/harness_test.go` (DA gate + spawn), `integration/chpipeline/harness_roles_test.go` (per-role payload stores), `integration/chpipeline/harness_ch_test.go` or wherever `h.payloads` and envelope refs are minted (locate `h.payloads` construction and the envelope-building helper with `grep -rn "payloads\|PayloadRef" integration/chpipeline/harness*.go`), `integration/chpipeline/cluster_test.go` (arbiter Custody wiring)
- Modify: `.github/workflows/ci.yml` (integration job)

**Interfaces:**
- Consumes: everything above.
- Produces: the end-to-end proof; no code interfaces.

- [ ] **Step 1: Add the DA harness mode**

In `harness_test.go`, next to the existing `ARBITER_CH_INTEGRATION` gate:

```go
// daStoreAddr returns the payload-store address for this run, or "" when
// the DA leg is not exercised (fs fallback). Modes:
//   DA_STORE_ADDR set          → attach to an external store (k8s port-forward, CI container)
//   DA_STORE_BIN set           → spawn `<bin> --dev --data-listen-addr=127.0.0.1:<free>` for the test
//   neither                    → "" (fs mode, today's behavior)
func daStoreAddr(t *testing.T) string {
    if addr := os.Getenv("DA_STORE_ADDR"); addr != "" {
        return addr
    }
    bin := os.Getenv("DA_STORE_BIN")
    if bin == "" {
        return ""
    }
    ln, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        t.Fatalf("free port: %v", err)
    }
    addr := ln.Addr().String()
    _ = ln.Close()
    cmd := exec.Command(bin, "--dev", "--data-listen-addr="+addr)
    cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
    if err := cmd.Start(); err != nil {
        t.Fatalf("start da-store: %v", err)
    }
    t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
    waitForDAStore(t, addr) // dial + GetStoreLimits with retry until ready (5s deadline)
    return addr
}
```

`waitForDAStore` loops `grpc.NewClient` + `pb.NewPayloadStoreClient(conn).GetStoreLimits` with 100ms sleeps.

- [ ] **Step 2: Thread DA mode through the harness**

Where the harness currently builds one shared `h.payloads` (`*fspayload.Store`) and mints envelope refs:

1. Store `h.daAddr = daStoreAddr(t)` on the harness.
2. **Ref minting:** the envelope-building path currently invents a ref for `fspayload`. Add a harness helper used by both modes:

```go
// mintPayloadRef plays HouseGate: in DA mode the STORE mints the ref and the
// harness learns it from the put result (dastore.Client.Put is the snode's
// assert-shaped call, so use the raw pb client here); the snode's later Put
// becomes the lease-refreshing duplicate put. fs mode keeps the harness's
// current invented-ref convention (keep whatever literal it uses today).
func (h *harness) mintPayloadRef(t *testing.T, payload []byte) string {
    t.Helper()
    if h.daAddr == "" {
        return "payload-" + replay.DigestBytes(payload)
    }
    conn, err := grpc.NewClient(h.daAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        t.Fatalf("dial da-store: %v", err)
    }
    defer func() { _ = conn.Close() }()
    res, err := pb.NewPayloadStoreClient(conn).PutPayloadInline(h.ctx, &pb.PutPayloadInlineRequest{
        Header: &pb.PutPayloadHeader{
            PayloadHash:   replay.DigestBytes(payload),
            PayloadLength: uint64(len(payload)),
        },
        Payload: payload,
    })
    if err != nil || res.GetCode() != pb.PutCode_PUT_CODE_OK {
        t.Fatalf("mint ref: %v %v", res, err)
    }
    return res.GetPayloadRef()
}
```
3. **Per-role stores:** in `startVerifiers`/`startSNode`, when `h.daAddr != ""` construct a fresh `dastore.New(dastore.Config{DataAddr: h.daAddr})` per role (cleanup-closed) instead of sharing `h.payloads`; fs mode keeps the shared `fspayload` store as today.
4. **Cluster custody:** in `cluster_test.go` where node deps are built (`Anchor: anchor.NewLocal()` etc.), when DA mode is on, build one `dastore.Client{DataAddr: h.daAddr, ControlAddr: h.daAddr}` (dev mode serves both services on one listener) and set it as `Custody` on both `server.Deps` and `orchestrator.Deps` for every node.

- [ ] **Step 3: Run the suite in both modes locally**

```bash
docker run -d --rm --name arbiter-da-ch -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:25.8
# fs mode (unchanged behavior):
ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:9000 go test ./integration/chpipeline/ -count=1 -timeout 900s
# DA mode (build the store first):
git -C /Users/uranuswch/Dev/sentio_xyz/network-da pull && (cd /Users/uranuswch/Dev/sentio_xyz/network-da && go build -o ./bin/da-store ./cmd/da-store)
ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:9000 DA_STORE_BIN=/Users/uranuswch/Dev/sentio_xyz/network-da/bin/da-store go test ./integration/chpipeline/ -count=1 -timeout 900s
```

Expected: both green. The DA run proves put→sequence→fetch→replay→promote against the real store with no shared filesystem (custody pin timing itself is covered by Task 9/10 unit tests — the real store has no pin-listing RPC to assert against).

- [ ] **Step 4: CI**

In `.github/workflows/ci.yml`'s `integration-clickhouse` job, before the test step:

```yaml
      - name: Start da-store (dev mode)
        id: dastore
        continue-on-error: true
        run: |
          docker rm -f arbiter-da-store 2>/dev/null || true
          if docker run -d --name arbiter-da-store -p 19001:19001 \
              ghcr.io/sentioxyz/da-store:dev --dev --data-listen-addr=:19001; then
            echo "addr=127.0.0.1:19001" >> "$GITHUB_OUTPUT"
          fi
```

and extend the test step's env: `DA_STORE_ADDR: ${{ steps.dastore.outputs.addr }}` (empty ⇒ tests run in fs mode — soft dependency until the GHCR image/pull-auth is confirmed available to this runner; note this in the PR description). Add a teardown `docker rm -f arbiter-da-store` mirroring however the job cleans up ClickHouse.

- [ ] **Step 5: Commit**

```bash
git add integration/chpipeline/ .github/workflows/ci.yml
git commit -m "test(integration): run chpipeline against a real da-store dev instance"
```

---

### Task 12: Docs + sample configs

**Files:**
- Modify: `README.md`, `configs/local.yaml`, `configs/snode.local.yaml`, `configs/verifier.local.yaml`

**Interfaces:** none (docs only).

- [ ] **Step 1: README**

Add a "## DA payload store" section after the P1d section covering: the three client legs and where they wire in; the custody chain one-liner (`lease → SEQUENCED(stmt) → REPLAY(block) → AUDIT(block, held in v1)`); config for all three binaries (the §7 blocks from the spec verbatim); the local dev recipe (`da-store --dev`) and the DA-mode test invocation from Task 11; the k8s smoke test:

```bash
kubectl --context sentio-sea -n da-test port-forward svc/da-store 9001:9001 9002:9002
```

with a note that production serves PayloadStore on 9001 and PayloadLifecycle on 9002 while `--dev` serves both on one listener. Also fix the two stale lines this work touches: `README.md:14` (orchestrator "lands with P1" → implemented in P1b) and `anchor/client.go:4-5` ("P1b ships only the local backend" → local + EVM since P1d).

- [ ] **Step 2: Sample configs**

Append commented-out `payload_store` blocks: `configs/local.yaml` gets `data_addr`/`control_addr` (with the "both required; absent ⇒ pins disabled" note); the snode/verifier samples get `backend: grpc` + `data_addr` (with "default fs uses payload_dir").

- [ ] **Step 3: Verify and commit**

```bash
go build ./... && go test ./... -count=1
git add README.md configs/ anchor/client.go
git commit -m "docs: DA payload-store client usage, custody chain, sample configs"
```

---

## Self-review notes (already applied)

- Spec §7 required both `data_addr` and `control_addr` for `cmd/arbiter` (GetStoreLimits lives on the data plane) — Task 9's config matches.
- Spec §6.2's "stage advances only when pin AND every release succeeded" is Task 10 test case 2.
- Spec §9's "pins advance to AUDIT" integration assertion is downgraded to unit coverage (Task 10 case 3): the real store exposes no pin-listing RPC; the harness proves the data path, the fake proves the timing.
- The FSM red lines (no `gen/pb`, no wall clock) forced `CustodyWork` to stay pb-free — purposes appear only in the orchestrator/server layers.
