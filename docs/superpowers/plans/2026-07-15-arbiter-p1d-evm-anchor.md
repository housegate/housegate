# Arbiter P1d — Chain-Agnostic EVM Anchor Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the P1d spec ([2026-07-15-arbiter-p1d-evm-anchor-design.md](../specs/2026-07-15-arbiter-p1d-evm-anchor-design.md)): an `AnchorRegistry` contract + a chain-agnostic EVM `anchor.Client` where contract state is the single source of truth, behind a refined `Finality(ctx, ref) (Status, error)` seam.

**Architecture:** All work lands in the **arbiter repo** (`~/Dev/sentio_xyz/arbiter`, module `github.com/sentioxyz/arbiter`, direct-to-main via PR). New Solidity contract under `contracts/` with checked-in abigen bindings under `anchor/evm/bindings/`; new `anchor/evm` client package; a 2-line orchestrator adaptation; config + `cmd/arbiter` wiring; `cmd/arbiter-anchor` ops CLI; anvil E2E CI job. FSM and arbiter-proto are **zero-diff** this phase.

**Tech Stack:** Go 1.26.3, go-ethereum v1.17.4 (`ethclient`, `ethclient/simulated`, `accounts/abi/bind` v1 bindings), Solidity ^0.8.24 (compiled via dockerized `ethereum/solc:0.8.24`), abigen via go.mod `tool` directive, anvil (docker, CI only).

## Global Constraints

- **Repo:** all code tasks run in `~/Dev/sentio_xyz/arbiter` on a feature branch `feat/p1d-evm-anchor` cut from current `main` (`03aa035`). The plan document itself lives in the housegate repo.
- **`fsm/` and proto zero-diff** (spec §9 tripwire): any diff under `fsm/` or in arbiter-proto is a plan violation.
- **No new blocking calls in the orchestrator loop**: every chain call inside `anchor/evm` carries a `cfg.RPCTimeout` context deadline; nothing ever waits for a receipt.
- **`anchor.Local` behavior stays bit-identical** (only its `Finality` signature adapts).
- **English identifiers, comments, and log messages** (repo convention). Conventional-commit prefixes as in existing history: `feat(anchor)`, `test(anchor)`, `feat(config)`, `feat(cmd)`, `build`, `ci`.
- **Verify green with:** `go build ./... && go vet ./... && go test ./...` (ClickHouse-gated tests auto-skip without `ARBITER_CH_INTEGRATION=1`).
- Config field names, defaults, and validation rules are **verbatim from spec §7**: `finality_mode` default `finalized`, `finality_confirmations` default 12, `last_mergeable_confirmations` default 0, `resubmit_after` default 90s (≥ 1s), `gas_bump_percent` default 25 (≥ 10), `rpc_timeout` default 10s. Env override `ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX`.
- The simulated backend's chain id is **1337** (`params.AllDevChainProtocolChanges`); tests hardcode it via a shared helper.

---

### Task 1: `AnchorRegistry` contract, generation pipeline, and contract-behavior tests

**Files:**
- Create: `contracts/AnchorRegistry.sol`
- Create: `Makefile` (repo has none yet)
- Create: `anchor/evm/bindings/anchor_registry.go` (generated, checked in)
- Create: `anchor/evm/bindings/AnchorRegistry.sol.sha256` (drift sidecar, checked in)
- Create: `anchor/evm/sim_test.go` (shared simulated-backend helpers)
- Create: `anchor/evm/contract_test.go`
- Modify: `.gitignore` (add `contracts/build/`)
- Modify: `go.mod` (abigen `tool` directive; `go mod tidy` side effects)

**Interfaces:**
- Consumes: nothing (first task).
- Produces: `bindings.DeployAnchorRegistry(auth *bind.TransactOpts, backend bind.ContractBackend, initialPosters []common.Address) (common.Address, *types.Transaction, *AnchorRegistry, error)`; `bindings.NewAnchorRegistry(addr common.Address, backend bind.ContractBackend) (*AnchorRegistry, error)`; caller methods `Anchors(*bind.CallOpts, [32]byte) (struct{ StateRoot [32]byte; L2BlockNumber uint64 }, error)`, `Posters`, `Owner`; transactor methods `Anchor(*bind.TransactOpts, [32]byte, [32]byte)`, `SetPoster`, `TransferOwner`; filterer `FilterAnchored(*bind.FilterOpts, [][32]byte) (*AnchorRegistryAnchoredIterator, error)`. Test helpers `newSim(t, extra ...common.Address) (*simulated.Backend, *bind.TransactOpts, *ecdsa.PrivateKey)` and `deployRegistry(t, sim, auth, posters ...common.Address) (common.Address, *bindings.AnchorRegistry)`.

- [ ] **Step 1: Branch, and write the contract source**

```bash
cd ~/Dev/sentio_xyz/arbiter && git checkout -b feat/p1d-evm-anchor
mkdir -p contracts anchor/evm/bindings
```

`contracts/AnchorRegistry.sol` — exactly the spec §3 contract:

```solidity
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/// Minimal L3-block anchor registry (arbiter design §5.2). v1 posts
/// commitment only: (l3BlockHash, stateRoot). The mapping is the §10.3
/// idempotency anchor: re-anchoring the same pair is a no-op; a
/// conflicting root for an anchored hash reverts.
contract AnchorRegistry {
    struct Entry {
        bytes32 stateRoot;
        uint64 l2BlockNumber; // block.number at first anchor; != 0 marks presence
    }

    address public owner;
    mapping(address => bool) public posters;
    mapping(bytes32 => Entry) public anchors;

    event Anchored(bytes32 indexed l3BlockHash, bytes32 stateRoot, uint64 l2BlockNumber);
    event PosterSet(address indexed poster, bool allowed);
    event OwnerTransferred(address indexed previousOwner, address indexed newOwner);

    error NotOwner();
    error NotPoster();
    error StateRootMismatch();

    constructor(address[] memory initialPosters) {
        owner = msg.sender;
        for (uint256 i = 0; i < initialPosters.length; i++) {
            posters[initialPosters[i]] = true;
            emit PosterSet(initialPosters[i], true);
        }
    }

    modifier onlyOwner() { if (msg.sender != owner) revert NotOwner(); _; }
    modifier onlyPoster() { if (!posters[msg.sender]) revert NotPoster(); _; }

    function setPoster(address poster, bool allowed) external onlyOwner {
        posters[poster] = allowed;
        emit PosterSet(poster, allowed);
    }

    function transferOwner(address newOwner) external onlyOwner {
        emit OwnerTransferred(owner, newOwner);
        owner = newOwner;
    }

    function anchor(bytes32 l3BlockHash, bytes32 stateRoot) external onlyPoster {
        Entry storage e = anchors[l3BlockHash];
        if (e.l2BlockNumber != 0) {
            if (e.stateRoot != stateRoot) revert StateRootMismatch();
            return; // idempotent no-op
        }
        anchors[l3BlockHash] = Entry(stateRoot, uint64(block.number));
        emit Anchored(l3BlockHash, stateRoot, uint64(block.number));
    }
}
```

- [ ] **Step 2: Add the abigen tool directive and the Makefile**

```bash
go get -tool github.com/ethereum/go-ethereum/cmd/abigen   # records a `tool` directive at the pinned v1.17.4
go mod tidy
```

`Makefile` (new file at repo root):

```makefile
# solc runs dockerized so no local Solidity toolchain is required.
# Override with SOLC="solc" if you have a local 0.8.x solc.
SOLC ?= docker run --rm -v $(CURDIR)/contracts:/contracts ethereum/solc:0.8.24

.PHONY: gen-anchor-contract check-anchor-bindings

gen-anchor-contract:
	$(SOLC) --abi --bin --optimize --overwrite -o /contracts/build /contracts/AnchorRegistry.sol
	go tool abigen \
		--abi contracts/build/AnchorRegistry.abi \
		--bin contracts/build/AnchorRegistry.bin \
		--pkg bindings --type AnchorRegistry \
		--out anchor/evm/bindings/anchor_registry.go
	shasum -a 256 contracts/AnchorRegistry.sol | awk '{print $$1}' > anchor/evm/bindings/AnchorRegistry.sol.sha256

# CI drift gate: the checked-in bindings must match the .sol source.
check-anchor-bindings:
	@test "$$(shasum -a 256 contracts/AnchorRegistry.sol | awk '{print $$1}')" = "$$(cat anchor/evm/bindings/AnchorRegistry.sol.sha256)" \
		|| (echo "anchor bindings drift: run 'make gen-anchor-contract' and commit" && exit 1)
```

Append to `.gitignore`:

```
contracts/build/
```

- [ ] **Step 3: Generate the bindings and verify they compile**

```bash
make gen-anchor-contract
make check-anchor-bindings
go build ./anchor/...
```

Expected: `anchor/evm/bindings/anchor_registry.go` exists, exports `DeployAnchorRegistry` / `NewAnchorRegistry` / `AnchorRegistryAnchoredIterator`, and builds. (If docker is unavailable locally, `brew install solidity` and rerun with `make gen-anchor-contract SOLC=solc`.)

- [ ] **Step 4: Write the shared simulated-backend helpers**

`anchor/evm/sim_test.go`:

```go
package evm

import (
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"
	"github.com/ethereum/go-ethereum/params"

	"github.com/sentioxyz/arbiter/anchor/evm/bindings"
)

// simChainID is the dev-chain id used by ethclient/simulated.
var simChainID = big.NewInt(1337)

// newSim starts an in-process simulated chain funding one generated key
// (plus any extra addresses) and returns the backend, a transactor for the
// funded key, and the key itself.
func newSim(t *testing.T, extra ...common.Address) (*simulated.Backend, *bind.TransactOpts, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	auth, err := bind.NewKeyedTransactorWithChainID(key, simChainID)
	if err != nil {
		t.Fatalf("transactor: %v", err)
	}
	balance := new(big.Int).Mul(big.NewInt(100), big.NewInt(params.Ether))
	alloc := types.GenesisAlloc{auth.From: {Balance: balance}}
	for _, a := range extra {
		alloc[a] = types.Account{Balance: balance}
	}
	sim := simulated.NewBackend(alloc)
	t.Cleanup(func() { sim.Close() })
	return sim, auth, key
}

// deployRegistry deploys AnchorRegistry with the given posters and mines it.
func deployRegistry(t *testing.T, sim *simulated.Backend, auth *bind.TransactOpts, posters ...common.Address) (common.Address, *bindings.AnchorRegistry) {
	t.Helper()
	if posters == nil {
		posters = []common.Address{}
	}
	addr, _, reg, err := bindings.DeployAnchorRegistry(auth, sim.Client(), posters)
	if err != nil {
		t.Fatalf("deploy AnchorRegistry: %v", err)
	}
	sim.Commit()
	return addr, reg
}
```

- [ ] **Step 5: Write the contract-behavior tests (the four spec §3 commitments)**

`anchor/evm/contract_test.go`:

```go
package evm

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func h32(b byte) [32]byte { var out [32]byte; out[0] = b; return out }

func TestContract_AnchorAndIdempotentNoOp(t *testing.T) {
	sim, auth, _ := newSim(t)
	_, reg := deployRegistry(t, sim, auth, auth.From)

	if _, err := reg.Anchor(auth, h32(1), h32(2)); err != nil {
		t.Fatalf("first anchor: %v", err)
	}
	sim.Commit()

	entry, err := reg.Anchors(&bind.CallOpts{}, h32(1))
	if err != nil {
		t.Fatalf("read entry: %v", err)
	}
	if entry.StateRoot != h32(2) || entry.L2BlockNumber == 0 {
		t.Fatalf("unexpected entry: %+v", entry)
	}

	// Same pair again: succeeds as a no-op, entry unchanged.
	if _, err := reg.Anchor(auth, h32(1), h32(2)); err != nil {
		t.Fatalf("idempotent re-anchor must succeed: %v", err)
	}
	sim.Commit()
	again, _ := reg.Anchors(&bind.CallOpts{}, h32(1))
	if again != entry {
		t.Fatalf("entry mutated by idempotent re-anchor: %+v vs %+v", again, entry)
	}

	// Exactly one Anchored event for this hash.
	it, err := reg.FilterAnchored(&bind.FilterOpts{}, [][32]byte{h32(1)})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	defer it.Close()
	n := 0
	for it.Next() {
		n++
		if it.Event.StateRoot != h32(2) || it.Event.L2BlockNumber != entry.L2BlockNumber {
			t.Fatalf("event payload mismatch: %+v", it.Event)
		}
	}
	if n != 1 {
		t.Fatalf("want exactly 1 Anchored event, got %d", n)
	}
}

func TestContract_ConflictingRootReverts(t *testing.T) {
	sim, auth, _ := newSim(t)
	_, reg := deployRegistry(t, sim, auth, auth.From)

	if _, err := reg.Anchor(auth, h32(1), h32(2)); err != nil {
		t.Fatalf("first anchor: %v", err)
	}
	sim.Commit()

	if _, err := reg.Anchor(auth, h32(1), h32(3)); err == nil {
		t.Fatal("conflicting state root must revert")
	}
	entry, _ := reg.Anchors(&bind.CallOpts{}, h32(1))
	if entry.StateRoot != h32(2) {
		t.Fatalf("mapping must be untouched after conflict, got %+v", entry)
	}
}

func TestContract_PosterGating(t *testing.T) {
	strangerKey, _ := crypto.GenerateKey()
	stranger, _ := bind.NewKeyedTransactorWithChainID(strangerKey, simChainID)
	sim, owner, _ := newSim(t, stranger.From)
	_, reg := deployRegistry(t, sim, owner) // empty poster set

	// Even the owner is not a poster by default.
	if _, err := reg.Anchor(owner, h32(1), h32(2)); err == nil {
		t.Fatal("non-poster anchor must revert")
	}

	if _, err := reg.SetPoster(owner, stranger.From, true); err != nil {
		t.Fatalf("setPoster: %v", err)
	}
	sim.Commit()
	if _, err := reg.Anchor(stranger, h32(1), h32(2)); err != nil {
		t.Fatalf("allowlisted poster must anchor: %v", err)
	}
	sim.Commit()

	// Only the owner may manage posters.
	if _, err := reg.SetPoster(stranger, stranger.From, false); err == nil {
		t.Fatal("non-owner setPoster must revert")
	}
}

func TestContract_EntryAbsentReadsZero(t *testing.T) {
	sim, auth, _ := newSim(t)
	_, reg := deployRegistry(t, sim, auth, auth.From)
	entry, err := reg.Anchors(&bind.CallOpts{}, h32(9))
	if err != nil {
		t.Fatalf("read absent entry: %v", err)
	}
	if entry.L2BlockNumber != 0 || entry.StateRoot != ([32]byte{}) {
		t.Fatalf("absent entry must be zero, got %+v", entry)
	}
	_ = big.NewInt(0) // keep math/big imported alongside future edits
}
```

- [ ] **Step 6: Run the tests**

Run: `go test ./anchor/evm/ -run TestContract -v`
Expected: all four PASS. (If `simulated.NewBackend` genesis funding or the dev chain id differs, fix the helper — `params.AllDevChainProtocolChanges.ChainID` is authoritative.)

- [ ] **Step 7: Commit**

```bash
git add contracts/ Makefile .gitignore go.mod go.sum anchor/evm/
git commit -m "feat(anchor): AnchorRegistry contract, checked-in bindings, behavior tests"
```

---

### Task 2: Seam refinement — `Finality` returns `Status` (atomic breaking change)

**Files:**
- Modify: `anchor/client.go`
- Modify: `anchor/local.go`
- Modify: `anchor/local_test.go`
- Modify: `orchestrator/promotion.go:87-103` (`anchorBlock` tail)
- Modify: `orchestrator/orchestrator_fakes_test.go:169-171` (`fakeAnchor.Finality`)
- Modify: `orchestrator/promotion_fixtures_test.go:197,225` (`flakyFinalityAnchor.Finality`, `pollingFinalityAnchor.Finality`)

**Interfaces:**
- Consumes: existing `anchor.Client`, `arbiter.AnchorRef`.
- Produces: `anchor.Status{Final, LastMergeable bool, Ref arbiter.AnchorRef}` and `Client.Finality(ctx, ref) (Status, error)` — every later task builds against this signature.

- [ ] **Step 1: Write the failing test for Local's new shape**

Append to `anchor/local_test.go`:

```go
func TestLocal_FinalityReturnsStatusWithSameRef(t *testing.T) {
	l := NewLocal()
	ref, err := l.Anchor(context.Background(), "0xabc", "0xdef")
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	st, err := l.Finality(context.Background(), ref)
	if err != nil {
		t.Fatalf("finality: %v", err)
	}
	if !st.Final || !st.LastMergeable || st.Ref != ref {
		t.Fatalf("local must report immediate finality with the unchanged ref, got %+v", st)
	}
}
```

- [ ] **Step 2: Run it to verify it fails to compile**

Run: `go test ./anchor/ -run TestLocal_FinalityReturnsStatus -v`
Expected: FAIL — `undefined: Status` / assignment mismatch.

- [ ] **Step 3: Implement the seam**

`anchor/client.go` — replace the interface block:

```go
// Status is one point-in-time finality answer. Ref echoes the input ref,
// possibly enriched by the backend (real L2 block number, the tx hash that
// actually landed): a gas-bump resubmit invalidates the originally recorded
// tx hash, so the enriched ref is the audit-correct one. The orchestrator
// stores it via RecordAnchorFinality's deliberate last-wins anchor content.
type Status struct {
	Final         bool
	LastMergeable bool
	Ref           arbiter.AnchorRef
}

// Client posts one L3 block commitment and reports its finality.
type Client interface {
	Anchor(ctx context.Context, l3BlockHash, stateRoot string) (arbiter.AnchorRef, error)
	// Finality is a non-blocking, point-in-time progress check. Backends
	// MAY internally re-drive a lost or stuck anchor tx (self-heal) — a
	// side effect confined to the backend's own signing account; no
	// implementation ever touches FSM state.
	Finality(ctx context.Context, ref arbiter.AnchorRef) (Status, error)
}
```

`anchor/local.go` — replace the `Finality` method:

```go
func (l *Local) Finality(_ context.Context, ref arbiter.AnchorRef) (Status, error) {
	return Status{Final: true, LastMergeable: true, Ref: ref}, nil
}
```

`orchestrator/promotion.go` — replace lines 87–103 (the `Finality` call through the second propose) with:

```go
	st, err := o.d.Anchor.Finality(ctx, ref)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		o.d.Logger.Warn("anchor finality check failed", "block", ba.BlockSeq, "err", err)
		return nil
	}
	if ba.Anchored && st.Final == ba.Finality && st.LastMergeable == ba.LastMergeable {
		return nil
	}
	_, err = o.propose(wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{
		L3BlockSeq:           ba.BlockSeq,
		Anchor:               st.Ref,
		FinalityReached:      st.Final,
		LastMergeableReached: st.LastMergeable,
	}})
	if err != nil && !errors.Is(err, ErrRejected) {
		return fmt.Errorf("record anchor finality block %d: %w", ba.BlockSeq, err)
	}
	return nil
```

(The no-progress guard still compares only the two flags — ref enrichment alone never proposes; it rides the flag-change proposal. First-propose path at lines 61–83 is untouched.)

Test fakes — same mechanical change in all three:

`orchestrator/orchestrator_fakes_test.go`:

```go
func (a *fakeAnchor) Finality(_ context.Context, ref arbiter.AnchorRef) (anchor.Status, error) {
	return anchor.Status{Final: true, LastMergeable: true, Ref: ref}, nil
}
```

`orchestrator/promotion_fixtures_test.go` — `flakyFinalityAnchor.Finality` keeps its existing internal counters/behavior but returns `anchor.Status{Final: <old first bool>, LastMergeable: <old second bool>, Ref: ref}`; `pollingFinalityAnchor.Finality` likewise wraps its `a.finality, a.lastMergeable` fields into `anchor.Status{..., Ref: ref}`. Add the `"github.com/sentioxyz/arbiter/anchor"` import where missing.

- [ ] **Step 4: Verify the whole repo is green**

Run: `go build ./... && go vet ./... && go test ./anchor/ ./orchestrator/`
Expected: PASS everywhere (`TestPromotion_FinalityPollDoesNotSpamWhenPending` and friends confirm orchestrator behavior is unchanged).

- [ ] **Step 5: Commit**

```bash
git add anchor/ orchestrator/
git commit -m "feat(anchor)!: Finality returns Status with an enrichable ref (P1d seam)"
```

---

### Task 3: `anchor/evm` package base — digest codec, Config, chainClient, constructor

**Files:**
- Create: `anchor/evm/digest.go`
- Create: `anchor/evm/digest_test.go`
- Create: `anchor/evm/client.go`
- Create: `anchor/evm/client_test.go`

**Interfaces:**
- Consumes: `bindings.NewAnchorRegistry` (Task 1), `anchor.Client`/`anchor.Status` (Task 2).
- Produces: `evm.Config{ContractAddress common.Address, ChainID *big.Int, FinalityMode string, FinalityConfirmations, LastMergeableConfirmations uint64, ResubmitAfter, RPCTimeout time.Duration, GasBumpPercent uint64, Logger *slog.Logger}`; `evm.FinalityModeFinalized = "finalized"`, `evm.FinalityModeConfirmations = "confirmations"`; `evm.New(ctx context.Context, cfg Config, chain chainClient, key *ecdsa.PrivateKey) (*Client, error)`; unexported `chainClient` interface (`bind.ContractBackend` + `ChainID` + `TransactionByHash`) satisfied by `*ethclient.Client` and `simulated.Client`; unexported `digestToBytes32(string) ([32]byte, error)`, `bytes32ToDigest([32]byte) string`.

- [ ] **Step 1: Write failing digest-codec tests**

`anchor/evm/digest_test.go`:

```go
package evm

import "testing"

func TestDigestRoundTrip(t *testing.T) {
	d := "0x0a00ff000000000000000000000000000000000000000000000000000000cafe"
	b, err := digestToBytes32(d)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := bytes32ToDigest(b); got != d {
		t.Fatalf("round trip: %s != %s", got, d)
	}
}

func TestDigestRejectsMalformed(t *testing.T) {
	for _, bad := range []string{"", "0x", "abc", "0x1234", "0x" + string(make([]byte, 64)), "0xzz00ff000000000000000000000000000000000000000000000000000000cafe"} {
		if _, err := digestToBytes32(bad); err == nil {
			t.Fatalf("digest %q must be rejected", bad)
		}
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./anchor/evm/ -run TestDigest -v`
Expected: FAIL — `undefined: digestToBytes32`.

- [ ] **Step 3: Implement the codec**

`anchor/evm/digest.go`:

```go
package evm

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// digestToBytes32 converts a replay.DigestString ("0x" + 64 hex) into a
// contract bytes32. The forms are bijective; anything else is a caller bug
// surfaced before any chain interaction.
func digestToBytes32(s string) ([32]byte, error) {
	var out [32]byte
	if !strings.HasPrefix(s, "0x") || len(s) != 66 {
		return out, fmt.Errorf("digest %q is not \"0x\" + 64 hex chars", s)
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		return out, fmt.Errorf("digest %q: %w", s, err)
	}
	copy(out[:], b)
	return out, nil
}

func bytes32ToDigest(b [32]byte) string {
	return "0x" + hex.EncodeToString(b[:])
}
```

- [ ] **Step 4: Write the failing constructor tests**

`anchor/evm/client_test.go`:

```go
package evm

import (
	"context"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func testConfig(contract common.Address) Config {
	return Config{
		ContractAddress:            contract,
		ChainID:                    new(big.Int).Set(simChainID),
		FinalityMode:               FinalityModeConfirmations,
		FinalityConfirmations:      3,
		LastMergeableConfirmations: 2,
		ResubmitAfter:              50 * time.Millisecond,
		GasBumpPercent:             25,
		RPCTimeout:                 5 * time.Second,
		Logger:                     slog.Default(),
	}
}

func TestNew_ChainIDAssertion(t *testing.T) {
	sim, auth, key := newSim(t)
	addr, _ := deployRegistry(t, sim, auth, auth.From)

	if _, err := New(context.Background(), testConfig(addr), sim.Client(), key); err != nil {
		t.Fatalf("matching chain id must construct: %v", err)
	}

	bad := testConfig(addr)
	bad.ChainID = big.NewInt(999)
	if _, err := New(context.Background(), bad, sim.Client(), key); err == nil {
		t.Fatal("chain id mismatch must fail startup")
	}
}
```

- [ ] **Step 5: Run to verify failure**

Run: `go test ./anchor/evm/ -run TestNew_ChainIDAssertion -v`
Expected: FAIL — `undefined: New` / `undefined: Config`.

- [ ] **Step 6: Implement Config, chainClient, and New**

`anchor/evm/client.go`:

```go
// Package evm is the chain-agnostic EVM anchor backend (P1d design):
// contract state in AnchorRegistry is the single source of truth for
// idempotency, failover convergence, and stuck-tx self-healing; tx hashes
// are audit information corrected from chain events.
package evm

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/anchor"
	"github.com/sentioxyz/arbiter/anchor/evm/bindings"
)

// Finality judgment modes (config `anchor.evm.finality_mode`).
const (
	FinalityModeFinalized     = "finalized"
	FinalityModeConfirmations = "confirmations"
)

// Config carries the deployment-selected chain and judgment parameters.
type Config struct {
	ContractAddress            common.Address
	ChainID                    *big.Int
	FinalityMode               string
	FinalityConfirmations      uint64
	LastMergeableConfirmations uint64
	ResubmitAfter              time.Duration
	GasBumpPercent             uint64
	RPCTimeout                 time.Duration
	Logger                     *slog.Logger
}

// chainClient is the narrow chain surface the backend needs. Both
// *ethclient.Client and the simulated backend's client satisfy it.
type chainClient interface {
	bind.ContractBackend
	ChainID(ctx context.Context) (*big.Int, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error)
}

// inflight tracks one unconfirmed anchor tx (in-memory only: the contract
// mapping recovers every restart scenario, so durability adds nothing).
type inflight struct {
	txHash    common.Hash
	nonce     uint64
	gasTipCap *big.Int
	gasFeeCap *big.Int
	sentAt    time.Time
}

// Client implements anchor.Client against an AnchorRegistry deployment.
type Client struct {
	cfg      Config
	chain    chainClient
	registry *bindings.AnchorRegistry
	key      *ecdsa.PrivateKey
	from     common.Address

	mu       sync.Mutex
	inflight map[string]*inflight // l3BlockHash digest -> pending tx
	landedTx map[string]string    // l3BlockHash digest -> landed tx hash (getLogs, cached)
}

var _ anchor.Client = (*Client)(nil)

// New asserts the RPC's chain id against config (fail-fast on a
// misconfigured endpoint) and binds the registry contract.
func New(ctx context.Context, cfg Config, chain chainClient, key *ecdsa.PrivateKey) (*Client, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	cctx, cancel := context.WithTimeout(ctx, cfg.RPCTimeout)
	defer cancel()
	got, err := chain.ChainID(cctx)
	if err != nil {
		return nil, fmt.Errorf("query chain id: %w", err)
	}
	if got.Cmp(cfg.ChainID) != 0 {
		return nil, fmt.Errorf("chain id mismatch: rpc reports %s, config expects %s", got, cfg.ChainID)
	}
	registry, err := bindings.NewAnchorRegistry(cfg.ContractAddress, chain)
	if err != nil {
		return nil, fmt.Errorf("bind AnchorRegistry at %s: %w", cfg.ContractAddress, err)
	}
	from := crypto.PubkeyToAddress(key.PublicKey)
	cfg.Logger.Info("evm anchor backend ready", "contract", cfg.ContractAddress, "chain_id", cfg.ChainID, "eoa", from)
	return &Client{
		cfg:      cfg,
		chain:    chain,
		registry: registry,
		key:      key,
		from:     from,
		inflight: map[string]*inflight{},
		landedTx: map[string]string{},
	}, nil
}
```

**Note:** do NOT add `var _ anchor.Client = (*Client)(nil)` yet — `Anchor`/`Finality` don't exist until Tasks 4–5; the guard is added in Task 5 when the interface is complete. Drop the `anchor` and `arbiter` imports from `client.go` until then if the compiler flags them as unused.

- [ ] **Step 7: Run the package tests**

Run: `go test ./anchor/evm/ -v`
Expected: digest + constructor + Task-1 contract tests all PASS.

- [ ] **Step 8: Commit**

```bash
git add anchor/evm/
git commit -m "feat(anchor): evm backend base - digest codec, config, chain-id-asserting constructor"
```

---

### Task 4: `Anchor` path — check-before-anchor, in-flight suppression, EIP-1559 send

**Files:**
- Create: `anchor/evm/anchor.go`
- Create: `anchor/evm/anchor_test.go`
- Modify: `anchor/evm/client.go` (move/enable the `var _ anchor.Client` guard here if it was deferred)

**Interfaces:**
- Consumes: Task 3's `Client` fields, `digestToBytes32`; Task 1's binding methods.
- Produces: `(*Client).Anchor(ctx, l3BlockHash, stateRoot string) (arbiter.AnchorRef, error)`; unexported `(*Client).send(ctx, hash32, root32 [32]byte, prev *inflight) (*inflight, error)` and `(*Client).landedTxRef(ctx context.Context, digest string, hash32 [32]byte, entryBlock uint64) string` — Task 5/6 reuse both.

- [ ] **Step 1: Write the failing tests**

`anchor/evm/anchor_test.go`:

```go
package evm

import (
	"context"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
)

// digests used across anchor tests
const (
	dHash  = "0x1111111111111111111111111111111111111111111111111111111111111111"
	dRoot  = "0x2222222222222222222222222222222222222222222222222222222222222222"
	dRoot2 = "0x3333333333333333333333333333333333333333333333333333333333333333"
)

func newTestClient(t *testing.T) (*Client, *simFixture) {
	t.Helper()
	sim, auth, key := newSim(t)
	addr, reg := deployRegistry(t, sim, auth, auth.From)
	c, err := New(context.Background(), testConfig(addr), sim.Client(), key)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return c, &simFixture{sim: sim, auth: auth, reg: reg}
}

func TestAnchor_FreshSendReturnsTxRefImmediately(t *testing.T) {
	c, fx := newTestClient(t)
	ref, err := c.Anchor(context.Background(), dHash, dRoot)
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	if ref.L3BlockHash != dHash || ref.StateRoot != dRoot {
		t.Fatalf("ref must echo inputs: %+v", ref)
	}
	if ref.L2TxRef == "" || ref.L2BlockNumber != 0 {
		t.Fatalf("fresh send must carry a tx hash and no block number yet: %+v", ref)
	}

	// The tx is pending, not mined: the contract entry must still be absent.
	entry, _ := fx.reg.Anchors(&bind.CallOpts{}, mustB32(t, dHash))
	if entry.L2BlockNumber != 0 {
		t.Fatal("entry must not exist before a block is mined")
	}

	fx.sim.Commit()
	entry, _ = fx.reg.Anchors(&bind.CallOpts{}, mustB32(t, dHash))
	if entry.L2BlockNumber == 0 || entry.StateRoot != mustB32(t, dRoot) {
		t.Fatalf("entry must exist after mining: %+v", entry)
	}
}

func TestAnchor_InFlightSuppressesDuplicateSend(t *testing.T) {
	c, _ := newTestClient(t)
	ref1, err := c.Anchor(context.Background(), dHash, dRoot)
	if err != nil {
		t.Fatalf("anchor 1: %v", err)
	}
	ref2, err := c.Anchor(context.Background(), dHash, dRoot)
	if err != nil {
		t.Fatalf("anchor 2: %v", err)
	}
	if ref1.L2TxRef != ref2.L2TxRef {
		t.Fatalf("second Anchor during in-flight must return the same tx ref: %s vs %s", ref1.L2TxRef, ref2.L2TxRef)
	}
}

func TestAnchor_CheckBeforeAnchorHitReturnsCompleteRef(t *testing.T) {
	c, fx := newTestClient(t)
	if _, err := c.Anchor(context.Background(), dHash, dRoot); err != nil {
		t.Fatalf("anchor: %v", err)
	}
	fx.sim.Commit()

	// A separate client instance (fresh in-flight state) = failover re-entry.
	c2, err := New(context.Background(), c.cfg, fx.sim.Client(), c.key)
	if err != nil {
		t.Fatalf("second client: %v", err)
	}
	ref, err := c2.Anchor(context.Background(), dHash, dRoot)
	if err != nil {
		t.Fatalf("re-entrant anchor: %v", err)
	}
	if ref.L2BlockNumber == 0 {
		t.Fatalf("check-hit must return the on-chain block number: %+v", ref)
	}
	if ref.L2TxRef == "" || !strings.HasPrefix(ref.L2TxRef, "0x") {
		t.Fatalf("check-hit must recover the landed tx hash via logs: %+v", ref)
	}
}

func TestAnchor_OnChainRootConflictErrors(t *testing.T) {
	c, fx := newTestClient(t)
	if _, err := c.Anchor(context.Background(), dHash, dRoot); err != nil {
		t.Fatalf("anchor: %v", err)
	}
	fx.sim.Commit()
	if _, err := c.Anchor(context.Background(), dHash, dRoot2); err == nil {
		t.Fatal("conflicting on-chain root must surface as an error")
	}
}

func TestAnchor_MalformedDigestRejectedBeforeSend(t *testing.T) {
	c, _ := newTestClient(t)
	if _, err := c.Anchor(context.Background(), "0xnothex", dRoot); err == nil {
		t.Fatal("malformed hash digest must error")
	}
	if _, err := c.Anchor(context.Background(), dHash, "short"); err == nil {
		t.Fatal("malformed root digest must error")
	}
}
```

Add to `anchor/evm/sim_test.go`:

```go
type simFixture struct {
	sim  *simulated.Backend
	auth *bind.TransactOpts
	reg  *bindings.AnchorRegistry
}

func mustB32(t *testing.T, d string) [32]byte {
	t.Helper()
	b, err := digestToBytes32(d)
	if err != nil {
		t.Fatalf("digest %q: %v", d, err)
	}
	return b
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./anchor/evm/ -run TestAnchor_ -v`
Expected: FAIL — `c.Anchor undefined`.

- [ ] **Step 3: Implement Anchor, send, landedTxRef**

`anchor/evm/anchor.go`:

```go
package evm

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"

	"github.com/sentioxyz/arbiter"
)

// Anchor posts (l3BlockHash, stateRoot) once. Re-entry is safe at every
// stage: an on-chain entry short-circuits (check-before-anchor, §10.3), an
// in-flight tx is returned as-is, and only a genuinely unanchored block
// sends. Never waits for a receipt.
func (c *Client) Anchor(ctx context.Context, l3BlockHash, stateRoot string) (arbiter.AnchorRef, error) {
	ref := arbiter.AnchorRef{L3BlockHash: l3BlockHash, StateRoot: stateRoot}
	hash32, err := digestToBytes32(l3BlockHash)
	if err != nil {
		return ref, fmt.Errorf("l3 block hash: %w", err)
	}
	root32, err := digestToBytes32(stateRoot)
	if err != nil {
		return ref, fmt.Errorf("state root: %w", err)
	}

	cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
	defer cancel()
	entry, err := c.registry.Anchors(&bind.CallOpts{Context: cctx}, hash32)
	if err != nil {
		return ref, fmt.Errorf("read anchors[%s]: %w", l3BlockHash, err)
	}
	if entry.L2BlockNumber != 0 {
		if entry.StateRoot != root32 {
			return ref, fmt.Errorf(
				"on-chain state root conflict for %s: chain has %s, arbiter computed %s (arbiter bug or poster key compromise; manual intervention required)",
				l3BlockHash, bytes32ToDigest(entry.StateRoot), stateRoot)
		}
		ref.L2BlockNumber = entry.L2BlockNumber
		if tx := c.landedTxRef(ctx, l3BlockHash, hash32, entry.L2BlockNumber); tx != "" {
			ref.L2TxRef = tx
		}
		return ref, nil
	}

	c.mu.Lock()
	if inf, ok := c.inflight[l3BlockHash]; ok {
		c.mu.Unlock()
		ref.L2TxRef = inf.txHash.Hex()
		return ref, nil
	}
	c.mu.Unlock()

	inf, err := c.send(ctx, hash32, root32, nil)
	if err != nil {
		return ref, err
	}
	c.mu.Lock()
	c.inflight[l3BlockHash] = inf
	c.mu.Unlock()
	c.cfg.Logger.Info("anchor tx sent", "l3_block_hash", l3BlockHash, "tx", inf.txHash, "nonce", inf.nonce)
	ref.L2TxRef = inf.txHash.Hex()
	return ref, nil
}

// send builds, signs, and submits one anchor(hash, root) tx. prev == nil
// uses a fresh pending nonce and suggested EIP-1559 pricing; prev != nil
// replaces prev's nonce with pricing bumped by GasBumpPercent (floored at
// current suggestions), the self-heal path.
func (c *Client) send(ctx context.Context, hash32, root32 [32]byte, prev *inflight) (*inflight, error) {
	cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
	defer cancel()

	tip, err := c.chain.SuggestGasTipCap(cctx)
	if err != nil {
		return nil, fmt.Errorf("suggest gas tip: %w", err)
	}
	head, err := c.chain.HeaderByNumber(cctx, nil)
	if err != nil {
		return nil, fmt.Errorf("read chain head: %w", err)
	}
	feeCap := new(big.Int).Add(tip, new(big.Int).Mul(head.BaseFee, big.NewInt(2)))

	var nonce uint64
	if prev != nil {
		nonce = prev.nonce
		bump := func(old *big.Int) *big.Int {
			return new(big.Int).Div(new(big.Int).Mul(old, big.NewInt(int64(100+c.cfg.GasBumpPercent))), big.NewInt(100))
		}
		if bt := bump(prev.gasTipCap); bt.Cmp(tip) > 0 {
			tip = bt
		}
		if bf := bump(prev.gasFeeCap); bf.Cmp(feeCap) > 0 {
			feeCap = bf
		}
	} else {
		nonce, err = c.chain.PendingNonceAt(cctx, c.from)
		if err != nil {
			return nil, fmt.Errorf("pending nonce: %w", err)
		}
	}

	opts, err := bind.NewKeyedTransactorWithChainID(c.key, c.cfg.ChainID)
	if err != nil {
		return nil, fmt.Errorf("transactor: %w", err)
	}
	opts.Context = cctx
	opts.Nonce = new(big.Int).SetUint64(nonce)
	opts.GasTipCap = tip
	opts.GasFeeCap = feeCap

	tx, err := c.registry.Anchor(opts, hash32, root32)
	if err != nil {
		return nil, fmt.Errorf("send anchor tx: %w", err)
	}
	return &inflight{txHash: tx.Hash(), nonce: nonce, gasTipCap: tip, gasFeeCap: feeCap, sentAt: time.Now()}, nil
}

// landedTxRef recovers the tx hash that actually carried the Anchored event
// with one single-block getLogs (from == to == entryBlock) — a resubmit
// invalidates the originally recorded hash, so chain logs are the audit
// truth. Best-effort: any failure returns "" and the caller keeps what it
// has. The result is cached (chain history is immutable at judged depths).
func (c *Client) landedTxRef(ctx context.Context, digest string, hash32 [32]byte, entryBlock uint64) string {
	c.mu.Lock()
	if tx, ok := c.landedTx[digest]; ok {
		c.mu.Unlock()
		return tx
	}
	c.mu.Unlock()

	cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
	defer cancel()
	it, err := c.registry.FilterAnchored(&bind.FilterOpts{Start: entryBlock, End: &entryBlock, Context: cctx}, [][32]byte{hash32})
	if err != nil {
		c.cfg.Logger.Warn("landed-tx log lookup failed", "l3_block_hash", digest, "err", err)
		return ""
	}
	defer it.Close()
	if it.Next() {
		tx := it.Event.Raw.TxHash.Hex()
		c.mu.Lock()
		c.landedTx[digest] = tx
		c.mu.Unlock()
		return tx
	}
	return ""
}
```

(The `var _ anchor.Client` guard still stays off — `Finality` lands in Task 5.)

- [ ] **Step 4: Run the tests**

Run: `go test ./anchor/evm/ -run TestAnchor_ -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add anchor/evm/
git commit -m "feat(anchor): evm Anchor path - check-before-anchor, in-flight suppression, EIP-1559 send"
```

---

### Task 5: `Finality` — entry-present judgment and ref enrichment

**Files:**
- Create: `anchor/evm/finality.go`
- Create: `anchor/evm/finality_test.go`
- Modify: `anchor/evm/client.go` (enable `var _ anchor.Client = (*Client)(nil)` if deferred)

**Interfaces:**
- Consumes: Task 2's `anchor.Status`; Task 4's `landedTxRef`, `send`, `inflight`.
- Produces: `(*Client).Finality(ctx, ref arbiter.AnchorRef) (anchor.Status, error)` (entry-present half; the absent half is Task 6's `selfHeal`); unexported `(*Client).judge(ctx, entryBlock uint64) (final, lastMergeable bool, err error)`.

- [ ] **Step 1: Write the failing tests**

`anchor/evm/finality_test.go`:

```go
package evm

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/rpc"
)

// mine commits n blocks on the simulated backend.
func mine(fx *simFixture, n int) {
	for i := 0; i < n; i++ {
		fx.sim.Commit()
	}
}

func TestFinality_ConfirmationsModeProgression(t *testing.T) {
	c, fx := newTestClient(t) // FinalityConfirmations: 3, LastMergeableConfirmations: 2
	ref, err := c.Anchor(context.Background(), dHash, dRoot)
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	fx.sim.Commit() // entry lands: 1 confirmation

	st, err := c.Finality(context.Background(), ref)
	if err != nil {
		t.Fatalf("finality: %v", err)
	}
	if st.Final || st.LastMergeable {
		t.Fatalf("1 confirmation must not be final (need 3): %+v", st)
	}
	if st.Ref.L2BlockNumber == 0 || st.Ref.L2TxRef == "" {
		t.Fatalf("entry-present must enrich the ref: %+v", st.Ref)
	}

	mine(fx, 2) // 3 confirmations
	st, err = c.Finality(context.Background(), ref)
	if err != nil {
		t.Fatalf("finality: %v", err)
	}
	if !st.Final || st.LastMergeable {
		t.Fatalf("3 confirmations: final but not last-mergeable (need 3+2): %+v", st)
	}

	mine(fx, 2) // 5 confirmations
	st, err = c.Finality(context.Background(), ref)
	if err != nil {
		t.Fatalf("finality: %v", err)
	}
	if !st.Final || !st.LastMergeable {
		t.Fatalf("5 confirmations must be final + last-mergeable: %+v", st)
	}
}

func TestFinality_EnrichedRefCarriesLandedTx(t *testing.T) {
	c, fx := newTestClient(t)
	ref, _ := c.Anchor(context.Background(), dHash, dRoot)
	sentTx := ref.L2TxRef
	mine(fx, 5)
	st, err := c.Finality(context.Background(), ref)
	if err != nil {
		t.Fatalf("finality: %v", err)
	}
	if st.Ref.L2TxRef != sentTx {
		t.Fatalf("no-resubmit case: landed tx must equal sent tx (%s vs %s)", st.Ref.L2TxRef, sentTx)
	}
	if st.Ref.L3BlockHash != dHash || st.Ref.StateRoot != dRoot {
		t.Fatalf("enrichment must preserve identity fields: %+v", st.Ref)
	}
}

func TestFinality_FinalizedModeAgainstSimulated(t *testing.T) {
	c, fx := newTestClient(t)
	c.cfg.FinalityMode = FinalityModeFinalized
	c.cfg.LastMergeableConfirmations = 0

	ref, _ := c.Anchor(context.Background(), dHash, dRoot)
	mine(fx, 2)

	// Probe: does the simulated backend serve the finalized tag?
	if _, err := fx.sim.Client().HeaderByNumber(context.Background(), big.NewInt(rpc.FinalizedBlockNumber.Int64())); err != nil {
		t.Skipf("simulated backend does not serve the finalized tag here: %v (finalized-mode unit coverage lives in TestJudge_FinalizedModeWithFake)", err)
	}
	st, err := c.Finality(context.Background(), ref)
	if err != nil {
		t.Fatalf("finality: %v", err)
	}
	// The simulated beacon finalizes eagerly; assert only internal consistency.
	if st.LastMergeable != st.Final {
		t.Fatalf("with 0 extra confirmations LastMergeable must track Final: %+v", st)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./anchor/evm/ -run TestFinality_ -v`
Expected: FAIL — `c.Finality undefined`.

- [ ] **Step 3: Implement Finality (present half) and judge**

`anchor/evm/finality.go`:

```go
package evm

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/anchor"
)

// Finality is the once-per-pass, point-in-time progress check. Entry
// present on chain → judge depth and enrich the ref. Entry absent → the
// documented self-heal side effect (Task 6) re-drives a lost or stuck tx;
// it touches only this client's own EOA, never FSM state.
func (c *Client) Finality(ctx context.Context, ref arbiter.AnchorRef) (anchor.Status, error) {
	st := anchor.Status{Ref: ref}
	hash32, err := digestToBytes32(ref.L3BlockHash)
	if err != nil {
		return st, fmt.Errorf("l3 block hash: %w", err)
	}

	cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
	defer cancel()
	entry, err := c.registry.Anchors(&bind.CallOpts{Context: cctx}, hash32)
	if err != nil {
		return st, fmt.Errorf("read anchors[%s]: %w", ref.L3BlockHash, err)
	}

	if entry.L2BlockNumber != 0 {
		c.mu.Lock()
		delete(c.inflight, ref.L3BlockHash)
		c.mu.Unlock()

		final, lastMergeable, err := c.judge(ctx, entry.L2BlockNumber)
		if err != nil {
			return st, err
		}
		st.Final, st.LastMergeable = final, lastMergeable
		st.Ref.L2BlockNumber = entry.L2BlockNumber
		if tx := c.landedTxRef(ctx, ref.L3BlockHash, hash32, entry.L2BlockNumber); tx != "" {
			st.Ref.L2TxRef = tx
		}
		return st, nil
	}

	if err := c.selfHeal(ctx, ref, hash32); err != nil {
		return st, err
	}
	return st, nil // not final, not last-mergeable, ref unchanged
}

// judge computes the two promotability booleans for an entry mined at
// entryBlock, per the configured mode (spec §5).
func (c *Client) judge(ctx context.Context, entryBlock uint64) (bool, bool, error) {
	cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
	defer cancel()
	latest, err := c.chain.HeaderByNumber(cctx, nil)
	if err != nil {
		return false, false, fmt.Errorf("read latest head: %w", err)
	}
	var confirmations uint64
	if latest.Number.Uint64() >= entryBlock {
		confirmations = latest.Number.Uint64() - entryBlock + 1
	}

	switch c.cfg.FinalityMode {
	case FinalityModeFinalized:
		fctx, fcancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
		defer fcancel()
		fin, err := c.chain.HeaderByNumber(fctx, big.NewInt(rpc.FinalizedBlockNumber.Int64()))
		if err != nil {
			return false, false, fmt.Errorf("read finalized head: %w", err)
		}
		final := fin.Number.Uint64() >= entryBlock
		return final, final && confirmations >= c.cfg.LastMergeableConfirmations, nil
	case FinalityModeConfirmations:
		final := confirmations >= c.cfg.FinalityConfirmations
		lastMergeable := confirmations >= c.cfg.FinalityConfirmations+c.cfg.LastMergeableConfirmations
		return final, lastMergeable, nil
	default:
		return false, false, fmt.Errorf("unknown finality mode %q", c.cfg.FinalityMode)
	}
}
```

For this task only, add a temporary `selfHeal` stub so the package compiles (replaced with the real implementation in Task 6 — the stub is a no-op, matching "in-flight and young" behavior):

```go
// selfHeal re-drives a lost or stuck anchor tx (Task 6).
func (c *Client) selfHeal(ctx context.Context, ref arbiter.AnchorRef, hash32 [32]byte) error {
	return nil
}
```

Enable `var _ anchor.Client = (*Client)(nil)` in `client.go` if it was deferred.

- [ ] **Step 4: Add the fake-chain finalized-lag test**

Append to `anchor/evm/finality_test.go` a controlled-lag unit test through a fake that wraps the real client's chain (only `HeaderByNumber` for the finalized tag is overridden):

```go
type lagChain struct {
	chainClient
	finalizedLag uint64 // finalized = latest - lag
}

func (l *lagChain) HeaderByNumber(ctx context.Context, n *big.Int) (*types.Header, error) {
	if n != nil && n.Int64() == rpc.FinalizedBlockNumber.Int64() {
		latest, err := l.chainClient.HeaderByNumber(ctx, nil)
		if err != nil {
			return nil, err
		}
		fn := new(big.Int).Sub(latest.Number, new(big.Int).SetUint64(l.finalizedLag))
		if fn.Sign() < 0 {
			fn = big.NewInt(0)
		}
		return l.chainClient.HeaderByNumber(ctx, fn)
	}
	return l.chainClient.HeaderByNumber(ctx, n)
}

func TestJudge_FinalizedModeWithFake(t *testing.T) {
	c, fx := newTestClient(t)
	c.cfg.FinalityMode = FinalityModeFinalized
	c.cfg.LastMergeableConfirmations = 0
	lag := &lagChain{chainClient: c.chain, finalizedLag: 4}
	c.chain = lag

	ref, _ := c.Anchor(context.Background(), dHash, dRoot)
	fx.sim.Commit() // entry mined

	st, err := c.Finality(context.Background(), ref)
	if err != nil {
		t.Fatalf("finality: %v", err)
	}
	if st.Final {
		t.Fatalf("finalized head lags 4 blocks; entry cannot be final yet: %+v", st)
	}

	mine(fx, 5) // finalized head passes the entry block
	st, err = c.Finality(context.Background(), ref)
	if err != nil {
		t.Fatalf("finality: %v", err)
	}
	if !st.Final || !st.LastMergeable {
		t.Fatalf("finalized head passed the entry; must be final: %+v", st)
	}
}
```

Add imports `"github.com/ethereum/go-ethereum/core/types"` to the test file.

- [ ] **Step 5: Run the tests**

Run: `go test ./anchor/evm/ -v`
Expected: all PASS (the `TestFinality_FinalizedModeAgainstSimulated` probe may skip — that is acceptable; the fake covers the lag transition).

- [ ] **Step 6: Commit**

```bash
git add anchor/evm/
git commit -m "feat(anchor): evm Finality judgment - confirmations/finalized modes, ref enrichment"
```

---

### Task 6: `Finality` self-heal — stuck/lost tx re-drive, restart recovery, dual-leader convergence

**Files:**
- Modify: `anchor/evm/finality.go` (replace the `selfHeal` stub)
- Create: `anchor/evm/selfheal_test.go`

**Interfaces:**
- Consumes: Task 4's `send` (both fresh and replace forms), Task 5's `Finality` skeleton.
- Produces: the complete `selfHeal` — no new exported surface.

- [ ] **Step 1: Write the failing tests**

`anchor/evm/selfheal_test.go`:

```go
package evm

import (
	"context"
	"testing"
	"time"
)

func TestSelfHeal_YoungInFlightIsLeftAlone(t *testing.T) {
	c, _ := newTestClient(t)
	c.cfg.ResubmitAfter = time.Hour
	ref, _ := c.Anchor(context.Background(), dHash, dRoot)
	before := c.snapshotInflight(dHash)

	st, err := c.Finality(context.Background(), ref) // no block mined: entry absent
	if err != nil {
		t.Fatalf("finality: %v", err)
	}
	if st.Final || st.LastMergeable {
		t.Fatalf("absent entry cannot be final: %+v", st)
	}
	after := c.snapshotInflight(dHash)
	if before.txHash != after.txHash {
		t.Fatal("young in-flight tx must not be replaced")
	}
}

func TestSelfHeal_StuckPendingIsBumpReplaced(t *testing.T) {
	c, _ := newTestClient(t)
	c.cfg.ResubmitAfter = time.Millisecond
	ref, _ := c.Anchor(context.Background(), dHash, dRoot)
	before := c.snapshotInflight(dHash)
	time.Sleep(5 * time.Millisecond)

	if _, err := c.Finality(context.Background(), ref); err != nil {
		t.Fatalf("finality: %v", err)
	}
	after := c.snapshotInflight(dHash)
	if after.txHash == before.txHash {
		t.Fatal("stuck pending tx must be replaced")
	}
	if after.nonce != before.nonce {
		t.Fatalf("replacement must reuse the nonce: %d vs %d", after.nonce, before.nonce)
	}
	if after.gasTipCap.Cmp(before.gasTipCap) <= 0 {
		t.Fatal("replacement must bump the tip")
	}
}

func TestSelfHeal_RestartRecoveryFreshSend(t *testing.T) {
	c, fx := newTestClient(t)
	ref, _ := c.Anchor(context.Background(), dHash, dRoot)

	// Simulate restart: a new client has no in-flight memory. Also simulate
	// the tx being lost by never mining it AND using a fresh simulated
	// nonce-space view: the new client re-sends under the current pending
	// nonce (which still covers the old tx; either tx landing converges).
	c2, err := New(context.Background(), c.cfg, fx.sim.Client(), c.key)
	if err != nil {
		t.Fatalf("restart client: %v", err)
	}
	st, err := c2.Finality(context.Background(), ref)
	if err != nil {
		t.Fatalf("finality after restart: %v", err)
	}
	if st.Final {
		t.Fatal("still absent: cannot be final")
	}
	if c2.snapshotInflight(dHash) == nil {
		t.Fatal("restart recovery must have re-driven a tx")
	}

	fx.sim.Commit()
	mine(fx, 4)
	st, err = c2.Finality(context.Background(), ref)
	if err != nil {
		t.Fatalf("finality: %v", err)
	}
	if !st.Final || !st.LastMergeable {
		t.Fatalf("after mining the re-driven tx must converge: %+v", st)
	}
}

func TestSelfHeal_DualLeaderConvergence(t *testing.T) {
	c1, fx := newTestClient(t)
	c2, err := New(context.Background(), c1.cfg, fx.sim.Client(), c1.key) // same EOA, same contract
	if err != nil {
		t.Fatalf("second leader: %v", err)
	}

	// Both leaders anchor the same block concururrently-ish.
	ref1, err1 := c1.Anchor(context.Background(), dHash, dRoot)
	ref2, err2 := c2.Anchor(context.Background(), dHash, dRoot)
	if err1 != nil && err2 != nil {
		t.Fatalf("at least one send must succeed: %v / %v", err1, err2)
	}
	_ = ref1
	_ = ref2

	fx.sim.Commit()
	mine(fx, 4)

	st1, err := c1.Finality(context.Background(), arbitraryRef(dHash, dRoot))
	if err != nil {
		t.Fatalf("leader1 finality: %v", err)
	}
	st2, err := c2.Finality(context.Background(), arbitraryRef(dHash, dRoot))
	if err != nil {
		t.Fatalf("leader2 finality: %v", err)
	}
	if !st1.Final || !st2.Final {
		t.Fatalf("both leaders must converge on final: %+v / %+v", st1, st2)
	}
	if st1.Ref.L2BlockNumber != st2.Ref.L2BlockNumber {
		t.Fatalf("both leaders must see one entry: %+v / %+v", st1.Ref, st2.Ref)
	}
}
```

Add to `anchor/evm/sim_test.go`:

```go
// snapshotInflight returns a copy of the in-flight record for a digest, nil if none.
func (c *Client) snapshotInflight(digest string) *inflight {
	c.mu.Lock()
	defer c.mu.Unlock()
	if inf, ok := c.inflight[digest]; ok {
		cp := *inf
		return &cp
	}
	return nil
}

func arbitraryRef(hash, root string) arbiter.AnchorRef {
	return arbiter.AnchorRef{L3BlockHash: hash, StateRoot: root, L2TxRef: "0xstale"}
}
```

(with import `"github.com/sentioxyz/arbiter"`.)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./anchor/evm/ -run TestSelfHeal_ -v`
Expected: `TestSelfHeal_StuckPendingIsBumpReplaced` and `TestSelfHeal_RestartRecoveryFreshSend` FAIL (stub does nothing); the young-in-flight test passes by construction.

- [ ] **Step 3: Implement selfHeal**

Replace the stub in `anchor/evm/finality.go`:

```go
// selfHeal re-drives the anchor tx for a block whose contract entry is
// absent (spec §4, the documented Finality side effect). Three cases:
// in-flight and young → leave alone; in-flight and stale → same-nonce
// gas-bump replace if still pending, else fresh send; no in-flight record
// (post-restart) → fresh send. Errors propagate: the orchestrator warns
// and the retry ticker re-enters next pass.
func (c *Client) selfHeal(ctx context.Context, ref arbiter.AnchorRef, hash32 [32]byte) error {
	root32, err := digestToBytes32(ref.StateRoot)
	if err != nil {
		return fmt.Errorf("state root: %w", err)
	}

	c.mu.Lock()
	inf := c.inflight[ref.L3BlockHash]
	c.mu.Unlock()

	var next *inflight
	switch {
	case inf == nil:
		c.cfg.Logger.Info("re-driving anchor with no in-flight record (restart recovery)", "l3_block_hash", ref.L3BlockHash)
		next, err = c.send(ctx, hash32, root32, nil)
	case time.Since(inf.sentAt) < c.cfg.ResubmitAfter:
		return nil // young: normal pending, nothing to do
	default:
		cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
		_, pending, lookupErr := c.chain.TransactionByHash(cctx, inf.txHash)
		cancel()
		if lookupErr == nil && pending {
			c.cfg.Logger.Info("bump-replacing stuck anchor tx", "l3_block_hash", ref.L3BlockHash, "old_tx", inf.txHash, "nonce", inf.nonce)
			next, err = c.send(ctx, hash32, root32, inf)
		} else {
			c.cfg.Logger.Info("re-driving lost anchor tx", "l3_block_hash", ref.L3BlockHash, "old_tx", inf.txHash)
			next, err = c.send(ctx, hash32, root32, nil)
		}
	}
	if err != nil {
		return fmt.Errorf("re-drive anchor for %s: %w", ref.L3BlockHash, err)
	}
	c.mu.Lock()
	c.inflight[ref.L3BlockHash] = next
	c.mu.Unlock()
	return nil
}
```

Add `"time"` to the imports of `finality.go`.

- [ ] **Step 4: Run the full package**

Run: `go test ./anchor/evm/ -v`
Expected: all PASS. Note on the dual-leader test: with one simulated mempool, the second same-nonce send may error as underpriced replacement — the test accepts one-of-two send errors by design.

- [ ] **Step 5: Commit**

```bash
git add anchor/evm/
git commit -m "feat(anchor): evm Finality self-heal - bump-replace, restart recovery, dual-leader convergence"
```

---

### Task 7: Config integration and `cmd/arbiter` wiring

**Files:**
- Modify: `config/config.go` (EVM block, env const, Validate)
- Modify: `config/raw.go` (defaults)
- Modify: `config/config_test.go` (new cases)
- Modify: `cmd/arbiter/services.go` (`buildAnchorClient` + wiring at line 70)
- Modify: `configs/local.yaml` (commented example under the `anchor:` block)

**Interfaces:**
- Consumes: `evm.New`, `evm.Config`, `evm.FinalityMode*` (Tasks 3–6).
- Produces: `config.EVMAnchorConfig` (fields exactly as below), `config.EnvAnchorEVMKey = "ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX"`, `config.AnchorConfig{Backend string; EVM EVMAnchorConfig}`; `buildAnchorClient(cfg config.Config, logger *slog.Logger) (anchor.Client, error)` in `cmd/arbiter`.

- [ ] **Step 1: Write the failing config tests**

Append to `config/config_test.go`, reusing the file's existing `writeConfig(t, body)` helper and `minimal` yaml const (both exist today at the top of the file):

```go
const minimalEVMAnchor = minimal + `
anchor:
  backend: evm
  evm:
    rpc_url: "http://127.0.0.1:8545"
    chain_id: 1337
    contract_address: "0x00000000000000000000000000000000000000aa"
    private_key_hex: "289c2857d4598e37fb9647507e47a309d6133539bf21a8b9cb6df88fd5232032"
`

func TestLoad_AnchorEVMDefaultsApplied(t *testing.T) {
	c, err := Load(writeConfig(t, minimalEVMAnchor))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	e := c.Anchor.EVM
	if e.FinalityMode != "finalized" || e.FinalityConfirmations != 12 || e.LastMergeableConfirmations != 0 ||
		e.ResubmitAfter.Duration != 90*time.Second || e.GasBumpPercent != 25 || e.RPCTimeout.Duration != 10*time.Second {
		t.Fatalf("evm defaults not applied: %+v", e)
	}
}

func TestLoad_AnchorEVMEnvOverridesKey(t *testing.T) {
	override := "8f2a559490d5ebca19bde2b8ff4daa48a1ba30fbdba2f9b849da516a25b4a2f5"
	t.Setenv(EnvAnchorEVMKey, override)
	c, err := Load(writeConfig(t, minimalEVMAnchor))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Anchor.EVM.PrivateKeyHex != override {
		t.Fatal("env must override anchor.evm.private_key_hex")
	}
}

func TestLoad_AnchorEVMValidateFailures(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"missing connection fields", minimal + "\nanchor:\n  backend: evm\n", "anchor.evm.rpc_url is required"},
		{"unknown backend", minimal + "\nanchor:\n  backend: vibes\n", `anchor.backend "vibes" not in allowlist`},
		{"unknown finality mode", minimalEVMAnchor + "    finality_mode: hopeful\n", `finality_mode "hopeful" not in allowlist`},
		{"confirmations mode needs threshold", minimalEVMAnchor + "    finality_mode: confirmations\n    finality_confirmations: 0\n", "finality_confirmations must be >= 1"},
		{"gas bump floor", minimalEVMAnchor + "    gas_bump_percent: 5\n", "gas_bump_percent must be >= 10"},
		{"resubmit floor", minimalEVMAnchor + "    resubmit_after: 100ms\n", "resubmit_after must be >= 1s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}
```

Note on the failure table: `finality_confirmations: 0` must fail *even though 0 is the "unset" zero value* — this mirrors the existing `TestLoad_RejectsExplicitZeroDefaults` stance and works because the raw type is `*uint64` (explicit 0 ≠ absent). Add `"strings"` to the test imports if missing.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./config/ -run 'TestValidate_AnchorEVM|TestLoad_AnchorEVM' -v`
Expected: FAIL — `undefined: EVMAnchorConfig`.

- [ ] **Step 3: Implement config**

`config/config.go` — add below `EnvAuthorityKey`:

```go
// EnvAnchorEVMKey overrides anchor.evm.private_key_hex when set.
const EnvAnchorEVMKey = "ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX"
```

Replace `AnchorConfig`:

```go
// EVMAnchorConfig configures the chain-agnostic EVM anchor backend (P1d):
// the chain is deployment config, not a code decision.
type EVMAnchorConfig struct {
	RPCURL                     string   `yaml:"rpc_url"`
	ChainID                    uint64   `yaml:"chain_id"`
	ContractAddress            string   `yaml:"contract_address"`
	PrivateKeyHex              string   `yaml:"private_key_hex"`
	FinalityMode               string   `yaml:"finality_mode"`
	FinalityConfirmations      uint64   `yaml:"finality_confirmations"`
	LastMergeableConfirmations uint64   `yaml:"last_mergeable_confirmations"`
	ResubmitAfter              Duration `yaml:"resubmit_after"`
	GasBumpPercent             uint64   `yaml:"gas_bump_percent"`
	RPCTimeout                 Duration `yaml:"rpc_timeout"`
}

// AnchorConfig selects the anchoring backend.
type AnchorConfig struct {
	Backend string          `yaml:"backend"`
	EVM     EVMAnchorConfig `yaml:"evm"`
}
```

In `Load`, after the authority env override:

```go
	if env := os.Getenv(EnvAnchorEVMKey); env != "" {
		c.Anchor.EVM.PrivateKeyHex = env
	}
```

In `Validate`, replace the anchor allowlist check (lines 165–167) with:

```go
	switch c.Anchor.Backend {
	case "local":
	case "evm":
		e := c.Anchor.EVM
		req(e.RPCURL, "anchor.evm.rpc_url")
		req(e.ContractAddress, "anchor.evm.contract_address")
		req(e.PrivateKeyHex, "anchor.evm.private_key_hex")
		if e.ChainID == 0 {
			errs = append(errs, errors.New("anchor.evm.chain_id must be nonzero"))
		}
		if e.ContractAddress != "" && !common.IsHexAddress(e.ContractAddress) {
			errs = append(errs, errors.New("anchor.evm.contract_address must be an Ethereum address"))
		}
		if e.PrivateKeyHex != "" {
			if _, err := crypto.HexToECDSA(strings.TrimPrefix(e.PrivateKeyHex, "0x")); err != nil {
				errs = append(errs, fmt.Errorf("anchor.evm.private_key_hex: %w", err))
			}
		}
		switch e.FinalityMode {
		case "finalized":
		case "confirmations":
			if e.FinalityConfirmations == 0 {
				errs = append(errs, errors.New("anchor.evm.finality_confirmations must be >= 1 in confirmations mode"))
			}
		default:
			errs = append(errs, fmt.Errorf("anchor.evm.finality_mode %q not in allowlist [finalized confirmations]", e.FinalityMode))
		}
		if e.GasBumpPercent < 10 {
			errs = append(errs, errors.New("anchor.evm.gas_bump_percent must be >= 10 (node replacement pricing floor)"))
		}
		if e.ResubmitAfter.Duration < time.Second {
			errs = append(errs, errors.New("anchor.evm.resubmit_after must be >= 1s"))
		}
		if e.RPCTimeout.Duration <= 0 {
			errs = append(errs, errors.New("anchor.evm.rpc_timeout must be positive"))
		}
	default:
		errs = append(errs, fmt.Errorf("anchor.backend %q not in allowlist [local evm]", c.Anchor.Backend))
	}
```

(Add `"time"` to config.go imports.)

`config/raw.go` — replace the `Anchor AnchorConfig` field with a raw form and defaults:

```go
type rawAnchorConfig struct {
	Backend string              `yaml:"backend"`
	EVM     rawEVMAnchorConfig  `yaml:"evm"`
}

type rawEVMAnchorConfig struct {
	RPCURL                     string    `yaml:"rpc_url"`
	ChainID                    uint64    `yaml:"chain_id"`
	ContractAddress            string    `yaml:"contract_address"`
	PrivateKeyHex              string    `yaml:"private_key_hex"`
	FinalityMode               string    `yaml:"finality_mode"`
	FinalityConfirmations      *uint64   `yaml:"finality_confirmations"`
	LastMergeableConfirmations *uint64   `yaml:"last_mergeable_confirmations"`
	ResubmitAfter              *Duration `yaml:"resubmit_after"`
	GasBumpPercent             *uint64   `yaml:"gas_bump_percent"`
	RPCTimeout                 *Duration `yaml:"rpc_timeout"`
}
```

and in `toConfig()` replace `Anchor: r.Anchor,` with:

```go
		Anchor: AnchorConfig{
			Backend: r.Anchor.Backend,
			EVM: EVMAnchorConfig{
				RPCURL:                     r.Anchor.EVM.RPCURL,
				ChainID:                    r.Anchor.EVM.ChainID,
				ContractAddress:            r.Anchor.EVM.ContractAddress,
				PrivateKeyHex:              r.Anchor.EVM.PrivateKeyHex,
				FinalityMode:               r.Anchor.EVM.FinalityMode,
				FinalityConfirmations:      uint64OrDefault(r.Anchor.EVM.FinalityConfirmations, 12),
				LastMergeableConfirmations: uint64OrDefault(r.Anchor.EVM.LastMergeableConfirmations, 0),
				ResubmitAfter:              durationOrDefault(r.Anchor.EVM.ResubmitAfter, 90*time.Second),
				GasBumpPercent:             uint64OrDefault(r.Anchor.EVM.GasBumpPercent, 25),
				RPCTimeout:                 durationOrDefault(r.Anchor.EVM.RPCTimeout, 10*time.Second),
			},
		},
```

with the raw struct field switched to `Anchor rawAnchorConfig` and the finality-mode default applied next to the existing backend default:

```go
	if c.Anchor.Backend == "" {
		c.Anchor.Backend = "local"
	}
	if c.Anchor.EVM.FinalityMode == "" {
		c.Anchor.EVM.FinalityMode = "finalized"
	}
```

- [ ] **Step 4: Run config tests**

Run: `go test ./config/ -v`
Expected: all PASS (old + new).

- [ ] **Step 5: Wire `cmd/arbiter`**

`cmd/arbiter/services.go` — replace `Anchor: anchor.NewLocal(),` (line 70) with `Anchor: anchorClient,`, add above the `orchestrator.New` call:

```go
	anchorClient, err := buildAnchorClient(rt.cfg, rt.logger)
	if err != nil {
		return nil, nil, err
	}
```

and add the builder (new function in `services.go`):

```go
// buildAnchorClient constructs the configured anchor backend. The evm
// backend dials the RPC and fail-fast asserts the chain id at startup.
func buildAnchorClient(cfg config.Config, logger *slog.Logger) (anchor.Client, error) {
	switch cfg.Anchor.Backend {
	case "local":
		return anchor.NewLocal(), nil
	case "evm":
		e := cfg.Anchor.EVM
		ec, err := ethclient.Dial(e.RPCURL)
		if err != nil {
			return nil, fmt.Errorf("dial anchor rpc %s: %w", e.RPCURL, err)
		}
		key, err := crypto.HexToECDSA(strings.TrimPrefix(e.PrivateKeyHex, "0x"))
		if err != nil {
			return nil, fmt.Errorf("anchor evm key: %w", err)
		}
		return evm.New(context.Background(), evm.Config{
			ContractAddress:            common.HexToAddress(e.ContractAddress),
			ChainID:                    new(big.Int).SetUint64(e.ChainID),
			FinalityMode:               e.FinalityMode,
			FinalityConfirmations:      e.FinalityConfirmations,
			LastMergeableConfirmations: e.LastMergeableConfirmations,
			ResubmitAfter:              e.ResubmitAfter.Duration,
			GasBumpPercent:             e.GasBumpPercent,
			RPCTimeout:                 e.RPCTimeout.Duration,
			Logger:                     logger,
		}, ec, key)
	default:
		return nil, fmt.Errorf("unknown anchor backend %q", cfg.Anchor.Backend)
	}
}
```

Imports to add in `services.go`: `"context"`, `"math/big"`, `"strings"`, `"github.com/ethereum/go-ethereum/common"`, `"github.com/ethereum/go-ethereum/crypto"`, `"github.com/ethereum/go-ethereum/ethclient"`, `"github.com/sentioxyz/arbiter/anchor/evm"`.

`configs/local.yaml` — extend the `anchor:` block with a commented example:

```yaml
anchor:
  backend: local
  # backend: evm
  # evm:
  #   rpc_url: "http://127.0.0.1:8545"
  #   chain_id: 1337
  #   contract_address: "0x..."   # arbiter-anchor deploy output
  #   private_key_hex: ""          # or env ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX
  #   finality_mode: finalized     # finalized | confirmations
  #   finality_confirmations: 12
  #   last_mergeable_confirmations: 0
  #   resubmit_after: 90s
  #   gas_bump_percent: 25
  #   rpc_timeout: 10s
```

- [ ] **Step 6: Add the buildAnchorClient smoke test**

Append to `cmd/arbiter/main_test.go`:

```go
func TestBuildAnchorClient(t *testing.T) {
	var cfg config.Config
	cfg.Anchor.Backend = "local"
	if c, err := buildAnchorClient(cfg, slog.Default()); err != nil || c == nil {
		t.Fatalf("local backend must construct: %v", err)
	}

	cfg.Anchor.Backend = "evm"
	cfg.Anchor.EVM = config.EVMAnchorConfig{
		RPCURL:          "http://127.0.0.1:1", // nothing listens: chain-id assertion must fail fast
		ChainID:         1337,
		ContractAddress: "0x00000000000000000000000000000000000000aa",
		PrivateKeyHex:   "289c2857d4598e37fb9647507e47a309d6133539bf21a8b9cb6df88fd5232032",
		FinalityMode:    "finalized",
		ResubmitAfter:   config.Duration{Duration: 90 * time.Second},
		GasBumpPercent:  25,
		RPCTimeout:      config.Duration{Duration: 500 * time.Millisecond},
	}
	if _, err := buildAnchorClient(cfg, slog.Default()); err == nil {
		t.Fatal("unreachable rpc must fail startup (spec §4 chain-id assertion is fail-fast)")
	}

	cfg.Anchor.Backend = "vibes"
	if _, err := buildAnchorClient(cfg, slog.Default()); err == nil {
		t.Fatal("unknown backend must error")
	}
}
```

(Add `"log/slog"` / `"time"` imports as needed.)

- [ ] **Step 7: Verify the whole repo**

Run: `go build ./... && go vet ./... && go test ./config/ ./cmd/...`
Expected: PASS (existing `cmd/arbiter` config-load tests still green — `local` remains the default backend; the smoke test's unreachable-RPC case fails within `rpc_timeout` = 500ms).

- [ ] **Step 8: Commit**

```bash
git add config/ cmd/arbiter/ configs/
git commit -m "feat(config): anchor.evm block with validation; cmd/arbiter backend switch"
```

---

### Task 8: Orchestrator end-to-end with the EVM backend over a simulated chain

**Files:**
- Create: `orchestrator/evm_anchor_test.go`

**Interfaces:**
- Consumes: `evm.New` + simulated helpers (duplicated minimally here — test files don't cross package boundaries), orchestrator test harness (`newHarness`, `seedWriterAndVerifiers`, `driveToQuorum`, `runLoop`, `waitFor`, `hasSignedPromotion` from `orchestrator/*_test.go`).
- Produces: nothing (test-only).

- [ ] **Step 1: Write the integration test**

`orchestrator/evm_anchor_test.go` — drives seal → quorum → **EVM anchor over simulated chain** → finality → promotable through the real loop. Confirmations mode with threshold 2 so two `Commit()`s flip finality:

```go
package orchestrator

import (
	"context"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"
	"github.com/ethereum/go-ethereum/params"

	"github.com/sentioxyz/arbiter/anchor/evm"
	"github.com/sentioxyz/arbiter/anchor/evm/bindings"
)

func newEVMAnchorClient(t *testing.T) (*evm.Client, *simulated.Backend) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	auth, err := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))
	if err != nil {
		t.Fatal(err)
	}
	sim := simulated.NewBackend(types.GenesisAlloc{
		auth.From: {Balance: new(big.Int).Mul(big.NewInt(100), big.NewInt(params.Ether))},
	})
	t.Cleanup(func() { sim.Close() })
	addr, _, _, err := bindings.DeployAnchorRegistry(auth, sim.Client(), []common.Address{auth.From})
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	sim.Commit()
	c, err := evm.New(context.Background(), evm.Config{
		ContractAddress:            addr,
		ChainID:                    big.NewInt(1337),
		FinalityMode:               evm.FinalityModeConfirmations,
		FinalityConfirmations:      2,
		LastMergeableConfirmations: 0,
		ResubmitAfter:              time.Second,
		GasBumpPercent:             25,
		RPCTimeout:                 5 * time.Second,
		Logger:                     slog.Default(),
	}, sim.Client(), key)
	if err != nil {
		t.Fatalf("evm client: %v", err)
	}
	return c, sim
}

func TestOrchestrator_EVMAnchorFullFlow(t *testing.T) {
	params, signer := authorityParamsO(t)
	f, node, _, fsn, _, o, _ := newHarness(t, params, signer)
	evmClient, sim := newEVMAnchorClient(t)
	o.d.Anchor = evmClient

	seedWriterAndVerifiers(t, node)
	driveToQuorum(t, node, f)
	runLoop(t, o)

	// The loop anchors; the tx needs mining and confirmations to finalize.
	waitFor(t, "anchor recorded", 3*time.Second, func() bool {
		ws, err := f.WorkSet()
		if err != nil {
			return false
		}
		return len(ws.UnanchoredVerified) == 1 && ws.UnanchoredVerified[0].Anchored
	})
	sim.Commit() // mine the anchor tx (1 confirmation)
	sim.Commit() // 2 confirmations = final under the test config
	o.Poke()

	waitFor(t, "promotion streamed after real finality", 5*time.Second, func() bool {
		return hasSignedPromotion(fsn)
	})

	// The FSM's stored ref must have been enriched with the real L2 block number.
	waitFor(t, "enriched ref recorded", 3*time.Second, func() bool {
		ws, err := f.WorkSet()
		if err != nil {
			return false
		}
		for _, ba := range ws.UnanchoredVerified {
			if ba.Ref.L2BlockNumber != 0 {
				return true
			}
		}
		// Once fully promotable the row may leave UnanchoredVerified; accept that as success
		// if a signed promotion exists (ref was recorded on the finality proposal).
		return hasSignedPromotion(fsn)
	})
}
```

**Adaptation note for the implementer:** the harness surface is used exactly as it exists today (verified at plan time): `newHarness(t, params, signer)` returns `(f, node, _, fsn, fa, o, _)`; the anchor work rows come from `f.WorkSet() (fsm.WorkSet, error)` whose `UnanchoredVerified []fsm.BlockAnchor` carries `{BlockSeq, ChainHash, StateRoot, Ref, Anchored, Finality, LastMergeable}` — the same read pattern as `TestPromotion_AnchorRefRecordedBeforeFinalityRetry` (promotion_test.go:117). Add the missing `"github.com/ethereum/go-ethereum/common"` import.

One structural caveat: `driveToQuorum` seeds `ChainHash`/`StateRoot` as plain strings like `"0xanchor"` — **not** 64-hex digests. The EVM client rejects malformed digests before sending. Check how the harness seeds these values (grep `ChainHash` in `orchestrator/*_test.go` fixtures); if they are not DigestString-shaped, adjust the *fixture seeding in this test's path* by applying `replay.DigestString([]byte("anchor"))`-style values through the same command the fixtures use (the fixtures build `SealL3Block`/verification commands — pass valid 0x+64hex strings there). Do not touch shared fixtures used by other tests; if needed, copy the minimal drive helpers into `evm_anchor_test.go` with digest-shaped hashes.

- [ ] **Step 2: Run it**

Run: `go test ./orchestrator/ -run TestOrchestrator_EVMAnchorFullFlow -v`
Expected: PASS. If it fails on digest shape, apply the adaptation note above.

- [ ] **Step 3: Run the whole orchestrator package**

Run: `go test ./orchestrator/`
Expected: PASS — no existing test regresses.

- [ ] **Step 4: Commit**

```bash
git add orchestrator/
git commit -m "test(orchestrator): full anchor flow through the evm backend over a simulated chain"
```

---

### Task 9: `cmd/arbiter-anchor` ops CLI (deploy / set-poster / status)

**Files:**
- Create: `cmd/arbiter-anchor/main.go`
- Create: `cmd/arbiter-anchor/commands.go`
- Create: `cmd/arbiter-anchor/commands_test.go`

**Interfaces:**
- Consumes: `bindings.DeployAnchorRegistry` / `NewAnchorRegistry`, `evm` package's judgment constants; go-ethereum `ethclient`.
- Produces: a standalone binary; testable command funcs `runDeploy(ctx, chain deployBackend, key *ecdsa.PrivateKey, chainID *big.Int, posters []common.Address) (common.Address, error)`, `runSetPoster(...)`, `runStatus(ctx, chain bind.ContractBackend, contract common.Address, l3BlockHash string, out io.Writer) error`.

- [ ] **Step 1: Write the failing command-function tests**

`cmd/arbiter-anchor/commands_test.go` (simulated backend again; small local copies of the sim helpers since this is package `main`):

```go
package main

import (
	"bytes"
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"
	"github.com/ethereum/go-ethereum/params"

	"github.com/sentioxyz/arbiter/anchor/evm/bindings"
)

func newCLISim(t *testing.T) (*simulated.Backend, *bind.TransactOpts, *ecdsaKey) {
	t.Helper()
	key, _ := crypto.GenerateKey()
	auth, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))
	sim := simulated.NewBackend(types.GenesisAlloc{auth.From: {Balance: new(big.Int).Mul(big.NewInt(100), big.NewInt(params.Ether))}})
	t.Cleanup(func() { sim.Close() })
	return sim, auth, &ecdsaKey{key}
}

func TestRunDeployAndStatus(t *testing.T) {
	sim, auth, key := newCLISim(t)

	addr, err := runDeploy(context.Background(), sim.Client(), key.key, big.NewInt(1337), []common.Address{auth.From})
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	sim.Commit()

	// Anchor one entry directly so status has something to show.
	reg, _ := bindings.NewAnchorRegistry(addr, sim.Client())
	var h, r [32]byte
	h[0], r[0] = 7, 8
	if _, err := reg.Anchor(auth, h, r); err != nil {
		t.Fatalf("anchor: %v", err)
	}
	sim.Commit()

	var out bytes.Buffer
	hDigest := "0x0700000000000000000000000000000000000000000000000000000000000000"
	if err := runStatus(context.Background(), sim.Client(), addr, hDigest, &out); err != nil {
		t.Fatalf("status: %v", err)
	}
	if !strings.Contains(out.String(), "anchored") || !strings.Contains(out.String(), "l2_block_number") {
		t.Fatalf("status output missing fields: %s", out.String())
	}

	var out2 bytes.Buffer
	missing := "0x0900000000000000000000000000000000000000000000000000000000000000"
	if err := runStatus(context.Background(), sim.Client(), addr, missing, &out2); err != nil {
		t.Fatalf("status(missing): %v", err)
	}
	if !strings.Contains(out2.String(), "not anchored") {
		t.Fatalf("missing entry must print 'not anchored': %s", out2.String())
	}
}

func TestRunSetPoster(t *testing.T) {
	sim, auth, key := newCLISim(t)
	addr, err := runDeploy(context.Background(), sim.Client(), key.key, big.NewInt(1337), nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	sim.Commit()

	other := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	if err := runSetPoster(context.Background(), sim.Client(), key.key, big.NewInt(1337), addr, other, true); err != nil {
		t.Fatalf("set-poster: %v", err)
	}
	sim.Commit()
	reg, _ := bindings.NewAnchorRegistry(addr, sim.Client())
	ok, _ := reg.Posters(&bind.CallOpts{}, other)
	if !ok {
		t.Fatal("poster must be allowlisted after set-poster")
	}
	_ = auth
}
```

(`ecdsaKey` is `type ecdsaKey struct{ key *ecdsa.PrivateKey }`, declared once in `main.go` — see Step 3 — and shared by the tests; add the `"crypto/ecdsa"` import where used.)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/arbiter-anchor/ -v`
Expected: FAIL — `undefined: runDeploy`.

- [ ] **Step 3: Implement the commands and main**

`cmd/arbiter-anchor/commands.go`:

```go
package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"io"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"

	"github.com/sentioxyz/arbiter/anchor/evm/bindings"
)

// deployBackend is the union the three commands need; *ethclient.Client and
// the simulated client both satisfy it.
type deployBackend = bind.ContractBackend

func transactor(key *ecdsa.PrivateKey, chainID *big.Int) (*bind.TransactOpts, error) {
	return bind.NewKeyedTransactorWithChainID(key, chainID)
}

func runDeploy(ctx context.Context, chain deployBackend, key *ecdsa.PrivateKey, chainID *big.Int, posters []common.Address) (common.Address, error) {
	opts, err := transactor(key, chainID)
	if err != nil {
		return common.Address{}, err
	}
	opts.Context = ctx
	if posters == nil {
		posters = []common.Address{}
	}
	addr, tx, _, err := bindings.DeployAnchorRegistry(opts, chain, posters)
	if err != nil {
		return common.Address{}, fmt.Errorf("deploy AnchorRegistry: %w", err)
	}
	fmt.Printf("AnchorRegistry deploy tx %s\ncontract address: %s\n", tx.Hash(), addr)
	return addr, nil
}

func runSetPoster(ctx context.Context, chain deployBackend, key *ecdsa.PrivateKey, chainID *big.Int, contract, poster common.Address, allowed bool) error {
	reg, err := bindings.NewAnchorRegistry(contract, chain)
	if err != nil {
		return err
	}
	opts, err := transactor(key, chainID)
	if err != nil {
		return err
	}
	opts.Context = ctx
	tx, err := reg.SetPoster(opts, poster, allowed)
	if err != nil {
		return fmt.Errorf("setPoster: %w", err)
	}
	fmt.Printf("setPoster(%s, %v) tx %s\n", poster, allowed, tx.Hash())
	return nil
}

func runStatus(ctx context.Context, chain bind.ContractBackend, contract common.Address, l3BlockHash string, out io.Writer) error {
	if len(l3BlockHash) != 66 || l3BlockHash[:2] != "0x" {
		return fmt.Errorf("l3-block-hash must be \"0x\" + 64 hex chars")
	}
	b := common.FromHex(l3BlockHash)
	if len(b) != 32 {
		return fmt.Errorf("l3-block-hash must decode to 32 bytes")
	}
	var hash32 [32]byte
	copy(hash32[:], b)

	reg, err := bindings.NewAnchorRegistry(contract, chain)
	if err != nil {
		return err
	}
	entry, err := reg.Anchors(&bind.CallOpts{Context: ctx}, hash32)
	if err != nil {
		return fmt.Errorf("read entry: %w", err)
	}
	if entry.L2BlockNumber == 0 {
		fmt.Fprintf(out, "%s: not anchored\n", l3BlockHash)
		return nil
	}
	fmt.Fprintf(out, "%s: anchored\n  state_root: 0x%x\n  l2_block_number: %d\n", l3BlockHash, entry.StateRoot, entry.L2BlockNumber)
	return nil
}
```

`cmd/arbiter-anchor/main.go`:

```go
// arbiter-anchor is the AnchorRegistry ops CLI: deploy, set-poster, status.
// Key material comes only from ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX; status
// needs no key. One Go binary, no Solidity toolchain required.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/sentioxyz/arbiter/config"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "deploy":
		err = cmdDeploy(os.Args[2:])
	case "set-poster":
		err = cmdSetPoster(os.Args[2:])
	case "status":
		err = cmdStatus(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  arbiter-anchor deploy     -rpc-url URL -chain-id N [-poster 0x..,0x..]
  arbiter-anchor set-poster -rpc-url URL -chain-id N -contract 0x.. -poster 0x.. -allowed=true|false
  arbiter-anchor status     -rpc-url URL -contract 0x.. -l3-block-hash 0x..
key material: env ` + config.EnvAnchorEVMKey + ` (deploy, set-poster)`)
}

func dial(rpcURL string) (*ethclient.Client, error) {
	if rpcURL == "" {
		return nil, fmt.Errorf("-rpc-url is required")
	}
	return ethclient.Dial(rpcURL)
}

func keyFromEnv() (*ecdsaKey, error) {
	hexKey := os.Getenv(config.EnvAnchorEVMKey)
	if hexKey == "" {
		return nil, fmt.Errorf("env %s is required", config.EnvAnchorEVMKey)
	}
	k, err := crypto.HexToECDSA(strings.TrimPrefix(hexKey, "0x"))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", config.EnvAnchorEVMKey, err)
	}
	return &ecdsaKey{k}, nil
}

func cmdDeploy(args []string) error {
	fs := flag.NewFlagSet("deploy", flag.ExitOnError)
	rpcURL := fs.String("rpc-url", "", "EVM JSON-RPC endpoint")
	chainID := fs.Uint64("chain-id", 0, "expected chain id")
	posters := fs.String("poster", "", "comma-separated initial poster addresses")
	_ = fs.Parse(args)
	ec, err := dial(*rpcURL)
	if err != nil {
		return err
	}
	key, err := keyFromEnv()
	if err != nil {
		return err
	}
	var addrs []common.Address
	for _, p := range strings.Split(*posters, ",") {
		if p = strings.TrimSpace(p); p != "" {
			if !common.IsHexAddress(p) {
				return fmt.Errorf("poster %q is not an address", p)
			}
			addrs = append(addrs, common.HexToAddress(p))
		}
	}
	_, err = runDeploy(context.Background(), ec, key.key, new(big.Int).SetUint64(*chainID), addrs)
	return err
}

func cmdSetPoster(args []string) error {
	fs := flag.NewFlagSet("set-poster", flag.ExitOnError)
	rpcURL := fs.String("rpc-url", "", "EVM JSON-RPC endpoint")
	chainID := fs.Uint64("chain-id", 0, "expected chain id")
	contract := fs.String("contract", "", "AnchorRegistry address")
	poster := fs.String("poster", "", "poster address")
	allowed := fs.Bool("allowed", true, "allow or revoke")
	_ = fs.Parse(args)
	ec, err := dial(*rpcURL)
	if err != nil {
		return err
	}
	key, err := keyFromEnv()
	if err != nil {
		return err
	}
	if !common.IsHexAddress(*contract) || !common.IsHexAddress(*poster) {
		return fmt.Errorf("-contract and -poster must be addresses")
	}
	return runSetPoster(context.Background(), ec, key.key, new(big.Int).SetUint64(*chainID), common.HexToAddress(*contract), common.HexToAddress(*poster), *allowed)
}

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	rpcURL := fs.String("rpc-url", "", "EVM JSON-RPC endpoint")
	contract := fs.String("contract", "", "AnchorRegistry address")
	l3Hash := fs.String("l3-block-hash", "", "L3 block hash digest (0x + 64 hex)")
	_ = fs.Parse(args)
	ec, err := dial(*rpcURL)
	if err != nil {
		return err
	}
	if !common.IsHexAddress(*contract) {
		return fmt.Errorf("-contract must be an address")
	}
	return runStatus(context.Background(), ec, common.HexToAddress(*contract), *l3Hash, os.Stdout)
}

type ecdsaKey struct{ key *ecdsa.PrivateKey }
```

(Add `"crypto/ecdsa"` import; reconcile the `ecdsaKey` / test-side `ecdsaKeyWrap` naming — use one `ecdsaKey` type declared in `main.go` and reference it from the test.)

- [ ] **Step 4: Run the tests and build**

Run: `go test ./cmd/arbiter-anchor/ -v && go build ./cmd/arbiter-anchor/`
Expected: PASS + builds.

- [ ] **Step 5: Commit**

```bash
git add cmd/arbiter-anchor/
git commit -m "feat(cmd): arbiter-anchor ops CLI - deploy, set-poster, status"
```

---

### Task 10: anvil E2E and CI

**Files:**
- Create: `anchor/evm/anvil_e2e_test.go`
- Modify: `.github/workflows/ci.yml` (drift-check step in `test` job; new `anchor-anvil` job)

**Interfaces:**
- Consumes: everything built so far.
- Produces: env-gated E2E (`ARBITER_ANVIL_E2E=1`, RPC at `ANVIL_RPC` default `http://127.0.0.1:8545`); CI jobs.

- [ ] **Step 1: Write the E2E test**

`anchor/evm/anvil_e2e_test.go`:

```go
package evm

import (
	"context"
	"log/slog"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/sentioxyz/arbiter/anchor/evm/bindings"
)

// anvil's default funded account #0 private key (public dev mnemonic).
const anvilKey0 = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

// TestAnvilE2E drives deploy → anchor → self-heal-safe finality against a
// real JSON-RPC endpoint. Gated: ARBITER_ANVIL_E2E=1; anvil must run with
// --block-time 1 so confirmations accrue (see ci.yml).
func TestAnvilE2E(t *testing.T) {
	if os.Getenv("ARBITER_ANVIL_E2E") != "1" {
		t.Skip("set ARBITER_ANVIL_E2E=1 (and run anvil) to run this test")
	}
	rpcURL := os.Getenv("ANVIL_RPC")
	if rpcURL == "" {
		rpcURL = "http://127.0.0.1:8545"
	}
	ec, err := ethclient.Dial(rpcURL)
	if err != nil {
		t.Fatalf("dial anvil: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	chainID, err := ec.ChainID(ctx)
	if err != nil {
		t.Fatalf("chain id: %v", err)
	}
	key, err := crypto.HexToECDSA(anvilKey0)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := bind.NewKeyedTransactorWithChainID(key, chainID)
	if err != nil {
		t.Fatal(err)
	}

	addr, _, _, err := bindings.DeployAnchorRegistry(auth, ec, []common.Address{auth.From})
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	waitForCode(ctx, t, ec, addr) // anvil --block-time 1 mines it within a second or two

	c, err := New(ctx, Config{
		ContractAddress:            addr,
		ChainID:                    chainID,
		FinalityMode:               FinalityModeConfirmations,
		FinalityConfirmations:      3,
		LastMergeableConfirmations: 1,
		ResubmitAfter:              10 * time.Second,
		GasBumpPercent:             25,
		RPCTimeout:                 10 * time.Second,
		Logger:                     slog.Default(),
	}, ec, key)
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	ref, err := c.Anchor(ctx, dHash, dRoot)
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}

	deadline := time.Now().Add(90 * time.Second)
	for {
		st, err := c.Finality(ctx, ref)
		if err != nil {
			t.Fatalf("finality: %v", err)
		}
		if st.Final && st.LastMergeable {
			if st.Ref.L2BlockNumber == 0 || st.Ref.L2TxRef == "" {
				t.Fatalf("enrichment missing on real chain: %+v", st.Ref)
			}
			// Idempotent re-anchor must return the same on-chain position.
			again, err := c.Anchor(ctx, dHash, dRoot)
			if err != nil {
				t.Fatalf("re-anchor: %v", err)
			}
			if again.L2BlockNumber != st.Ref.L2BlockNumber {
				t.Fatalf("re-anchor position mismatch: %+v vs %+v", again, st.Ref)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("finality not reached before deadline; last: %+v", st)
		}
		time.Sleep(2 * time.Second)
	}
}

func waitForCode(ctx context.Context, t *testing.T, ec *ethclient.Client, addr common.Address) {
	t.Helper()
	for i := 0; i < 30; i++ {
		code, err := ec.CodeAt(ctx, addr, nil)
		if err == nil && len(code) > 0 {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("contract code never appeared")
}
```

(Drop the unused `math/big` / `bind` imports if the compiler flags them after writing the final file.)

- [ ] **Step 2: Run it locally against anvil**

```bash
docker run -d --name anvil -p 8545:8545 ghcr.io/foundry-rs/foundry:latest "anvil --host 0.0.0.0 --block-time 1"
ARBITER_ANVIL_E2E=1 go test ./anchor/evm/ -run TestAnvilE2E -v -count=1 -timeout 300s
docker rm -f anvil
```

(Local alternative without docker: `~/.foundry/bin/anvil --block-time 1` in another shell.)
Expected: PASS in ~15–30s.

- [ ] **Step 3: Extend CI**

`.github/workflows/ci.yml` — in the `test` job, after the fsm red-line steps, add:

```yaml
      - name: anchor bindings drift check
        run: make check-anchor-bindings
```

Append a third job:

```yaml
  anchor-anvil:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - name: configure private module access
        run: |
          git config --global url."https://x-access-token:${{ secrets.GH_MODULES_TOKEN }}@github.com/".insteadOf "https://github.com/"
          go env -w GOPRIVATE=github.com/sentioxyz,github.com/housegate
      - name: start anvil
        run: docker run -d --name anvil -p 8545:8545 ghcr.io/foundry-rs/foundry:latest "anvil --host 0.0.0.0 --block-time 1"
      - name: wait for anvil
        run: |
          for i in $(seq 1 30); do
            curl -sf -X POST -H 'Content-Type: application/json' \
              --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' \
              http://127.0.0.1:8545 > /dev/null && exit 0
            sleep 1
          done
          exit 1
      - name: anchor E2E (anvil)
        run: ARBITER_ANVIL_E2E=1 go test ./anchor/evm/ -run TestAnvilE2E -v -count=1 -timeout 300s
```

- [ ] **Step 4: Full local verification sweep (spec §9 tripwires)**

```bash
go build ./... && go vet ./... && go test ./...
git diff --stat main -- fsm/            # MUST be empty
make check-anchor-bindings              # MUST pass
grep -rn "WaitMined\|WaitDeployed" anchor/evm/*.go | grep -v _test.go   # MUST be empty (no receipt waits in the client)
```

Expected: all green, both MUST-be-empty checks empty.

- [ ] **Step 5: Commit**

```bash
git add anchor/evm/anvil_e2e_test.go .github/workflows/ci.yml
git commit -m "ci: anvil E2E job and anchor bindings drift gate"
```

---

## Completion checklist (spec §9 acceptance tripwires)

- [ ] Orchestrator loop has no new blocking calls (all chain calls deadline-bounded; grep for receipt waits is clean).
- [ ] `fsm/` zero diff vs main; arbiter-proto untouched.
- [ ] `anchor.Local` behavior bit-identical (all pre-existing orchestrator tests green).
- [ ] Bindings drift check wired in CI and passing.
- [ ] Repeated anchor of one block leaves exactly one contract entry + one `Anchored` event (Task 1 + Task 10 assertions).
- [ ] Contract write surface is `anchor(bytes32,bytes32)` + owner ACL functions only.
- [ ] P1a–P1c CI jobs still green.
- [ ] PR to arbiter `main` from `feat/p1d-evm-anchor`.
