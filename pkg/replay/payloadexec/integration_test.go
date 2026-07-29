package payloadexec_test

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// This file is the Appendix C integration test: it wires the real payload-local
// LtHash executor, in-memory snapshot/payload stores, and an ed25519 signer
// through the replay.Verifier core — no fakes — and exercises the adversarial
// walkthroughs (§12) the design layer is built to catch.

const (
	network = "sentio-net"
	table   = "balances"
	schema  = "schema-1"
	profile = "ch-26.x-pinned"
)

func newExecutor() *payloadexec.Executor {
	return payloadexec.New(network, payloadexec.TableSchema{
		TableID: table,
		Columns: []lthash.Column{
			{Name: "user_id", Type: "String"},
			{Name: "balance", Type: "UInt64"},
		},
	})
}

// harness bundles the fully wired verifier and the stores feeding it.
type harness struct {
	exec      *payloadexec.Executor
	snapshots *payloadexec.MemSnapshotStore
	payloads  *payloadexec.MemPayloadStore
	signer    *payloadexec.Ed25519Signer
	verifier  *replay.Verifier
	genesis   replay.SafeSnapshotManifest
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	exec := newExecutor()
	gen, err := exec.GenesisSnapshot(0, schema, profile)
	if err != nil {
		t.Fatalf("genesis: %v", err)
	}
	snapshots := payloadexec.NewMemSnapshotStore()
	snapshots.Put(gen)

	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 7
	signer, err := payloadexec.NewEd25519Signer("replica-a", seed)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}

	payloads := payloadexec.NewMemPayloadStore()
	return &harness{
		exec:      exec,
		snapshots: snapshots,
		payloads:  payloads,
		signer:    signer,
		verifier: &replay.Verifier{
			Snapshots: snapshots,
			Payloads:  payloads,
			Executor:  exec,
			Signer:    signer,
		},
		genesis: gen,
	}
}

func buildJob(snap replay.SafeSnapshotManifest, statementID, payloadRef string, payload []byte, sourceClaimRoot string) replay.ReplayJob {
	sql := "INSERT INTO balances FORMAT CSVWithNames"
	return replay.ReplayJob{
		BlockSeq:           snap.SafeBlockSeq + 1,
		PrevSafeSnapshotID: snap.SnapshotID,
		PrevStateRoot:      snap.StateRoot,
		SchemaSnapshotID:   snap.SchemaSnapshotID,
		ExecutorProfileID:  snap.ExecutorProfileID,
		SourceClaimRoot:    sourceClaimRoot,
		Statements: []replay.Statement{{
			StatementID:   statementID,
			StatementSeq:  snap.SafeBlockSeq + 1,
			SQL:           sql,
			SQLHash:       replay.DigestString(sql),
			SettingsHash:  replay.DigestString("settings"),
			PayloadRef:    payloadRef,
			PayloadHash:   replay.DigestBytes(payload),
			PayloadLength: uint64(len(payload)),
			TargetTableID: table,
		}},
	}
}

// rootFor simulates a source executing a payload under a given statement_id and
// registering the state root it computed. The root is bound to statement_id (via
// _hg_row_id, §5.2), so the probe must use the same statement_id as the job it
// will be compared against.
func rootFor(t *testing.T, exec *payloadexec.Executor, snap replay.SafeSnapshotManifest, statementID string, payload []byte) string {
	t.Helper()
	job := buildJob(snap, statementID, "probe-ref", payload, "")
	prepared := []replay.PreparedStatement{{Statement: job.Statements[0], Payload: payload}}
	_, res, err := exec.Apply(snap, job, prepared)
	if err != nil {
		t.Fatalf("probe Apply: %v", err)
	}
	return res.ComputedStateRoot
}

func TestIntegrationHonestInsertProducesMatchingSignedAttestation(t *testing.T) {
	h := newHarness(t)
	payload := []byte("user_id,balance\n0x123,10\n")
	h.payloads.Put("payload-1", payload)
	claim := rootFor(t, h.exec, h.genesis, "stmt-1", payload)

	job := buildJob(h.genesis, "stmt-1", "payload-1", payload, claim)
	att, err := h.verifier.Verify(context.Background(), job)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !att.MatchSourceRoot {
		t.Fatal("honest insert should match the source root")
	}
	if att.Receipt.ComputedStateRoot != claim {
		t.Fatalf("computed %s != claim %s", att.Receipt.ComputedStateRoot, claim)
	}
	assertSignatureValid(t, h.signer, att)
}

// TestIntegrationFraudulentSourceProducesSignedMismatch is design walkthrough #2
// + Appendix C.4: the user signs balance=10 but a malicious source executes
// balance=0 and registers that root. The verifier replays the *signed* payload,
// computes the balance=10 root, and signs a NON-matching receipt as
// non-repudiable challenge evidence (it does not error).
func TestIntegrationFraudulentSourceProducesSignedMismatch(t *testing.T) {
	h := newHarness(t)
	signedPayload := []byte("user_id,balance\n0x123,10\n")
	tamperedPayload := []byte("user_id,balance\n0x123,0\n")
	h.payloads.Put("payload-1", signedPayload)

	fraudRoot := rootFor(t, h.exec, h.genesis, "stmt-1", tamperedPayload) // root the bad source registers
	truthRoot := rootFor(t, h.exec, h.genesis, "stmt-1", signedPayload)
	if fraudRoot == truthRoot {
		t.Fatal("test setup broken: tampered and signed payloads hash equal")
	}

	job := buildJob(h.genesis, "stmt-1", "payload-1", signedPayload, fraudRoot)
	att, err := h.verifier.Verify(context.Background(), job)
	if err != nil {
		t.Fatalf("Verify must sign a mismatch, not error: %v", err)
	}
	if att.MatchSourceRoot {
		t.Fatal("fraudulent source root must not match")
	}
	if att.Receipt.SourceClaimRoot != fraudRoot {
		t.Fatalf("receipt source claim = %s, want %s", att.Receipt.SourceClaimRoot, fraudRoot)
	}
	if att.Receipt.ComputedStateRoot != truthRoot {
		t.Fatalf("receipt computed = %s, want recomputed truth %s", att.Receipt.ComputedStateRoot, truthRoot)
	}
	assertSignatureValid(t, h.signer, att)
}

// TestIntegrationRecomputabilityAcrossVerifiers is the §7 safety property:
// the correct commitment is recomputable, so two independent verifiers with
// different keys derive the identical receipt hash for the same job.
func TestIntegrationRecomputabilityAcrossVerifiers(t *testing.T) {
	payload := []byte("user_id,balance\n0x123,10\nveritas,42\n")

	verifyWith := func(replicaSeed byte, replicaID string) replay.ReplayAttestation {
		h := newHarness(t)
		h.payloads.Put("payload-1", payload)
		seed := make([]byte, ed25519.SeedSize)
		seed[0] = replicaSeed
		signer, err := payloadexec.NewEd25519Signer(replicaID, seed)
		if err != nil {
			t.Fatalf("signer: %v", err)
		}
		h.verifier.Signer = signer
		claim := rootFor(t, h.exec, h.genesis, "stmt-1", payload)
		att, err := h.verifier.Verify(context.Background(), buildJob(h.genesis, "stmt-1", "payload-1", payload, claim))
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
		return att
	}

	a := verifyWith(1, "replica-a")
	b := verifyWith(2, "replica-b")

	if a.ReceiptHash != b.ReceiptHash {
		t.Fatalf("independent verifiers disagree on receipt hash: %s vs %s", a.ReceiptHash, b.ReceiptHash)
	}
	if a.ReplicaID == b.ReplicaID || a.Signature == b.Signature {
		t.Fatal("distinct replicas should still produce distinct identities/signatures")
	}
}

// TestIntegrationPayloadTamperRejectedBeforeExecutor is Appendix C.1: payload
// bytes that do not match the signed payload_hash are rejected before any
// executor runs (local refusal to attest — no signed receipt).
func TestIntegrationPayloadTamperRejectedBeforeExecutor(t *testing.T) {
	h := newHarness(t)
	signedPayload := []byte("user_id,balance\n0x123,10\n")
	// Store DIFFERENT bytes than the statement's payload_hash commits to.
	h.payloads.Put("payload-1", []byte("user_id,balance\n0x123,999999\n"))

	job := buildJob(h.genesis, "stmt-1", "payload-1", signedPayload, "anything")
	if _, err := h.verifier.Verify(context.Background(), job); err == nil {
		t.Fatal("expected payload_hash mismatch to be rejected before execution")
	}
}

// TestIntegrationRejectsUnknownPrevSnapshot is Appendix C.1: replay cannot start
// from a snapshot the verifier cannot load as safe.
func TestIntegrationRejectsUnknownPrevSnapshot(t *testing.T) {
	h := newHarness(t)
	payload := []byte("user_id,balance\n0x123,10\n")
	h.payloads.Put("payload-1", payload)

	job := buildJob(h.genesis, "stmt-1", "payload-1", payload, "x")
	job.PrevSafeSnapshotID = "not-in-store"
	if _, err := h.verifier.Verify(context.Background(), job); err == nil {
		t.Fatal("expected unknown prev snapshot to be rejected")
	}
}

// TestIntegrationSafeChainAdvancesAcrossBlocks verifies block 1, promotes its
// post-state to a new safe snapshot, and verifies block 2 against it — the
// end-to-end safe -> safe progression of §10 / Appendix A.2.
func TestIntegrationSafeChainAdvancesAcrossBlocks(t *testing.T) {
	h := newHarness(t)

	p1 := []byte("user_id,balance\n0x123,10\n")
	h.payloads.Put("p1", p1)
	claim1 := rootFor(t, h.exec, h.genesis, "stmt-1", p1)
	att1, err := h.verifier.Verify(context.Background(), buildJob(h.genesis, "stmt-1", "p1", p1, claim1))
	if err != nil {
		t.Fatalf("block 1 verify: %v", err)
	}
	if !att1.MatchSourceRoot {
		t.Fatal("block 1 should match")
	}

	// Promote block 1's verified post-state to a safe snapshot (what Keeper does
	// after quorum + finality), then verify block 2 against it.
	job1 := buildJob(h.genesis, "stmt-1", "p1", p1, claim1)
	prepared1 := []replay.PreparedStatement{{Statement: job1.Statements[0], Payload: p1}}
	safe1, _, err := h.exec.Apply(h.genesis, job1, prepared1)
	if err != nil {
		t.Fatalf("apply block 1: %v", err)
	}
	if safe1.StateRoot != att1.Receipt.ComputedStateRoot {
		t.Fatalf("promoted snapshot root %s != attested root %s", safe1.StateRoot, att1.Receipt.ComputedStateRoot)
	}
	h.snapshots.Put(safe1)

	p2 := []byte("user_id,balance\n0xabc,250\n")
	h.payloads.Put("p2", p2)
	claim2 := rootFor(t, h.exec, safe1, "stmt-2", p2)
	att2, err := h.verifier.Verify(context.Background(), buildJob(safe1, "stmt-2", "p2", p2, claim2))
	if err != nil {
		t.Fatalf("block 2 verify: %v", err)
	}
	if !att2.MatchSourceRoot {
		t.Fatal("block 2 should match against the promoted safe snapshot")
	}
	if att2.Receipt.PrevSafeSnapshotID != safe1.SnapshotID {
		t.Fatalf("block 2 prev = %s, want %s", att2.Receipt.PrevSafeSnapshotID, safe1.SnapshotID)
	}
}

func assertSignatureValid(t *testing.T, signer *payloadexec.Ed25519Signer, att replay.ReplayAttestation) {
	t.Helper()
	if att.ReceiptHash == "" || att.Signature == "" {
		t.Fatal("attestation must carry a receipt hash and signature")
	}
	recomputed, err := att.Receipt.Hash()
	if err != nil {
		t.Fatalf("receipt hash: %v", err)
	}
	if recomputed != att.ReceiptHash {
		t.Fatalf("attestation receipt hash %s != recomputed %s", att.ReceiptHash, recomputed)
	}
	sig, err := hex.DecodeString(att.Signature)
	if err != nil {
		t.Fatalf("signature not hex: %v", err)
	}
	if !ed25519.Verify(signer.PublicKey(), []byte(att.ReceiptHash), sig) {
		t.Fatal("attestation signature did not verify against the replica public key")
	}
}
