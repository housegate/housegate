package storageintegrity

import (
	"testing"

	"housegate/housegate/pkg/replay"
)

func newTestLeader(t *testing.T) (*Secp256k1LeaderSigner, *LeaderSignatureVerifier) {
	t.Helper()
	// A fixed 32-byte hex seed (deterministic secp256k1 scalar).
	seedHex := "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	signer, err := NewSecp256k1LeaderSignerFromSeed(seedHex)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	verifier, err := NewLeaderSignatureVerifier(signer.Address())
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	return signer, verifier
}

func TestPromotionLeaderSignatureRoundTrip(t *testing.T) {
	signer, verifier := newTestLeader(t)
	task := PromotionTask{
		PromotionID:  "promotion-s1",
		PromotionSeq: 5,
		Kind:         "insert",
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607", "202606"},
		StatementIDs: []string{"s1"},
	}
	digest, err := PromotionLeaderCommandDigest(task)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	task.LeaderSignature = signer.SignCommandDigest(digest)

	if err := ValidatePromotionLeaderSignature(verifier, false, task); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}

	// Partition order must not matter (canonical sort).
	reordered := task
	reordered.PartitionIDs = []string{"202606", "202607"}
	if err := ValidatePromotionLeaderSignature(verifier, false, reordered); err != nil {
		t.Fatalf("re-ordered partitions rejected: %v", err)
	}

	// Tampering with a bound field must fail closed.
	tampered := task
	tampered.SafeTable = "`hg_safe`.`other`"
	if err := ValidatePromotionLeaderSignature(verifier, false, tampered); err == nil {
		t.Fatalf("expected tampered task to fail leader verification")
	}

	// A missing signature must fail closed.
	unsigned := task
	unsigned.LeaderSignature = ""
	if err := ValidatePromotionLeaderSignature(verifier, false, unsigned); err == nil {
		t.Fatalf("expected missing signature to fail closed")
	}
}

func TestCompactionLeaderSignatureRoundTrip(t *testing.T) {
	signer, verifier := newTestLeader(t)
	task := CompactionTask{
		CompactionID: "c1",
		PromotionSeq: 9,
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607"},
	}
	digest, err := CompactionLeaderCommandDigest(task)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	task.LeaderSignature = signer.SignCommandDigest(digest)
	if err := ValidateCompactionLeaderSignature(verifier, false, task); err != nil {
		t.Fatalf("valid compaction signature rejected: %v", err)
	}
	tampered := task
	tampered.PromotionSeq = 10
	if err := ValidateCompactionLeaderSignature(verifier, false, tampered); err == nil {
		t.Fatalf("expected tampered compaction task to fail leader verification")
	}
}

func TestPublicationAuthorityRecoverableSignatureRoundTrip(t *testing.T) {
	signer, verifier := newTestLeader(t)
	task := PromotionTask{
		PromotionID:      "promotion-authority",
		PromotionSeq:     11,
		Kind:             "insert",
		TableID:          "tenant.events",
		SafeTable:        "`hg_safe`.`events`",
		SourceTable:      "`hg_unsafe`.`events`",
		PartitionIDs:     []string{"202607"},
		StatementIDs:     []string{"s1"},
		CandidateParts:   []ByteSidePart{{PartitionID: "202607", PartName: "all_1_1_0", PartRowLtHash: "0xaa", RowCount: 1}},
		ExpectedPostRoot: "0xaa",
	}
	digest, err := PromotionLeaderCommandDigest(task)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	task.LeaderSignature = signer.SignCommandDigest(digest)

	if task.LeaderSignature == "" || task.LeaderSignature[:2] == "0x" {
		t.Fatalf("leader signature must be a recoverable JWS, got %q", task.LeaderSignature)
	}
	if got := signer.Address(); got == "" || got[:2] != "0x" {
		t.Fatalf("signer address = %q, want 0x-prefixed address", got)
	}
	if err := ValidatePromotionLeaderSignature(verifier, true, task); err != nil {
		t.Fatalf("recoverable authority signature rejected: %v", err)
	}
}

func TestPublicationAuthorityRejectsAddressOutsideAllowlist(t *testing.T) {
	signer, err := NewSecp256k1LeaderSignerFromSeed("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff")
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	other, err := NewSecp256k1LeaderSignerFromSeed("10112233445566778899aabbccddeeff00112233445566778899aabbccddeeff")
	if err != nil {
		t.Fatalf("other signer: %v", err)
	}
	verifier, err := NewLeaderSignatureVerifier(other.Address())
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	task := PromotionTask{PromotionID: "promotion-authority", PromotionSeq: 1, TableID: "tenant.events", SafeTable: "`hg_safe`.`events`", PartitionIDs: []string{"p1"}}
	digest, err := PromotionLeaderCommandDigest(task)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	task.LeaderSignature = signer.SignCommandDigest(digest)
	if err := ValidatePromotionLeaderSignature(verifier, true, task); err == nil {
		t.Fatalf("signature from address outside allowlist must fail closed")
	}
}

func TestDisabledVerifierIsNoOp(t *testing.T) {
	verifier, err := NewLeaderSignatureVerifier("")
	if err != nil {
		t.Fatalf("new disabled verifier: %v", err)
	}
	if verifier.Enabled() {
		t.Fatalf("empty key should be disabled")
	}
	// A disabled verifier lets an unsigned task through (opt-out).
	if err := ValidatePromotionLeaderSignature(verifier, false, PromotionTask{PromotionID: "p"}); err != nil {
		t.Fatalf("disabled verifier should be a no-op, got %v", err)
	}
}

// TestLeaderCommandDigestGoldenVector pins the canonical leader-command digest
// to the value sentio-storage-mocks' port asserts (mockstorage's
// TestLeaderCommandDigestGoldenVector). The mock cannot import this package, so
// this is the tripwire: if the command projection / domain tag / hashing
// profile changes here, this and the mock's constant must move in lockstep or
// leader-signed publications will fail verification in the e2e.
func TestLeaderCommandDigestGoldenVector(t *testing.T) {
	const (
		wantPromo   = "0xf716c6b32b913fb693a744aeed2fd09bbfc49e0b6164495e1d30205e73451d63"
		wantCompact = "0x33df6438d5bcfc8e62fb2cb71c60e181edf5f9fe45e52b85dfa0f86a360dbc09"
	)
	promo, err := PromotionLeaderCommandDigest(PromotionTask{
		PromotionID:  "promotion-golden",
		PromotionSeq: 42,
		Kind:         "insert",
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607", "202606"},
		StatementIDs: []string{"s2", "s1"},
	})
	if err != nil {
		t.Fatalf("promo digest: %v", err)
	}
	if promo != wantPromo {
		t.Fatalf("promotion leader digest = %s, want %s — update mock leader.go golden", promo, wantPromo)
	}
	compact, err := CompactionLeaderCommandDigest(CompactionTask{
		CompactionID: "compaction-golden",
		PromotionSeq: 7,
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607"},
	})
	if err != nil {
		t.Fatalf("compact digest: %v", err)
	}
	if compact != wantCompact {
		t.Fatalf("compaction leader digest = %s, want %s — update mock leader.go golden", compact, wantCompact)
	}
}

// TestLeaderSignatureRequiredFailsClosedWithoutKey proves protected mode: a
// required leader signature with no configured authority key fails closed
// rather than silently skipping verification (HG-P0-03).
func TestLeaderSignatureRequiredFailsClosedWithoutKey(t *testing.T) {
	disabled, err := NewLeaderSignatureVerifier("")
	if err != nil {
		t.Fatalf("new disabled verifier: %v", err)
	}
	if err := ValidatePromotionLeaderSignature(disabled, true, PromotionTask{PromotionID: "p"}); err == nil {
		t.Fatalf("required verification with no key must fail closed")
	}
	if err := ValidateCompactionLeaderSignature(disabled, true, CompactionTask{CompactionID: "c"}); err == nil {
		t.Fatalf("required compaction verification with no key must fail closed")
	}
}

// TestLeaderSignatureCoversResultAffectingFields proves HG-P0-03 field
// coverage: tampering with any field that changes the physical publication
// result — candidate parts, expected post roots, publication action flags,
// unsafe buffer identity, source table, CAS requirements — breaks the
// signature.
func TestLeaderSignatureCoversResultAffectingFields(t *testing.T) {
	signer, verifier := newTestLeader(t)
	base := PromotionTask{
		PromotionID:        "promotion-cover",
		PromotionSeq:       3,
		Kind:               "insert",
		TableID:            "tenant.events",
		SafeTable:          "`hg_safe`.`events`",
		SourceTable:        "`hg_unsafe`.`events`",
		UnsafeTable:        "`hg_unsafe`.`events`",
		UnsafeBufferID:     1,
		UnsafeBufferEpoch:  7,
		PartitionIDs:       []string{"202607"},
		StatementIDs:       []string{"s1"},
		CandidateParts:     []ByteSidePart{{PartitionID: "202607", PartRowLtHash: "0xaa", RowCount: 1}},
		ExpectedPostRoots:  []replay.PartitionCommitment{{TableID: "tenant.events", PartitionID: "202607", Root: "0xbb"}},
		ReplacePartition:   false,
		RequireBaseRootCAS: true,
		PromoteDatabase:    "hg_promote",
		CleanupUnsafeParts: []ByteSidePart{{PartitionID: "202607", PartName: "all_1_1_0", PartRowLtHash: "0xaa", RowCount: 1}},
	}
	digest, err := PromotionLeaderCommandDigest(base)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	base.LeaderSignature = signer.SignCommandDigest(digest)
	if err := ValidatePromotionLeaderSignature(verifier, true, base); err != nil {
		t.Fatalf("valid signed task rejected: %v", err)
	}

	mutations := map[string]func(*PromotionTask){
		"candidate parts":     func(p *PromotionTask) { p.CandidateParts[0].PartRowLtHash = "0xcc" },
		"expected post roots": func(p *PromotionTask) { p.ExpectedPostRoots[0].Root = "0xdd" },
		"source table":        func(p *PromotionTask) { p.SourceTable = "`hg_unsafe`.`other`" },
		"unsafe buffer epoch": func(p *PromotionTask) { p.UnsafeBufferEpoch = 8 },
		"replace partition":   func(p *PromotionTask) { p.ReplacePartition = true },
		"internal drop":       func(p *PromotionTask) { p.InternalDropPartition = true },
		"require base cas":    func(p *PromotionTask) { p.RequireBaseRootCAS = false },
		"promote database":    func(p *PromotionTask) { p.PromoteDatabase = "other_promote" },
		"cleanup parts":       func(p *PromotionTask) { p.CleanupUnsafeParts[0].PartName = "other_1_1_0" },
	}
	for name, mutate := range mutations {
		tampered := base
		// deep-copy the slices we mutate so cases don't leak into each other
		tampered.CandidateParts = append([]ByteSidePart(nil), base.CandidateParts...)
		tampered.ExpectedPostRoots = append([]replay.PartitionCommitment(nil), base.ExpectedPostRoots...)
		tampered.CleanupUnsafeParts = append([]ByteSidePart(nil), base.CleanupUnsafeParts...)
		mutate(&tampered)
		if err := ValidatePromotionLeaderSignature(verifier, true, tampered); err == nil {
			t.Fatalf("tampering with %q did not break the leader signature", name)
		}
	}
}

func TestCompactionLeaderSignatureCoversResultAffectingFields(t *testing.T) {
	signer, verifier := newTestLeader(t)
	base := CompactionTask{
		CompactionID:       "compaction-cover",
		PromotionSeq:       12,
		TableID:            "tenant.events",
		SafeTable:          "`hg_safe`.`events`",
		CompactDatabase:    "hg_compact",
		CompactTable:       "`hg_compact`.`events_p1`",
		BaseSafeSnapshotID: "snap-1",
		BasePartitionRoot:  "0xaa",
		ExpectedPostRoot:   "0xbb",
		PartitionIDs:       []string{"202607"},
		InputParts:         []replay.PartManifestEntry{{TableID: "tenant.events", PartitionID: "202607", PartName: "p_1_1_0", PartRowLtHash: "0xaa", RowCount: 1, PartPhysHash: "0x11", Bytes: 8}},
		RequireBaseRootCAS: true,
		RequirePostRootCAS: true,
		DropCompactTable:   true,
	}
	digest, err := CompactionLeaderCommandDigest(base)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	base.LeaderSignature = signer.SignCommandDigest(digest)
	if err := ValidateCompactionLeaderSignature(verifier, true, base); err != nil {
		t.Fatalf("valid signed task rejected: %v", err)
	}

	mutations := map[string]func(*CompactionTask){
		"compact database": func(c *CompactionTask) { c.CompactDatabase = "other_compact" },
		"compact table":    func(c *CompactionTask) { c.CompactTable = "`hg_compact`.`other`" },
		"input parts":      func(c *CompactionTask) { c.InputParts[0].PartPhysHash = "0x22" },
		"drop compact":     func(c *CompactionTask) { c.DropCompactTable = false },
		"post cas":         func(c *CompactionTask) { c.RequirePostRootCAS = false },
	}
	for name, mutate := range mutations {
		tampered := base
		tampered.InputParts = append([]replay.PartManifestEntry(nil), base.InputParts...)
		mutate(&tampered)
		if err := ValidateCompactionLeaderSignature(verifier, true, tampered); err == nil {
			t.Fatalf("tampering with %q did not break the compaction leader signature", name)
		}
	}
}

func TestLeaderVerifierRejectsMalformedAuthorityAddress(t *testing.T) {
	if _, err := NewLeaderSignatureVerifier("0x00"); err == nil {
		t.Fatalf("expected error for malformed authority address")
	}
	if _, err := NewLeaderSignatureVerifier("0xnothex"); err == nil {
		t.Fatalf("expected error for non-hex authority address")
	}
	signer, err := NewSecp256k1LeaderSignerFromSeed("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff")
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	v, err := NewLeaderSignatureVerifier(signer.Address())
	if err != nil || !v.Enabled() {
		t.Fatalf("valid authority address must construct an enabled verifier: %v", err)
	}
}
