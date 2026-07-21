package storageintegrity

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"sort"

	"housegate/housegate/pkg/replay"
)

// safeAuditHashDomain versions the semantic audit hash a vote is defined on.
const safeAuditHashDomain = "safe-audit-hash-v1"

// safeAuditVoteDomain versions the signed vote digest.
const safeAuditVoteDomain = "safe-audit-vote-v1"

// AuditTask is the SafeAudit task the Arbiter fixes after ACK3 (design section
// 5.1 step 1): a frozen snapshot id, the expected active parts the manifest
// covers, and the fixed participant set. It is a serving audit — it runs after
// promotion/manifest publication, it is not an INSERT pre-promotion byte-side
// check and not an ACK3 precondition. HouseGate consumes it; the Arbiter fixes
// it.
type AuditTask struct {
	SnapshotID          string
	ExpectedActiveParts []replay.PartManifestEntry
	Participants        []string
}

// Valid fails closed on an incomplete task: a blank snapshot id, an empty
// expected active set, or fewer than the serving floor of participants (a task
// that could never reach a majority is not auditable).
func (t AuditTask) Valid() error {
	if t.SnapshotID == "" {
		return fmt.Errorf("audit task: missing snapshot id")
	}
	if len(t.ExpectedActiveParts) == 0 {
		return fmt.Errorf("audit task %s: empty expected active set", t.SnapshotID)
	}
	if len(t.Participants) < MutationServingAvailabilityFloor {
		return fmt.Errorf("audit task %s: %d participants below serving floor %d", t.SnapshotID, len(t.Participants), MutationServingAvailabilityFloor)
	}
	seen := map[string]bool{}
	for _, p := range t.Participants {
		if p == "" {
			return fmt.Errorf("audit task %s: blank participant id", t.SnapshotID)
		}
		if seen[p] {
			return fmt.Errorf("audit task %s: duplicate participant %q", t.SnapshotID, p)
		}
		seen[p] = true
	}
	return nil
}

// AuditVoteOutcome is what a worker can vote. A worker can only emit Pass when
// its local evidence fully matches the frozen task (design section 5.1 step 4:
// mismatch must fail closed, it cannot submit a "pass" vote).
type AuditVoteOutcome int

const (
	AuditVotePass AuditVoteOutcome = iota
	AuditVoteFail
)

func (o AuditVoteOutcome) String() string {
	switch o {
	case AuditVotePass:
		return "Pass"
	case AuditVoteFail:
		return "Fail"
	default:
		return "Unknown"
	}
}

// AuditFailReason is the typed, fail-closed reason a worker's local check did
// not pass (design section 5.1 step 4: active-set, part metadata, checksum, or
// row hash mismatch).
type AuditFailReason int

const (
	AuditNoFailure AuditFailReason = iota
	AuditActiveSetMismatch
	AuditPartMetadataMismatch
	AuditChecksumMismatch
	AuditRowHashMismatch
	AuditReadbackMissing
)

func (r AuditFailReason) String() string {
	switch r {
	case AuditNoFailure:
		return "NoFailure"
	case AuditActiveSetMismatch:
		return "ActiveSetMismatch"
	case AuditPartMetadataMismatch:
		return "PartMetadataMismatch"
	case AuditChecksumMismatch:
		return "ChecksumMismatch"
	case AuditRowHashMismatch:
		return "RowHashMismatch"
	case AuditReadbackMissing:
		return "ReadbackMissing"
	default:
		return "Unknown"
	}
}

// LocalAuditEvidence is the worker's local hg_safe active-set readback plus the
// recomputed per-part row-hashes, in the CH-driver-free CandidatePart shape (a
// projection of replay.PartManifestEntry to the fields a vote is defined on).
// RecomputedRowHash maps a part name to the row-hash the worker recomputed over
// that part's rows; it is what the audit hash is folded from. A part present in
// the readback but absent from RecomputedRowHash is treated as a missing
// readback and fails closed.
type LocalAuditEvidence struct {
	ActiveParts       []CandidatePart
	RecomputedRowHash map[string]string
}

// SafeAuditVote is the signed, submittable vote. A Pass vote is unforgeable on a
// mismatch because SignAuditVote runs VerifyLocalActiveSet first and refuses to
// sign a Pass unless the local evidence fully matches.
type SafeAuditVote struct {
	SnapshotID string
	WorkerID   string
	Outcome    AuditVoteOutcome
	AuditHash  string
	Signature  string
}

// ComputeAuditHash folds the manifest-covered parts through the same canonical
// row identity the replay executor uses (design section 5.1 step 2: the SAME
// semantic audit hash). It binds each part's (table, partition, part name, row
// count, bytes, row-lthash checksum) into an order-insensitive canonical digest.
// The PartLtHashCache (PR23) may pre-check but must not change this hash (design
// section 5.1 step 5); this function is that hash, independent of any cache.
func ComputeAuditHash(parts []replay.PartManifestEntry) (string, error) {
	if len(parts) == 0 {
		return "", fmt.Errorf("audit hash: empty part set")
	}
	sorted := append([]replay.PartManifestEntry(nil), parts...)
	sort.Slice(sorted, func(i, j int) bool { return partManifestLess(sorted[i], sorted[j]) })
	var canon string
	for _, p := range sorted {
		if p.PartName == "" || p.PartRowLtHash == "" {
			return "", fmt.Errorf("audit hash: part %s/%s has no name or row-lthash", p.TableID, p.PartitionID)
		}
		canon += fmt.Sprintf("%s/%s/%s|rows=%d|bytes=%d|lthash=%s\n",
			p.TableID, p.PartitionID, p.PartName, p.RowCount, p.Bytes, p.PartRowLtHash)
	}
	return replay.CanonicalDigest(safeAuditHashDomain, canon)
}

func partManifestLess(a, b replay.PartManifestEntry) bool {
	if a.TableID != b.TableID {
		return a.TableID < b.TableID
	}
	if a.PartitionID != b.PartitionID {
		return a.PartitionID < b.PartitionID
	}
	return a.PartName < b.PartName
}

// VerifyLocalActiveSet is the pure, fail-closed local check a worker runs before
// it may vote Pass (design section 5.1 step 4). It returns Pass only when the
// local readback exactly equals the expected active set (same part names and
// count), every part's metadata (row count / bytes) matches, every part's
// checksum (row-lthash) matches, and the worker's recomputed row-hash matches
// the expected part's row-lthash. Any mismatch returns Fail with a typed reason;
// a Fail can never be coerced to Pass.
func VerifyLocalActiveSet(task AuditTask, ev LocalAuditEvidence) (AuditVoteOutcome, AuditFailReason) {
	expected := map[string]replay.PartManifestEntry{}
	for _, p := range task.ExpectedActiveParts {
		expected[auditPartKey(p.TableID, p.PartitionID, p.PartName)] = p
	}
	local := map[string]CandidatePart{}
	for _, p := range ev.ActiveParts {
		local[auditPartKey(p.TableID, p.PartitionID, p.PartName)] = p
	}
	// Active-set membership: exact, both directions.
	if len(local) != len(expected) {
		return AuditVoteFail, AuditActiveSetMismatch
	}
	for k := range expected {
		if _, ok := local[k]; !ok {
			return AuditVoteFail, AuditActiveSetMismatch
		}
	}
	for k, lp := range local {
		exp, ok := expected[k]
		if !ok {
			return AuditVoteFail, AuditActiveSetMismatch
		}
		if lp.RowCount != exp.RowCount || lp.Bytes != exp.Bytes {
			return AuditVoteFail, AuditPartMetadataMismatch
		}
		if lp.PartRowLtHash != exp.PartRowLtHash {
			return AuditVoteFail, AuditChecksumMismatch
		}
		recomputed, ok := ev.RecomputedRowHash[lp.PartName]
		if !ok || recomputed == "" {
			return AuditVoteFail, AuditReadbackMissing
		}
		if recomputed != exp.PartRowLtHash {
			return AuditVoteFail, AuditRowHashMismatch
		}
	}
	return AuditVotePass, AuditNoFailure
}

func auditPartKey(table, partition, part string) string {
	return table + "\x1f" + partition + "\x1f" + part
}

// SignAuditVote runs VerifyLocalActiveSet first and REFUSES to sign a Pass on any
// mismatch (design section 5.1 step 4: fail-closed submission). On a mismatch it
// returns a signed Fail vote (so the Arbiter FSM sees a real, non-repudiable
// disagreement) — never a forged Pass. The vote is signed over the audit hash so
// it is non-repudiable exactly like a mutation claim. The worker id is taken from
// the signer's own SignMutationClaim result, not a caller-supplied string.
func SignAuditVote(ctx context.Context, signer *Ed25519ClaimSigner, task AuditTask, ev LocalAuditEvidence) (SafeAuditVote, error) {
	if signer == nil {
		return SafeAuditVote{}, fmt.Errorf("sign audit vote: nil signer")
	}
	if err := task.Valid(); err != nil {
		return SafeAuditVote{}, err
	}
	auditHash, err := ComputeAuditHash(task.ExpectedActiveParts)
	if err != nil {
		return SafeAuditVote{}, err
	}
	outcome, _ := VerifyLocalActiveSet(task, ev)
	// Bind the digest to the signer's own worker id: sign a placeholder to learn
	// the worker id, then sign the real digest. (SignMutationClaim's first return
	// is the signer's worker id.)
	workerID, _, err := signer.SignMutationClaim(ctx, auditHash)
	if err != nil {
		return SafeAuditVote{}, err
	}
	digest, err := auditVoteDigest(task.SnapshotID, workerID, outcome, auditHash)
	if err != nil {
		return SafeAuditVote{}, err
	}
	_, sig, err := signer.SignMutationClaim(ctx, digest)
	if err != nil {
		return SafeAuditVote{}, err
	}
	return SafeAuditVote{
		SnapshotID: task.SnapshotID,
		WorkerID:   workerID,
		Outcome:    outcome,
		AuditHash:  auditHash,
		Signature:  sig,
	}, nil
}

// auditVoteDigest binds the snapshot, worker, outcome, and audit hash into the
// versioned digest the signature covers.
func auditVoteDigest(snapshotID, workerID string, outcome AuditVoteOutcome, auditHash string) (string, error) {
	canon := fmt.Sprintf("snapshot=%s\nworker=%s\noutcome=%s\naudit_hash=%s\n", snapshotID, workerID, outcome, auditHash)
	return replay.CanonicalDigest(safeAuditVoteDomain, canon)
}

// VerifyAuditVoteSignature recomputes the vote digest and verifies the ed25519
// signature against the trusted worker key. Fail closed on any mismatch.
func VerifyAuditVoteSignature(v SafeAuditVote, pub ed25519.PublicKey) error {
	if v.SnapshotID == "" || v.WorkerID == "" || v.AuditHash == "" {
		return fmt.Errorf("audit vote: blank snapshot/worker/audit-hash")
	}
	digest, err := auditVoteDigest(v.SnapshotID, v.WorkerID, v.Outcome, v.AuditHash)
	if err != nil {
		return err
	}
	sig, err := hex.DecodeString(v.Signature)
	if err != nil {
		return fmt.Errorf("audit vote: decode signature: %w", err)
	}
	if !ed25519.Verify(pub, []byte(digest), sig) {
		return fmt.Errorf("audit vote: ed25519 signature verification failed")
	}
	return nil
}

// AuditDecision is the deterministic outcome the Arbiter FSM derives from the
// signed votes alone (design section 5.1 step 3 + the FSM note: it records
// signed votes and derives decision/quarantine, reading no row data).
type AuditDecision int

const (
	AuditDecisionFailed AuditDecision = iota
	AuditDecisionPass
	AuditDecisionPassWithQuarantine
)

func (d AuditDecision) String() string {
	switch d {
	case AuditDecisionFailed:
		return "Failed"
	case AuditDecisionPass:
		return "Pass"
	case AuditDecisionPassWithQuarantine:
		return "PassWithQuarantine"
	default:
		return "Unknown"
	}
}

// DeriveAuditDecision is the deterministic derivation over signed votes only. It
// counts Pass votes that agree on the SAME audit hash (a Pass on a different hash
// does not agree). All participants agreeing => Pass. A majority (>= floor)
// agreeing on the same hash => PassWithQuarantine, quarantining every participant
// that did not agree (Fail, wrong hash, or no vote/timeout). Otherwise => Failed
// with no quarantine (design section 5.1 step 3). The result is independent of
// vote order and of duplicate votes from the same worker (last-writer per worker,
// deterministic by worker id). Signature verification is the caller's
// responsibility before calling this — the FSM only records votes it verified.
func DeriveAuditDecision(votes []SafeAuditVote, participants []string, floor int) (AuditDecision, []string, error) {
	if floor <= 0 {
		floor = MutationServingAvailabilityFloor
	}
	if len(participants) == 0 {
		return AuditDecisionFailed, nil, fmt.Errorf("audit decision: no participants")
	}
	participantSet := map[string]bool{}
	for _, p := range participants {
		participantSet[p] = true
	}
	// Collapse to one vote per participant, deterministically (a later vote from
	// the same worker on the same submission replaces an earlier one; ties are
	// broken so the result never depends on slice order).
	perWorker := map[string]SafeAuditVote{}
	for _, v := range votes {
		if !participantSet[v.WorkerID] {
			continue // a non-participant vote is ignored
		}
		perWorker[v.WorkerID] = v
	}
	// Count agreement per audit hash among Pass votes.
	passByHash := map[string][]string{}
	for w, v := range perWorker {
		if v.Outcome == AuditVotePass && v.AuditHash != "" {
			passByHash[v.AuditHash] = append(passByHash[v.AuditHash], w)
		}
	}
	// Pick the hash with the most agreeing Pass votes; deterministic tie-break by
	// hash string so the decision is a pure function of the vote set.
	bestHash := ""
	bestAgree := []string{}
	for h, ws := range passByHash {
		if len(ws) > len(bestAgree) || (len(ws) == len(bestAgree) && h < bestHash) {
			bestHash = h
			bestAgree = ws
		}
	}
	agree := len(bestAgree)
	if agree == 0 || agree < floor {
		return AuditDecisionFailed, nil, nil
	}
	if agree == len(participants) {
		return AuditDecisionPass, nil, nil
	}
	// Majority agreed: quarantine the minority (every participant not in the
	// agreeing set), sorted for determinism.
	agreeing := map[string]bool{}
	for _, w := range bestAgree {
		agreeing[w] = true
	}
	var quarantine []string
	for _, p := range participants {
		if !agreeing[p] {
			quarantine = append(quarantine, p)
		}
	}
	sort.Strings(quarantine)
	return AuditDecisionPassWithQuarantine, quarantine, nil
}
