package storageintegrity

import (
	"context"
	"fmt"
	"sort"

	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
)

// PromotionReadbackMapping is the result of reading back a promoted table's
// active parts: the post-promotion state root AND the exact candidate-to-safe
// part mapping. The fast path (a metadata/cache readback) must produce a mapping
// identical to the strict path (a full row scan) — design §9 acceptance:
// "promotion readback fast path: post root 和 exact Parts[] mapping 都与 strict path
// 一致".
type PromotionReadbackMapping struct {
	PostStateRoot string
	Parts         []replay.PartManifestEntry
}

// AssertReadbackFastPathEquivalent is the pure fail-closed predicate that the
// fast path equals the strict path: identical PostStateRoot AND an
// order-insensitive exact match of the Parts mapping by every field
// (table/partition/part name/phys hash/row-lthash/rows/bytes). Any divergence is
// an error — the fast path may only reduce work, never change the result. This is
// the invariant the gated fast path is checked against.
func AssertReadbackFastPathEquivalent(strict, fast PromotionReadbackMapping) error {
	if strict.PostStateRoot == "" || fast.PostStateRoot == "" {
		return fmt.Errorf("promotion readback: blank post state root (strict=%q fast=%q)", strict.PostStateRoot, fast.PostStateRoot)
	}
	if strict.PostStateRoot != fast.PostStateRoot {
		return fmt.Errorf("promotion readback: fast post root %q != strict %q", fast.PostStateRoot, strict.PostStateRoot)
	}
	if len(strict.Parts) != len(fast.Parts) {
		return fmt.Errorf("promotion readback: fast mapping has %d parts, strict has %d", len(fast.Parts), len(strict.Parts))
	}
	strictByKey := map[string]replay.PartManifestEntry{}
	for _, p := range strict.Parts {
		strictByKey[auditPartKey(p.TableID, p.PartitionID, p.PartName)] = p
	}
	for _, f := range fast.Parts {
		s, ok := strictByKey[auditPartKey(f.TableID, f.PartitionID, f.PartName)]
		if !ok {
			return fmt.Errorf("promotion readback: fast part %s/%s/%s is not in the strict mapping", f.TableID, f.PartitionID, f.PartName)
		}
		if !samePartManifestEntry(s, f) {
			return fmt.Errorf("promotion readback: fast part %s/%s/%s does not match the strict mapping", f.TableID, f.PartitionID, f.PartName)
		}
	}
	return nil
}

// samePartManifestEntry compares two entries by every field, treating StorageRefs
// order-insensitively.
func samePartManifestEntry(a, b replay.PartManifestEntry) bool {
	if a.TableID != b.TableID || a.PartitionID != b.PartitionID || a.PartName != b.PartName {
		return false
	}
	if a.PartPhysHash != b.PartPhysHash || a.PartRowLtHash != b.PartRowLtHash {
		return false
	}
	if a.RowCount != b.RowCount || a.Bytes != b.Bytes {
		return false
	}
	ar := append([]string(nil), a.StorageRefs...)
	br := append([]string(nil), b.StorageRefs...)
	sort.Strings(ar)
	sort.Strings(br)
	if len(ar) != len(br) {
		return false
	}
	for i := range ar {
		if ar[i] != br[i] {
			return false
		}
	}
	return true
}

// SNodeReadbackPort is the gated C0 port that reads back the exact active parts a
// promotion produced from the SNode. The fast path uses it to avoid a full row
// scan. No implementation exists: it needs the companion C0 SNode exact-parts
// readback seam, which is absent from arbiter/arbiter-proto. See
// CompanionMutationConsensusAvailable.
type SNodeReadbackPort interface {
	ExactActivePartsReadback(ctx context.Context, qualifiedTable string, schema payloadexec.TableSchema, candidates []CandidatePart) (PromotionReadbackMapping, error)
}

// PromotionReadback drives the fast-path readback and checks it against the
// strict path via AssertReadbackFastPathEquivalent. It holds the gated SNode port
// and a strict-path scanner; until the companion C0 seam lands it fails closed.
type PromotionReadback struct {
	fast   SNodeReadbackPort
	strict PartScanner
}

// NewPromotionReadback constructs the fast-path driver over the gated SNode port
// and a strict-path scanner (the PR23 PartScanner seam). Both are required.
func NewPromotionReadback(fast SNodeReadbackPort, strict PartScanner) (*PromotionReadback, error) {
	if fast == nil || strict == nil {
		return nil, fmt.Errorf("promotion readback: fast SNode port and strict scanner are required")
	}
	return &PromotionReadback{fast: fast, strict: strict}, nil
}

// Readback would take the fast-path SNode readback and, when strict verification
// is requested, assert it equals the strict-path scan before returning it. It
// fails closed while the companion C0 seam is absent: no real SNode readback
// exists, so the fast path cannot run and must not fabricate the SNode protocol.
func (r *PromotionReadback) Readback(_ context.Context, qualifiedTable string) (PromotionReadbackMapping, error) {
	if !CompanionMutationConsensusAvailable {
		return PromotionReadbackMapping{}, fmt.Errorf("promotion readback: companion C0 SNode exact-parts readback seam absent; fast path cannot run (%s)", qualifiedTable)
	}
	return PromotionReadbackMapping{}, fmt.Errorf("promotion readback: end-to-end fast path not implemented")
}
