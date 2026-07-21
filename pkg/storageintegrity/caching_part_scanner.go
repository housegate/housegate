package storageintegrity

import (
	"context"
	"fmt"

	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
)

// PartScanner is the local scanner seam: it folds a part's rows into its LtHash
// by reading the part. The real implementation wraps chexec.ScanParts against a
// pinned ClickHouse; a fake stands in for tests. A cache sits in front of this
// seam and never changes its result — a hit returns exactly what a real scan
// would (design §5.1: the cache may pre-check but must not change the vote hash).
type PartScanner interface {
	ScanPart(ctx context.Context, entry replay.PartManifestEntry, schema payloadexec.TableSchema) (PartLtHashResult, error)
}

// CachingPartScanner wraps an inner PartScanner with a PartLtHashCache: it
// derives the cache key via the inspector, returns a validated hit, or falls
// through to a real scan on a miss and populates the cache. It is a discardable
// local performance layer: with a nil cache it is a pass-through, and it never
// returns a result the inner scanner would not.
type CachingPartScanner struct {
	inner     PartScanner
	cache     *PartLtHashCache
	inspector *PartInspector
	hits      uint64
	misses    uint64
}

// NewCachingPartScanner constructs the caching scanner. A nil cache makes it a
// pass-through to the inner scanner (still correct, no caching). The inner
// scanner and inspector are required.
func NewCachingPartScanner(inner PartScanner, cache *PartLtHashCache, inspector *PartInspector) (*CachingPartScanner, error) {
	if inner == nil {
		return nil, fmt.Errorf("caching part scanner: inner scanner is required")
	}
	if inspector == nil {
		return nil, fmt.Errorf("caching part scanner: inspector is required")
	}
	return &CachingPartScanner{inner: inner, cache: cache, inspector: inspector}, nil
}

// ScanPart returns the part's LtHash, from the cache when every key field matches
// (and the cached result is consistent with the observed entry), else from a real
// inner scan whose result is then cached. A part that cannot be keyed (no
// physical hash) always falls through to a real scan.
func (s *CachingPartScanner) ScanPart(ctx context.Context, entry replay.PartManifestEntry, schema payloadexec.TableSchema) (PartLtHashResult, error) {
	if s.cache != nil {
		if key, err := s.inspector.KeyFor(entry, schema); err == nil {
			if cached, ok := s.cache.Get(key); ok {
				// A hit must still be consistent with the observed entry; an
				// inconsistent hit (e.g. a phys-hash collision) is discarded and the
				// part is really scanned.
				if s.inspector.ValidateCachedAgainstEntry(cached, entry) == nil {
					s.hits++
					return cached, nil
				}
			}
		}
	}
	// Miss (or un-keyable / inconsistent): real scan.
	s.misses++
	res, err := s.inner.ScanPart(ctx, entry, schema)
	if err != nil {
		return PartLtHashResult{}, err
	}
	if s.cache != nil {
		if key, kerr := s.inspector.KeyFor(entry, schema); kerr == nil {
			_ = s.cache.Put(key, res) // caching is best-effort; a Put error never fails a scan
		}
	}
	return res, nil
}

// Stats returns the hit/miss counters (observability/test accessor).
func (s *CachingPartScanner) Stats() (hits, misses uint64) {
	return s.hits, s.misses
}
