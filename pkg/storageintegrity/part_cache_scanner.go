package storageintegrity

import (
	"context"
	"fmt"

	"housegate/housegate/pkg/replay"
)

// NamedPartReader recomputes the per-part row LtHash for exactly the named
// physical parts, using the same fold as a full active-part read.
// ClickHouseActivePartReader.ReadNamedParts satisfies it.
type NamedPartReader interface {
	ReadNamedParts(ctx context.Context, table string, partNames []string) ([]replay.PartManifestEntry, error)
}

// SchemaColumnReader resolves the live schema hash of a physical table, so a
// cache key binds the exact column (name,type) set the fold used. It is
// separate from KeyColumnProvider (which resolves protected key columns).
type SchemaColumnReader interface {
	SchemaHash(ctx context.Context, database, table, tableID string) (string, error)
}

// CachingPartScanner computes active-part row-LtHash entries with a
// content-addressed cache in front of the row scan. It is the shared engine of
// the byte-side / base-commitment / promotion fast paths.
//
// Flow per call:
//  1. Inspector.ActiveParts → live descriptors (name, partition, rows, bytes,
//     hash_of_all_files). Metadata always comes from here.
//  2. For each part with a usable content address, look up the cache.
//  3. Scan (ReadNamedParts) exactly the parts that missed (or have no content
//     address), folding rows the same way a full read would.
//  4. Emit one PartManifestEntry per live part: LIVE name/partition/rowcount,
//     PartRowLtHash from cache-hit or fresh scan. Never emit a cached name.
//  5. Fail closed if a part's scanned/cached row count disagrees with
//     system.parts.rows (anti-cancellation: a 2^16-identical-row swap sums to
//     zero in the raw fold, so the row count is the independent guard).
//
// A miss is always safe (recompute); the cache only ever elides the row scan.
type CachingPartScanner struct {
	Inspector PartInspector
	Cache     PartLtHashCache
	Scanner   NamedPartReader
	// Schema resolves the live schema hash used in the cache key. Optional: when
	// nil (or when it returns ""), the scanner treats every part as
	// content-address-incomplete and folds all rows (correct, just not cached).
	Schema SchemaColumnReader
	// Source labels cache writes for observability (byte_side_scan, etc.).
	Source string
}

// ScanParts returns the active-part entries for database.table (optionally
// scoped to partitionIDs), using the cache where possible. The result is
// byte-identical to a full ClickHouseActivePartReader.ReadActiveParts of the
// same parts.
func (s CachingPartScanner) ScanParts(ctx context.Context, database, table string, partitionIDs []string) ([]replay.PartManifestEntry, error) {
	if s.Inspector == nil {
		return nil, fmt.Errorf("caching part scanner requires an inspector")
	}
	if s.Scanner == nil {
		return nil, fmt.Errorf("caching part scanner requires a named-part reader")
	}
	descriptors, err := s.Inspector.ActiveParts(ctx, database, table, partitionIDs)
	if err != nil {
		return nil, err
	}
	if len(descriptors) == 0 {
		return nil, nil
	}
	qualified := qualifiedTable(database, table)
	tableID := descriptors[0].TableID

	// Resolve schema hash once (cache key input). A failure or empty result
	// disables caching for this call — never fails the scan.
	schemaHash := ""
	if s.Schema != nil {
		if h, herr := s.Schema.SchemaHash(ctx, database, table, tableID); herr == nil {
			schemaHash = h
		}
	}

	// Partition results into cache hits and the parts that still need scanning.
	cachedLtHash := make(map[string]string, len(descriptors)) // partName -> lthash
	var missParts []string
	for i := range descriptors {
		d := descriptors[i]
		d.SchemaHash = schemaHash
		descriptors[i] = d
		key, ok := d.CacheKey()
		if !ok || s.Cache == nil {
			// No content address (or no cache): must fold this part's rows.
			missParts = append(missParts, d.PartName)
			continue
		}
		entry, hit, gerr := s.Cache.Get(ctx, key)
		if gerr != nil || !hit {
			missParts = append(missParts, d.PartName)
			continue
		}
		// Anti-cancellation: a cached row LtHash is only trustworthy if the
		// cached row count still matches the live part's row count. If they
		// disagree (should be impossible for a content-addressed hit, but the
		// content address is metadata we do not fully control), fall back to a
		// scan rather than trust the cache.
		if entry.RowCount != d.Rows {
			missParts = append(missParts, d.PartName)
			continue
		}
		cachedLtHash[d.PartName] = entry.PartRowLtHash
	}

	// Scan exactly the miss parts (if any) with the shared fold.
	scanned := make(map[string]replay.PartManifestEntry, len(missParts))
	if len(missParts) > 0 {
		entries, serr := s.Scanner.ReadNamedParts(ctx, qualified, missParts)
		if serr != nil {
			return nil, serr
		}
		for _, e := range entries {
			scanned[e.PartName] = e
		}
	}

	out := make([]replay.PartManifestEntry, 0, len(descriptors))
	for _, d := range descriptors {
		entry := replay.PartManifestEntry{
			TableID:     tableID,
			PartitionID: d.PartitionID, // LIVE
			PartName:    d.PartName,    // LIVE — never a cached name
			RowCount:    d.Rows,        // LIVE (system.parts.rows)
		}
		if h, ok := cachedLtHash[d.PartName]; ok {
			entry.PartRowLtHash = h
		} else if sc, ok := scanned[d.PartName]; ok {
			// Fail closed on anti-cancellation: the fold's row count must equal
			// system.parts.rows for this part, or a hidden 2^16-identical-row
			// swap (which sums to zero) would pass silently.
			if sc.RowCount != d.Rows {
				return nil, fmt.Errorf("part %q row count mismatch: scanned %d, system.parts.rows %d (fail closed)", d.PartName, sc.RowCount, d.Rows)
			}
			entry.PartRowLtHash = sc.PartRowLtHash
			// Backfill the cache (only when we have a usable, complete key).
			if key, ok := d.CacheKey(); ok && s.Cache != nil {
				_ = s.Cache.Put(ctx, PartLtHashEntry{
					Key:           key,
					PartitionID:   d.PartitionID,
					PartName:      d.PartName,
					RowCount:      d.Rows,
					Bytes:         d.Bytes,
					PartRowLtHash: sc.PartRowLtHash,
					Source:        s.Source,
				})
			}
		} else {
			// A live part that neither hit the cache nor appeared in the scan
			// result means the scan lost a part (e.g. it merged away mid-scan).
			// Fail closed rather than emit an incomplete active set.
			return nil, fmt.Errorf("active part %q disappeared between metadata read and scan (fail closed)", d.PartName)
		}
		out = append(out, entry)
	}
	return out, nil
}
