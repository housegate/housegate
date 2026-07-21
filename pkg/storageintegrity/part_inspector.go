package storageintegrity

import (
	"fmt"

	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
)

// PartInspector builds the PartCacheKey for a part under a schema, binding the
// row-hash profile, the table, the schema hash, the part's physical hash, and its
// row/byte size. It is the single place the cache key is derived, so the key
// binding is consistent across the cache and the scanner.
type PartInspector struct {
	networkID string
}

// NewPartInspector constructs an inspector for a network id (used to derive the
// schema hash the same way the replay executor does).
func NewPartInspector(networkID string) *PartInspector {
	return &PartInspector{networkID: networkID}
}

// KeyFor derives the PartCacheKey from a manifest entry and its schema. It fails
// closed if the part has no physical hash (nothing anchors the content) — such a
// part must always be really scanned, never cached.
func (p *PartInspector) KeyFor(entry replay.PartManifestEntry, schema payloadexec.TableSchema) (PartCacheKey, error) {
	if entry.PartPhysHash == "" {
		return PartCacheKey{}, fmt.Errorf("part inspector: part %s/%s/%s has no physical hash; cannot cache", entry.TableID, entry.PartitionID, entry.PartName)
	}
	if entry.TableID == "" {
		return PartCacheKey{}, fmt.Errorf("part inspector: part %s has no table id", entry.PartName)
	}
	key := PartCacheKey{
		RowHashVersion: RowHashProfileVersion,
		TableID:        entry.TableID,
		SchemaHash:     payloadexec.TableSchemaHash(p.networkID, schema),
		PartPhysHash:   entry.PartPhysHash,
		RowCount:       entry.RowCount,
		Bytes:          entry.Bytes,
	}
	if err := key.Valid(); err != nil {
		return PartCacheKey{}, err
	}
	return key, nil
}

// ValidateCachedAgainstEntry checks that a cached result is consistent with a
// freshly observed manifest entry before it is trusted: the part name and row
// count must match. This guards against a phys-hash collision producing a hit for
// a differently-named part.
func (p *PartInspector) ValidateCachedAgainstEntry(cached PartLtHashResult, entry replay.PartManifestEntry) error {
	if cached.PartName != entry.PartName {
		return fmt.Errorf("part inspector: cached part name %q != observed %q", cached.PartName, entry.PartName)
	}
	if cached.RowCount != entry.RowCount {
		return fmt.Errorf("part inspector: cached row count %d != observed %d for %s", cached.RowCount, entry.RowCount, entry.PartName)
	}
	return nil
}
