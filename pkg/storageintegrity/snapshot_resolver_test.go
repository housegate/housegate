package storageintegrity

import (
	"context"
	"strings"
	"testing"

	"housegate/housegate/pkg/replay"
)

func TestSnapshotResolverLoadsLatestWatermarkManifest(t *testing.T) {
	manifest := sealedResolverTestManifest(t, []replay.PartManifestEntry{{
		TableID:       "tenant.events",
		PartitionID:   "202607",
		PartName:      "p1",
		PartRowLtHash: "0xaaa",
		RowCount:      2,
	}})
	reader := &fakeSnapshotReader{
		watermark: SafeWatermark{SnapshotID: manifest.SnapshotID, SafeBlockSeq: manifest.SafeBlockSeq, StateRoot: manifest.StateRoot, ManifestRoot: manifest.ManifestRoot},
		manifests: map[string]replay.SafeSnapshotManifest{
			manifest.SnapshotID: manifest,
		},
	}

	got, err := SnapshotResolver{Reader: reader}.Latest(context.Background())
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got.Manifest.SnapshotID != manifest.SnapshotID || got.Watermark.ManifestRoot != manifest.ManifestRoot {
		t.Fatalf("resolved latest = %+v", got)
	}
}

func TestSnapshotResolverRejectsLocalActivePartMismatch(t *testing.T) {
	manifest := sealedResolverTestManifest(t, []replay.PartManifestEntry{{
		TableID:       "tenant.events",
		PartitionID:   "202607",
		PartName:      "p1",
		PartRowLtHash: "0xaaa",
		RowCount:      2,
	}})
	resolver := SnapshotResolver{
		Reader: &fakeSnapshotReader{
			watermark: SafeWatermark{SnapshotID: manifest.SnapshotID, SafeBlockSeq: manifest.SafeBlockSeq, StateRoot: manifest.StateRoot, ManifestRoot: manifest.ManifestRoot},
			manifests: map[string]replay.SafeSnapshotManifest{
				manifest.SnapshotID: manifest,
			},
		},
		ActiveParts: &fakeActivePartReader{parts: []replay.PartManifestEntry{{
			TableID:       "tenant.events",
			PartitionID:   "202607",
			PartName:      "p2",
			PartRowLtHash: "0xbbb",
			RowCount:      2,
		}}},
	}

	_, err := resolver.ResolveLocal(context.Background(), SnapshotResolveRequest{
		SnapshotID:   manifest.SnapshotID,
		SafeTable:    "hg_safe.events",
		TableID:      "tenant.events",
		PartitionIDs: []string{"202607"},
		VerifyActive: true,
	})
	if err == nil || !strings.Contains(err.Error(), "active parts mismatch") {
		t.Fatalf("ResolveLocal error = %v, want active parts mismatch", err)
	}
}

func sealedResolverTestManifest(t *testing.T, parts []replay.PartManifestEntry) replay.SafeSnapshotManifest {
	t.Helper()
	partitions := make([]replay.PartitionCommitment, 0, len(parts))
	for _, part := range parts {
		partitions = append(partitions, replay.PartitionCommitment{
			TableID:     part.TableID,
			PartitionID: part.PartitionID,
			Root:        part.PartRowLtHash,
		})
	}
	manifest, err := (replay.SafeSnapshotManifest{
		SafeBlockSeq:      7,
		SchemaSnapshotID:  "schema-1",
		SchemaRoot:        "schema-root",
		ExecutorProfileID: "exec-1",
		Tables: []replay.TableManifest{{
			TableID:        "tenant.events",
			SchemaHash:     "schema-hash",
			PartitionRoots: partitions,
			ActiveParts:    parts,
		}},
	}).Seal()
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return manifest
}

type fakeSnapshotReader struct {
	watermark SafeWatermark
	manifests map[string]replay.SafeSnapshotManifest
}

func (f *fakeSnapshotReader) GetSafeWatermark(context.Context) (SafeWatermark, error) {
	return f.watermark, nil
}

func (f *fakeSnapshotReader) GetSafeSnapshot(_ context.Context, snapshotID string) (replay.SafeSnapshotManifest, bool, error) {
	manifest, ok := f.manifests[snapshotID]
	return manifest, ok, nil
}
