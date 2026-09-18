// Package snapshotquery defines pure validation and leaf I/O ports for the
// snapshot-query lane. Durable archive/retention and scratch implementations
// live in their owning layers and are not provided here.
package snapshotquery

import (
	"context"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// SnapshotReadStore opens one exact pinned snapshot and complete selected read
// set under a durable caller-owned reference.
type SnapshotReadStore interface {
	Open(ctx context.Context, pin replay.SnapshotPin, reads replay.SnapshotReadSet, referenceID string) (ReadSnapshot, error)
}

// Relation maps one authenticated table identity to its restricted scratch
// database/table relation.
type Relation struct {
	TableID  string
	Database string
	Table    string
}

// RowStream yields target-typed values from the restricted scratch query.
type RowStream interface {
	// Next returns io.EOF as the only successful end of stream.
	Next(context.Context) ([]any, error)
	Close() error
}

// ReadSnapshot exposes verified immutable metadata plus restricted row reads.
type ReadSnapshot interface {
	Manifest() replay.SafeSnapshotManifest
	SchemaArtifact() replay.AuthenticatedSnapshotQuerySchemaV1
	Schemas() []payloadexec.TableSchema
	Relations() []Relation
	QueryRows(context.Context, string, payloadexec.TableSchema) (RowStream, error)
	// Close releases the local scratch handle. It does not drop the durable
	// replay/challenge reference owned by the caller's referenceID.
	Close() error
}

// VerifiedPart is one download-verified immutable part. LocalPath is a regular
// file already checked by the caller against PartPhysHash; Restore rechecks.
type VerifiedPart struct {
	Entry     replay.PartManifestEntry
	LocalPath string
}

// ScratchRestorer materializes authenticated read relations onto restricted
// scratch handles. It does not publish artifacts or mint current-use references.
type ScratchRestorer interface {
	Restore(ctx context.Context, manifest replay.SafeSnapshotManifest, schemaArtifact replay.AuthenticatedSnapshotQuerySchemaV1, schemas []payloadexec.TableSchema, reads replay.SnapshotReadSet, parts []VerifiedPart) (ReadSnapshot, error)
}
