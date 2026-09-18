package snapshotquery

import "github.com/housegate/housegate/pkg/replay"

// Copies preserve nil versus empty legacy collections and isolate every mutable
// layer. Callers must not mutate inputs concurrently with ownership transfer.
func copySlice[T any](in []T) []T {
	if in == nil {
		return nil
	}
	out := make([]T, len(in))
	copy(out, in)
	return out
}
func copyParts(in []replay.PartManifestEntry) []replay.PartManifestEntry {
	out := copySlice(in)
	for i := range out {
		out[i].StorageRefs = copySlice(out[i].StorageRefs)
	}
	return out
}
func cloneExecutionRequest(r replay.ExecutionRequest) replay.ExecutionRequest {
	if r.SnapshotQuery != nil {
		j := cloneJob(*r.SnapshotQuery)
		r.SnapshotQuery = &j
	}
	r.Job.Statements = copySlice(r.Job.Statements)
	r.Statements = copySlice(r.Statements)
	for i := range r.Statements {
		r.Statements[i].Payload = copySlice(r.Statements[i].Payload)
	}
	r.Snapshot.Tables = copySlice(r.Snapshot.Tables)
	for i := range r.Snapshot.Tables {
		t := &r.Snapshot.Tables[i]
		t.PartitionRoots = copySlice(t.PartitionRoots)
		t.ActiveParts = copyParts(t.ActiveParts)
	}
	return r
}
func cloneExecutionResult(r replay.ExecutionResult) replay.ExecutionResult {
	if r.SnapshotQuery != nil {
		evidence := *r.SnapshotQuery
		r.SnapshotQuery = &evidence
	}
	r.PartitionCommitmentsAfter = copySlice(r.PartitionCommitmentsAfter)
	r.AffectedParts = copyParts(r.AffectedParts)
	return r
}
