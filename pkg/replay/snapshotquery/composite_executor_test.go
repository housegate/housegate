package snapshotquery

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

type consumerExecutor func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error)

func (f consumerExecutor) Replay(ctx context.Context, r replay.ExecutionRequest) (replay.ExecutionResult, error) {
	return f(ctx, r)
}

func TestCompositeConstructorAndExactRoutes(t *testing.T) {
	ctx := context.Background()
	key := QueryProfileKey{"old-executor", replay.DigestString("old-query")}
	calls, payloadCalls := 0, 0
	ex := consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) {
		calls++
		return replay.ExecutionResult{BlockSeq: 7}, nil
	})
	payload := consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) {
		payloadCalls++
		return replay.ExecutionResult{BlockSeq: 8}, nil
	})
	routes := []QueryRoute{{key, ex}}
	d, err := NewCompositeExecutor(payload, routes)
	if err != nil {
		t.Fatal(err)
	}
	routes[0].Executor = payload
	routes[0].Key.QueryProfileID = replay.DigestString("current")
	req := replay.ExecutionRequest{SnapshotQuery: &replay.SnapshotQueryJob{ExecutorProfileID: key.ExecutorProfileID, QueryProfileID: key.QueryProfileID}, SnapshotQueryReferenceID: " exact reference "}
	result, err := d.Replay(ctx, req)
	if err != nil || result.BlockSeq != 7 || calls != 1 || payloadCalls != 0 {
		t.Fatal(result, err, calls, payloadCalls)
	}
	req.SnapshotQuery.QueryProfileID = replay.DigestString("unknown")
	if _, err = d.Replay(ctx, req); err == nil || calls != 1 || payloadCalls != 0 {
		t.Fatal("unknown route fell back", err)
	}
	if _, err = d.Replay(ctx, replay.ExecutionRequest{Job: replay.ReplayJob{BlockSeq: 1}}); err != nil || payloadCalls != 1 {
		t.Fatal(err)
	}
	for name, r := range map[string][]QueryRoute{
		"duplicate": {{key, ex}, {key, ex}}, "nil": {{key, nil}}, "typed-nil": {{key, consumerExecutor(nil)}},
		"empty-executor": {{QueryProfileKey{"", key.QueryProfileID}, ex}}, "spaced-executor": {{QueryProfileKey{" old", key.QueryProfileID}, ex}},
		"invalid-query": {{QueryProfileKey{key.ExecutorProfileID, "old"}, ex}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewCompositeExecutor(payload, r); err == nil {
				t.Fatal("invalid route accepted")
			}
		})
	}
	if _, err := NewCompositeExecutor(nil, nil); err == nil {
		t.Fatal("empty dispatcher")
	}
	if _, err := NewCompositeExecutor(consumerExecutor(nil), nil); err == nil {
		t.Fatal("typed-nil payload")
	}
	legacy, err := NewCompositeExecutor(payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.Replay(ctx, replay.ExecutionRequest{Job: replay.ReplayJob{BlockSeq: 1}}); err != nil {
		t.Fatal(err)
	}
	query, err := NewCompositeExecutor(nil, []QueryRoute{{key, ex}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = query.Replay(ctx, replay.ExecutionRequest{Job: replay.ReplayJob{BlockSeq: 1}}); err == nil {
		t.Fatal("missing payload accepted")
	}
}

func TestCompositeRefusesMixedVariantsAndFailures(t *testing.T) {
	key := QueryProfileKey{"executor", replay.DigestString("query")}
	calls := 0
	ex := consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) {
		calls++
		return replay.ExecutionResult{BlockSeq: 1}, errors.New("execution failed")
	})
	d, _ := NewCompositeExecutor(ex, []QueryRoute{{key, ex}})
	req := replay.ExecutionRequest{SnapshotQuery: &replay.SnapshotQueryJob{ExecutorProfileID: key.ExecutorProfileID, QueryProfileID: key.QueryProfileID}, SnapshotQueryReferenceID: "reference"}
	for _, change := range []func(*replay.ExecutionRequest){
		func(r *replay.ExecutionRequest) { r.Job.BlockSeq = 1 }, func(r *replay.ExecutionRequest) { r.Snapshot.StateRoot = "root" },
		func(r *replay.ExecutionRequest) { r.Statements = []replay.PreparedStatement{} }, func(r *replay.ExecutionRequest) { r.SnapshotQueryReferenceID = " \t" },
		func(r *replay.ExecutionRequest) { r.SnapshotQuery = nil },
	} {
		r := req
		change(&r)
		if _, err := d.Replay(context.Background(), r); err == nil {
			t.Fatal("mixed request accepted")
		}
	}
	if calls != 0 {
		t.Fatal(calls)
	}
	result, err := d.Replay(context.Background(), req)
	if err == nil || !reflect.DeepEqual(result, replay.ExecutionResult{}) || calls != 1 {
		t.Fatal("partial result escaped", result, err)
	}
	if _, err = d.Replay(nil, req); err == nil {
		t.Fatal("nil context")
	}
	var zero CompositeExecutor
	if _, err = zero.Replay(context.Background(), req); err == nil {
		t.Fatal("zero dispatcher")
	}
}

func TestCompositeOwnsLegacyRequestAndResult(t *testing.T) {
	req := replay.ExecutionRequest{Job: replay.ReplayJob{BlockSeq: 4, Statements: []replay.Statement{{SQL: "original"}}}, Statements: []replay.PreparedStatement{{Payload: []byte{1, 2}}}, Snapshot: replay.SafeSnapshotManifest{Tables: []replay.TableManifest{{PartitionRoots: []replay.PartitionCommitment{{Root: "original"}}, ActiveParts: []replay.PartManifestEntry{{StorageRefs: []string{"hint"}}}}}}}
	original := cloneExecutionRequest(req)
	result := replay.ExecutionResult{SnapshotQuery: &replay.SnapshotQueryEvidence{OutputRowCount: 3}, PartitionCommitmentsAfter: []replay.PartitionCommitment{{Root: "original"}}, AffectedParts: []replay.PartManifestEntry{{StorageRefs: []string{"hint"}}}}
	ex := consumerExecutor(func(_ context.Context, r replay.ExecutionRequest) (replay.ExecutionResult, error) {
		if !reflect.DeepEqual(r, original) {
			t.Fatal("legacy request changed")
		}
		r.Job.Statements[0].SQL = "changed"
		r.Statements[0].Payload[0] = 9
		r.Snapshot.Tables[0].PartitionRoots[0].Root = "changed"
		r.Snapshot.Tables[0].ActiveParts[0].StorageRefs[0] = "changed"
		return result, nil
	})
	d, _ := NewCompositeExecutor(ex, nil)
	got, err := d.Replay(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(req, original) {
		t.Fatal("caller input aliased")
	}
	result.SnapshotQuery.OutputRowCount = 99
	result.PartitionCommitmentsAfter[0].Root = "changed"
	result.AffectedParts[0].StorageRefs[0] = "changed"
	if got.SnapshotQuery.OutputRowCount != 3 || got.PartitionCommitmentsAfter[0].Root != "original" || got.AffectedParts[0].StorageRefs[0] != "hint" {
		t.Fatal("result aliased")
	}
	for _, r := range []replay.ExecutionRequest{{}, {Job: replay.ReplayJob{Statements: []replay.Statement{}}, Statements: []replay.PreparedStatement{}, Snapshot: replay.SafeSnapshotManifest{Tables: []replay.TableManifest{}}}} {
		if !reflect.DeepEqual(r, cloneExecutionRequest(r)) {
			t.Fatal("nil/empty legacy shape changed")
		}
	}
}

func TestCompositeOwnsNestedQuery(t *testing.T) {
	f, _, _, _ := consumerSetup(t)
	j := cloneJob(f.job)
	j.Statement.Envelope.Input.ReadSet.Tables[0].PartitionRoots = []replay.PartitionCommitment{{Root: "read-root"}}
	j.Statement.Envelope.Input.ReadSet.Tables[0].ActiveParts = []replay.SnapshotReadPart{{PartName: "read-part"}}
	j.SourceClaim.PartitionDeltas = []replay.PartitionCommitment{{Root: "delta"}}
	original := cloneJob(j)
	ex := consumerExecutor(func(_ context.Context, r replay.ExecutionRequest) (replay.ExecutionResult, error) {
		if r.SnapshotQueryReferenceID != " exact ref " {
			t.Fatal("reference normalized")
		}
		q := r.SnapshotQuery
		q.Statement.Envelope.Input.ReadSet.Tables[0].PartitionRoots[0].Root = "changed"
		q.Statement.Envelope.Input.ReadSet.Tables[0].ActiveParts[0].PartName = "changed"
		q.SourceClaim.PartitionDeltas[0].Root = "changed"
		q.SourceClaim.PartitionCommitmentsAfter[0].Root = "changed"
		q.SourceClaim.CandidateParts[0].PartName = "changed"
		return replay.ExecutionResult{}, nil
	})
	d, _ := NewCompositeExecutor(nil, []QueryRoute{{QueryProfileKey{j.ExecutorProfileID, j.QueryProfileID}, ex}})
	if _, err := d.Replay(context.Background(), replay.ExecutionRequest{SnapshotQuery: &j, SnapshotQueryReferenceID: " exact ref "}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(j, original) {
		t.Fatal("nested query input aliased")
	}
}

func TestCompositeOwnsTableSetJobFields(t *testing.T) {
	fresh := func() replay.ExecutionRequest {
		return replay.ExecutionRequest{Job: replay.ReplayJob{
			BlockSeq:           5,
			TableSchemas:       []replay.ReplayTableSchema{{TableID: "db.a", SchemaJSON: "a"}},
			TableSetTransition: &replay.ReplayTableSetTransition{Adds: []replay.ReplayTableSchema{{TableID: "db.b", SchemaJSON: "b"}}, Retires: []string{"db.c"}, NewSchemaRoot: "root"},
		}}
	}
	req, original := fresh(), fresh()
	ex := consumerExecutor(func(_ context.Context, r replay.ExecutionRequest) (replay.ExecutionResult, error) {
		if !reflect.DeepEqual(r, original) {
			t.Fatal("table-set job fields changed in transit")
		}
		r.Job.TableSchemas[0].SchemaJSON = "changed"
		r.Job.TableSetTransition.Adds[0].SchemaJSON = "changed"
		r.Job.TableSetTransition.Retires[0] = "changed"
		r.Job.TableSetTransition.NewSchemaRoot = "changed"
		return replay.ExecutionResult{}, nil
	})
	d, _ := NewCompositeExecutor(ex, nil)
	if _, err := d.Replay(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(req, original) {
		t.Fatal("caller table-set job fields aliased")
	}
}
