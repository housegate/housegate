package snapshotquery

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/housegate/housegate/pkg/replay"
)

// QueryProfileKey selects an exact retained executable pair, never an endpoint.
type QueryProfileKey struct{ ExecutorProfileID, QueryProfileID string }

// QueryRoute preserves duplicate entries until constructor validation.
type QueryRoute struct {
	Key      QueryProfileKey
	Executor replay.Executor
}

// CompositeExecutor freezes routes, not the internals of borrowed executors.
// Their implementations must remain fixed trusted configuration. Each pair
// supports one B4 pin/O/name scope; the outer owner selects the historical core.
// Dispatch itself provides neither historical nor current-use authority.
type CompositeExecutor struct {
	payload        replay.Executor
	queryByProfile map[QueryProfileKey]replay.Executor
}

func NewCompositeExecutor(payload replay.Executor, routes []QueryRoute) (*CompositeExecutor, error) {
	if payload != nil && isNil(payload) {
		return nil, fmt.Errorf("payload executor is typed nil")
	}
	if payload == nil && len(routes) == 0 {
		return nil, fmt.Errorf("at least one executor is required")
	}
	d := &CompositeExecutor{payload: payload, queryByProfile: make(map[QueryProfileKey]replay.Executor, len(routes))}
	for _, route := range routes {
		k := route.Key
		if k.ExecutorProfileID == "" || strings.TrimSpace(k.ExecutorProfileID) != k.ExecutorProfileID || !canonicalDigest(k.QueryProfileID) || isNil(route.Executor) {
			return nil, fmt.Errorf("invalid query profile route")
		}
		if _, exists := d.queryByProfile[k]; exists {
			return nil, fmt.Errorf("duplicate query profile route")
		}
		d.queryByProfile[k] = route.Executor
	}
	return d, nil
}

func (d *CompositeExecutor) Replay(ctx context.Context, req replay.ExecutionRequest) (replay.ExecutionResult, error) {
	if d == nil || (isNil(d.payload) && len(d.queryByProfile) == 0) {
		return replay.ExecutionResult{}, fmt.Errorf("uninitialized composite executor")
	}
	if ctx == nil {
		return replay.ExecutionResult{}, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return replay.ExecutionResult{}, err
	}
	var executor replay.Executor
	if req.SnapshotQuery != nil {
		if strings.TrimSpace(req.SnapshotQueryReferenceID) == "" || !reflect.DeepEqual(req.Job, replay.ReplayJob{}) || !reflect.DeepEqual(req.Snapshot, replay.SafeSnapshotManifest{}) || req.Statements != nil {
			return replay.ExecutionResult{}, fmt.Errorf("query requires its exact reference and no legacy fields")
		}
		executor = d.queryByProfile[QueryProfileKey{req.SnapshotQuery.ExecutorProfileID, req.SnapshotQuery.QueryProfileID}]
		if isNil(executor) {
			return replay.ExecutionResult{}, fmt.Errorf("snapshot query profile route unavailable")
		}
	} else {
		if req.SnapshotQueryReferenceID != "" {
			return replay.ExecutionResult{}, fmt.Errorf("legacy replay cannot carry a query reference")
		}
		executor = d.payload
		if isNil(executor) {
			return replay.ExecutionResult{}, fmt.Errorf("payload executor unavailable")
		}
	}
	result, err := executor.Replay(ctx, cloneExecutionRequest(req))
	if err != nil {
		return replay.ExecutionResult{}, err
	}
	if err = ctx.Err(); err != nil {
		return replay.ExecutionResult{}, err
	}
	return cloneExecutionResult(result), nil
}
