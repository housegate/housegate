package network

// rpc.go — JSON-RPC-backed read-only NetworkState consumer for sidecar
// mode. Hits a sentio storage-node JSON-RPC endpoint (e.g.
// http://node.example.com:10003) and translates four sentio_* methods to
// the State interface methods sidecar.Selector consults:
//
//   sentio_getIndexerInfos          -> RetrieveAllIndexerInfos
//   sentio_getIndexerInfoById       -> RetrieveIndexerInfo
//   sentio_getDatabaseInfoById      -> RetrieveDatabaseInfo
//   sentio_getDatabaseInfoByAccount -> RetrieveDatabasePermissions
//
// Methods used only by server mode (RetrieveProcessorAllocation,
// RetrieveProcessorInfo, RetrieveAllDatabaseInfos,
// AccountHasPermissionForDatabase) return zero values + false; this
// backend is intentionally restricted to sidecar use today, and config
// validation rejects it in server mode.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"housegate/housegate/pkg/log"
	"sentioxyz/sentio-core/network/registry"
	"sentioxyz/sentio-core/network/state"
)

// RpcNetworkState is a read-only State backed by a sentio storage-node
// JSON-RPC endpoint. Concurrency-safe: nextID is atomic; the http
// client is goroutine-safe; no other state is held between calls.
type RpcNetworkState struct {
	endpoint string
	client   *http.Client
	nextID   atomic.Uint64
}

// RpcOptions tunes the RpcNetworkState http client. Zero values pick
// safe defaults (5 s timeout, default transport).
type RpcOptions struct {
	// Timeout caps each JSON-RPC call. Ignored when HTTPClient is set.
	Timeout time.Duration
	// HTTPClient overrides the internal http.Client. Useful for tests
	// that want to inject httptest.Server's RoundTripper or an
	// instrumented transport.
	HTTPClient *http.Client
}

// NewRpcNetworkState returns a State reading from the given JSON-RPC
// endpoint. The endpoint must be a full URL, e.g. http://host:port .
func NewRpcNetworkState(endpoint string, opts RpcOptions) (*RpcNetworkState, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("rpc network state: endpoint is required")
	}
	client := opts.HTTPClient
	if client == nil {
		t := opts.Timeout
		if t <= 0 {
			t = 5 * time.Second
		}
		client = &http.Client{Timeout: t}
	}
	return &RpcNetworkState{endpoint: endpoint, client: client}, nil
}

// jsonrpcRequest mirrors the JSON-RPC 2.0 request envelope.
type jsonrpcRequest struct {
	JsonRpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      uint64        `json:"id"`
}

// jsonrpcResponse mirrors the JSON-RPC 2.0 response envelope. Result is
// kept as RawMessage so callers decode into whatever shape the method
// returns (single object, list, null).
type jsonrpcResponse struct {
	JsonRpc string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *jsonrpcError   `json:"error,omitempty"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *jsonrpcError) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

// call executes a single JSON-RPC call. result must be a pointer to the
// shape the method returns; pass nil if the caller doesn't need it.
// Returns ok=false when the response.result is JSON null — distinguishes
// "method succeeded but returned no record" from a transport error.
func (r *RpcNetworkState) call(ctx context.Context, method string, params []interface{}, result interface{}) (bool, error) {
	if params == nil {
		params = []interface{}{}
	}
	body, err := json.Marshal(jsonrpcRequest{
		JsonRpc: "2.0",
		Method:  method,
		Params:  params,
		ID:      r.nextID.Add(1),
	})
	if err != nil {
		return false, fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("rpc http %d: %s", resp.StatusCode, raw)
	}
	var envelope jsonrpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return false, fmt.Errorf("decode response: %w", err)
	}
	if envelope.Error != nil {
		return false, envelope.Error
	}
	if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return false, nil
	}
	if result != nil {
		if err := json.Unmarshal(envelope.Result, result); err != nil {
			return false, fmt.Errorf("decode result: %w", err)
		}
	}
	return true, nil
}

// RetrieveAllIndexerInfos calls sentio_getIndexerInfos. Returns a non-nil
// (possibly empty) map on success; nil signals the call itself failed
// so callers can distinguish "no indexers" from "lookup error".
func (r *RpcNetworkState) RetrieveAllIndexerInfos() map[uint64]IndexerInfo {
	var infos []state.IndexerInfo
	ok, err := r.call(context.Background(), "sentio_getIndexerInfos", nil, &infos)
	if err != nil {
		log.Warnfe(err, "rpc: getIndexerInfos failed")
		return nil
	}
	out := make(map[uint64]IndexerInfo, len(infos))
	if !ok {
		return out
	}
	for _, info := range infos {
		out[info.IndexerId] = info
	}
	return out
}

// RetrieveIndexerInfo calls sentio_getIndexerInfoById. ok=false signals
// either a network-level error (logged) or a JSON-null result.
func (r *RpcNetworkState) RetrieveIndexerInfo(indexerId IndexId) (IndexerInfo, bool) {
	var info state.IndexerInfo
	ok, err := r.call(context.Background(), "sentio_getIndexerInfoById", []interface{}{indexerId}, &info)
	if err != nil {
		log.Warnfe(err, "rpc: getIndexerInfoById id=%v failed", indexerId)
		return IndexerInfo{}, false
	}
	if !ok {
		return IndexerInfo{}, false
	}
	return info, true
}

// RetrieveDatabaseInfo calls sentio_getDatabaseInfoById.
func (r *RpcNetworkState) RetrieveDatabaseInfo(database Database) (DatabaseInfo, bool) {
	var info state.DatabaseInfo
	ok, err := r.call(context.Background(), "sentio_getDatabaseInfoById", []interface{}{string(database)}, &info)
	if err != nil {
		log.Warnfe(err, "rpc: getDatabaseInfoById id=%v failed", database)
		return DatabaseInfo{}, false
	}
	if !ok {
		return DatabaseInfo{}, false
	}
	return info, true
}

// RetrieveDatabasePermissions calls sentio_getDatabaseInfoByAccount and
// returns a map granting DbAuthOwner on every database the account owns.
// The RPC returns a list (an account can own multiple databases across
// indexers); the schema does not yet model a per-account permission
// bitmap, so sidecar.Selector only needs "which databases is this
// account allowed to query at all", which is satisfied by mapping each
// owned database to DbAuthOwner (Owner implies all capabilities).
//
// An account with no database returns (empty-but-non-nil map, true) —
// matches the State contract: ok=false is reserved for lookup failure.
func (r *RpcNetworkState) RetrieveDatabasePermissions(account AccountAddress) (DatabasePermissions, bool) {
	var infos []state.DatabaseInfo
	ok, err := r.call(context.Background(), "sentio_getDatabaseInfoByAccount", []interface{}{string(account)}, &infos)
	if err != nil {
		log.Warnfe(err, "rpc: getDatabaseInfoByAccount account=%v failed", account)
		return nil, false
	}
	out := make(DatabasePermissions, len(infos))
	if !ok {
		return out, true
	}
	for _, info := range infos {
		if info.DatabaseId == "" {
			continue
		}
		out[Database(info.DatabaseId)] = registry.DbAuthOwner
	}
	return out, true
}

// RetrieveProcessorAllocation: not supported by the RPC backend.
// Sidecar mode never calls this; server-mode wiring is rejected by
// config validation.
func (r *RpcNetworkState) RetrieveProcessorAllocation(processorId ProcessorId) ([]ProcessorAllocation, bool) {
	return nil, false
}

// RetrieveProcessorInfo: not supported by the RPC backend (server-mode only).
func (r *RpcNetworkState) RetrieveProcessorInfo(processorId ProcessorId) (ProcessorInfo, bool) {
	return ProcessorInfo{}, false
}

// RetrieveAllDatabaseInfos: not supported — the RPC has no enumeration
// method. Returns an empty map so callers that range over it stay safe.
func (r *RpcNetworkState) RetrieveAllDatabaseInfos() map[Database]DatabaseInfo {
	return map[Database]DatabaseInfo{}
}

// AccountHasPermissionForDatabase: not supported by the RPC backend.
// Server-mode-only entrypoint; sidecar.Selector consults
// RetrieveDatabasePermissions directly.
func (r *RpcNetworkState) AccountHasPermissionForDatabase(account AccountAddress, database Database, action registry.Action) (bool, error) {
	return false, fmt.Errorf("rpc network state: AccountHasPermissionForDatabase not supported")
}

// IsOperator: not supported by the RPC backend. The server-side
// PermissionCommitGateObserver runs against RedisNetworkState (which
// reads from the on-chain mirror); sidecar mode never invokes this
// method.
func (r *RpcNetworkState) IsOperator(owner, signer AccountAddress) bool {
	return owner != "" && owner == signer
}

// Type returns StateTypeRpc.
func (r *RpcNetworkState) Type() StateType { return StateTypeRpc }

// Compile-time check the rpc backend satisfies State.
var _ State = (*RpcNetworkState)(nil)
