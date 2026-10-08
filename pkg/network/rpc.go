package network

// rpc.go — JSON-RPC-backed read-only registry.Registry for agent
// mode. Hits a sentio storage-node JSON-RPC endpoint (e.g.
// http://node.example.com:10003) and translates sentio_* methods to the
// registry.Registry methods agent.Selector consults:
//
//   sentio_getIndexerInfos          -> AllIndexers
//   sentio_getIndexerInfoById       -> ProxyByIndexerId
//   sentio_getDatabaseInfoById      -> Get
//   sentio_getDatabaseInfoByAccount -> PermissionsFor
//
// plus the storage-integrity discovery methods, answered by the indexer that
// hosts the database (registry.TableStatuses, registry.StorageIntegrityDiscovery):
//
//   sentio_getStorageIntegrityTableStatus -> StorageIntegrityTableStatus
//   sentio_getStorageIntegrityInfo        -> StorageIntegrityInfo
//   sentio_isDatabaseWriter               -> StorageIntegrityWriterCheck
//
// Methods that have no JSON-RPC counterpart (All, HasPermission)
// return zero-value/error responses. This backend is intentionally
// restricted to agent use; config validation rejects it in server
// mode where the missing methods would be exercised.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/registry"
)

// RpcNetworkState is a read-only registry.Registry backed by a sentio
// storage-node JSON-RPC endpoint. Concurrency-safe: nextID is atomic;
// the http client is goroutine-safe; no other state is held between
// calls.
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

// NewRpcNetworkState returns a registry.Registry reading from the
// given JSON-RPC endpoint. The endpoint must be a full URL, e.g.
// http://host:port .
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
	return r.callAt(ctx, r.endpoint, method, params, result)
}

// callAt is call against an explicit endpoint (the indexer hosting a
// database, for the storage-integrity discovery methods).
func (r *RpcNetworkState) callAt(ctx context.Context, endpoint, method string, params []interface{}, result interface{}) (bool, error) {
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
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

// --- registry.Topology

// AllIndexers calls sentio_getIndexerInfos. Returns a non-nil
// (possibly empty) map on success; nil signals the call itself
// failed so callers can distinguish "no indexers" from "lookup
// error".
func (r *RpcNetworkState) AllIndexers() map[uint64]registry.ProxyAddress {
	var infos []IndexerInfo
	ok, err := r.call(context.Background(), "sentio_getIndexerInfos", nil, &infos)
	if err != nil {
		log.Warnfe(err, "rpc: getIndexerInfos failed")
		return nil
	}
	out := make(map[uint64]registry.ProxyAddress, len(infos))
	if !ok {
		return out
	}
	for _, info := range infos {
		out[info.IndexerId] = registry.ProxyAddress{
			Url:            info.IndexerUrl,
			HousegatePort:  info.ClickhouseProxyPort,
			StorageRPCPort: info.StorageNodeRpcPort,
		}
	}
	return out
}

// ProxyByIndexerId calls sentio_getIndexerInfoById. ok=false signals
// either a network-level error (logged) or a JSON-null result.
func (r *RpcNetworkState) ProxyByIndexerId(indexerId uint64) (registry.ProxyAddress, bool) {
	var info IndexerInfo
	ok, err := r.call(context.Background(), "sentio_getIndexerInfoById", []interface{}{indexerId}, &info)
	if err != nil {
		log.Warnfe(err, "rpc: getIndexerInfoById id=%v failed", indexerId)
		return registry.ProxyAddress{}, false
	}
	if !ok {
		return registry.ProxyAddress{}, false
	}
	return registry.ProxyAddress{
		Url:            info.IndexerUrl,
		HousegatePort:  info.ClickhouseProxyPort,
		StorageRPCPort: info.StorageNodeRpcPort,
	}, true
}

// --- registry.Databases

// Get calls sentio_getDatabaseInfoById.
func (r *RpcNetworkState) Get(database string) (registry.Database, bool) {
	var info DatabaseInfo
	ok, err := r.call(context.Background(), "sentio_getDatabaseInfoById", []interface{}{database}, &info)
	if err != nil {
		log.Warnfe(err, "rpc: getDatabaseInfoById id=%v failed", database)
		return registry.Database{}, false
	}
	if !ok {
		return registry.Database{}, false
	}
	return convertDatabase(info), true
}

// All has no JSON-RPC counterpart (no enumeration method); returns an
// empty map so callers that range over it stay safe.
func (r *RpcNetworkState) All() map[string]registry.Database {
	return map[string]registry.Database{}
}

// --- registry.Access

// PermissionsFor calls sentio_getDatabaseInfoByAccount and returns a
// map granting DbAuthOwner on every database the account owns. The
// RPC returns a list (an account can own multiple databases across
// indexers); the schema does not yet model a per-account permission
// bitmap, so agent.Selector only needs "which databases is this
// account allowed to query at all", which is satisfied by mapping
// each owned database to DbAuthOwner (Owner implies all capabilities).
//
// An account with no database returns (empty-but-non-nil map, true) —
// matches the registry.Access contract: ok=false is reserved for
// lookup failure.
func (r *RpcNetworkState) PermissionsFor(account string) (map[string]registry.DbAuth, bool) {
	var infos []DatabaseInfo
	ok, err := r.call(context.Background(), "sentio_getDatabaseInfoByAccount", []interface{}{account}, &infos)
	if err != nil {
		log.Warnfe(err, "rpc: getDatabaseInfoByAccount account=%v failed", account)
		return nil, false
	}
	out := make(map[string]registry.DbAuth, len(infos))
	if !ok {
		return out, true
	}
	for _, info := range infos {
		if info.DatabaseId == "" {
			continue
		}
		out[info.DatabaseId] = registry.DbAuthOwner
	}
	return out, true
}

// HasPermission has no JSON-RPC counterpart in the agent mode this
// backend serves; server-side gates run against the redis-statemirror
// path instead. Always returns an error.
func (r *RpcNetworkState) HasPermission(account, database string, action registry.Action) (bool, error) {
	return false, fmt.Errorf("rpc network state: HasPermission not supported")
}

// IsOperator has no JSON-RPC counterpart. Returns true only for the
// trivial owner==signer case; the server-side
// PermissionCommitGateObserver runs against the redis-mirror path
// (sentio-node's adapter) where IsOperator is fully supported.
func (r *RpcNetworkState) IsOperator(owner, signer string) bool {
	return owner != "" && owner == signer
}

// --- registry.TableStatuses

// storageEndpointFor returns the storage RPC endpoint of the indexer hosting
// database, falling back to the bootstrap endpoint only when the database is
// unknown (spec 2026-10-09 §6.4 step 2).
func (r *RpcNetworkState) storageEndpointFor(ctx context.Context, database string) (string, error) {
	endpoint, hosted, err := r.hostingStorageEndpoint(ctx, database)
	if err != nil {
		return "", err
	}
	if !hosted {
		return r.endpoint, nil
	}
	return endpoint, nil
}

// hostingStorageEndpoint returns the storage RPC endpoint of the indexer
// hosting database; hosted=false (and no error) means the database is
// unknown to the registry.
func (r *RpcNetworkState) hostingStorageEndpoint(ctx context.Context, database string) (endpoint string, hosted bool, err error) {
	var db DatabaseInfo
	ok, err := r.call(ctx, "sentio_getDatabaseInfoById", []interface{}{database}, &db)
	if err != nil {
		// Not "unknown": the bootstrap may not host the database, so its
		// answer could be wrong. The caller treats the error as a failed
		// lookup (status: unsigned pass-through, the server answers 733).
		return "", false, fmt.Errorf("rpc: database %s lookup: %w", database, err)
	}
	if !ok {
		return "", false, nil
	}
	var info IndexerInfo
	ok, err = r.call(ctx, "sentio_getIndexerInfoById", []interface{}{db.IndexerId}, &info)
	if err != nil {
		return "", false, fmt.Errorf("rpc: indexer %d hosting %s: %w", db.IndexerId, database, err)
	}
	if !ok || info.IndexerUrl == "" || info.StorageNodeRpcPort == 0 {
		return "", false, fmt.Errorf("rpc: indexer %d hosting %s advertises no storage RPC endpoint", db.IndexerId, database)
	}
	scheme := "http"
	if u, err := url.Parse(r.endpoint); err == nil && u.Scheme != "" {
		scheme = u.Scheme
	}
	host := info.IndexerUrl
	if u, err := url.Parse(host); err == nil && u.Host != "" {
		host = u.Hostname() // tolerate a full URL in indexerUrl
	}
	return scheme + "://" + net.JoinHostPort(host, strconv.Itoa(int(info.StorageNodeRpcPort))), true, nil
}

// StorageIntegrityTableStatus calls sentio_getStorageIntegrityTableStatus
// (spec 2026-09-24 §10.2). A transport error, a JSON-null result, or an
// unknown status name is an error: the agent then passes the INSERT through
// unsigned and counts the failure.
func (r *RpcNetworkState) StorageIntegrityTableStatus(ctx context.Context, database, table string) (registry.TableStatus, error) {
	endpoint, err := r.storageEndpointFor(ctx, database)
	if err != nil {
		return registry.TableStatus{}, fmt.Errorf("rpc: getStorageIntegrityTableStatus %s.%s: %w", database, table, err)
	}
	var status registry.TableStatus
	ok, err := r.callAt(ctx, endpoint, "sentio_getStorageIntegrityTableStatus", []interface{}{database, table}, &status)
	if err != nil {
		return registry.TableStatus{}, fmt.Errorf("rpc: getStorageIntegrityTableStatus %s.%s: %w", database, table, err)
	}
	if !ok {
		return registry.TableStatus{}, fmt.Errorf("rpc: getStorageIntegrityTableStatus %s.%s returned null", database, table)
	}
	switch status.Status {
	case registry.TableStatusOrdinary, registry.TableStatusPending, registry.TableStatusRefused,
		registry.TableStatusActive, registry.TableStatusGone:
		return status, nil
	default:
		return registry.TableStatus{}, fmt.Errorf("rpc: getStorageIntegrityTableStatus %s.%s: unknown status %q", database, table, status.Status)
	}
}

var _ registry.TableStatuses = (*RpcNetworkState)(nil)

// StorageIntegrityInfo calls sentio_getStorageIntegrityInfo (no params) on the
// indexer hosting database (spec 2026-10-09 §6.7).
func (r *RpcNetworkState) StorageIntegrityInfo(ctx context.Context, database string) (registry.StorageIntegrityInfo, error) {
	endpoint, err := r.storageEndpointFor(ctx, database)
	if err != nil {
		return registry.StorageIntegrityInfo{}, err
	}
	var info registry.StorageIntegrityInfo
	ok, err := r.callAt(ctx, endpoint, "sentio_getStorageIntegrityInfo", nil, &info)
	if err != nil {
		return registry.StorageIntegrityInfo{}, fmt.Errorf("rpc: getStorageIntegrityInfo at %s: %w", endpoint, err)
	}
	if !ok {
		return registry.StorageIntegrityInfo{}, fmt.Errorf("rpc: getStorageIntegrityInfo at %s returned null", endpoint)
	}
	return info, nil
}

// StorageIntegrityWriterCheck calls sentio_isDatabaseWriter(database,
// account) on the hosting indexer (Plan A2). Errors mean "unknown". Unlike
// the status and info lookups it never asks the bootstrap: a database no
// indexer hosts answers registry.ErrDatabaseNotHosted, because a node that
// does not host the database cannot know its writers.
func (r *RpcNetworkState) StorageIntegrityWriterCheck(ctx context.Context, database, account string) (bool, error) {
	endpoint, hosted, err := r.hostingStorageEndpoint(ctx, database)
	if err != nil {
		return false, err
	}
	if !hosted {
		return false, fmt.Errorf("rpc: isDatabaseWriter %s: %w", database, registry.ErrDatabaseNotHosted)
	}
	var writer bool
	ok, err := r.callAt(ctx, endpoint, "sentio_isDatabaseWriter", []interface{}{database, account}, &writer)
	if err != nil {
		return false, fmt.Errorf("rpc: isDatabaseWriter at %s: %w", endpoint, err)
	}
	if !ok {
		return false, fmt.Errorf("rpc: isDatabaseWriter at %s returned null", endpoint)
	}
	return writer, nil
}

var _ registry.StorageIntegrityDiscovery = (*RpcNetworkState)(nil)

// Compile-time check the rpc backend satisfies registry.Registry.
var _ registry.Registry = (*RpcNetworkState)(nil)
