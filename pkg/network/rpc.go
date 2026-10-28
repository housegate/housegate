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
//   sentio_getStorageIntegrityInfo        -> StorageIntegrityInfo, StorageIntegrityReplicaHosts
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
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/registry"
)

// RpcNetworkState is a read-only registry.Registry backed by a sentio
// storage-node JSON-RPC endpoint. Concurrency-safe: nextID is atomic;
// the http client is goroutine-safe; the only state held between calls
// is the replica-hosts cache, guarded by replicaMu.
type RpcNetworkState struct {
	endpoint string
	client   *http.Client
	nextID   atomic.Uint64

	// now is the replica-hosts cache's clock; nil means time.Now.
	now          func() time.Time
	replicaMu    sync.Mutex
	replicaHosts map[string]replicaHostsEntry
}

// DefaultReplicaHostsTTL is how long RpcNetworkState reuses one database's
// replica_hosts answer (Plan S3-C4 K50).
const DefaultReplicaHostsTTL = 30 * time.Second

// replicaHostsEntry is a cached successful replica_hosts answer.
type replicaHostsEntry struct {
	hosts []uint64
	until time.Time
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
	return r.ProxyByIndexerIdContext(context.Background(), indexerId)
}

// ProxyByIndexerIdContext is the caller-context form used by replica failover.
func (r *RpcNetworkState) ProxyByIndexerIdContext(ctx context.Context, indexerId uint64) (registry.ProxyAddress, bool) {
	var info IndexerInfo
	ok, err := r.call(ctx, "sentio_getIndexerInfoById", []interface{}{indexerId}, &info)
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
	return r.GetContext(context.Background(), database)
}

// GetContext is the caller-context form used by replica failover.
func (r *RpcNetworkState) GetContext(ctx context.Context, database string) (registry.Database, bool) {
	var info DatabaseInfo
	ok, err := r.call(ctx, "sentio_getDatabaseInfoById", []interface{}{database}, &info)
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
	return r.storageEndpoint(info.IndexerUrl, info.StorageNodeRpcPort), true, nil
}

// storageEndpoint is the storage-node JSON-RPC endpoint an indexer
// advertises, with the scheme of the network-state source.
func (r *RpcNetworkState) storageEndpoint(indexerURL string, port uint16) string {
	scheme := "http"
	if u, err := url.Parse(r.endpoint); err == nil && u.Scheme != "" {
		scheme = u.Scheme
	}
	host := indexerURL
	if u, err := url.Parse(host); err == nil && u.Host != "" {
		host = u.Hostname() // tolerate a full URL in indexerUrl
	}
	return scheme + "://" + net.JoinHostPort(host, strconv.Itoa(int(port)))
}

// DatabaseHosting implements registry.DatabaseHosting with the separation
// hostingStorageEndpoint uses: an RPC error is an error, never "not hosted";
// a JSON-null or PendingDelete database is not hosted; a hosted database
// whose indexer is unknown or advertises no housegate address is an error.
func (r *RpcNetworkState) DatabaseHosting(ctx context.Context, database string) (registry.ProxyAddress, uint64, bool, error) {
	var db DatabaseInfo
	ok, err := r.call(ctx, "sentio_getDatabaseInfoById", []interface{}{database}, &db)
	if err != nil {
		return registry.ProxyAddress{}, 0, false, fmt.Errorf("rpc: database %s lookup: %w", database, err)
	}
	if !ok || db.PendingDelete {
		return registry.ProxyAddress{}, 0, false, nil
	}
	var info IndexerInfo
	ok, err = r.call(ctx, "sentio_getIndexerInfoById", []interface{}{db.IndexerId}, &info)
	if err != nil {
		return registry.ProxyAddress{}, 0, false, fmt.Errorf("rpc: indexer %d hosting %s: %w", db.IndexerId, database, err)
	}
	if !ok {
		return registry.ProxyAddress{}, 0, false, fmt.Errorf("rpc: indexer %d hosting %s is unknown", db.IndexerId, database)
	}
	return hostingProxyAddress(database, db.IndexerId, info)
}

// StorageIntegrityTableStatus calls sentio_getStorageIntegrityTableStatus
// (spec 2026-09-24 §10.2; the status replica and its additive fields, spec
// 2026-10-14 §11). A transport error, a JSON-null result, or an unknown
// status name is an error: the agent then passes the INSERT through unsigned
// and counts the failure.
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
		registry.TableStatusActive, registry.TableStatusGone, registry.TableStatusReplica:
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

// StorageIntegrityReplicaHosts implements
// registry.StorageIntegrityReplicaDiscovery (spec 2026-10-14 §11, Plan S3-C4
// K50). It returns replica_hosts[database] of the first decoded
// sentio_getStorageIntegrityInfo answer, an empty list included (an older
// server answers none), asking the network-state source endpoint first and
// then, in ascending id, the storage endpoint of every indexer but owner, so
// an unreachable owner never blocks the lookup; every host computes the same
// map. A successful answer is reused for DefaultReplicaHostsTTL per database;
// a failed lookup is not cached.
func (r *RpcNetworkState) StorageIntegrityReplicaHosts(ctx context.Context, database string, owner uint64) ([]uint64, error) {
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	at := now()
	r.replicaMu.Lock()
	if e, ok := r.replicaHosts[database]; ok && at.Before(e.until) {
		r.replicaMu.Unlock()
		return append([]uint64(nil), e.hosts...), nil
	}
	r.replicaMu.Unlock()
	hosts, ok := r.replicaHostsAt(ctx, r.endpoint, database)
	if !ok {
		hosts, ok = r.replicaHostsFromPeers(ctx, database, owner)
	}
	if !ok {
		return nil, fmt.Errorf("rpc: replica hosts of %s: no indexer answered sentio_getStorageIntegrityInfo", database)
	}
	r.replicaMu.Lock()
	if r.replicaHosts == nil {
		r.replicaHosts = make(map[string]replicaHostsEntry)
	}
	r.replicaHosts[database] = replicaHostsEntry{hosts: hosts, until: at.Add(DefaultReplicaHostsTTL)}
	r.replicaMu.Unlock()
	return append([]uint64(nil), hosts...), nil
}

// replicaHostsAt asks one endpoint; ok=false means it gave no decoded answer.
func (r *RpcNetworkState) replicaHostsAt(ctx context.Context, endpoint, database string) ([]uint64, bool) {
	var info registry.StorageIntegrityInfo
	ok, err := r.callAt(ctx, endpoint, "sentio_getStorageIntegrityInfo", nil, &info)
	if err != nil || !ok {
		log.Debugw("rpc: replica-hosts lookup failed at this endpoint", "endpoint", endpoint, "database", database, "err", err)
		return nil, false
	}
	return append([]uint64(nil), info.ReplicaHosts[database]...), true
}

// replicaHostsFromPeers asks, in ascending id, every indexer but owner that
// advertises a storage endpoint not asked yet.
func (r *RpcNetworkState) replicaHostsFromPeers(ctx context.Context, database string, owner uint64) ([]uint64, bool) {
	// AllIndexers uses a background context for the legacy topology port.
	// Use the same RPC with the failover caller's context here, so its
	// candidate lookup deadline also bounds this fallback's topology read.
	var infos []IndexerInfo
	ok, err := r.call(ctx, "sentio_getIndexerInfos", nil, &infos)
	if err != nil || !ok {
		return nil, false
	}
	all := make(map[uint64]registry.ProxyAddress, len(infos))
	for _, info := range infos {
		all[info.IndexerId] = registry.ProxyAddress{
			Url: info.IndexerUrl, HousegatePort: info.ClickhouseProxyPort,
			StorageRPCPort: info.StorageNodeRpcPort,
		}
	}
	ids := make([]uint64, 0, len(all))
	for id := range all {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	asked := map[string]bool{r.endpoint: true}
	for _, id := range ids {
		addr := all[id]
		if id == owner || addr.Url == "" || addr.StorageRPCPort == 0 {
			continue
		}
		endpoint := r.storageEndpoint(addr.Url, addr.StorageRPCPort)
		if asked[endpoint] {
			continue
		}
		asked[endpoint] = true
		if hosts, ok := r.replicaHostsAt(ctx, endpoint, database); ok {
			return hosts, true
		}
	}
	return nil, false
}

var _ registry.StorageIntegrityReplicaDiscovery = (*RpcNetworkState)(nil)

// Compile-time check the rpc backend satisfies registry.Registry.
var _ registry.Registry = (*RpcNetworkState)(nil)
