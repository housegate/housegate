package network

// redis.go — Redis-statemirror-backed NetworkState. Uses sentio-core's
// ProcessorRegistry for allocation/processor lookups, DbRegistry for
// database/permission lookups, and reads IndexerInfo directly from the
// underlying statemirror.

import (
	"context"
	"encoding/json"
	"fmt"

	"sentioxyz/sentio-core/common/log"
	"sentioxyz/sentio-core/common/statemirror"
	"sentioxyz/sentio-core/network/registry"
	"sentioxyz/sentio-core/network/state"

	"github.com/redis/go-redis/v9"
)

// RedisNetworkState reads from the statemirror schema that the sentio
// analytic-service also consumes, ensuring the proxy sees the same
// values with zero schema drift.
type RedisNetworkState struct {
	registry    registry.ProcessorRegistry
	dbRegistry  registry.DbRegistry
	redisClient *redis.Client
	mirror      statemirror.Mirror
}

// NewRedisNetworkState wraps an existing redis client as a State.
// The client is retained for Close to tear down.
func NewRedisNetworkState(client *redis.Client) (*RedisNetworkState, error) {
	mirror := statemirror.NewRedisMirror(client)
	return &RedisNetworkState{
		registry:    registry.NewProcessorRegistry(mirror),
		dbRegistry:  registry.NewUserDbRegistry(mirror),
		redisClient: client,
		mirror:      mirror,
	}, nil
}

func (r *RedisNetworkState) RetrieveProcessorAllocation(processorId ProcessorId) ([]ProcessorAllocation, bool) {
	ctx := context.Background()
	allocs, err := r.registry.RetrieveProcessorAllocation(ctx, processorId)
	if err != nil {
		// sentio-core ProcessorRegistry logs at "error" level when a
		// processor allocation is missing; in the proxy a miss is
		// benign (query passes through without rewrite), so we downgrade.
		log.Warnw("processor allocation not found, query will pass through without rewrite", "processor_id", processorId)
		return nil, false
	}
	result := make([]ProcessorAllocation, len(allocs))
	for i, a := range allocs {
		result[i] = ProcessorAllocation{
			ProcessorId: a.ProcessorId,
			IndexerId:   a.IndexerId,
		}
	}
	return result, true
}

func (r *RedisNetworkState) RetrieveIndexerInfo(indexerId IndexId) (IndexerInfo, bool) {
	ctx := context.Background()
	field := fmt.Sprintf("%d", indexerId)
	value, ok, err := r.mirror.Get(ctx, statemirror.MappingIndexerInfos, field)
	if err != nil || !ok {
		return IndexerInfo{}, false
	}
	var info state.IndexerInfo
	if err := decodeJSON(value, &info); err != nil {
		log.Warnfe(err, "failed to decode IndexerInfo indexer_id=%v", indexerId)
		return IndexerInfo{}, false
	}
	return IndexerInfo{
		IndexerId:           info.IndexerId,
		IndexerUrl:          info.IndexerUrl,
		ComputeNodeRpcPort:  info.ComputeNodeRpcPort,
		StorageNodeRpcPort:  info.StorageNodeRpcPort,
		ClickhouseProxyPort: info.ClickhouseProxyPort,
	}, true
}

func (r *RedisNetworkState) RetrieveProcessorInfo(processorId ProcessorId) (ProcessorInfo, bool) {
	ctx := context.Background()
	info, err := r.registry.RetrieveProcessorInfo(ctx, processorId)
	if err != nil {
		log.Warnfe(err, "failed to get processor info processor_id=%v", processorId)
		return ProcessorInfo{}, false
	}
	return ProcessorInfo{
		ProcessorId:         info.ProcessorId,
		EntitySchema:        info.EntitySchema,
		EntitySchemaVersion: info.EntitySchemaVersion,
	}, true
}

func (r *RedisNetworkState) RetrieveAllIndexerInfos() map[uint64]IndexerInfo {
	ctx := context.Background()
	all, err := r.mirror.GetAll(ctx, statemirror.MappingIndexerInfos)
	if err != nil {
		log.Warne(err, "failed to get all indexer infos")
		return nil
	}
	result := make(map[uint64]IndexerInfo, len(all))
	for field, value := range all {
		id, err := ParseIndexerId(field)
		if err != nil {
			log.Warnfe(err, "failed to parse indexer id field=%v", field)
			continue
		}
		var info state.IndexerInfo
		if err := decodeJSON(value, &info); err != nil {
			log.Warnfe(err, "failed to decode IndexerInfo field=%v", field)
			continue
		}
		result[id] = IndexerInfo{
			IndexerId:           info.IndexerId,
			IndexerUrl:          info.IndexerUrl,
			ComputeNodeRpcPort:  info.ComputeNodeRpcPort,
			StorageNodeRpcPort:  info.StorageNodeRpcPort,
			ClickhouseProxyPort: info.ClickhouseProxyPort,
		}
	}
	return result
}

func (r *RedisNetworkState) RetrieveDatabaseInfo(database Database) (DatabaseInfo, bool) {
	ctx := context.Background()
	info, err := r.dbRegistry.RetrieveDatabaseInfo(ctx, database)
	if err != nil {
		// DbRegistry already logs at error level; soften the proxy-side
		// signal because callers treat the bool as "do we know about
		// this database" rather than "is the network healthy".
		log.Debugw("database info not available", "database", database, "err", err.Error())
		return DatabaseInfo{}, false
	}
	return info, true
}

func (r *RedisNetworkState) RetrieveAllDatabaseInfos() map[Database]DatabaseInfo {
	ctx := context.Background()
	all, err := r.mirror.GetAll(ctx, statemirror.MappingDatabases)
	if err != nil {
		log.Warne(err, "failed to get all database infos")
		return nil
	}
	result := make(map[Database]DatabaseInfo, len(all))
	for field, value := range all {
		var info state.DatabaseInfo
		if err := decodeJSON(value, &info); err != nil {
			log.Warnfe(err, "failed to decode DatabaseInfo field=%v", field)
			continue
		}
		result[Database(field)] = info
	}
	return result
}

func (r *RedisNetworkState) RetrieveDatabasePermissions(account AccountAddress) (DatabasePermissions, bool) {
	ctx := context.Background()
	perms, err := r.dbRegistry.RetrievePermissionsByAccount(ctx, account)
	if err != nil {
		log.Warnfe(err, "failed to retrieve database permissions account=%v", account)
		return nil, false
	}
	// DbRegistry returns a non-nil empty map when the account has no
	// permissions on record; surface that as (empty, true) so callers
	// can distinguish "looked up but empty" from "lookup failed".
	return perms, true
}

func (r *RedisNetworkState) AccountHasPermissionForDatabase(account AccountAddress, database Database, action registry.Action) (bool, error) {
	ctx := context.Background()
	return r.dbRegistry.AccountHasPermission(ctx, account, database, action)
}

// Close tears down the underlying Redis connection.
func (r *RedisNetworkState) Close() error {
	return r.redisClient.Close()
}

func (r *RedisNetworkState) Type() StateType {
	return StateTypeRedis
}

func decodeJSON(s string, target interface{}) error {
	return json.Unmarshal([]byte(s), target)
}

// Compile-time check the production wrapper satisfies State.
var _ State = (*RedisNetworkState)(nil)
