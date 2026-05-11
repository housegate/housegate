package credentials

// ckhmanager.go — CredentialProvider backed by sentio-core's
// ckhmanager. The manager knows per-shard ClickHouse credentials; the
// provider looks them up using the AdminRole (maps to the "subgraph"
// credential key in the manager config).

import (
	"fmt"

	ckhmanager "sentioxyz/sentio-core/common/clickhousemanager"
	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/network"
)

type ckhManagerCredentialProvider struct {
	ckhMgr        ckhmanager.Manager
	privateKeyHex string // Ethereum private key for proxy auth (passed to WithPrivateKeyHex)
}

// NewCkhManagerCredentialProvider returns a CredentialProvider that
// resolves credentials by delegating to ckhmanager. privateKeyHex is
// the relay private key used for proxy-to-proxy auth (may be empty when
// relay signing is disabled).
func NewCkhManagerCredentialProvider(ckhMgr ckhmanager.Manager, privateKeyHex string) CredentialProvider {
	return &ckhManagerCredentialProvider{ckhMgr: ckhMgr, privateKeyHex: privateKeyHex}
}

// GetDefaultCredential returns the admin credential from the default
// shard. AdminRole resolves to the "subgraph" credential key inside the
// ckhmanager config.
func (p *ckhManagerCredentialProvider) GetDefaultCredential() (string, string, error) {
	defaultIndex := p.ckhMgr.DefaultIndex()
	shard := p.ckhMgr.GetShardByIndex(defaultIndex)
	if shard == nil {
		return "", "", fmt.Errorf("default shard (index %d) not found", defaultIndex)
	}

	options := []func(*ckhmanager.ShardingParameter){
		ckhmanager.WithUnderlyingProxy(true),
		ckhmanager.WithRole(ckhmanager.AdminRole),
	}
	if p.privateKeyHex != "" {
		options = append(options, ckhmanager.WithPrivateKeyHex(p.privateKeyHex))
	}

	username, password, _, _, err := shard.GetConnInfo(options...)
	if err != nil {
		return "", "", fmt.Errorf("get default credential: %w", err)
	}
	log.Debugw("default credential resolved", "source", "credential_provider", "user", username)
	return username, password, nil
}

// GetCredentialForIndexer creates a temporary shard for the indexer and
// retrieves its admin credential.
func (p *ckhManagerCredentialProvider) GetCredentialForIndexer(indexerInfo network.IndexerInfo) (string, string, error) {
	shard := p.ckhMgr.NewShardByStateIndexer(indexerInfo)
	if shard == nil {
		return "", "", fmt.Errorf("failed to create shard for indexer %d", indexerInfo.IndexerId)
	}

	options := []func(*ckhmanager.ShardingParameter){
		ckhmanager.WithUnderlyingProxy(true),
		ckhmanager.WithRole(ckhmanager.AdminRole),
	}
	if p.privateKeyHex != "" {
		options = append(options, ckhmanager.WithPrivateKeyHex(p.privateKeyHex))
	}

	username, password, _, _, err := shard.GetConnInfo(options...)
	if err != nil {
		return "", "", fmt.Errorf("get credential for indexer %d: %w", indexerInfo.IndexerId, err)
	}
	log.Debugw("indexer credential resolved", "source", "credential_provider", "indexer", indexerInfo.IndexerId, "user", username)
	return username, password, nil
}
