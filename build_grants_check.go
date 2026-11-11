package housegate

import (
	"github.com/housegate/housegate/pkg/chgrants"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/credentials"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugins/tablerefguard"
)

// buildGrantsChecker builds the upstream grants check (housegate/housegate#341)
// for a server that forwards to ClickHouse, or returns nil when there is
// nothing to check:
//
//   - upstream_grants_check.mode is off;
//   - the ClickHouse cluster was injected by the host, so HouseGate does not
//     know its addresses;
//   - there is no shard or upstream address (a router-only server);
//   - there is no credential provider (credential_replace_enabled is off), so
//     each client brings its own ClickHouse credentials and there is no shared
//     user to check.
//
// Every target is checked as the credential provider's default credential, the
// user every tenant, maintenance, platform-operator and peer-trusted session
// is forwarded as.
func buildGrantsChecker(cfg *config.Config, credProvider credentials.CredentialProvider, injectedCluster bool) *chgrants.Checker {
	gc := cfg.UpstreamGrantsCheck
	mode := gc.EffectiveMode()
	if mode == chgrants.ModeOff {
		log.Info("upstream grants check: off")
		return nil
	}
	if injectedCluster {
		log.Info("upstream grants check: skipped, the ClickHouse cluster is host-injected")
		return nil
	}
	var addrs []string
	switch {
	case cfg.Shard != nil && len(cfg.Shard.Replicas) > 0:
		for _, r := range cfg.Shard.Replicas {
			addrs = append(addrs, r.Addr())
		}
	case cfg.Upstream != "":
		addrs = []string{cfg.Upstream}
	}
	if len(addrs) == 0 {
		return nil
	}
	if credProvider == nil {
		log.Info("upstream grants check: skipped, credential_replace_enabled is off so clients bring their own ClickHouse credentials")
		return nil
	}
	user, password, err := credProvider.GetDefaultCredential()
	if err != nil {
		log.Warnw("upstream grants check: skipped, no default ClickHouse credential", "err", err)
		return nil
	}
	c := &chgrants.Checker{
		Mode: mode,
		Policy: chgrants.Policy{
			PhysicalDatabase: cfg.Rewriter.PhysicalDatabase,
			StorageIntegrity: cfg.StorageIntegrity.IsEnabled(),
			ReplicaCopies:    gc.ReplicaCopies,
			EngineAllowed:    tablerefguard.TableEngineGrantAllowed,
		},
	}
	for _, addr := range addrs {
		c.Upstreams = append(c.Upstreams, chgrants.Upstream{Addr: addr, Query: chgrants.ShowGrants(addr, user, password)})
	}
	return c
}
