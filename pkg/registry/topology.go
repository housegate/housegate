// Package registry is housegate's read-only view of the indexer network:
// where each indexer is reachable, what logical databases exist, and who
// has access. It defines the contracts (Topology, Databases, Access)
// housegate's proxy chain consumes; implementations are supplied by the
// embedder.
//
// The package deliberately avoids external producer types — its
// vocabulary is the narrow slice of fields housegate actually reads.
// Producers that have richer data should adapt at the boundary.
package registry

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// ProxyAddress is the dialing target for a peer housegate instance
// fronting an indexer's ClickHouse. From housegate's perspective the
// remote endpoint is another housegate (which itself proxies CH), hence
// HousegatePort rather than the producer-side "ClickhouseProxyPort".
type ProxyAddress struct {
	Url           string
	HousegatePort uint16
	// StorageRPCPort is the indexer's storage-node JSON-RPC port
	// (IndexerInfo.storageNodeRpcPort); zero when not advertised. The agent
	// asks the hosting indexer for SI table status and info there.
	StorageRPCPort uint16
	// ComputeRPCPort is the indexer's compute-node RPC port
	// (IndexerInfo.computeNodeRpcPort); zero when not advertised.
	ComputeRPCPort uint16
	// Signer is the indexer's registered signer address
	// (IndexerInfo.signer); empty when the producer does not supply it. The
	// endpoint TLS signer mode authenticates the indexer's endpoints by it.
	Signer string
}

// Addr is the housegate dialing target "<url>:<port>".
func (a ProxyAddress) Addr() string { return fmt.Sprintf("%s:%d", a.Url, a.HousegatePort) }

// Registry is the union of Topology, Databases, and Access — the
// full read-only network view housegate consumes as a single
// dependency. Callers that need only a subset should depend on the
// narrower interface; this composite exists for wiring sites
// (proxy.Options, multi-method plugins) where it's natural to pass
// one value.
type Registry interface {
	Topology
	Databases
	Access
}

// Topology resolves indexer ids to dialing targets.
type Topology interface {
	// ProxyByIndexerId returns the dialing target for indexerId. The
	// (zero, false) return signals an unknown indexer; callers commonly
	// probe for existence and treat miss as a non-error.
	ProxyByIndexerId(indexerId uint64) (ProxyAddress, bool)

	// AllIndexers returns a snapshot of every known indexer keyed by id.
	// The returned map is owned by the caller and safe to retain or
	// mutate; implementations must return a fresh copy.
	AllIndexers() map[uint64]ProxyAddress
}

// IndexerSigners is implemented by a registry that knows the registered signer
// address of each indexer. It is optional: a server with an empty
// auth.allowed_addresses needs it to accept peer-relay logins, because there
// the signer of a peer login must be a registered indexer signer. A registry
// without it leaves such a server refusing every peer login.
type IndexerSigners interface {
	// IsIndexerSigner reports whether address (0x-prefixed, any case) is
	// the registered signer of a known indexer. An empty or all-zeros
	// address is never one. A lookup that fails reports false.
	IsIndexerSigner(address string) bool
}

// advertisedHost is the host of an indexer url, which producers may write as
// a bare host or as a full URL (the storage RPC endpoint tolerates both).
func advertisedHost(indexerURL string) string {
	if u, err := url.Parse(indexerURL); err == nil && u.Host != "" {
		return u.Hostname()
	}
	return indexerURL
}

// SignerForEndpoint returns the registered signer of the indexer that
// advertises addr ("host:port") as its housegate, storage RPC or compute RPC
// endpoint. It fails when no indexer advertises addr, when the indexers that
// do disagree on the signer, or when the topology cannot be read; it returns
// "" (no error) when the advertising indexer has no signer, which the signer
// mode refuses.
func SignerForEndpoint(t Topology, addr string) (string, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("endpoint %q: %w", addr, err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return "", fmt.Errorf("endpoint %q: invalid port", addr)
	}
	all := t.AllIndexers()
	if all == nil {
		return "", fmt.Errorf("endpoint %q: the network state could not be read", addr)
	}
	var signer string
	found := false
	for id, a := range all {
		if !strings.EqualFold(advertisedHost(a.Url), host) {
			continue
		}
		p := uint16(port)
		if p != a.HousegatePort && p != a.StorageRPCPort && p != a.ComputeRPCPort {
			continue
		}
		s := strings.ToLower(strings.TrimSpace(a.Signer))
		if found && s != signer {
			return "", fmt.Errorf("endpoint %q is advertised by indexers with different signers (one is indexer %d)", addr, id)
		}
		signer, found = s, true
	}
	if !found {
		return "", fmt.Errorf("endpoint %q is not advertised by any indexer", addr)
	}
	return signer, nil
}
