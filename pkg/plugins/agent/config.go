package agent

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// Config is the operator-tunable surface for agent mode — a
// pass-through proxy that signs every outgoing query and forwards to a
// remote server-side proxy.
type Config struct {
	// Mode toggles the entire agent runtime. When true the proxy
	// runs in agent mode and most server-side features are disabled.
	Mode bool `json:"mode" yaml:"mode"`

	// Upstream is the remote server-side proxy address.
	Upstream string `json:"upstream" yaml:"upstream"`

	// PrivateKeyHex is the agent's Ethereum private key (with or
	// without 0x prefix) used to sign every query's JWS.
	//
	// When Owner is empty this key both signs and pays for queries.
	// When Owner is set, this key acts as an *operator* signing on
	// behalf of Owner — the upstream's billing layer treats the
	// recovered signer as the operator and Owner as the billed
	// account, gated by an on-chain IsOperator(owner, operator) check.
	PrivateKeyHex string `json:"private_key_hex" yaml:"private_key_hex"`

	// Owner is the optional account billed for queries when this
	// agent's PrivateKeyHex is an operator key rather than the
	// owner's own. Lowercase or checksum 0x-prefixed Ethereum
	// address. Empty (default) means the signer pays for itself.
	//
	// Wire-side: when set, the agent plugin emits the address as
	// the SQL_x_payer ClickHouse setting. The wire protocol uses
	// "payer" terminology because that's what the upstream usage
	// plugin already consumes; the operator-vs-owner naming lives
	// at the deployment-config layer.
	Owner string `json:"owner" yaml:"owner"`

	// Driver toggles indexer-driver setting injection. When true the
	// agent plugin appends SQL_sentio_driver=1 to every outgoing query,
	// telling the upstream server-mode housegate that this connection
	// carries indexer-driver traffic (skip usage + commitgate, keep
	// rewrite — see pkg/auth EthValidator.IsDriver gate which still
	// requires the recovered signer to equal the server's
	// IndexerAddress, so a misconfigured non-indexer sidecar fails
	// loudly rather than silently leaking the bypass).
	//
	// Deployment-time flag rather than a per-connection client
	// declaration: the sidecar knows it is colocated with an indexer
	// driver because its operator says so via this config; clients
	// (the driver itself) stay ignorant of the housegate-side bypass
	// semantics.
	Driver bool `json:"driver" yaml:"driver"`

	// StorageIntegrity enables trusted client-side INSERT materialization for
	// the HouseKeeper storage-integrity path. It is separate from the top-level
	// server-mode storage_integrity section because agent mode is the side that
	// must materialize before signing.
	StorageIntegrity StorageIntegrityConfig `json:"storage_integrity" yaml:"storage_integrity"`
}

type StorageIntegrityConfig struct {
	Enabled   bool   `json:"enabled"    yaml:"enabled"`
	NetworkID string `json:"network_id" yaml:"network_id"`
}

// Validate checks fields that are always required regardless of how the
// upstream is selected. The "must have either Upstream or auto-discovery"
// rule lives in the root Config because Upstream may be supplied by the
// top-level NetworkState selector — a fact this struct can't see.
//
// Only meaningful when Mode is true; the root Config invokes it
// conditionally.
func (c Config) Validate() error {
	if c.PrivateKeyHex == "" {
		return errors.New("agent.private_key_hex is required (or HOUSEGATE_AGENT_KEY env)")
	}
	if c.Owner != "" && !common.IsHexAddress(c.Owner) {
		return fmt.Errorf("agent.owner is not a valid Ethereum address: %q", c.Owner)
	}
	if c.StorageIntegrity.Enabled && c.StorageIntegrity.NetworkID == "" {
		return errors.New("agent.storage_integrity.network_id is required when agent.storage_integrity.enabled")
	}
	return nil
}
