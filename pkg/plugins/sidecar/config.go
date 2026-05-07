package sidecar

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// Config is the operator-tunable surface for sidecar mode — a
// pass-through proxy that signs every outgoing query and forwards to a
// remote server-side proxy.
type Config struct {
	// Mode toggles the entire sidecar runtime. When true the proxy
	// runs in sidecar mode and most server-side features are disabled.
	Mode bool `json:"mode" yaml:"mode"`

	// Upstream is the remote server-side proxy address.
	Upstream string `json:"upstream" yaml:"upstream"`

	// PrivateKeyHex is the sidecar's Ethereum private key (with or
	// without 0x prefix) used to sign every query's JWS.
	//
	// When Owner is empty this key both signs and pays for queries.
	// When Owner is set, this key acts as an *operator* signing on
	// behalf of Owner — the upstream's billing layer treats the
	// recovered signer as the operator and Owner as the billed
	// account, gated by an on-chain IsOperator(owner, operator) check.
	PrivateKeyHex string `json:"private_key_hex" yaml:"private_key_hex"`

	// Owner is the optional account billed for queries when this
	// sidecar's PrivateKeyHex is an operator key rather than the
	// owner's own. Lowercase or checksum 0x-prefixed Ethereum
	// address. Empty (default) means the signer pays for itself.
	//
	// Wire-side: when set, the sidecar plugin emits the address as
	// the SQL_x_payer ClickHouse setting. The wire protocol uses
	// "payer" terminology because that's what the upstream usage
	// plugin already consumes; the operator-vs-owner naming lives
	// at the deployment-config layer.
	Owner string `json:"owner" yaml:"owner"`
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
		return errors.New("sidecar.private_key_hex is required (or HOUSEGATE_SIDECAR_KEY env)")
	}
	if c.Owner != "" && !common.IsHexAddress(c.Owner) {
		return fmt.Errorf("sidecar.owner is not a valid Ethereum address: %q", c.Owner)
	}
	return nil
}
