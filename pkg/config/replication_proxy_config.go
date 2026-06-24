package config

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	defaultReplicationProxyPeerUserHeader  = "X-Housegate-Peer-User"
	defaultReplicationProxyPeerTokenHeader = "X-Housegate-Peer-Token"
)

// ReplicationProxyConfig is the optional replication-plane forwarding surface.
// It is disabled by default and valid only in server mode.
type ReplicationProxyConfig struct {
	Keeper      ReplicationProxyKeeperConfig      `json:"keeper"      yaml:"keeper"`
	Interserver ReplicationProxyInterserverConfig `json:"interserver" yaml:"interserver"`
}

// ReplicationProxyKeeperConfig controls raw Keeper/ZooKeeper TCP forwarding.
type ReplicationProxyKeeperConfig struct {
	Enabled     bool     `json:"enabled"      yaml:"enabled"`
	Listen      string   `json:"listen"       yaml:"listen"`
	Upstreams   []string `json:"upstreams"    yaml:"upstreams"`
	DialTimeout Duration `json:"dial_timeout" yaml:"dial_timeout"`
}

// ReplicationProxyInterserverConfig controls ClickHouse interserver HTTP
// forwarding between peer HouseGate instances.
type ReplicationProxyInterserverConfig struct {
	Enabled         bool                               `json:"enabled"           yaml:"enabled"`
	Listen          string                             `json:"listen"            yaml:"listen"`
	LocalUpstream   string                             `json:"local_upstream"    yaml:"local_upstream"`
	Routes          []ReplicationProxyInterserverRoute `json:"routes"            yaml:"routes"`
	DialTimeout     Duration                           `json:"dial_timeout"      yaml:"dial_timeout"`
	ReadTimeout     Duration                           `json:"read_timeout"      yaml:"read_timeout"`
	WriteTimeout    Duration                           `json:"write_timeout"     yaml:"write_timeout"`
	PeerUserHeader  string                             `json:"peer_user_header"  yaml:"peer_user_header"`
	PeerTokenHeader string                             `json:"peer_token_header" yaml:"peer_token_header"`
}

// ReplicationProxyInterserverRoute maps a peer indexer to that peer's
// HouseGate interserver listener.
type ReplicationProxyInterserverRoute struct {
	Peer     string `json:"peer"     yaml:"peer"`
	Upstream string `json:"upstream" yaml:"upstream"`
}

func defaultReplicationProxyConfig() ReplicationProxyConfig {
	return ReplicationProxyConfig{
		Keeper: ReplicationProxyKeeperConfig{
			DialTimeout: Duration{5 * time.Second},
		},
		Interserver: ReplicationProxyInterserverConfig{
			DialTimeout:     Duration{5 * time.Second},
			ReadTimeout:     Duration{30 * time.Second},
			WriteTimeout:    Duration{30 * time.Second},
			PeerUserHeader:  defaultReplicationProxyPeerUserHeader,
			PeerTokenHeader: defaultReplicationProxyPeerTokenHeader,
		},
	}
}

func (c ReplicationProxyConfig) enabled() bool {
	return c.Keeper.Enabled || c.Interserver.Enabled
}

func (c ReplicationProxyConfig) validate(root Config, mode Mode) error {
	if !c.enabled() {
		return nil
	}
	if mode != ModeServer {
		return errors.New("replication_proxy is server mode only")
	}

	var errs []error
	if c.Keeper.Enabled {
		if err := c.Keeper.validate(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.Interserver.Enabled {
		if err := c.Interserver.validate(); err != nil {
			errs = append(errs, err)
		}
		if err := validateReplicationProxyInterserverPeerAuth(root); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c ReplicationProxyKeeperConfig) validate() error {
	var errs []error
	errs = append(errs, validateReplicationProxyAddr("replication_proxy.keeper.listen", c.Listen))
	if len(c.Upstreams) == 0 {
		errs = append(errs, errors.New("replication_proxy.keeper.upstreams must contain at least one upstream when enabled"))
	}
	for i, upstream := range c.Upstreams {
		field := fmt.Sprintf("replication_proxy.keeper.upstreams[%d]", i)
		errs = append(errs, validateReplicationProxyAddr(field, upstream))
	}
	errs = append(errs, validateReplicationProxyDuration("replication_proxy.keeper.dial_timeout", c.DialTimeout))
	return errors.Join(errs...)
}

func (c ReplicationProxyInterserverConfig) validate() error {
	var errs []error
	errs = append(errs, validateReplicationProxyAddr("replication_proxy.interserver.listen", c.Listen))
	errs = append(errs, validateReplicationProxyAddr("replication_proxy.interserver.local_upstream", c.LocalUpstream))
	if len(c.Routes) == 0 {
		errs = append(errs, errors.New("replication_proxy.interserver.routes must contain at least one route when enabled"))
	}
	seenPeers := make(map[string]struct{}, len(c.Routes))
	for i, route := range c.Routes {
		peer := strings.TrimSpace(route.Peer)
		if peer == "" {
			errs = append(errs, fmt.Errorf("replication_proxy.interserver.routes[%d].peer is required", i))
		} else {
			field := fmt.Sprintf("replication_proxy.interserver.routes[%d].peer", i)
			if err := validateReplicationProxyRoutePeer(field, route.Peer); err != nil {
				errs = append(errs, err)
			} else if _, ok := seenPeers[peer]; ok {
				errs = append(errs, fmt.Errorf("replication_proxy.interserver.routes[%d].peer duplicate peer %q", i, peer))
			} else {
				seenPeers[peer] = struct{}{}
			}
		}
		field := fmt.Sprintf("replication_proxy.interserver.routes[%d].upstream", i)
		errs = append(errs, validateReplicationProxyAddr(field, route.Upstream))
	}
	errs = append(errs, validateReplicationProxyDuration("replication_proxy.interserver.dial_timeout", c.DialTimeout))
	errs = append(errs, validateReplicationProxyDuration("replication_proxy.interserver.read_timeout", c.ReadTimeout))
	errs = append(errs, validateReplicationProxyDuration("replication_proxy.interserver.write_timeout", c.WriteTimeout))
	errs = append(errs, validateReplicationProxyHeader("replication_proxy.interserver.peer_user_header", c.PeerUserHeader))
	errs = append(errs, validateReplicationProxyHeader("replication_proxy.interserver.peer_token_header", c.PeerTokenHeader))
	return errors.Join(errs...)
}

func validateReplicationProxyAddr(field string, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required when enabled", field)
	}
	if _, _, err := net.SplitHostPort(value); err != nil {
		return fmt.Errorf("%s: invalid host:port %q: %w", field, value, err)
	}
	return nil
}

func validateReplicationProxyDuration(field string, value Duration) error {
	if value.Duration <= 0 {
		return fmt.Errorf("%s must be > 0 when enabled", field)
	}
	return nil
}

func validateReplicationProxyHeader(field string, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required when enabled", field)
	}
	return nil
}

func validateReplicationProxyRoutePeer(field string, value string) error {
	if _, err := strconv.ParseUint(value, 10, 64); err != nil {
		return fmt.Errorf("%s must be a decimal target indexer id: %w", field, err)
	}
	return nil
}

func validateReplicationProxyInterserverPeerAuth(root Config) error {
	var errs []error
	if strings.TrimSpace(root.RelayPrivateKeyHex) == "" {
		errs = append(errs, errors.New("relay_private_key_hex is required when replication_proxy.interserver.enabled"))
	}
	if !root.Auth.Enabled {
		errs = append(errs, errors.New("auth.enabled is required when replication_proxy.interserver.enabled"))
	}
	if !hasReplicationProxyTrustedPeer(root.Auth.AllowedAddresses) {
		errs = append(errs, errors.New("auth.allowed_addresses must contain at least one trusted peer when replication_proxy.interserver.enabled"))
	}
	return errors.Join(errs...)
}

func hasReplicationProxyTrustedPeer(addresses []string) bool {
	for _, address := range addresses {
		if strings.TrimSpace(address) != "" {
			return true
		}
	}
	return false
}
