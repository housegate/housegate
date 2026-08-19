package agent

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/housegate/housegate/pkg/registry"
)

// ErrNoNetworkState is returned by Selector.Pick when the selector has
// no registry backing it. Distinct from "state has zero indexers"
// because nil deps are a wiring bug, not an empty network.
var ErrNoNetworkState = errors.New("agent selector: no network state")

// Selector picks an upstream proxy address for an agent session by
// consulting the registry. The selection is two-tier:
//
//  1. Permissioned tier — indexers that host at least one logical
//     database the Account holds permissions on. Preferred because
//     the user has something to query there.
//  2. Bootstrap tier — any bound indexer at all. Used only when tier 1
//     is empty (a brand-new account with no DB perms yet, per
//     §8.1 of docs/superpowers/specs/2026-04-28-two-port-server-mode.md).
//     The caller is expected to log + meter these picks so operators
//     can spot accounts that should not be in the bootstrap path.
//
// Selector is read-only; callers create one per agent process and
// re-use it across sessions, providing fresh randomness on each Pick.
type Selector struct {
	// Topology, Databases, Access are the read-only registry consumers
	// (yaml-backed in dev/tests, redis-backed in production via the
	// pkg/network adapter).
	Topology  registry.Topology
	Databases registry.Databases
	Access    registry.Access

	// Account is the lowercase Ethereum address used as the lookup key for
	// Access.PermissionsFor. The agent owner takes precedence when configured;
	// otherwise callers use the address derived from PrivateKeyHex. This choice
	// affects routing only; the private-key address still signs every query.
	Account string
}

// Choice records which indexer Pick selected and whether the choice
// came from the bootstrap tier. Callers use IsBootstrap to decide
// whether to emit a fallback log line + metric.
type Choice struct {
	IndexerId   uint64
	Address     registry.ProxyAddress
	IsBootstrap bool
}

// Addr returns "host:port" suitable for net.Dial.
func (c Choice) Addr() string {
	return fmt.Sprintf("%s:%d", c.Address.Url, c.Address.HousegatePort)
}

// Pick chooses an indexer per the two-tier algorithm. The supplied
// *rand.Rand decides ties so the caller controls determinism (tests
// pass a fixed-seed Rand; production passes time-seeded).
//
// Returns an error when:
//   - Topology / Databases / Access is nil (ErrNoNetworkState)
//   - topology contains zero bound indexers (no candidates at all)
func (s *Selector) Pick(r *rand.Rand) (Choice, error) {
	if s.Topology == nil || s.Databases == nil || s.Access == nil {
		return Choice{}, ErrNoNetworkState
	}

	// Build the set of indexers the account has at least one
	// permissioned database on. Empty perms (or unknown account) yields
	// an empty set — both fall through to the bootstrap tier.
	perms, _ := s.Access.PermissionsFor(s.Account)
	permissionedIndexers := make(map[uint64]struct{}, len(perms))
	for db := range perms {
		if info, ok := s.Databases.Get(db); ok {
			permissionedIndexers[info.IndexerId] = struct{}{}
		}
	}

	// Walk every known indexer once, partitioning into bound vs
	// bound-and-permissioned.
	type entry struct {
		id   uint64
		addr registry.ProxyAddress
	}
	var bound, permissioned []entry
	for id, addr := range s.Topology.AllIndexers() {
		if addr.HousegatePort == 0 {
			continue
		}
		e := entry{id: id, addr: addr}
		bound = append(bound, e)
		if _, ok := permissionedIndexers[id]; ok {
			permissioned = append(permissioned, e)
		}
	}

	switch {
	case len(permissioned) > 0:
		e := permissioned[r.Intn(len(permissioned))]
		return Choice{IndexerId: e.id, Address: e.addr}, nil
	case len(bound) > 0:
		e := bound[r.Intn(len(bound))]
		return Choice{IndexerId: e.id, Address: e.addr, IsBootstrap: true}, nil
	default:
		return Choice{}, errors.New("agent selector: no bound indexers in network state")
	}
}
