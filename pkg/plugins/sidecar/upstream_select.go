package sidecar

import (
	"errors"
	"fmt"
	"math/rand"

	"housegate/housegate/pkg/network"
)

// ErrNoNetworkState is returned by Selector.Pick when the selector has
// no NetworkState backing it. Distinct from "state has zero indexers"
// because a nil State is a wiring bug, not an empty network.
var ErrNoNetworkState = errors.New("sidecar selector: no network state")

// Selector picks an upstream proxy address for a sidecar session by
// consulting NetworkState. The selection is two-tier:
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
// Selector is read-only; callers create one per sidecar process and
// re-use it across sessions, providing fresh randomness on each Pick.
type Selector struct {
	// NS is the read-only NetworkState consumer (yaml-backed in
	// dev/tests, redis-backed in production).
	NS network.State

	// Account is the sidecar's Ethereum address (derived from
	// PrivateKeyHex). Used as the lookup key for
	// RetrieveDatabasePermissions.
	Account network.AccountAddress
}

// Choice records which indexer Pick selected and whether the choice
// came from the bootstrap tier. Callers use IsBootstrap to decide
// whether to emit a fallback log line + metric.
type Choice struct {
	Indexer     network.IndexerInfo
	IsBootstrap bool
}

// Addr returns "host:port" suitable for net.Dial. ClickhouseProxyPort
// is the only port the sidecar dials.
func (c Choice) Addr() string {
	return fmt.Sprintf("%s:%d", c.Indexer.IndexerUrl, c.Indexer.ClickhouseProxyPort)
}

// Pick chooses an indexer per the two-tier algorithm. The supplied
// *rand.Rand decides ties so the caller controls determinism (tests
// pass a fixed-seed Rand; production passes time-seeded).
//
// Returns an error when:
//   - NS is nil (ErrNoNetworkState)
//   - NetworkState contains zero bound indexers (no candidates at all)
func (s *Selector) Pick(r *rand.Rand) (Choice, error) {
	if s.NS == nil {
		return Choice{}, ErrNoNetworkState
	}

	// Build the set of indexers the account has at least one
	// permissioned database on. Empty perms (or unknown account) yields
	// an empty set — both fall through to the bootstrap tier.
	perms, _ := s.NS.RetrieveDatabasePermissions(s.Account)
	permissionedIndexers := make(map[uint64]struct{}, len(perms))
	for db := range perms {
		if info, ok := s.NS.RetrieveDatabaseInfo(db); ok {
			permissionedIndexers[info.IndexerId] = struct{}{}
		}
	}

	// Walk every known indexer once, partitioning into bound vs
	// bound-and-permissioned.
	var bound, permissioned []network.IndexerInfo
	for _, info := range s.NS.RetrieveAllIndexerInfos() {
		if info.ClickhouseProxyPort == 0 {
			continue
		}
		bound = append(bound, info)
		if _, ok := permissionedIndexers[info.IndexerId]; ok {
			permissioned = append(permissioned, info)
		}
	}

	switch {
	case len(permissioned) > 0:
		return Choice{Indexer: permissioned[r.Intn(len(permissioned))]}, nil
	case len(bound) > 0:
		return Choice{Indexer: bound[r.Intn(len(bound))], IsBootstrap: true}, nil
	default:
		return Choice{}, errors.New("sidecar selector: no bound indexers in network state")
	}
}
