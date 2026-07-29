package concurrency

// resolvers.go — built-in DimensionResolver factories.
//
// A resolver maps a session + in-flight query to a Dimension {name,
// value, quota}. Returning an empty Value tells the Plugin to skip the
// dimension for this query, so resolvers can be wired in before their
// data source is ready.

import (
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

// Dimension names used by the built-in resolvers. Other resolvers should
// pick stable, distinct names; the limiter uses them in error messages
// and in Redis key composition.
const (
	DimensionUser       = "user"
	DimensionStakeLevel = "stake_level"
)

// UserDimension produces a "user" dimension keyed on the recovered
// Ethereum address from SessionState.Identity.UserID. Anonymous sessions
// (no recovered identity) yield an empty Value and are skipped.
//
// quotaPerUser is the maximum concurrent queries per user; <= 0 disables
// enforcement on this dimension while keeping it tracked.
func UserDimension(quotaPerUser int) DimensionResolver {
	return func(sess chsession.Session, _ *plugin.QueryContext) Dimension {
		return Dimension{
			Name:  DimensionUser,
			Value: sess.State().Snapshot().Identity.UserID,
			Quota: quotaPerUser,
		}
	}
}

// NoneStakeLevelResolver is a placeholder for a future stake-level
// dimension. It always returns a Dimension with an empty Value so the
// dimension is skipped at runtime, while reserving the dimension name
// (DimensionStakeLevel) so wiring it into the plugin chain ahead of
// time costs nothing.
//
// Replace with a real resolver once the data source for stake levels
// (Redis cache / on-chain lookup / SessionState extension) is in place;
// the rest of the plugin chain does not need to change.
func NoneStakeLevelResolver() DimensionResolver {
	return func(_ chsession.Session, _ *plugin.QueryContext) Dimension {
		return Dimension{Name: DimensionStakeLevel} // empty Value → skipped
	}
}
