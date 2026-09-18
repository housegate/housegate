package snapshotquery

import (
	"context"
	"github.com/housegate/housegate/pkg/replay"
)

// QueryUseAdmission is implemented by the trusted lifecycle owner, with fixed
// registry/scope/principal/role. It atomically admits a tracked invocation of
// the exact accepted job under an already registered, retained, live reference.
// It checks assignment/JWS/roots/grant/fence/pin/O/activation and any required
// claim. History alone and reference spelling never confer current admission.
// Accepted is only for its actual source lifecycle; independent source/verifier
// runs need their own replay role/principal acquisition. Challenge/reservation/
// publication/unknown/spent references do not confer execution permission.
// B4 supplies separate owned copies; implementations must not mutate them.
type QueryUseAdmission interface {
	AcquireQueryUse(context.Context, string, replay.SnapshotQueryJob) (QueryUseLease, error)
}

// QueryUseLease releases ONLY a tracked invocation, never durable ownership or
// GC authority. The owner must retain uncertainty on Close failure. If an
// S-reading handle cannot establish quiescence, B4 does not call Close: the
// trusted owner must track and recover the live/uncertain invocation itself.
type QueryUseLease interface{ Close() error }
