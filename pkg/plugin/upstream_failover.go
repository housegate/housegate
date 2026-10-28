package plugin

import (
	"context"

	"github.com/housegate/housegate/pkg/chproto"
)

// FailoverPhase is where an agent session's upstream failed (spec 2026-10-14
// §11, Plan S3-C4 K46). pkg/proxy re-exports it, with the constants and the
// two interfaces below, as proxy.FailoverPhase, proxy.FailoverAtHello,
// proxy.FailoverAtRead, proxy.UpstreamFailover and proxy.FailoverReporter;
// they live here so a plugin package implements them without importing
// pkg/proxy.
type FailoverPhase uint8

const (
	// FailoverAtHello: the dial to the session's upstream, or phase 2 of its
	// handshake, failed before the client received a ServerHello.
	FailoverAtHello FailoverPhase = 1
	// FailoverAtRead: the first answer of an eligible read was an Exception,
	// or the connection broke before any packet of that read reached the
	// client.
	FailoverAtRead FailoverPhase = 2
)

// UpstreamFailover moves an agent session whose owner refuses a read as
// quarantined, or cannot be reached, to a replica host. Candidates returns
// host:port addresses in the order to try. Reason classifies a failure (cause
// for a dial, transport or handshake error, exc for an Exception); ok=false
// leaves the failure to the client. The relay decides when a failover may
// happen at all; a nil proxy.Server.UpstreamFailover keeps it exactly as
// before.
type UpstreamFailover interface {
	Candidates(ctx context.Context, database string) ([]string, error)
	Reason(phase FailoverPhase, cause error, exc *chproto.Exception) (reason string, ok bool)
}

// FailoverReporter is implemented by an UpstreamFailover that records every
// failover that reached a serving upstream: from is the failed upstream's
// address ("" when it is unknown), to the candidate that now serves the
// session.
type FailoverReporter interface {
	FailedOver(ctx context.Context, database, reason, from, to string)
}
