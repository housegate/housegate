package billing

import "context"

// RejectionReason is the cause of a CheckBalance denial.
type RejectionReason int32

const (
	RejectionUnknown             RejectionReason = 0
	RejectionInsufficientBalance RejectionReason = 1
	RejectionUnauthorizedSigner  RejectionReason = 2
)

// UsageClient is the consumer-facing surface of a billing client.
//
// *Client is the production implementation; library users may inject
// their own (e.g. a record-and-replay test stub). Close lives on the
// concrete type only — it is the owner's responsibility, not the
// consumer's.
type UsageClient interface {
	CheckBalance(ctx context.Context, payer, signer string) (bool, RejectionReason, error)
	ReportUsage(ctx context.Context, payer, signer string, amount uint64)
}

// Compile-time guard: *Client must satisfy UsageClient.
var _ UsageClient = (*Client)(nil)
