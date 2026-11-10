package billing

import (
	"context"
	"fmt"
)

// RejectionReason is the cause of a CheckBalance denial.
type RejectionReason int32

const (
	RejectionUnknown             RejectionReason = 0
	RejectionInsufficientBalance RejectionReason = 1
	RejectionUnauthorizedSigner  RejectionReason = 2
)

// UsageClient is the consumer-facing surface of a billing client.
//
// housegate does not ship a production implementation; embedders
// (sentio-node) provide one and inject via Options.UsageClient. Tests
// supply record-and-replay stubs. The owner of the implementation is
// responsible for any teardown (no Close on this interface).
type UsageClient interface {
	CheckBalance(ctx context.Context, payer, signer string) (bool, RejectionReason, error)
	ReportUsage(ctx context.Context, payer, signer string, amount uint64)
}

// UserGuideURL is the user guide that refusal messages point to.
const UserGuideURL = "https://github.com/housegate/housegate/blob/main/docs/user-guide.md"

// RejectionException maps a CheckBalance rejection reason to a
// ClickHouse Exception packet tuple (code, name, message) so the
// rejection reaches the client as a meaningful error. Each message ends
// with the section of the user guide that explains the fix.
func RejectionException(reason RejectionReason, payer, signer string) (int32, string, string) {
	switch reason {
	case RejectionInsufficientBalance:
		return 1003, "INSUFFICIENT_BALANCE",
			fmt.Sprintf("Insufficient balance: payer=%s signer=%s; fund the payer's Billing balance, see %s#funding-an-account",
				payer, signer, UserGuideURL)
	case RejectionUnauthorizedSigner:
		return 1002, "UNAUTHORIZED_SIGNER",
			fmt.Sprintf("Signer not authorized to query on behalf of payer: payer=%s signer=%s; the payer must add the signer as its operator, see %s#operator-keys",
				payer, signer, UserGuideURL)
	default:
		return 1001, "QUERY_REJECTED",
			fmt.Sprintf("Query rejected (reason=%v) payer=%s signer=%s; the billing check could not complete, retry the query",
				reason, payer, signer)
	}
}
