// Package billing owns the sentio-node query-usage client used by the
// proxy for balance checks and usage reporting.
//
// Consumers (the usage plugin and any future library callers) hold the
// UsageClient interface defined in usage_client.go; *Client is the
// production implementation and is also what owners (cmd-layer or
// library hosts) construct and Close. Nothing here imports pkg/proxy
// or the plugin packages.
package billing

import (
	"context"
	"fmt"
	"time"

	"housegate/housegate/pkg/log"
	usageProtos "sentioxyz/sentio-core/service/usage/protos"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	queryCheckCachePrefix = "query_check:" // key: query_check:{payer}:{signer}
	queryCheckCacheTTL    = 5 * time.Minute

	reportMaxRetries    = 5
	reportRetryBaseWait = 1 * time.Second
	reportRetryTimeout  = 10 * time.Second
)

// Client wraps the sentio-node QueryUsageService gRPC client plus a
// Redis cache for balance-check results.
type Client struct {
	grpcClient  usageProtos.QueryUsageServiceClient
	grpcConn    *grpc.ClientConn
	redisClient *redis.Client
}

// NewClient dials sentioNodeAddr and pairs the resulting gRPC client
// with the provided Redis instance (used for balance-check caching).
func NewClient(sentioNodeAddr string, redisClient *redis.Client) (*Client, error) {
	conn, err := grpc.NewClient(sentioNodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	return &Client{
		grpcClient:  usageProtos.NewQueryUsageServiceClient(conn),
		grpcConn:    conn,
		redisClient: redisClient,
	}, nil
}

// CheckBalance returns (allowed, rejectionReason, err). Results are
// cached in Redis for queryCheckCacheTTL. Fail-open on Redis or gRPC
// error — returns (true, 0, nil).
func (c *Client) CheckBalance(ctx context.Context, payer string, signer string) (bool, RejectionReason, error) {
	cacheKey := queryCheckCachePrefix + payer + ":" + signer

	if c.redisClient != nil {
		cached, err := c.redisClient.Get(ctx, cacheKey).Result()
		if err == nil {
			if cached == "ok" {
				log.Infow("cache hit", "source", "usage_client", "payer", payer, "signer", signer, "result", "ok")
				return true, 0, nil
			}
			var reason RejectionReason
			switch cached {
			case "1":
				reason = RejectionInsufficientBalance
			case "2":
				reason = RejectionUnauthorizedSigner
			default:
				reason = RejectionUnknown
			}
			log.Infow("cache hit", "source", "usage_client", "payer", payer, "signer", signer, "result", "rejected", "reason", reason)
			return false, reason, nil
		}
	}

	resp, err := c.grpcClient.CheckQueryBalance(ctx, &usageProtos.CheckQueryBalanceRequest{
		Account: payer,
		Signer:  signer,
	})
	if err != nil {
		log.Warnfe(err, "CheckQueryBalance failed source=%v payer=%v signer=%v", "usage_client", payer, signer)
		return true, 0, nil
	}

	if c.redisClient != nil {
		val := "ok"
		if !resp.Status {
			val = fmt.Sprintf("%d", int32(resp.Reason))
		}
		_ = c.redisClient.Set(ctx, cacheKey, val, queryCheckCacheTTL).Err()
	}
	return resp.Status, protoReason(resp.Reason), nil
}

func protoReason(r usageProtos.CheckQueryBalanceRejection) RejectionReason {
	switch r {
	case usageProtos.CheckQueryBalanceRejection_INSUFFICIENT_BALANCE:
		return RejectionInsufficientBalance
	case usageProtos.CheckQueryBalanceRejection_UNAUTHORIZED_SIGNER:
		return RejectionUnauthorizedSigner
	default:
		return RejectionUnknown
	}
}

// ReportUsage asynchronously reports one query against payer/signer
// with retries so usage is eventually persisted even if sentio-node is
// briefly unavailable.
func (c *Client) ReportUsage(ctx context.Context, payer string, signer string, amount uint64) {
	go func() {
		req := &usageProtos.ReportQueryUsageRequest{
			Account: payer,
			Amount:  amount,
			Signer:  signer,
		}
		wait := reportRetryBaseWait
		for attempt := 1; attempt <= reportMaxRetries; attempt++ {
			rctx, cancel := context.WithTimeout(context.Background(), reportRetryTimeout)
			_, err := c.grpcClient.ReportQueryUsage(rctx, req)
			cancel()
			if err == nil {
				return
			}
			if attempt == reportMaxRetries {
				log.Errorfe(err, "ReportQueryUsage failed, giving up source=%v attempts=%v payer=%v signer=%v", "usage_client", reportMaxRetries, payer, signer)
				return
			}
			log.Warnfe(err, "ReportQueryUsage attempt failed, retrying source=%v attempt=%v max_attempts=%v payer=%v signer=%v retry_in=%v", "usage_client", attempt, reportMaxRetries, payer, signer, wait)
			time.Sleep(wait)
			wait *= 2
		}
	}()
}

// RejectionException maps a CheckBalance rejection reason to a
// ClickHouse Exception packet tuple (code, name, message) so the
// rejection reaches the client as a meaningful error.
func RejectionException(reason RejectionReason, payer, signer string) (int32, string, string) {
	switch reason {
	case RejectionInsufficientBalance:
		return 1003, "INSUFFICIENT_BALANCE",
			fmt.Sprintf("Insufficient balance: payer=%s signer=%s", payer, signer)
	case RejectionUnauthorizedSigner:
		return 1002, "UNAUTHORIZED_SIGNER",
			fmt.Sprintf("Signer not authorized to query on behalf of payer: payer=%s signer=%s", payer, signer)
	default:
		return 1001, "QUERY_REJECTED",
			fmt.Sprintf("Query rejected (reason=%v) payer=%s signer=%s", reason, payer, signer)
	}
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	if c.grpcConn != nil {
		return c.grpcConn.Close()
	}
	return nil
}
