package rewriter

// aliases.go — re-export the network types this package consumes so
// callers can use them by short name without an extra import.

import "github.com/housegate/housegate/pkg/network"

type (
	IndexerInfo         = network.IndexerInfo
	ProcessorAllocation = network.ProcessorAllocation
	ProcessorInfo       = network.ProcessorInfo
	AccountAddress      = network.AccountAddress
)

// Re-exported in-memory state for tests/external callers.
var NewInMemoryNetworkState = network.NewInMemoryNetworkState
