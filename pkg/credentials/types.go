// Package credentials owns the CredentialProvider contract — the seam
// that lets the proxy swap client-supplied credentials with the real
// ClickHouse credentials before dialling upstream.
//
// The contract is consumed by both the credential-injection plugin (new
// path) and the legacy *Proxy, and implemented by a ckh-manager backed
// type in this package. Nothing in pkg/credentials imports pkg/proxy or
// plugin packages; the dependency flows the other way.
package credentials

import "housegate/housegate/pkg/network"

// CredentialProvider sources real ClickHouse credentials so the proxy
// can replace a client's placeholder credentials before forwarding.
type CredentialProvider interface {
	// GetDefaultCredential returns the credential for the local
	// upstream ClickHouse — used during the Hello handshake when
	// credential replacement is enabled.
	GetDefaultCredential() (username, password string, err error)

	// GetCredentialForIndexer returns credentials for a remote indexer's
	// ClickHouse — used for remote() function calls targeting other
	// indexers discovered via NetworkState.
	GetCredentialForIndexer(indexerInfo network.IndexerInfo) (username, password string, err error)
}
