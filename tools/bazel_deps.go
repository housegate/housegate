//go:build bazel_deps

// Package tools is excluded from normal builds (build tag `bazel_deps`
// is never set). Its only role is to anchor transitive dependencies
// that bazel needs to satisfy at the BUILD-loading layer but that the
// housegate Go source does not import directly.
//
// Without these blank imports, `go mod tidy` drops the modules from
// go.mod, gazelle's go_deps extension stops generating bazel repos
// for them, and BUILD files inside go-ethereum (e.g. crypto/kzg4844)
// fail to load with "unknown repo @com_github_crate_crypto_go_eth_kzg"
// — even though no housegate code path actually reaches kzg4844.
//
// Adding (or removing) entries here is correct when bazel reports an
// unknown-repo error at load time for a transitive package no Go
// source imports.
package tools

import (
	_ "github.com/ethereum/go-ethereum/crypto/kzg4844"
)
