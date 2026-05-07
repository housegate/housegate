package auth

import "time"

// Signer produces JWS tokens binding a query's SQL.
//
// *RelaySigner is the production implementation; library users may
// inject their own (e.g. a KMS-backed signer, or a test stub).
type Signer interface {
	// Address returns the lowercase 0x-prefixed Ethereum address of
	// this signer; the verifying side's allowlist must contain it.
	Address() string

	// SignToken returns a JWS compact-serialized token whose payload
	// binds the SQL via Keccak256.
	SignToken(sql string) (string, error)
}

// Compile-time guard: *RelaySigner must satisfy Signer.
var _ Signer = (*RelaySigner)(nil)

// PeerSigner produces peer-relay JWS tokens that authenticate one
// housegate proxy to another at handshake time.
//
// *RelaySigner is the production implementation; library users may
// inject their own (e.g. KMS-backed). The audience argument is the
// receiving proxy's identity claim (typically its indexer-id
// stringified) and ttl bounds the token's validity window.
type PeerSigner interface {
	// Address returns the lowercase 0x-prefixed Ethereum address of
	// this signer; the verifier on the receiving side must have it in
	// AllowedAddresses.
	Address() string
	// SignPeerLogin returns a JWS compact-serialized token whose
	// payload binds (audience, iat, exp, purpose=peer-relay).
	SignPeerLogin(audience string, ttl time.Duration) (string, error)
}

// PeerValidator verifies peer-relay JWS tokens and returns the
// recovered signer address. expectedAudience is the receiving proxy's
// identity claim — implementations must reject tokens whose `aud`
// does not match.
type PeerValidator interface {
	ValidatePeerLogin(token, expectedAudience string) (string, error)
}

// Compile-time guards.
var (
	_ PeerSigner    = (*RelaySigner)(nil)
	_ PeerValidator = (*EthValidator)(nil)
)
