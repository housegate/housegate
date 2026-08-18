// Package auth owns the wire-protocol contracts used for proxy query
// authentication: the Validator interface, the QueryMeta struct callers
// pass through it, the JWS payload shape, and the shared Setting key
// that carries a JWS from client to validator.
//
// Concrete implementations also live here — EthValidator for the server
// side and RelaySigner for the client/relay side — because they share
// the JWS encoding helpers and moving them together keeps the crypto
// surface in one package.
//
// Nothing in pkg/auth imports pkg/proxy or the plugin packages; the
// dependency flows the other way.
package auth

import (
	"context"

	"github.com/housegate/housegate/pkg/log"
)

// AuthTokenSettingKey is the ClickHouse Query Setting key that carries a
// JWS authentication token from client to server. Both the signer
// (agent plugin, relay signer plugin) and the verifier (EthValidator)
// consult this constant; any change must be made in lockstep.
const (
	AuthTokenSettingKey = "SQL_x_auth_token"
	// StatementTokenSettingKey carries the storage-integrity statement JWS
	// (purpose StatementPurposeV2). It is deliberately distinct from
	// AuthTokenSettingKey so the ordinary auth plugin's purpose-optional legacy
	// path can never accept a statement token and vice versa.
	StatementTokenSettingKey   = "SQL_x_statement_token"
	MaintenanceSettingKey      = "SQL_sentio_maintenance"
	PlatformOperatorSettingKey = "SQL_sentio_platform_operator"
	DriverSettingKey           = "SQL_sentio_driver"
	PayerSettingKey            = "SQL_x_payer"
)

// QueryMeta describes one ClickHouse Query as the Validator sees it.
// The struct is intentionally minimal — enough to locate a signer in
// Settings and to bind the signature to the SQL body, and enough client
// context for log correlation.
type QueryMeta struct {
	ConnID       int64
	ClientAddr   string
	UpstreamAddr string

	// QueryPreview is a brief summary used only for log lines.
	QueryPreview string

	// Raw optionally carries the packet bytes for deeper inspection.
	Raw []byte

	// SQL is the Query.Body parsed precisely according to ClickHouse
	// native protocol. Empty string when parsing failed.
	SQL string

	// Settings contains per-query settings extracted from the Query
	// packet; look up AuthTokenSettingKey here to find the JWS.
	Settings map[string]string
}

// ValidationResult is the outcome of a successful Validator.ValidateQuery
// call. The zero value (returned when auth is disabled or the validator
// allows anonymous queries) means "no authenticated identity, no
// bypass".
//
// New bypass-style flags should be added here rather than as additional
// return values — keeping the wire shape of Validator stable as the
// auth surface grows.
type ValidationResult struct {
	// Address is the authenticated Ethereum signer (lowercase, 0x
	// prefixed). Empty when auth is disabled or anonymous traffic is
	// allowed by the validator's configuration.
	Address string

	// Maintenance is true when the caller declared this query a
	// trusted maintenance call (via SQL_sentio_maintenance) AND the
	// validator authenticated it normally. When true, downstream
	// plugins (rewrite, usage, commitgate) short-circuit and forward
	// verbatim. The flag does NOT bypass authentication — it is only
	// set after a successful JWS verify + allow-list match.
	Maintenance bool

	// PlatformOperator is true when the caller declared this query a
	// platform-operator call (via SQL_sentio_platform_operator) AND the
	// recovered signer is in the validator's platform-operator
	// allowlist. Same bypass semantics as Maintenance — rewrite, usage,
	// commitgate skip — but gated on operator identity rather than
	// indexer identity.
	PlatformOperator bool

	// IsDriver is true when the caller declared this query as
	// indexer-driver traffic (via SQL_sentio_driver) AND the recovered
	// signer matches the validator's IndexerAddress. Narrower than
	// Maintenance: usage and commitgate plugins skip, but rewrite still
	// runs because the driver writes logical names that require
	// logical→physical translation. Gated identically to Maintenance —
	// the setting alone is not sufficient.
	IsDriver bool
}

// Validator authenticates a single query and, on success, reports the
// authenticated identity plus any bypass flags that apply to this
// query. Returning a zero-valued ValidationResult means auth is
// disabled or the validator allows anonymous queries in its
// configuration.
type Validator interface {
	ValidateQuery(context.Context, QueryMeta) (ValidationResult, error)
}

// NoopValidator is the default validation implementation: it always
// allows the query and logs the SQL/preview.
type NoopValidator struct{}

func (NoopValidator) ValidateQuery(_ context.Context, meta QueryMeta) (ValidationResult, error) {
	if meta.SQL != "" {
		log.Infow("allow query", "source", "validator", "client", meta.ClientAddr, "upstream", meta.UpstreamAddr, "sql", meta.SQL)
	} else if meta.QueryPreview != "" {
		log.Infow("allow query (preview)", "source", "validator", "client", meta.ClientAddr, "upstream", meta.UpstreamAddr, "preview", meta.QueryPreview)
	}
	return ValidationResult{}, nil
}

// JWSHeader is the protected header of a JWS token. Only Alg and Typ
// are consulted; other fields are ignored.
type JWSHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// JWSPayload is the minimal JWS payload housegate signs and verifies.
// Iat is the issued-at unix timestamp (seconds); QueryHash is the
// Keccak256 hash of the SQL body hex-encoded with 0x prefix. Purpose is
// optional for legacy query auth and required only by domain-separated lanes
// such as storage integrity.
type JWSPayload struct {
	Iat       int64  `json:"iat"`
	QueryHash string `json:"qhash"`
	Purpose   string `json:"purpose,omitempty"`
}

// QueryPurpose domain-separates ordinary SQL query tokens from peer-login and
// other JWS payloads. Legacy query validation remains purpose-optional, while
// correctness-sensitive lanes can require this exact value.
const QueryPurpose = "housegate-query"

// StatementPurposeV2 is the JWSStatementPayloadV2.Purpose value.
const StatementPurposeV2 = "housegate-statement-v2"

// JWSStatementPayloadV2 is the signed payload of a storage-integrity
// statement token. Every field except Iat is bound: verifiers derive the
// expected value from the envelope and require exact equality.
type JWSStatementPayloadV2 struct {
	Purpose        string `json:"purpose"`
	Iat            int64  `json:"iat"`
	NetworkID      string `json:"network_id"`
	KeeperShardID  uint32 `json:"keeper_shard_id"`
	StatementID    string `json:"statement_id"`
	SQLHash        string `json:"sql_hash"`
	SettingsHash   string `json:"settings_hash"`
	SchemaHash     string `json:"schema_hash"`
	PayloadHash    string `json:"payload_hash"`
	PayloadLength  uint64 `json:"payload_length"`
	PayloadFormat  string `json:"payload_format"`
	ClientRevision uint32 `json:"client_revision"`
	TargetTableID  string `json:"target_table_id"`
	RowIDProfileID string `json:"row_id_profile_id"`
}

// StatementPayloadV2Mismatch returns the JSON name of the first bound field
// that differs between got and want, or "" when every bound field is equal.
// Iat is not a bound field (freshness is checked separately at the ingress
// edge); Purpose is compared. Deterministic and side-effect free so the
// Arbiter FSM can reuse it inside Apply.
func StatementPayloadV2Mismatch(got, want JWSStatementPayloadV2) string {
	switch {
	case got.Purpose != want.Purpose:
		return "purpose"
	case got.NetworkID != want.NetworkID:
		return "network_id"
	case got.KeeperShardID != want.KeeperShardID:
		return "keeper_shard_id"
	case got.StatementID != want.StatementID:
		return "statement_id"
	case got.SQLHash != want.SQLHash:
		return "sql_hash"
	case got.SettingsHash != want.SettingsHash:
		return "settings_hash"
	case got.SchemaHash != want.SchemaHash:
		return "schema_hash"
	case got.PayloadHash != want.PayloadHash:
		return "payload_hash"
	case got.PayloadLength != want.PayloadLength:
		return "payload_length"
	case got.PayloadFormat != want.PayloadFormat:
		return "payload_format"
	case got.ClientRevision != want.ClientRevision:
		return "client_revision"
	case got.TargetTableID != want.TargetTableID:
		return "target_table_id"
	case got.RowIDProfileID != want.RowIDProfileID:
		return "row_id_profile_id"
	}
	return ""
}

// PeerLoginPurpose is the JWSPeerPayload.Purpose value for a
// peer-relay login token. Domain-separated from the SQL-binding query
// JWS so a query token cannot be replayed as a peer login (and vice
// versa). Constant value because validators key off it directly.
const PeerLoginPurpose = "peer-relay"

// JWSPeerPayload is the payload housegate proxies sign when one
// rewriter emits a remote(...) clause that lands at another housegate.
// Distinct from JWSPayload because peer login authenticates an
// identity at handshake time — the SQL body is not yet known and the
// receiving proxy will further rewrite the inner statement, so a
// qhash binding wouldn't survive.
//
// Aud carries the receiving proxy's indexer-id (decimal-stringified)
// so a token signed for proxy A can't be replayed against proxy B.
// Exp is mandatory; validators reject expired tokens regardless of
// MaxTokenAge.
type JWSPeerPayload struct {
	Iat     int64  `json:"iat"`
	Exp     int64  `json:"exp"`
	Aud     string `json:"aud"`
	Purpose string `json:"purpose"`
}

// JWSJSON is the JWS JSON Serialization shape accepted by the validator
// (in addition to compact serialization). Used when multiple signers
// cosign the same payload.
type JWSJSON struct {
	Payload    string             `json:"payload"`
	Signatures []JWSJSONSignature `json:"signatures"`
}

// JWSJSONSignature is one entry in a JSON-serialization JWS.
type JWSJSONSignature struct {
	Protected string `json:"protected"`
	Signature string `json:"signature"`
}
