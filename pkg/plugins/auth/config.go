package authplugin

import "github.com/housegate/housegate/pkg/cfgtypes"

// Config is the operator-tunable surface for the JWS auth plugin. It
// lives here (next to Plugin) so adding fields stays a single-package
// change.
type Config struct {
	// Enabled toggles JWS verification. When false, every query passes
	// through unauthenticated.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// AllowedAddresses lists the lowercase 0x-prefixed Ethereum
	// addresses whose signatures are accepted. Empty list means "any
	// authenticated signer is accepted".
	AllowedAddresses []string `json:"allowed_addresses" yaml:"allowed_addresses"`

	// MaxTokenAge caps the iat claim age. Tokens older than this are
	// rejected.
	MaxTokenAge cfgtypes.Duration `json:"max_token_age" yaml:"max_token_age"`

	// AllowNoAuth, when true, lets queries arrive without any token at
	// all. Useful during soft rollouts.
	AllowNoAuth bool `json:"allow_no_auth" yaml:"allow_no_auth"`

	// PlatformOperatorAddresses lists the lowercase 0x-prefixed
	// Ethereum addresses authorised to invoke
	// SQL_sentio_platform_operator. A query carrying that setting
	// whose recovered signer is in this list is marked as a
	// platform-operator session — rewrite, usage, and commitgate
	// plugins forward verbatim. Empty list disables the bypass
	// entirely; any query carrying the setting is rejected.
	PlatformOperatorAddresses []string `json:"platform_operator_addresses" yaml:"platform_operator_addresses"`
}
