package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"housegate/housegate/pkg/log"
)

// EthValidator validates queries using secp256k1 signatures produced by
// an Ethereum-style signer. Verification confirms that:
//
//   - the token is well-formed JWS (compact or JSON serialisation),
//   - the algorithm is ES256K / secp256k1,
//   - the issued-at timestamp is within MaxTokenAge (with a small clock
//     skew tolerance),
//   - the payload QueryHash matches Keccak256(SQL), binding the
//     signature to this exact query body,
//   - the recovered signer address is in AllowedAddresses (or the
//     allowlist is empty, i.e. any authenticated signer is accepted).
type EthValidator struct {
	// AllowedAddresses is the lowercase, 0x-prefixed allowlist.
	AllowedAddresses map[string]bool
	// MaxTokenAge caps the age of the iat claim.
	MaxTokenAge time.Duration
	// Enabled=false short-circuits ValidateQuery to a no-op pass.
	Enabled bool
	// AllowNoAuth=true lets queries without a token pass.
	AllowNoAuth bool
	// IndexerAddress, when non-empty, is the lowercase 0x-prefixed
	// Ethereum address authorised to invoke SQL_sentio_maintenance=1.
	// Set via SetIndexerAddress to the address derived from this
	// housegate's RelayPrivateKeyHex (the co-located indexer's key).
	// When empty, every maintenance request is silently downgraded to
	// non-maintenance — the host has no indexer identity to trust.
	IndexerAddress string
	// PlatformOperatorAddresses is the lowercase 0x-prefixed allowlist
	// of Ethereum addresses authorised to invoke
	// SQL_sentio_platform_operator. Empty map (or nil) disables the
	// platform-operator bypass entirely — every request carrying the
	// setting is rejected.
	PlatformOperatorAddresses map[string]bool
}

// NewEthValidator constructs an EthValidator from the supplied options.
// Addresses are lowercased internally. indexerAddress is the address
// authorised to invoke SQL_sentio_maintenance=1 — pass the address
// derived from cfg.RelayPrivateKeyHex (this housegate's co-located
// indexer), or empty to disable the maintenance bypass entirely.
// platformOperatorAddresses is the allowlist of addresses authorised
// to invoke SQL_sentio_platform_operator; nil/empty disables the
// platform-operator bypass entirely.
func NewEthValidator(addresses []string, maxAge time.Duration, enabled bool, allowNoAuth bool, indexerAddress string, platformOperatorAddresses []string) *EthValidator {
	allowed := make(map[string]bool, len(addresses))
	for _, addr := range addresses {
		allowed[strings.ToLower(addr)] = true
	}
	var operators map[string]bool
	if len(platformOperatorAddresses) > 0 {
		operators = make(map[string]bool, len(platformOperatorAddresses))
		for _, addr := range platformOperatorAddresses {
			operators[strings.ToLower(addr)] = true
		}
	}
	return &EthValidator{
		AllowedAddresses:          allowed,
		MaxTokenAge:               maxAge,
		Enabled:                   enabled,
		AllowNoAuth:               allowNoAuth,
		IndexerAddress:            strings.ToLower(indexerAddress),
		PlatformOperatorAddresses: operators,
	}
}

// ValidateQuery implements Validator. On the success path it returns a
// ValidationResult populated with the recovered signer plus any bypass
// flags that match the query.
//
// The bypass flags are never set on any error-return path: if JWS verify
// fails (no token, expired, signature mismatch, signer not in
// allow-list) the request is rejected as it would be without any flag.
// The flags only signal "this authenticated identity is invoking the
// corresponding bypass" — downstream plugins (rewrite, usage, commitgate)
// treat the request as trusted and forward verbatim once the flag is
// set on SessionState.
//
// The two bypass flags are independent — a single query may carry one,
// the other, or both. Maintenance is gated on the co-located indexer
// address; platform-operator is gated on its own allowlist (configured
// via authplugin.Config.PlatformOperatorAddresses).
func (v *EthValidator) ValidateQuery(ctx context.Context, meta QueryMeta) (ValidationResult, error) {
	if !v.Enabled {
		return ValidationResult{}, nil
	}

	token, ok := meta.Settings[AuthTokenSettingKey]
	if !ok || token == "" {
		if v.AllowNoAuth {
			log.Infow("no auth token, allowing due to allow_no_auth=true", "source", "eth_validator")
			return ValidationResult{}, nil
		}
		return ValidationResult{}, errors.New("missing authentication token")
	}

	// Trim possible quotes added by some client libs.
	token = strings.Trim(token, "\"'")

	var (
		addr string
		err  error
	)
	if strings.HasPrefix(strings.TrimSpace(token), "{") {
		addr, err = v.validateJWSJSON(token, meta.SQL)
	} else {
		addr, err = v.validateJWSCompact(token, meta.SQL)
	}
	if err != nil {
		return ValidationResult{}, err
	}
	res := ValidationResult{Address: addr}
	// Maintenance bypass: only signaled after a successful JWS verify +
	// allow-list match AND the recovered signer matching this housegate's
	// configured indexer address. AllowedAddresses can include non-indexer
	// signers (project owners, end-user wallets, etc.); maintenance is
	// strictly tighter — it bypasses rewrite, billing, and per-database
	// permission checks, so it must be gated on indexer identity.
	//
	// When the setting is supplied by a non-indexer signer (or by anyone
	// when no indexer address is configured), the request is rejected
	// outright rather than silently downgraded — a misconfigured caller
	// should hear about it loudly.
	if isMaintenance(meta.Settings) {
		if v.IndexerAddress == "" || !strings.EqualFold(addr, v.IndexerAddress) {
			return ValidationResult{}, fmt.Errorf("SQL_sentio_maintenance is reserved for the co-located indexer; signer %s is not authorized", addr)
		}
		res.Maintenance = true
	}
	// Platform-operator bypass: same shape as maintenance but gated on
	// the operator allowlist instead of the indexer address. Rejected
	// outright when the setting is present but the signer is not in the
	// allowlist (or no allowlist is configured) so misconfiguration is
	// loud rather than silent.
	if isPlatformOperator(meta.Settings) {
		if len(v.PlatformOperatorAddresses) == 0 || !v.PlatformOperatorAddresses[strings.ToLower(addr)] {
			return ValidationResult{}, fmt.Errorf("SQL_sentio_platform_operator is reserved for the platform-operator allowlist; signer %s is not authorized", addr)
		}
		res.PlatformOperator = true
	}
	return res, nil
}

func (v *EthValidator) validateJWSCompact(token, sql string) (string, error) {
	header, payload, signature, err := parseJWSCompact(token)
	if err != nil {
		return "", fmt.Errorf("invalid JWS token: %w", err)
	}
	if err := v.verifyPayloadAndHeader(header, payload, sql); err != nil {
		return "", err
	}
	signingInput := token[:strings.LastIndex(token, ".")]
	recoveredAddr, err := v.recoverAddressFromInput(signingInput, signature)
	if err != nil {
		return "", fmt.Errorf("signature verification failed: %w", err)
	}
	if len(v.AllowedAddresses) > 0 && !v.AllowedAddresses[strings.ToLower(recoveredAddr)] {
		return "", fmt.Errorf("address %s not in allowlist", recoveredAddr)
	}
	log.Debugw("authenticated query", "source", "eth_validator", "format", "compact", "address", recoveredAddr)
	return recoveredAddr, nil
}

func (v *EthValidator) validateJWSJSON(token, sql string) (string, error) {
	var jws JWSJSON
	if err := json.Unmarshal([]byte(token), &jws); err != nil {
		return "", fmt.Errorf("invalid JWS JSON: %w", err)
	}
	if len(jws.Signatures) == 0 {
		return "", errors.New("no signatures found in JWS JSON")
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(jws.Payload)
	if err != nil {
		return "", fmt.Errorf("invalid payload encoding: %w", err)
	}
	var payload JWSPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return "", fmt.Errorf("invalid payload JSON: %w", err)
	}

	var authenticatedAddresses []string
	for i, sig := range jws.Signatures {
		headerBytes, err := base64.RawURLEncoding.DecodeString(sig.Protected)
		if err != nil {
			return "", fmt.Errorf("sig[%d]: invalid protected header encoding: %w", i, err)
		}
		var header JWSHeader
		if err := json.Unmarshal(headerBytes, &header); err != nil {
			return "", fmt.Errorf("sig[%d]: invalid header JSON: %w", i, err)
		}
		if err := v.verifyPayloadAndHeader(header, payload, sql); err != nil {
			return "", fmt.Errorf("sig[%d]: %w", i, err)
		}
		signatureBytes, err := base64.RawURLEncoding.DecodeString(sig.Signature)
		if err != nil {
			return "", fmt.Errorf("sig[%d]: invalid signature encoding: %w", i, err)
		}
		signingInput := sig.Protected + "." + jws.Payload
		recoveredAddr, err := v.recoverAddressFromInput(signingInput, signatureBytes)
		if err != nil {
			return "", fmt.Errorf("sig[%d]: signature verification failed: %w", i, err)
		}
		if len(v.AllowedAddresses) > 0 && !v.AllowedAddresses[strings.ToLower(recoveredAddr)] {
			return "", fmt.Errorf("sig[%d]: address %s not in allowlist", i, recoveredAddr)
		}
		authenticatedAddresses = append(authenticatedAddresses, recoveredAddr)
	}

	log.Infow("authenticated query", "source", "eth_validator", "format", "json", "addresses", authenticatedAddresses)
	return authenticatedAddresses[0], nil
}

// isMaintenance reports whether the caller marked this query as a
// trusted maintenance call by setting SQL_sentio_maintenance to a
// truthy value ("1" or case-insensitive "true"). The flag is only
// honoured AFTER a successful JWS verify + allow-list match — it does
// not bypass authentication.
//
// Surrounding `'` / `"` are trimmed before matching: clickhouse-go's
// CustomSetting wraps every Custom-flagged string value through
// Field::restoreFromDump (single-quoted form), and sidecar/route
// signers do the same so CH parses the value correctly. The validator
// trims the same quoting on AuthTokenSettingKey above; mirror it here
// so the maintenance check is invariant to the client's wire format.
func isMaintenance(settings map[string]string) bool {
	v, ok := settings[MaintenanceSettingKey]
	if !ok {
		return false
	}
	v = strings.Trim(strings.TrimSpace(v), "\"'")
	return v == "1" || strings.EqualFold(v, "true")
}

// isPlatformOperator mirrors isMaintenance: same truthy parsing
// (`1` / case-insensitive `true`, surrounding quotes trimmed to handle
// clickhouse-go's CustomSetting wire wrapping). Any presence of the
// setting with a truthy value triggers the bypass — the validator
// then enforces the address allowlist before honouring it.
func isPlatformOperator(settings map[string]string) bool {
	v, ok := settings[PlatformOperatorSettingKey]
	if !ok {
		return false
	}
	v = strings.Trim(strings.TrimSpace(v), "\"'")
	return v == "1" || strings.EqualFold(v, "true")
}

func (v *EthValidator) verifyPayloadAndHeader(header JWSHeader, payload JWSPayload, sql string) error {
	if header.Alg != "ES256K" && header.Alg != "secp256k1" {
		return fmt.Errorf("unsupported algorithm: %s", header.Alg)
	}

	const clockSkewTolerance int64 = 5
	now := time.Now().Unix()
	tokenAge := now - payload.Iat
	if tokenAge < -clockSkewTolerance {
		return errors.New("token issued in the future")
	}
	if tokenAge < 0 {
		tokenAge = 0
	}
	if time.Duration(tokenAge)*time.Second > v.MaxTokenAge {
		return fmt.Errorf("token expired: age %ds exceeds max %s", tokenAge, v.MaxTokenAge)
	}

	expectedHash := keccak256Hex([]byte(sql))
	if !strings.EqualFold(payload.QueryHash, expectedHash) {
		return fmt.Errorf("query hash mismatch: expected %s, got %s", expectedHash, payload.QueryHash)
	}
	return nil
}

func (v *EthValidator) recoverAddressFromInput(signingInput string, signature []byte) (string, error) {
	messageHash := keccak256([]byte(signingInput))
	return recoverAddress(messageHash, signature)
}

// ValidatePeerLogin verifies a peer-relay JWS (produced by
// RelaySigner.SignPeerLogin) and returns the recovered signer address.
// expectedAudience must match the token's `aud` claim — pass the
// receiving proxy's indexer-id (decimal-stringified) so a token signed
// for proxy A cannot be replayed against proxy B.
//
// Validation rules:
//   - alg is ES256K / secp256k1
//   - purpose claim equals PeerLoginPurpose (domain separation from
//     query-binding JWS)
//   - aud claim equals expectedAudience
//   - now is within [iat-skew, exp+skew]
//   - recovered signer address is in AllowedAddresses (or allowlist
//     is empty, which means "any authenticated signer is accepted")
//
// Validates only the compact serialization — peer login tokens are
// always single-signer and short-lived; the JSON / multi-sig form is
// not allowed here.
func (v *EthValidator) ValidatePeerLogin(token, expectedAudience string) (string, error) {
	parts := strings.Split(strings.Trim(token, "\"'"), ".")
	if len(parts) != 3 {
		return "", errors.New("invalid peer JWS format: expected 3 parts")
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", fmt.Errorf("invalid header encoding: %w", err)
	}
	var header JWSHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return "", fmt.Errorf("invalid header JSON: %w", err)
	}
	if header.Alg != "ES256K" && header.Alg != "secp256k1" {
		return "", fmt.Errorf("unsupported algorithm: %s", header.Alg)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("invalid payload encoding: %w", err)
	}
	var payload JWSPeerPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return "", fmt.Errorf("invalid peer payload JSON: %w", err)
	}
	if payload.Purpose != PeerLoginPurpose {
		return "", fmt.Errorf("unexpected purpose %q (want %q)", payload.Purpose, PeerLoginPurpose)
	}
	if payload.Aud != expectedAudience {
		return "", fmt.Errorf("audience mismatch: got %q, expected %q", payload.Aud, expectedAudience)
	}
	const clockSkewTolerance int64 = 5
	now := time.Now().Unix()
	if payload.Iat-now > clockSkewTolerance {
		return "", errors.New("peer token issued in the future")
	}
	if payload.Exp == 0 || now-payload.Exp > clockSkewTolerance {
		return "", fmt.Errorf("peer token expired: now=%d exp=%d", now, payload.Exp)
	}

	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", fmt.Errorf("invalid signature encoding: %w", err)
	}
	signingInput := parts[0] + "." + parts[1]
	recoveredAddr, err := v.recoverAddressFromInput(signingInput, signature)
	if err != nil {
		return "", fmt.Errorf("peer signature recovery failed: %w", err)
	}
	if len(v.AllowedAddresses) > 0 && !v.AllowedAddresses[strings.ToLower(recoveredAddr)] {
		return "", fmt.Errorf("peer address %s not in allowlist", recoveredAddr)
	}
	log.Debugw("peer-relay login validated",
		"source", "eth_validator",
		"address", recoveredAddr,
		"audience", payload.Aud,
	)
	return recoveredAddr, nil
}
