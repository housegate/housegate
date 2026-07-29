package commitgate

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// databaseNamePattern is the canonical name regex for newly-created
// databases:
//
//   - 1-63 chars total
//   - leading letter
//   - body of letters / digits, optionally split by single underscores
//
// Names that violate this are rejected before reaching ClickHouse so
// downstream consumers (NetworkState registrars, peer routing, table
// prefixing) never have to handle exotic identifiers.
var databaseNamePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9]*(?:_[a-zA-Z0-9]+)*$`)

const databaseNameMaxLen = 63

// ethereumAddressHexLen is the canonical Ethereum address body length:
// 40 hex characters after the 0x prefix.
const ethereumAddressHexLen = 40

// ethereumAddressPattern matches a full Ethereum address — exactly 40
// hex characters (case-insensitive), with an optional "0x"/"0X"
// prefix. Short / truncated addresses are rejected; callers that want
// to accept short forms must left-zero-pad before calling
// NormalizeEthereumAddress.
var ethereumAddressPattern = regexp.MustCompile(`^(0[xX])?[0-9a-fA-F]{40}$`)

// ArgsValidatorObserver fail-closes statements whose surface arguments
// violate identifier rules:
//
//   - StatementTypeCreateDatabase: the target database name must match
//     databaseNamePattern.
//   - StatementTypeGrant / StatementTypeRevoke: every (non-CURRENT_USER)
//     grantee must be a full Ethereum address — exactly 40 hex
//     characters with an optional "0x"/"0X" prefix.
//
// Stateless; safe to share across sessions. The observer does not
// mutate the Event — its sole effect is fail-closing malformed
// statements before any downstream observer runs.
type ArgsValidatorObserver struct{}

// NewArgsValidatorObserver returns a ready-to-use validator.
func NewArgsValidatorObserver() *ArgsValidatorObserver {
	return &ArgsValidatorObserver{}
}

// SubscribedTypes returns the union of statement kinds whose surface
// arguments this observer polices.
func (o *ArgsValidatorObserver) SubscribedTypes() []sqlmeta.StatementType {
	return []sqlmeta.StatementType{
		sqlmeta.StatementTypeCreateDatabase,
		sqlmeta.StatementTypeGrant,
		sqlmeta.StatementTypeRevoke,
	}
}

// BeforeStatement dispatches per StatementType. Returns nil on success
// or a non-nil error to fail-close the statement.
func (o *ArgsValidatorObserver) BeforeStatement(_ context.Context, ev *Event) error {
	switch ev.Type {
	case sqlmeta.StatementTypeCreateDatabase:
		return validateDatabaseName(ev)
	case sqlmeta.StatementTypeGrant, sqlmeta.StatementTypeRevoke:
		return validateGrantees(ev)
	default:
		return nil
	}
}

// validateDatabaseName checks ev.AccessedTables[0].LogicalDatabase. The
// rewriter populates LogicalDatabase from the user-typed name so we
// reject the surface form, not a downstream prefixed/munged variant.
// Falls back to OriginalDatabase when LogicalDatabase is empty.
func validateDatabaseName(ev *Event) error {
	if len(ev.AccessedTables) == 0 {
		return fmt.Errorf("name: CREATE DATABASE has no target (rewriter surfaced no AccessedTables)")
	}
	t := ev.AccessedTables[0]
	name := t.LogicalDatabase
	if name == "" {
		name = t.OriginalDatabase
	}
	if name == "" {
		return fmt.Errorf("name: CREATE DATABASE has empty database name")
	}
	if len(name) > databaseNameMaxLen {
		return fmt.Errorf("name: database %q exceeds %d-char limit", name, databaseNameMaxLen)
	}
	if !databaseNamePattern.MatchString(name) {
		return fmt.Errorf(
			"name: database %q does not match required pattern (must start with a letter, "+
				"contain only letters/digits/underscores, and not begin or end with an underscore "+
				"or contain consecutive underscores)",
			name,
		)
	}
	return nil
}

// validateGrantees walks every (delta, grantee) pair and rejects any
// grantee name that does not normalise to a canonical Ethereum
// address. CURRENT_USER grantees are exempt — they are resolved at
// evaluation time by the downstream auth service.
func validateGrantees(ev *Event) error {
	if len(ev.PrivilegesDeltas) == 0 {
		return fmt.Errorf("name: %s has no privilege deltas (rewriter surfaced no PrivilegesDeltas)", ev.Type)
	}
	for _, d := range ev.PrivilegesDeltas {
		if len(d.Grantees) == 0 {
			return fmt.Errorf("name: %s has empty grantee list", ev.Type)
		}
		for _, g := range d.Grantees {
			if g.IsCurrentUser {
				continue
			}
			if _, err := NormalizeEthereumAddress(g.Name); err != nil {
				return fmt.Errorf("name: %s grantee %q is not a valid Ethereum address: %w", ev.Type, g.Name, err)
			}
		}
	}
	return nil
}

// NormalizeEthereumAddress validates an Ethereum address and returns
// its canonical form: a lower-cased "0x" prefix followed by exactly
// ethereumAddressHexLen hex characters. The "0x"/"0X" prefix is
// optional on input but always present on output.
//
// Examples:
//
//	"0xABCDef0123456789ABCDef0123456789ABCDef01"
//	  → "0xabcdef0123456789abcdef0123456789abcdef01"
//	"abcdef0123456789abcdef0123456789abcdef01"
//	  → "0xabcdef0123456789abcdef0123456789abcdef01"
//
// Inputs that are not exactly 40 hex characters (with or without the
// 0x prefix) are rejected — short / truncated addresses must be
// left-zero-padded by the caller before normalisation.
func NormalizeEthereumAddress(s string) (string, error) {
	if !ethereumAddressPattern.MatchString(s) {
		return "", fmt.Errorf("address must be exactly %d hex characters with optional 0x prefix", ethereumAddressHexLen)
	}
	body := s
	if strings.HasPrefix(body, "0x") || strings.HasPrefix(body, "0X") {
		body = body[2:]
	}
	return "0x" + strings.ToLower(body), nil
}

// OnStatementException is a no-op — the validator never mutates state.
func (o *ArgsValidatorObserver) OnStatementException(_ context.Context, _ *Event, _ *chproto.Exception) {
}

var _ Observer = (*ArgsValidatorObserver)(nil)
