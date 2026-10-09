package storageintegrity

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrClientLanesNotEnabled refuses a laned statement id on a network whose
// arbiter has not activated client lanes (spec 2026-10-09 §6.1, D13). In
// release A1 every laned id is refused with it; Plan B gates it on the
// table-registry snapshot instead.
var ErrClientLanesNotEnabled = errors.New("storage_integrity: client lanes are not enabled on this network")

// StatementID is the structured statement identity of spec 2026-10-09 §5.1.
// Lane is empty for the legacy default lane. The nonce is entropy only; the
// uniqueness key is (Account, Lane, Seq).
type StatementID struct {
	Account string
	Lane    string
	Seq     uint64
	Nonce   string
}

const statementIDShape = "<client_account>:<client_seq>:<client_nonce> or <client_account>:<lane>:<client_seq>:<client_nonce>"

// ParseStatementID parses either flat form. The forms are disjoint by segment
// count because a nonce cannot contain ':'. Every rule is spec §5.1's; this is
// the only parser, shared by the ingress, the intake, the arbiter conversion
// and the agent.
func ParseStatementID(flat string) (StatementID, error) {
	parts := strings.Split(flat, ":")
	var (
		id      StatementID
		seqText string
	)
	switch len(parts) {
	case 3:
		id.Account, seqText, id.Nonce = parts[0], parts[1], parts[2]
	case 4:
		id.Account, id.Lane, seqText, id.Nonce = parts[0], parts[1], parts[2], parts[3]
		if !isLane(id.Lane) {
			return StatementID{}, fmt.Errorf("requires a 16-character lowercase hex lane, got %q", id.Lane)
		}
	default:
		return StatementID{}, fmt.Errorf("requires %s", statementIDShape)
	}
	if id.Account == "" || seqText == "" || id.Nonce == "" {
		return StatementID{}, fmt.Errorf("requires %s", statementIDShape)
	}
	if id.Account != strings.ToLower(id.Account) || !strings.HasPrefix(id.Account, "0x") || !isLowerHex(id.Account[2:]) {
		return StatementID{}, errors.New("requires lowercase 0x client_account")
	}
	if (len(seqText) > 1 && seqText[0] == '0') || !isDecimalDigits(seqText) {
		return StatementID{}, errors.New("requires canonical decimal client_seq")
	}
	seq, err := strconv.ParseUint(seqText, 10, 64)
	if err != nil || seq == 0 {
		return StatementID{}, errors.New("requires non-zero decimal client_seq")
	}
	if strings.TrimSpace(id.Nonce) != id.Nonce {
		return StatementID{}, errors.New("requires non-empty client_nonce")
	}
	id.Seq = seq
	return id, nil
}

// ParseLegacyStatementID is ParseStatementID for components that do not
// accept lanes yet: a laned id fails with ErrClientLanesNotEnabled.
func ParseLegacyStatementID(flat string) (StatementID, error) {
	id, err := ParseStatementID(flat)
	if err != nil {
		return StatementID{}, err
	}
	if id.IsLaned() {
		return StatementID{}, fmt.Errorf("statement id %s: %w", flat, ErrClientLanesNotEnabled)
	}
	return id, nil
}

// Flat renders the canonical flat form: three segments for the legacy lane,
// four for a laned id. It is the string the JWS, the ClickHouse query id and
// _hg_row_id bind.
func (id StatementID) Flat() string {
	seq := strconv.FormatUint(id.Seq, 10)
	if id.Lane == "" {
		return id.Account + ":" + seq + ":" + id.Nonce
	}
	return id.Account + ":" + id.Lane + ":" + seq + ":" + id.Nonce
}

// Subject is the accumulator key of spec D12: the account for the legacy
// lane, account + ":" + lane otherwise.
func (id StatementID) Subject() string {
	if id.Lane == "" {
		return id.Account
	}
	return id.Account + ":" + id.Lane
}

// IsLaned reports whether the id carries a lane segment.
func (id StatementID) IsLaned() bool { return id.Lane != "" }

func isLane(s string) bool { return len(s) == 16 && isLowerHex(s) }

func isLowerHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f')) {
			return false
		}
	}
	return true
}

func isDecimalDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
