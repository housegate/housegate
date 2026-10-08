// Package sipeerguard refuses, on the storage-integrity host, a peer-trusted
// statement that reads the ordinary physical table of a governed table (spec
// 2026-10-09 §6.3, D19). Another indexer's rewriter turns a read of an SI
// table into a remote() whose secondary query names that ordinary table; it
// arrives peer-trusted and non-forwarded, bypasses rewrite by design, and
// would silently read 0 rows instead of the hg_safe/hg_unsafe data.
package sipeerguard

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/sireserved"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlsurface"
)

var refusals = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "storage_integrity_peer_guard_refusals_total",
	Help: "Peer-trusted statements refused because they name the ordinary physical table of a governed storage-integrity table.",
})

func init() { prometheus.MustRegister(refusals) }

// Plugin is the host guard. PhysicalDatabase is rewriter.physical_database;
// TableState is the host's storage-integrity table state.
type Plugin struct {
	PhysicalDatabase string
	TableState       sitable.TableState
}

// OnQuery acts only on peer-trusted, non-forwarded sessions.
func (p *Plugin) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if p == nil || p.TableState == nil || p.PhysicalDatabase == "" || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return nil
	}
	snap := qctx.Session.State().Snapshot()
	if !snap.IsPeerTrusted || snap.IsForwardedFromPeer {
		return nil
	}
	surfaces, err := sqlsurface.ScanWith(qctx.Query.Body, sqlsurface.Options{DecodeStringEscapes: true})
	if err != nil {
		refusals.Inc()
		return &chproto.ClientError{Code: chproto.CodeQueryIsProhibited,
			Message: "storage_integrity: a peer statement that cannot be checked for governed tables is refused: " + err.Error()}
	}
	tables := p.TableState.Current()
	for _, candidate := range candidates(surfaces.Tokens, p.PhysicalDatabase) {
		for _, split := range splits(candidate) {
			if table := tables.Lookup(split[0], split[1]); table.Status != sitable.Ordinary {
				refusals.Inc()
				return &chproto.ClientError{Code: chproto.CodeQueryIsProhibited, Message: fmt.Sprintf(
					"storage_integrity: table %s.%s is governed by storage integrity and must be read through its host indexer; connect with --database %s or USE %s",
					split[0], split[1], split[0], split[0])}
			}
		}
	}
	return nil
}

// candidates returns every table text the statement may read as an ordinary
// physical table: (1) <physical>.<quoted> in any quoting of the database;
// (2) a standalone quoted identifier containing '.'; (3) a string literal
// containing '.' among a carrier's arguments, skipping the first (an address
// or cluster name, plan decision P9).
func candidates(tokens []sqlsurface.Token, physical string) []string {
	isPunct := func(i int, text string) bool {
		return i >= 0 && i < len(tokens) && tokens[i].Kind == sqlsurface.TokenPunct && tokens[i].Text == text
	}
	var out []string
	for i, tok := range tokens {
		if (tok.Kind == sqlsurface.TokenWord || tok.Kind == sqlsurface.TokenQuoted) && tok.Text == physical &&
			isPunct(i+1, ".") && i+2 < len(tokens) && tokens[i+2].Kind == sqlsurface.TokenQuoted {
			out = append(out, tokens[i+2].Text)
		}
		if tok.Kind == sqlsurface.TokenQuoted && strings.Contains(tok.Text, ".") && !isPunct(i-1, ".") && !isPunct(i+1, ".") {
			out = append(out, tok.Text)
		}
		if tok.Kind == sqlsurface.TokenWord && sireserved.IsObjectCarrierName(tok.Text) && isPunct(i+1, "(") {
			out = append(out, carrierLiterals(tokens, i+1)...)
		}
	}
	return out
}

func carrierLiterals(tokens []sqlsurface.Token, open int) []string {
	var out []string
	depth, argument := 0, 0
	for i := open; i < len(tokens); i++ {
		tok := tokens[i]
		if tok.Kind == sqlsurface.TokenPunct {
			switch tok.Text {
			case "(":
				depth++
			case ")":
				depth--
				if depth == 0 {
					return out
				}
			case ",":
				if depth == 1 {
					argument++
				}
			}
			continue
		}
		if tok.Kind == sqlsurface.TokenString && argument > 0 && strings.Contains(tok.Text, ".") {
			out = append(out, tok.Text)
		}
	}
	return out
}

// splits returns the (database, table) readings of a physical table text: at
// the first and at the last '.', deduplicated (plan decision P9).
func splits(text string) [][2]string {
	var out [][2]string
	add := func(i int) {
		if i <= 0 || i >= len(text)-1 {
			return
		}
		pair := [2]string{text[:i], text[i+1:]}
		for _, seen := range out {
			if seen == pair {
				return
			}
		}
		out = append(out, pair)
	}
	add(strings.IndexByte(text, '.'))
	add(strings.LastIndexByte(text, '.'))
	return out
}

// RunOnPeerTrust keeps the guard on peer-trusted sessions: they are its job.
func (*Plugin) RunOnPeerTrust() bool { return true }

// RejectUndecodableQuery refuses a Query Relay cannot decode instead of
// raw-splicing it past the guard.
func (*Plugin) RejectUndecodableQuery() bool { return true }

var (
	_ plugin.QueryPlugin             = (*Plugin)(nil)
	_ plugin.PeerTrustAware          = (*Plugin)(nil)
	_ plugin.StrictQueryDecodePlugin = (*Plugin)(nil)
)
