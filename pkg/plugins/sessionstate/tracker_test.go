package sessionstate

import (
	"context"
	"net"
	"testing"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
)

// TestOnHello_RecordsLogicalDatabase: the only thing this plugin
// does — copy hello.Database into SessionState.LogicalDatabase. The
// hello message itself is not mutated; wire-level rewriting is the
// rewrite plugin's job, and mid-session USE tracking is handled by
// the rewriter via its own response.
func TestOnHello_RecordsLogicalDatabase(t *testing.T) {
	s := chsession.NewSessionState()
	p := &Plugin{}
	hello := &chproto.ClientHello{Database: "database1", User: "alice"}
	if err := p.OnHello(context.Background(), sessionWithState(s), hello); err != nil {
		t.Fatalf("OnHello: %v", err)
	}
	snap := s.Snapshot()
	if snap.LogicalDatabase != "database1" {
		t.Errorf("LogicalDatabase=%q, want database1", snap.LogicalDatabase)
	}
	if hello.Database != "database1" {
		t.Errorf("hello.Database=%q was mutated; want unchanged", hello.Database)
	}
}

// TestOnHello_EmptyDatabase_NoOp: clients can connect without a
// default database. Nothing should be recorded in that case.
func TestOnHello_EmptyDatabase_NoOp(t *testing.T) {
	s := chsession.NewSessionState()
	p := &Plugin{}
	hello := &chproto.ClientHello{User: "alice"}
	if err := p.OnHello(context.Background(), sessionWithState(s), hello); err != nil {
		t.Fatalf("OnHello: %v", err)
	}
	if snap := s.Snapshot(); snap.LogicalDatabase != "" {
		t.Errorf("LogicalDatabase=%q; want unchanged empty", snap.LogicalDatabase)
	}
}

// sessionWithState returns a minimal Session backed by the given state.
func sessionWithState(s *chsession.SessionState) chsession.Session {
	return &stateOnlySession{state: s}
}

type stateOnlySession struct {
	state *chsession.SessionState
}

func (s *stateOnlySession) ID() int64                                          { return 0 }
func (s *stateOnlySession) State() *chsession.SessionState                     { return s.state }
func (s *stateOnlySession) Client() *chproto.Codec                             { return nil }
func (s *stateOnlySession) Upstream() *chproto.Codec                           { return nil }
func (s *stateOnlySession) RemoteAddr() net.Addr                               { return nil }
func (s *stateOnlySession) Close() error                                       { return nil }
func (s *stateOnlySession) BindUpstream(context.Context, *chproto.Codec) error { return nil }
func (s *stateOnlySession) RebindUpstream(context.Context, *chproto.Codec, bool) error {
	return nil
}
func (s *stateOnlySession) RebindToPeer(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}
func (s *stateOnlySession) RebindToLocal(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}
