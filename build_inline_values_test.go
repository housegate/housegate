package housegate

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugins/sistatement"
	"github.com/housegate/housegate/pkg/rewriter"
)

func TestBuildAgent_InlineValuesPrerequisites(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		mutate     func(*config.Config)
	}{
		{"materializer disabled", "requires materialize.enabled", func(c *config.Config) { c.Materialize.Enabled = false }},
		{"statement lane disabled", "requires storage_integrity.agent.enabled", func(c *config.Config) { c.StorageIntegrity.Agent.Enabled = false }},
		{"signer missing", "failed to create agent signer", func(c *config.Config) { c.Agent.PrivateKeyHex = "" }},
		{"selector unavailable", "network_state", func(c *config.Config) { c.Agent.Upstream = ""; c.NetworkState.Source = "" }},
		{"timeout too small", "evaluation timeout", func(c *config.Config) {
			c.StorageIntegrity.Agent.InlineValues.EvaluationTimeout.Duration = time.Millisecond
		}},
		{"row bound missing", "max rows", func(c *config.Config) { c.StorageIntegrity.Agent.InlineValues.MaxRows = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := agentSICfg(t)
			cfg.Materialize.Enabled = true
			cfg.StorageIntegrity.Agent.InlineValues.Enabled = true
			tc.mutate(cfg)
			bs, err := buildAgentWithMaterializerBuilder(Options{Config: cfg, StorageIntegrityTableSchemas: network.NewInMemoryNetworkState()}, nil,
				func(*config.Config) (rewriter.Materializer, error) { return &recordingAgentMaterializer{}, nil })
			if bs != nil {
				bs.teardown()
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("build error=%v, want %q", err, tc.want)
			}
		})
	}
}

type recordingInlineEvaluator struct{ closeCalls int }

func (*recordingInlineEvaluator) Evaluate(context.Context, sistatement.ValuesEvaluation) ([][]proto.InputColumn, error) {
	return nil, errors.New("unused evaluator")
}
func (e *recordingInlineEvaluator) Close() error { e.closeCalls++; return nil }

func TestBuildAgent_InlineValuesLifecycleAndSelectedEndpoint(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal teardown", true: "failed build"}[failed], func(t *testing.T) {
			cfg := agentSICfg(t)
			cfg.Materialize.Enabled = true
			cfg.StorageIntegrity.Agent.InlineValues.Enabled = true
			cfg.Agent.Upstream = ""
			if failed {
				cfg.StorageIntegrity.Agent.NetworkID = ""
			}
			m, e := &recordingAgentMaterializer{}, &recordingInlineEvaluator{}
			var capturedDial func(context.Context, string) (net.Conn, error)
			bs, err := buildAgentWithBuilders(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), StorageIntegrityTableSchemas: network.NewInMemoryNetworkState()}, nil,
				func(*config.Config) (rewriter.Materializer, error) { return m, nil },
				func(dial func(context.Context, string) (net.Conn, error), signer auth.Signer) inlineValuesEvaluator {
					if signer == nil {
						t.Fatal("evaluator signer missing")
					}
					capturedDial = dial
					return e
				})
			if failed {
				if err == nil || !strings.Contains(err.Error(), "network id is required") {
					t.Fatalf("build error=%v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if m.closeCalls != 0 || e.closeCalls != 0 {
					t.Fatal("resources closed before teardown")
				}
				// Empty topology makes Selector.Pick fail. The helper callback
				// must instead dial the supplied, already selected endpoint.
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				conn, err := capturedDial(ctx, listener.Addr().String())
				if err != nil {
					t.Fatalf("dial selected address: %v", err)
				}
				defer conn.Close()
				accepted, err := listener.Accept()
				if err != nil {
					t.Fatal(err)
				}
				accepted.Close()
				if got := conn.(interface{ UpstreamAddress() string }).UpstreamAddress(); got != listener.Addr().String() {
					t.Fatalf("endpoint identity=%q", got)
				}
				canceled, cancelNow := context.WithCancel(context.Background())
				cancelNow()
				if _, err := capturedDial(canceled, listener.Addr().String()); !errors.Is(err, context.Canceled) {
					t.Fatalf("dial ignored cancellation: %v", err)
				}
				bs.teardown()
			}
			if m.closeCalls != 1 || e.closeCalls != 1 {
				t.Fatalf("close calls materializer=%d evaluator=%d, want 1/1", m.closeCalls, e.closeCalls)
			}
		})
	}
}

func TestBuildAgent_InlineValuesDisabledDoesNotBuildEvaluator(t *testing.T) {
	cfg := agentSICfg(t)
	called := false
	bs, err := buildAgentWithBuilders(Options{Config: cfg, StorageIntegrityTableSchemas: network.NewInMemoryNetworkState()}, nil,
		func(*config.Config) (rewriter.Materializer, error) {
			t.Fatal("disabled materializer constructed")
			return nil, nil
		},
		func(func(context.Context, string) (net.Conn, error), auth.Signer) inlineValuesEvaluator {
			called = true
			return &recordingInlineEvaluator{}
		})
	if err != nil {
		t.Fatal(err)
	}
	bs.teardown()
	if called {
		t.Fatal("disabled inline lane constructed evaluator")
	}
}
