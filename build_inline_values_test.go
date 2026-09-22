package housegate

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
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

func TestBuildAgent_InlineValuesHelperPreservesConfiguredAccountContext(t *testing.T) {
	const owner = "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD"
	for _, tc := range []struct {
		name, owner     string
		driver, enabled bool
	}{
		{"default", "", false, true},
		{"owner", owner, false, true},
		{"driver", "", true, true},
		{"owner and driver", owner, true, true},
		{"disabled", owner, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := agentSICfg(t)
			cfg.Materialize.Enabled = true
			cfg.Agent.Owner, cfg.Agent.Driver = tc.owner, tc.driver
			cfg.StorageIntegrity.Agent.InlineValues.Enabled = tc.enabled
			seen := make(chan *chproto.Query, 1)
			serveErr := make(chan error, 1)
			stop := make(chan struct{})
			defer close(stop)
			builtEvaluator := false
			bs, err := buildAgentWithBuilders(Options{Config: cfg, StorageIntegrityTableSchemas: buildTestStorageIntegrityNetworkState()}, nil,
				func(*config.Config) (rewriter.Materializer, error) { return &recordingAgentMaterializer{}, nil },
				func(_ func(context.Context, string) (net.Conn, error), signer auth.Signer) inlineValuesEvaluator {
					builtEvaluator = true
					return sistatement.NewUpstreamValuesEvaluator(func(context.Context, string) (net.Conn, error) {
						client, server := net.Pipe()
						go func() {
							defer server.Close()
							err := serveInlineAccountContext(server, seen)
							serveErr <- err
							if err == nil {
								// Keep the pooled connection alive through the
								// evaluator's successful deadline reset/release.
								<-stop
							}
						}()
						return client, nil
					}, signer)
				})
			if err != nil {
				t.Fatal(err)
			}
			defer bs.teardown()
			chain := requireProxyServer(t, bs.listeners[0]).Hooks.(*plugin.PluginChain)
			var si *sistatement.Plugin
			for _, p := range chain.QueryPlugins {
				if p, ok := p.(*sistatement.Plugin); ok {
					si = p
				}
			}
			if si == nil {
				t.Fatal("sistatement missing")
			}
			client, peer := net.Pipe()
			defer peer.Close()
			sess := chsession.New(1, client)
			defer sess.Close()
			upClient, upPeer := net.Pipe()
			defer upPeer.Close()
			up := chproto.NewCodec(&configuredAddressConn{Conn: upClient, upstreamAddress: "selected:9000"}, chproto.DirToUpstream)
			up.SetRevision(54460)
			if err := sess.BindUpstream(context.Background(), up); err != nil {
				t.Fatal(err)
			}
			sess.State().ClientRevision = 54460
			sess.State().SetUpstreamHello(&chproto.ClientHello{ProtocolVersion: 54460, User: "writer", Database: "tenant"})
			sql := "INSERT INTO tenant.events VALUES (1, 'eu')"
			qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{Body: sql, ID: "original"}, Values: map[string]any{plugin.ValuesKeyMaterialized: "noop"}}
			if err := si.OnQuery(context.Background(), qctx); err != nil {
				t.Fatal(err)
			}
			if !tc.enabled {
				if builtEvaluator || len(seen) != 0 || qctx.SynthesizedInsert != nil || qctx.Query.Body != sql {
					t.Fatal("disabled inline behavior changed")
				}
				return
			}
			if !builtEvaluator || qctx.SynthesizedInsert == nil {
				t.Fatal("enabled inline lane did not evaluate")
			}
			if err := <-serveErr; err != nil {
				t.Fatal(err)
			}
			q := <-seen
			settings := map[string]string{}
			for _, s := range q.Settings {
				if _, exists := settings[s.Key]; exists {
					t.Fatalf("duplicate helper setting %s", s.Key)
				}
				settings[s.Key] = s.Value
				if (s.Key == auth.PayerSettingKey || s.Key == auth.DriverSettingKey) && !s.Custom {
					t.Fatalf("context setting not Custom: %+v", s)
				}
			}
			wantPayer, wantDriver := "", ""
			if tc.owner != "" {
				wantPayer = "'" + tc.owner + "'"
			}
			if tc.driver {
				wantDriver = "'1'"
			}
			if settings[auth.PayerSettingKey] != wantPayer || settings[auth.DriverSettingKey] != wantDriver {
				t.Fatalf("configured owner/driver lost before helper wire: payer=%q driver=%q, want %q/%q", settings[auth.PayerSettingKey], settings[auth.DriverSettingKey], wantPayer, wantDriver)
			}
			signer, err := auth.NewRelaySigner(cfg.Agent.PrivateKeyHex)
			if err != nil {
				t.Fatal(err)
			}
			validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, signer.Address(), nil)
			result, err := validator.ValidateQuery(context.Background(), auth.QueryMeta{SQL: q.Body, Settings: settings})
			if err != nil || result.IsDriver != tc.driver {
				t.Fatalf("helper auth context=%+v, err=%v", result, err)
			}
		})
	}
}

// serveInlineAccountContext decodes the real helper Query from its Native wire.
func serveInlineAccountContext(conn net.Conn, seen chan<- *chproto.Query) error {
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}
	srv := chproto.NewCodec(conn, chproto.DirFromClient)
	pkt, err := srv.ReadPacket(uint64(chproto.ClientHelloCode))
	if err != nil {
		return err
	}
	hello, ok := pkt.Decoded.(*chproto.ClientHello)
	if !ok {
		return fmt.Errorf("hello type %T", pkt.Decoded)
	}
	revision := int(hello.ProtocolVersion)
	srv.SetRevision(revision)
	if err := srv.WriteServerHello(&chproto.ServerHello{Name: "fake", Major: 25, Minor: 8, Revision: revision, Timezone: "UTC", DisplayName: "fake"}); err != nil {
		return err
	}
	if chproto.SupportsAddendum(revision) {
		if _, err := srv.NegotiateAddendum(chproto.AddendumOpts{ProposedRecv: "notchunked", ProposedSend: "notchunked"}); err != nil {
			return err
		}
	}
	pkt, err = srv.ReadPacket(uint64(chproto.ClientQueryCode))
	if err != nil {
		return err
	}
	q, ok := pkt.Decoded.(*chproto.Query)
	if !ok {
		return fmt.Errorf("query type %T", pkt.Decoded)
	}
	if _, err := srv.ReadPacket(); err != nil {
		return err
	}
	seen <- q
	id, region := &proto.ColUInt64{}, &proto.ColStr{}
	id.Append(1)
	region.Append("eu")
	var b proto.Buffer
	b.PutUVarInt(uint64(proto.ServerCodeData))
	b.PutString("")
	if err := (proto.Block{Rows: 1, Columns: 2}).EncodeBlock(&b, revision, proto.Input{{Name: "id", Data: id}, {Name: "region", Data: region}}); err != nil {
		return err
	}
	if err := srv.WriteRawPacket(b.Buf); err != nil {
		return err
	}
	var end proto.Buffer
	end.PutUVarInt(uint64(proto.ServerCodeEndOfStream))
	return srv.WriteRawPacket(end.Buf)
}
