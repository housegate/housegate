package housegate

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/endpointtls"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

func signerModeConfig() *config.Config {
	on := true
	cfg := &config.Config{}
	cfg.NetworkState.EndpointTLS = &on
	cfg.NetworkState.EndpointVerify = config.EndpointVerifySigner
	cfg.NetworkState.EndpointChainID = 7892301
	return cfg
}

func TestEndpointTLSConfigModes(t *testing.T) {
	cfg := &config.Config{}
	if c, err := endpointTLSConfig(cfg, nil); err != nil || c.Enabled() {
		t.Fatalf("endpoint_tls unset: %v %v", c, err)
	}
	on := true
	cfg.NetworkState.EndpointTLS = &on
	if c, err := endpointTLSConfig(cfg, nil); err != nil || !c.Enabled() || c.SignerMode() {
		t.Fatalf("hostname mode: %v", err)
	}
	if _, err := endpointTLSConfig(signerModeConfig(), nil); err == nil || !strings.Contains(err.Error(), "needs a network state") {
		t.Fatalf("signer mode without a network state: %v", err)
	}
	// A pinned upstream stays on host-name verification in the signer mode.
	pinned := signerModeConfig()
	pinned.Agent.Upstream = "hg.example.com:9001"
	pinned.Agent.UpstreamTLS = true
	if c, err := agentUpstreamTLS(pinned, nil); err != nil || !c.Enabled() || c.SignerMode() {
		t.Fatalf("pinned upstream: %v", err)
	}
}

// TestEndpointSignerModeDialsTheRegisteredSigner dials an advertised housegate
// address through the build's signer-mode configuration, looked up in an
// in-memory network state.
func TestEndpointSignerModeDialsTheRegisteredSigner(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	cert, err := endpointtls.NewSignerCertificate(key, 7892301, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	_, portText, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.ParseUint(portText, 10, 16)

	state := network.NewInMemoryNetworkState()
	state.IndexerInfos[1] = network.IndexerInfo{IndexerId: 1, IndexerUrl: "127.0.0.1", ClickhouseProxyPort: uint16(port), Signer: crypto.PubkeyToAddress(key.PublicKey).Hex()}
	cfg, err := endpointTLSConfig(signerModeConfig(), func() (registry.Topology, error) { return state, nil })
	if err != nil || !cfg.SignerMode() {
		t.Fatalf("signer config: %v", err)
	}
	c, err := cfg.Dial(context.Background(), ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial the registered signer: %v", err)
	}
	_ = c.Close()

	state.IndexerInfos[1] = network.IndexerInfo{IndexerId: 1, IndexerUrl: "127.0.0.1", ClickhouseProxyPort: uint16(port)}
	if _, err := cfg.Dial(context.Background(), ln.Addr().String(), 2*time.Second); err == nil || !strings.Contains(err.Error(), "registers no signer") {
		t.Fatalf("indexer without a signer: %v", err)
	}
}
