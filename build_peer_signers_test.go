package housegate

import (
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugins/credential"
	"github.com/housegate/housegate/pkg/registry"
)

const (
	peerTrustIndexerKey  = "1111111111111111111111111111111111111111111111111111111111111111"
	peerTrustStrangerKey = "2222222222222222222222222222222222222222222222222222222222222222"
)

// registryWithoutSigners keeps a registry's WriterAccess and hides its
// IndexerSigners, like an embedder's adapter that predates the interface.
type registryWithoutSigners struct {
	registry.Registry
	registry.WriterAccess
}

// TestBuildServer_PeerLoginTrust covers who a server accepts as a peer. A
// peer-relay login skips the receiving proxy's auth and rewrite, so an empty
// auth.allowed_addresses, which opens the SQL path to every account, must not
// open the peer path too: there the signer has to be a registered indexer
// signer, and a registry that cannot name them admits no one.
func TestBuildServer_PeerLoginTrust(t *testing.T) {
	const selfIndexerID = "1"
	indexer, err := auth.NewRelaySigner(peerTrustIndexerKey)
	if err != nil {
		t.Fatalf("indexer signer: %v", err)
	}
	stranger, err := auth.NewRelaySigner(peerTrustStrangerKey)
	if err != nil {
		t.Fatalf("stranger signer: %v", err)
	}
	withIndexer := func() *network.InMemoryNetworkState {
		ns := network.NewInMemoryNetworkState()
		ns.IndexerInfos[2] = network.IndexerInfo{IndexerId: 2, Signer: indexer.Address()}
		return ns
	}

	withoutSigners := func() registry.Registry {
		ns := withIndexer()
		return registryWithoutSigners{Registry: ns, WriterAccess: ns}
	}

	for name, tc := range map[string]struct {
		allowlist    []string
		reg          registry.Registry
		wantIndexer  bool
		wantStranger bool
	}{
		"empty allowlist follows the registry's indexer signers": {
			reg: withIndexer(), wantIndexer: true, wantStranger: false,
		},
		"empty allowlist and a registry without signers refuses every peer": {
			reg: withoutSigners(), wantIndexer: false, wantStranger: false,
		},
		"a configured allowlist stays the only trust anchor": {
			allowlist: []string{stranger.Address()}, reg: withIndexer(), wantIndexer: false, wantStranger: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := withoutRewriter(minimalServerCfg(t))
			cfg.Auth.Enabled = true
			cfg.Auth.AllowedAddresses = tc.allowlist
			bs, err := buildServer(Options{Config: cfg, NetworkState: tc.reg, GetIndexerId: func() uint64 { return 1 }}, nil)
			if err != nil {
				t.Fatalf("buildServer: %v", err)
			}
			defer bs.teardown()

			var validator auth.PeerValidator
			for _, candidate := range requireExternalChain(t, bs).HelloPlugins {
				if cred, ok := candidate.(*credential.Plugin); ok {
					validator = cred.PeerValidator
				}
			}
			if validator == nil {
				t.Fatal("credential plugin has no peer validator")
			}
			for _, login := range []struct {
				who    string
				signer *auth.RelaySigner
				want   bool
			}{
				{"registered indexer signer", indexer, tc.wantIndexer},
				{"unregistered signer", stranger, tc.wantStranger},
			} {
				token, err := login.signer.SignPeerLogin(selfIndexerID, time.Minute)
				if err != nil {
					t.Fatalf("sign peer login: %v", err)
				}
				_, err = validator.ValidatePeerLogin(token, selfIndexerID)
				if accepted := err == nil; accepted != login.want {
					t.Errorf("%s: accepted=%v, want %v (err=%v)", login.who, accepted, login.want, err)
				}
			}
		})
	}
}
