// Command imesh is a tiny standalone wrapper around pkg/interserver's
// Egress + Ingress (the two-hop mTLS interserver-mesh sidecar). The
// integration test runs it as a Docker sidecar sharing the ClickHouse
// container's network namespace so CH's outbound "localhost:9010" dials
// THIS sidecar's egress; the ingress on 0.0.0.0:19009 accepts peer mTLS
// and forwards to the co-located CH on a private loopback IP.
//
// Production housegate links these in directly (build.go's
// buildInterserverMesh); this binary exists only to package the same
// pkg/interserver code into a container the test can run alongside CH.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"housegate/housegate/pkg/interserver"
)

func main() {
	egressListen := flag.String("egress-listen", "127.0.0.1:9010", "local-CH-facing bind")
	ingressListen := flag.String("ingress-listen", "0.0.0.0:19009", "peer-mTLS bind")
	localCH := flag.String("local-ch", "127.0.0.2:9010", "co-located CH real interserver address")
	certFile := flag.String("cert", "", "PEM cert (this housegate's identity)")
	keyFile := flag.String("key", "", "PEM key matching -cert")
	caFile := flag.String("ca", "", "PEM CA pool (peer client + server cert verification)")
	peers := flag.String("peers", "", "comma-separated peer routes <replica>=<host:port>")
	flag.Parse()

	if *certFile == "" || *keyFile == "" || *caFile == "" {
		log.Fatal("imesh: -cert, -key, -ca all required (mTLS)")
	}
	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		log.Fatalf("imesh: load cert/key: %v", err)
	}
	caPEM, err := os.ReadFile(*caFile)
	if err != nil {
		log.Fatalf("imesh: read CA: %v", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		log.Fatalf("imesh: CA file %s: no PEM certs parsed", *caFile)
	}

	peerMap := map[string]string{}
	for _, p := range strings.Split(*peers, ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		k, v, ok := strings.Cut(p, "=")
		if !ok {
			log.Fatalf("imesh: bad peer entry %q (want replica=host:port)", p)
		}
		peerMap[k] = v
	}

	egress, err := interserver.NewEgress(interserver.EgressConfig{
		PeerLookup: func(r string) (string, bool) { a, ok := peerMap[r]; return a, ok },
		TLSClient: &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caPool,
			MinVersion:   tls.VersionTLS12,
		},
	})
	if err != nil {
		log.Fatalf("imesh egress: %v", err)
	}
	ingress, err := interserver.NewIngress(interserver.IngressConfig{
		LocalCH: *localCH,
		TLSServer: &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientCAs:    caPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS12,
		},
	})
	if err != nil {
		log.Fatalf("imesh ingress: %v", err)
	}

	egLn, err := net.Listen("tcp", *egressListen)
	if err != nil {
		log.Fatalf("imesh egress listen %s: %v", *egressListen, err)
	}
	inLn, err := net.Listen("tcp", *ingressListen)
	if err != nil {
		log.Fatalf("imesh ingress listen %s: %v", *ingressListen, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("imesh: egress %s, ingress %s, local CH %s, peers=%v",
		*egressListen, *ingressListen, *localCH, peerMap)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = egress.Serve(ctx, egLn) }()
	go func() { defer wg.Done(); _ = ingress.Serve(ctx, inLn) }()
	wg.Wait()
}
