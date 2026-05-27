// Command igw is a tiny standalone wrapper around pkg/interserver. The
// interserver-replication integration test builds it with bazel and runs it
// in a container on the keeper Docker network, so ClickHouse replicas reach
// the gateway container-to-container (a CH container cannot reliably reach a
// host process on all Docker setups — notably WSL2 + a user-defined
// network). It exercises the real pkg/interserver code, just hosted in a
// container instead of in the test process.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os/signal"
	"syscall"

	"housegate/housegate/pkg/interserver"
)

func main() {
	listen := flag.String("listen", ":9009", "gateway bind address")
	target := flag.String("target", "", "local CH interserver host:port to forward to")
	flag.Parse()
	if *target == "" {
		log.Fatal("igw: -target is required")
	}

	srv, err := interserver.NewServer(interserver.ServerConfig{
		Target: func() string { return *target },
	})
	if err != nil {
		log.Fatalf("igw: %v", err)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("igw listen %s: %v", *listen, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("igw listening %s -> %s", *listen, *target)
	if err := srv.Serve(ctx, ln); err != nil {
		log.Fatalf("igw serve: %v", err)
	}
}
