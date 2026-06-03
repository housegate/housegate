// Command kpx is a tiny standalone wrapper around pkg/keeper. The
// interserver-replication integration test runs it in a container on the
// keeper Docker network so a real ClickHouse can point its <zookeeper> at
// it container-to-container (the keeper proxy fronts the quorum; CH only
// ever talks to this stable endpoint). It exercises the real pkg/keeper
// code, just hosted in a container instead of inside the housegate binary.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os/signal"
	"strings"
	"syscall"

	"housegate/housegate/pkg/keeper"
)

func main() {
	listen := flag.String("listen", ":9181", "keeper-client bind address")
	members := flag.String("members", "", "comma-separated keeper client endpoints (host:port)")
	flag.Parse()

	var ms []string
	for _, m := range strings.Split(*members, ",") {
		if m = strings.TrimSpace(m); m != "" {
			ms = append(ms, m)
		}
	}
	if len(ms) == 0 {
		log.Fatal("kpx: -members host:port[,host:port...] is required")
	}

	tracker := keeper.NewTracker(keeper.TrackerConfig{Members: ms})
	srv, err := keeper.NewServer(keeper.ServerConfig{
		Tracker:  tracker,
		Strategy: keeper.LeaderPref,
	})
	if err != nil {
		log.Fatalf("kpx: %v", err)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("kpx listen %s: %v", *listen, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("kpx listening %s -> %v", *listen, ms)
	if err := srv.Serve(ctx, ln); err != nil { // Serve also runs the tracker + reconcile loops
		log.Fatalf("kpx serve: %v", err)
	}
}
