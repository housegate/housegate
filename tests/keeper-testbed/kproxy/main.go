// kproxy is a minimal L4 stand-in for housegate's (not-yet-built) keeper_proxy.
//
// It fronts the CH-side keeper-client port (9181): a co-located ClickHouse
// points its <zookeeper> at this proxy, and kproxy forwards the byte stream to
// one of the real keeper quorum members. It exposes an HTTP control port so a
// test can force the two behaviours the design depends on:
//
//	POST /drop      close every live connection (simulate "housegate detects a
//	                quorum change and re-steers" -> CH must self-heal on reconnect)
//	POST /retarget  rotate the active upstream to the next member AND drop, so
//	                CH's reconnect lands on a *different* keeper (preview of A1/A2)
//	GET  /status    active upstream, live/served/dropped connection counts
//
// This is deliberately protocol-unaware (pure byte relay): A4 validates the
// ClickHouse keeper client's reconnect/session behaviour, not housegate code.
// It is the seed of pkg/keeper for Phase 1.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type conn struct {
	id     int
	client net.Conn
	up     net.Conn
}

type proxy struct {
	upstreams []string

	mu     sync.Mutex
	active int
	conns  map[int]*conn
	nextID int

	served  atomic.Int64
	dropped atomic.Int64
}

func (p *proxy) currentUpstream() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.upstreams[p.active]
}

func (p *proxy) register(c *conn) {
	p.mu.Lock()
	c.id = p.nextID
	p.nextID++
	p.conns[c.id] = c
	p.mu.Unlock()
}

func (p *proxy) unregister(id int) {
	p.mu.Lock()
	delete(p.conns, id)
	p.mu.Unlock()
}

// dropAll closes every live connection (both legs). CH sees a reset and
// reconnects to the same listen address.
func (p *proxy) dropAll() int {
	p.mu.Lock()
	victims := make([]*conn, 0, len(p.conns))
	for _, c := range p.conns {
		victims = append(victims, c)
	}
	p.conns = map[int]*conn{}
	p.mu.Unlock()
	for _, c := range victims {
		c.client.Close()
		if c.up != nil {
			c.up.Close()
		}
	}
	p.dropped.Add(int64(len(victims)))
	return len(victims)
}

func (p *proxy) handle(client net.Conn) {
	upAddr := p.currentUpstream()
	up, err := net.DialTimeout("tcp", upAddr, 3*time.Second)
	if err != nil {
		log.Printf("dial upstream %s failed: %v", upAddr, err)
		client.Close()
		return
	}
	c := &conn{client: client, up: up}
	p.register(c)
	p.served.Add(1)

	done := make(chan struct{}, 2)
	go func() { io.Copy(up, client); done <- struct{}{} }()
	go func() { io.Copy(client, up); done <- struct{}{} }()
	<-done
	client.Close()
	up.Close()
	p.unregister(c.id)
}

func (p *proxy) serveControl(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		st := map[string]any{
			"active_upstream": p.upstreams[p.active],
			"active_index":    p.active,
			"upstreams":       p.upstreams,
			"live_conns":      len(p.conns),
			"served_total":    p.served.Load(),
			"dropped_total":   p.dropped.Load(),
		}
		p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st)
	})
	mux.HandleFunc("/drop", func(w http.ResponseWriter, r *http.Request) {
		n := p.dropAll()
		log.Printf("CONTROL /drop: closed %d conns", n)
		fmt.Fprintf(w, "dropped %d conns\n", n)
	})
	mux.HandleFunc("/retarget", func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		if to := r.URL.Query().Get("to"); to != "" {
			if i, err := strconv.Atoi(to); err == nil && i >= 0 && i < len(p.upstreams) {
				p.active = i
			}
		} else {
			p.active = (p.active + 1) % len(p.upstreams)
		}
		newUp := p.upstreams[p.active]
		p.mu.Unlock()
		n := p.dropAll()
		log.Printf("CONTROL /retarget: now %s, closed %d conns", newUp, n)
		fmt.Fprintf(w, "retargeted to %s, dropped %d conns\n", newUp, n)
	})
	log.Printf("control listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("control server: %v", err)
	}
}

func main() {
	listen := flag.String("listen", ":9181", "listen address for CH-facing keeper traffic")
	upstreams := flag.String("upstreams", "", "comma-separated keeper host:port members")
	control := flag.String("control", ":8181", "HTTP control endpoint")
	flag.Parse()

	ups := []string{}
	for _, u := range strings.Split(*upstreams, ",") {
		if u = strings.TrimSpace(u); u != "" {
			ups = append(ups, u)
		}
	}
	if len(ups) == 0 {
		log.Fatal("need -upstreams host:port[,host:port...]")
	}

	p := &proxy{upstreams: ups, conns: map[int]*conn{}}
	go p.serveControl(*control)

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen %s: %v", *listen, err)
	}
	log.Printf("kproxy listening %s -> %v", *listen, ups)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go p.handle(c)
	}
}
