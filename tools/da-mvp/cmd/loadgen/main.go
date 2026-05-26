// loadgen drives synthetic INSERTs into the source ClickHouse at a
// configurable steady rate. Used by the DA MVP experiments to measure
// publisher lag, end-to-end latency, and DA cost.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"housegate/housegate/tools/da-mvp/pkg/chimport"
)

func main() {
	host := flag.String("host", "localhost", "")
	port := flag.Int("port", 9000, "")
	rate := flag.Int("rate", 1000, "rows per second")
	batch := flag.Int("batch", 1000, "rows per INSERT")
	duration := flag.Duration("duration", 2*time.Hour, "")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if *batch <= 0 || *rate <= 0 {
		fmt.Fprintln(os.Stderr, "--rate and --batch must be > 0")
		os.Exit(2)
	}
	batchEvery := time.Second / time.Duration(*rate / *batch)
	if batchEvery <= 0 {
		batchEvery = time.Millisecond
	}

	cfg := chimport.Config{Host: *host, Port: *port}
	var inserted int64
	deadline := time.After(*duration)
	ticker := time.NewTicker(batchEvery)
	defer ticker.Stop()

	report := time.NewTicker(10 * time.Second)
	defer report.Stop()
	startedAt := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			fmt.Printf("loadgen done: %d rows in %s\n", atomic.LoadInt64(&inserted), time.Since(startedAt))
			return
		case <-report.C:
			fmt.Printf("loadgen progress: %d rows, %.0f rows/s\n",
				atomic.LoadInt64(&inserted),
				float64(atomic.LoadInt64(&inserted))/time.Since(startedAt).Seconds())
		case <-ticker.C:
			payload := buildBatch(*batch)
			if err := chimport.Insert(ctx, cfg, "exp", "events", "TabSeparated", []byte(payload)); err != nil {
				fmt.Fprintf(os.Stderr, "insert: %v\n", err)
				continue
			}
			atomic.AddInt64(&inserted, int64(*batch))
		}
	}
}

func buildBatch(n int) string {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		id := time.Now().UnixNano()
		txnHash := randHex(32)
		contractAddr := randHex(20)
		topic1 := randHex(32)
		topic2 := randHex(32)
		now := time.Now().UTC().Format("2006-01-02 15:04:05.000000")
		fmt.Fprintf(&buf, "%d\t\\x%s\t%d\t%d\t\\x%s\tTransfer\t\\x%s\t\\x%s\t%d.123456789012345678\t%d\t%s\thello world\n",
			id, txnHash, 12345678, i, contractAddr, topic1, topic2, i+1, uint64(i)*100, now,
		)
	}
	return buf.String()
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
