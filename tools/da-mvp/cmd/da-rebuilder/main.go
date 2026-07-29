// da-rebuilder reads anchor events from the local anvil, fetches the
// referenced blobs from Celestia, assembles multi-chunk parts, verifies
// blake3, and INSERTs into the target ClickHouse.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	log "github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/tools/da-mvp/pkg/anchor"
	"github.com/housegate/housegate/tools/da-mvp/pkg/celestia"
	"github.com/housegate/housegate/tools/da-mvp/pkg/chexport"
	"github.com/housegate/housegate/tools/da-mvp/pkg/chimport"
	"github.com/housegate/housegate/tools/da-mvp/pkg/chunk"
	"github.com/housegate/housegate/tools/da-mvp/pkg/ids"
	"github.com/housegate/housegate/tools/da-mvp/pkg/schema"
	"github.com/housegate/housegate/tools/da-mvp/pkg/verify"
)

func main() {
	chHost := flag.String("target-ch-host", "localhost", "")
	chPort := flag.Int("target-ch-port", 9000, "")
	chUser := flag.String("target-ch-user", "default", "")
	chPass := flag.String("target-ch-password", "", "")
	database := flag.String("database", "", "")
	table := flag.String("table", "", "")
	celestiaRPC := flag.String("celestia-rpc", "http://localhost:26658", "")
	celestiaToken := flag.String("celestia-token", "", "or set CELESTIA_TOKEN")
	anchorRPC := flag.String("anchor-rpc", "http://localhost:8545", "")
	anchorAddr := flag.String("anchor-contract", "", "")
	anchorKey := flag.String("anchor-private-key", "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80", "")
	sinceSeq := flag.Uint64("since-seq", 0, "")
	follow := flag.Bool("follow", false, "keep polling for new anchors")
	pollEvery := flag.Duration("poll", 5*time.Second, "follow poll interval")
	verifyAfter := flag.String("verify-after", "", "after each drain run verify mode {count|sample|full|''=off}")
	srcHost := flag.String("source-ch-host", "", "required when --verify-after is set")
	srcPort := flag.Int("source-ch-port", 9000, "")
	srcUser := flag.String("source-ch-user", "default", "")
	srcPass := flag.String("source-ch-password", "", "")
	flag.Parse()

	if *celestiaToken == "" {
		*celestiaToken = os.Getenv("CELESTIA_TOKEN")
	}
	if *database == "" || *table == "" || *anchorAddr == "" {
		fmt.Fprintln(os.Stderr, "--database, --table, --anchor-contract required")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	chCfg := chimport.Config{Host: *chHost, Port: *chPort, User: *chUser, Password: *chPass}
	chCfgExport := chexport.Config{Host: *chHost, Port: *chPort, User: *chUser, Password: *chPass}

	// Compute target schema hash for validation.
	showCreate, err := chexport.QueryShowCreate(ctx, chCfgExport, *database, *table)
	if err != nil {
		fatal("target schema: %v", err)
	}
	expectedHash, err := schema.Hash(showCreate)
	if err != nil {
		fatal("hash target schema: %v", err)
	}

	priv, err := crypto.HexToECDSA(stripHex(*anchorKey))
	if err != nil {
		fatal("parse key: %v", err)
	}
	a, err := anchor.NewClient(ctx, *anchorRPC, *anchorAddr, priv)
	if err != nil {
		fatal("anchor: %v", err)
	}
	c := celestia.NewClient(*celestiaRPC, *celestiaToken)
	ns := ids.Namespace(*database, *table)
	dbId := ids.DBID(*database)
	tableId := ids.TableID(*table)

	cur := *sinceSeq
	for {
		next, err := drain(ctx, a, c, chCfg, ns, dbId, tableId, expectedHash, *database, *table, cur)
		if err != nil {
			log.Errorw("drain failed", "err", err)
		} else {
			cur = next
			if *verifyAfter != "" {
				mode, perr := verify.ParseMode(*verifyAfter)
				if perr != nil {
					fatal("%v", perr)
				}
				srcCfg := chexport.Config{Host: *srcHost, Port: *srcPort, User: *srcUser, Password: *srcPass}
				dstCfg := chexport.Config{Host: *chHost, Port: *chPort, User: *chUser, Password: *chPass}
				diff, verr := verify.Run(ctx, mode, srcCfg, dstCfg, *database, *table)
				if verr != nil {
					log.Errorw("verify failed", "err", verr)
				} else if diff != "" {
					log.Warnw("verify diff detected", "detail", diff)
				} else {
					log.Infow("verify OK", "mode", *verifyAfter)
				}
			}
		}
		if !*follow {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(*pollEvery):
		}
	}
}

func drain(ctx context.Context, a *anchor.Client, c *celestia.Client, chCfg chimport.Config,
	ns []byte, dbId, tableId [32]byte, expectedHash [32]byte,
	database, table string, since uint64) (uint64, error) {

	events, err := a.QueryPublished(ctx, dbId, tableId, since)
	if err != nil {
		return since, fmt.Errorf("query events: %w", err)
	}
	if len(events) == 0 {
		return since, nil
	}
	// Group by partSeq.
	groups := map[uint64][]anchor.PublishedEvent{}
	for _, e := range events {
		groups[e.PartSeq] = append(groups[e.PartSeq], e)
	}
	// Iterate in seq order.
	seqs := make([]uint64, 0, len(groups))
	for s := range groups {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	cur := since
	for _, s := range seqs {
		grp := groups[s]
		sort.Slice(grp, func(i, j int) bool { return grp[i].ChunkIdx < grp[j].ChunkIdx })
		if len(grp) != int(grp[0].ChunkCount) {
			return cur, fmt.Errorf("partSeq=%d incomplete: have %d of %d chunks", s, len(grp), grp[0].ChunkCount)
		}
		if grp[0].SchemaHash != expectedHash {
			return cur, fmt.Errorf("partSeq=%d schema drift detected; target hash %x, anchor hash %x", s, expectedHash, grp[0].SchemaHash)
		}
		chunks := make([][]byte, len(grp))
		for i, e := range grp {
			b, err := c.Get(ctx, e.DAHeight, ns, e.DACommitment[:])
			if err != nil {
				return cur, fmt.Errorf("celestia get partSeq=%d chunk=%d: %w", s, e.ChunkIdx, err)
			}
			chunks[i] = b
		}
		blob := chunk.Assemble(chunks)
		want := grp[0].BlobHash
		got := blake3Sum256(blob)
		if got != want {
			return cur, fmt.Errorf("partSeq=%d blob hash mismatch: got %x want %x", s, got, want)
		}
		if err := chimport.Insert(ctx, chCfg, database, table, "Parquet", blob); err != nil {
			return cur, fmt.Errorf("import partSeq=%d: %w", s, err)
		}
		cur = s + 1
	}
	return cur, nil
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func stripHex(s string) string {
	if len(s) >= 2 && (s[:2] == "0x" || s[:2] == "0X") {
		return s[2:]
	}
	return s
}
