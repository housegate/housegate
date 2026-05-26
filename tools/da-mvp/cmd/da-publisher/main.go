// da-publisher polls a source ClickHouse for new parts, exports each
// to Parquet, splits into ≤1.5 MiB chunks, submits each chunk to
// Celestia, and writes one anchor event per chunk to the local anvil.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	log "housegate/housegate/pkg/log"
	"housegate/housegate/tools/da-mvp/pkg/anchor"
	"housegate/housegate/tools/da-mvp/pkg/celestia"
	"housegate/housegate/tools/da-mvp/pkg/checkpoint"
	"housegate/housegate/tools/da-mvp/pkg/chexport"
	"housegate/housegate/tools/da-mvp/pkg/chunk"
	"housegate/housegate/tools/da-mvp/pkg/ids"
	"housegate/housegate/tools/da-mvp/pkg/schema"
)

var (
	partsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "housegate_da_publisher_parts_published_total",
		Help: "Total parts fully anchored",
	})
	blobsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "housegate_da_publisher_blobs_published_total",
		Help: "Total Celestia blob submissions",
	})
	bytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "housegate_da_publisher_bytes_published_total",
		Help: "Total uncompressed Parquet bytes published",
	})
	publishLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "housegate_da_publisher_publish_lag_seconds",
		Help: "now() - last checkpoint mod time",
	})
	submitDur = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "housegate_da_publisher_celestia_submit_seconds",
		Help:    "PFB submit latency",
		Buckets: prometheus.DefBuckets,
	})
	celestiaErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "housegate_da_publisher_celestia_errors_total",
		Help: "Celestia submit errors",
	})
)

func init() {
	prometheus.MustRegister(partsTotal, blobsTotal, bytesTotal, publishLag, submitDur, celestiaErrors)
}

func main() {
	chHost := flag.String("source-ch-host", "localhost", "")
	chPort := flag.Int("source-ch-port", 9000, "")
	chUser := flag.String("source-ch-user", "default", "")
	chPass := flag.String("source-ch-password", "", "")
	database := flag.String("database", "", "source database name")
	table := flag.String("table", "", "source table name")
	celestiaRPC := flag.String("celestia-rpc", "http://localhost:26658", "")
	celestiaToken := flag.String("celestia-token", "", "or set CELESTIA_TOKEN")
	anchorRPC := flag.String("anchor-rpc", "http://localhost:8545", "")
	anchorAddr := flag.String("anchor-contract", "", "deployed DAAnchor address")
	anchorKey := flag.String("anchor-private-key", "", "hex private key for anchor tx")
	intervalStr := flag.String("interval", "60s", "poll interval")
	checkpointPath := flag.String("checkpoint-file", "./pub.state", "")
	metricsListen := flag.String("metrics-listen", ":9100", "Prometheus /metrics listen addr")
	flag.Parse()

	if *celestiaToken == "" {
		*celestiaToken = os.Getenv("CELESTIA_TOKEN")
	}
	if *database == "" || *table == "" || *anchorAddr == "" || *anchorKey == "" {
		fmt.Fprintln(os.Stderr, "--database, --table, --anchor-contract, --anchor-private-key are required")
		os.Exit(2)
	}
	interval, err := time.ParseDuration(*intervalStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad --interval: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(*metricsListen, mux); err != nil {
			log.Errorw("metrics server failed", "err", err, "addr", *metricsListen)
			cancel()
		}
	}()

	chCfg := chexport.Config{Host: *chHost, Port: *chPort, User: *chUser, Password: *chPass}

	// Compute schemaHash once at startup.
	showCreate, err := chexport.QueryShowCreate(ctx, chCfg, *database, *table)
	if err != nil {
		fatal("query schema: %v", err)
	}
	schemaHash, err := schema.Hash(showCreate)
	if err != nil {
		fatal("hash schema: %v", err)
	}

	priv, err := crypto.HexToECDSA(stripHex(*anchorKey))
	if err != nil {
		fatal("parse key: %v", err)
	}
	anchorCli, err := anchor.NewClient(ctx, *anchorRPC, *anchorAddr, priv)
	if err != nil {
		fatal("anchor client: %v", err)
	}
	celestiaCli := celestia.NewClient(*celestiaRPC, *celestiaToken)
	ns := ids.Namespace(*database, *table)
	dbId := ids.DBID(*database)
	tableId := ids.TableID(*table)

	cp, err := checkpoint.Load(*checkpointPath)
	if err != nil {
		fatal("load checkpoint: %v", err)
	}

	for {
		if err := runCycle(ctx, runOpts{
			ChCfg: chCfg, Database: *database, Table: *table,
			Namespace: ns, DBID: dbId, TableID: tableId,
			SchemaHash: schemaHash,
			Celestia:   celestiaCli, Anchor: anchorCli,
			Checkpoint: &cp, CheckpointPath: *checkpointPath,
		}); err != nil {
			log.Errorw("cycle failed", "err", err)
		}
		if !cp.LastModificationTime.IsZero() {
			publishLag.Set(time.Since(cp.LastModificationTime).Seconds())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

type runOpts struct {
	ChCfg          chexport.Config
	Database       string
	Table          string
	Namespace      []byte
	DBID           [32]byte
	TableID        [32]byte
	SchemaHash     [32]byte
	Celestia       *celestia.Client
	Anchor         *anchor.Client
	Checkpoint     *checkpoint.Checkpoint
	CheckpointPath string
}

func runCycle(ctx context.Context, o runOpts) error {
	parts, err := chexport.QueryNewParts(ctx, o.ChCfg, o.Database, o.Table, o.Checkpoint.LastModificationTime.Unix())
	if err != nil {
		return fmt.Errorf("query parts: %w", err)
	}
	for _, p := range parts {
		parquet, err := chexport.ExportPart(ctx, o.ChCfg, o.Database, o.Table, p.Name)
		if err != nil {
			return fmt.Errorf("export %s: %w", p.Name, err)
		}
		blobHash := blake3Sum256(parquet)
		chunks := chunk.Split(parquet)
		var partSeq uint64
		partFailed := false
		for i, ck := range chunks {
			t0 := time.Now()
			height, commitment, err := o.Celestia.Submit(ctx, o.Namespace, ck)
			submitDur.Observe(time.Since(t0).Seconds())
			if err != nil {
				celestiaErrors.Inc()
				if i == 0 {
					// No anchor written yet — safe to retry next cycle.
					return fmt.Errorf("celestia submit chunk 0 of %s: %w", p.Name, err)
				}
				// Chunks 0..i-1 are already on Celestia and anchored. Skipping the
				// part advances the checkpoint past it so we don't duplicate; the
				// rebuilder will see an incomplete part and abort. That's the
				// honest signal — better than overlapping partSeq chains.
				log.Errorw("partial chunk failure — part skipped to avoid duplicate partSeq",
					"part", p.Name, "chunkIdx", i, "err", err)
				partFailed = true
				break
			}
			if len(commitment) != 32 {
				return fmt.Errorf("celestia commitment for %s chunk %d is %d bytes, expected 32", p.Name, i, len(commitment))
			}
			var daCommitment [32]byte
			copy(daCommitment[:], commitment)
			params := anchor.PublishParams{
				DBID: o.DBID, TableID: o.TableID,
				DAHeight: height, DACommitment: daCommitment,
				BlobSize: uint32(len(ck)), BlobHash: blobHash, SchemaHash: o.SchemaHash,
				ChunkCount: uint8(len(chunks)),
			}
			if i == 0 {
				partSeq, err = o.Anchor.PublishFirstChunk(ctx, params)
			} else {
				err = o.Anchor.PublishChunk(ctx, params, partSeq, uint8(i))
			}
			if err != nil {
				if i == 0 {
					return fmt.Errorf("anchor publish chunk 0 of %s: %w", p.Name, err)
				}
				log.Errorw("partial chunk failure — part skipped to avoid duplicate partSeq",
					"part", p.Name, "chunkIdx", i, "err", err)
				partFailed = true
				break
			}
			blobsTotal.Inc()
			bytesTotal.Add(float64(len(ck)))
		}
		if !partFailed {
			partsTotal.Inc()
		}
		o.Checkpoint.LastModificationTime = time.Unix(p.ModificationUnix, 0).UTC()
		if err := checkpoint.Save(o.CheckpointPath, *o.Checkpoint); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}
	return nil
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
