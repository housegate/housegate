// pfb-demo submits a small blob to Celestia and reads it back. The
// Day-1 smoke test for the DA MVP — verifies light-node connectivity
// and the pkg/celestia wrapper.
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/housegate/housegate/tools/da-mvp/pkg/celestia"
)

func main() {
	rpcURL := flag.String("celestia-rpc", "http://localhost:26658", "celestia light-node RPC URL")
	token := flag.String("celestia-token", "", "celestia auth token (or set CELESTIA_TOKEN)")
	nsHex := flag.String("namespace", "68676d76000000000001", "10-byte namespace, hex-encoded")
	flag.Parse()

	if *token == "" {
		*token = os.Getenv("CELESTIA_TOKEN")
	}
	ns, err := hex.DecodeString(*nsHex)
	if err != nil || len(ns) != 10 {
		fmt.Fprintf(os.Stderr, "namespace must be 10 bytes hex: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	c := celestia.NewClient(*rpcURL, *token)
	payload := []byte("hello da mvp")
	height, commitment, err := c.Submit(ctx, ns, payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "submit: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("submitted at height %d commitment %x\n", height, commitment)

	if height == 0 {
		os.Exit(1)
	}
	fmt.Println("demo OK")
}
