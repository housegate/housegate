// genkey is a tiny helper that prints a freshly-generated secp256k1
// keypair (or, with -from-priv, derives the Ethereum address of an
// existing one). It exists to back scripts/setup-local-test-key.sh —
// no production use.
//
// Output is shell-friendly KEY=value lines so the caller can `eval` it.
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
)

func main() {
	fromPriv := flag.String("from-priv", "", "derive address from this hex private key (with optional 0x prefix); skip key generation")
	flag.Parse()

	if *fromPriv != "" {
		priv, err := crypto.HexToECDSA(strings.TrimPrefix(*fromPriv, "0x"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse private key: %v\n", err)
			os.Exit(1)
		}
		addr := crypto.PubkeyToAddress(priv.PublicKey)
		fmt.Printf("ADDRESS_CHECKSUM=%s\n", addr.Hex())
		fmt.Printf("ADDRESS_LOWER=%s\n", strings.ToLower(addr.Hex()))
		return
	}

	priv, err := crypto.GenerateKey()
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate key: %v\n", err)
		os.Exit(1)
	}
	addr := crypto.PubkeyToAddress(priv.PublicKey)
	fmt.Printf("PRIVATE_KEY=0x%s\n", hex.EncodeToString(crypto.FromECDSA(priv)))
	fmt.Printf("ADDRESS_CHECKSUM=%s\n", addr.Hex())
	fmt.Printf("ADDRESS_LOWER=%s\n", strings.ToLower(addr.Hex()))
}
