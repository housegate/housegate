// Package celestia is a thin JSON-RPC client over HTTP for the
// celestia-node light node. Only the methods used by the MVP
// (blob.Submit, blob.Get) are wrapped.
package celestia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/celestiaorg/go-square/v2/inclusion"
	"github.com/celestiaorg/go-square/v2/share"
)

type Client struct {
	url   string
	token string
	http  *http.Client
}

func NewClient(url, token string) *Client {
	return &Client{url: url, token: token, http: &http.Client{}}
}

type rpcReq struct {
	Jsonrpc string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type rpcErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResp struct {
	Jsonrpc string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcErr         `json:"error,omitempty"`
}

func (c *Client) call(ctx context.Context, method string, params []interface{}, out interface{}) error {
	body, err := json.Marshal(rpcReq{Jsonrpc: "2.0", ID: 1, Method: method, Params: params})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Surface HTTP-level errors (auth, server) as auth/server errors —
	// not as confusing JSON-decode errors. 4 MiB ceiling is generous
	// for any valid celestia-node response (a 1.5 MiB blob base64-encodes
	// to ~2 MiB) while bounding OOM exposure from a misbehaving node.
	const maxBody = 4 * 1024 * 1024
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return fmt.Errorf("celestia: read response body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("celestia: http %d: %s", resp.StatusCode, bytes.TrimSpace(respBody))
	}
	var parsed rpcResp
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return fmt.Errorf("celestia: decode response: %w (body=%s)", err, respBody)
	}
	if parsed.Error != nil {
		return fmt.Errorf("celestia: rpc error: %s", parsed.Error.Message)
	}
	if out != nil {
		return json.Unmarshal(parsed.Result, out)
	}
	return nil
}

// blobJSON is the JSON shape the celestia-node API exposes.
type blobJSON struct {
	Namespace    string `json:"namespace"`
	Data         string `json:"data"`
	ShareVersion int    `json:"share_version"`
	Commitment   string `json:"commitment,omitempty"`
}

// subtreeRootThreshold is the celestia-app network parameter governing
// how many subtree roots are produced per blob commitment. 64 is the
// mainnet/mocha value.
const subtreeRootThreshold = 64

// Submit publishes data under namespace and returns the inclusion height
// and the Celestia share commitment (the NMT-root-derived key used by
// Get to look the blob back up).
func (c *Client) Submit(ctx context.Context, namespace, data []byte) (uint64, []byte, error) {
	ns, err := resolveNamespace(namespace)
	if err != nil {
		return 0, nil, fmt.Errorf("celestia: namespace: %w", err)
	}
	commitment, err := computeCommitment(ns, data)
	if err != nil {
		return 0, nil, fmt.Errorf("celestia: compute commitment: %w", err)
	}
	b := blobJSON{
		Namespace:    base64.StdEncoding.EncodeToString(ns.Bytes()),
		Data:         base64.StdEncoding.EncodeToString(data),
		ShareVersion: 0,
		Commitment:   base64.StdEncoding.EncodeToString(commitment),
	}
	var height uint64
	if err := c.call(ctx, "blob.Submit", []interface{}{[]blobJSON{b}, nil}, &height); err != nil {
		return 0, nil, err
	}
	return height, commitment, nil
}

// Get retrieves a blob previously submitted under namespace/commitment.
func (c *Client) Get(ctx context.Context, height uint64, namespace, commitment []byte) ([]byte, error) {
	ns, err := resolveNamespace(namespace)
	if err != nil {
		return nil, fmt.Errorf("celestia: namespace: %w", err)
	}
	params := []interface{}{
		height,
		base64.StdEncoding.EncodeToString(ns.Bytes()),
		base64.StdEncoding.EncodeToString(commitment),
	}
	var b blobJSON
	if err := c.call(ctx, "blob.Get", params, &b); err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(b.Data)
}

// resolveNamespace accepts either the 10-byte v0 sub-ID produced by
// pkg/ids.Namespace or a full 29-byte (version+id) encoded namespace,
// and returns a parsed share.Namespace. Anything else is an error.
func resolveNamespace(ns []byte) (share.Namespace, error) {
	switch len(ns) {
	case share.NamespaceVersionZeroIDSize: // 10
		return share.NewV0Namespace(ns)
	case share.NamespaceSize: // 29
		return share.NewNamespaceFromBytes(ns)
	default:
		return share.Namespace{}, fmt.Errorf("expected %d or %d bytes, got %d",
			share.NamespaceVersionZeroIDSize, share.NamespaceSize, len(ns))
	}
}

// computeCommitment derives the Celestia share commitment for a blob
// client-side, using the same algorithm celestia-node runs internally.
// This is the lookup key blob.Get later uses to fetch the blob back.
//
// Algorithm: split the blob into shares, group shares into a merkle
// mountain range whose max tree size is governed by subtreeRootThreshold,
// compute one NMT root per subtree (with namespace re-prefixed onto each
// leaf to match celestia-app's parity-data convention), then fold the
// subtree roots into a single root using a balanced binary merkle tree
// with the cometbft prefix scheme (leaf = sha256(0x00||leaf),
// inner = sha256(0x01||left||right)).
func computeCommitment(ns share.Namespace, data []byte) ([]byte, error) {
	blob, err := share.NewBlob(ns, data, share.ShareVersionZero, nil)
	if err != nil {
		return nil, fmt.Errorf("blob: %w", err)
	}
	return inclusion.CreateCommitment(blob, merkleRoot, subtreeRootThreshold)
}

// merkleRoot computes a balanced binary merkle tree root over the
// provided byte slices, using cometbft's leaf/inner-prefix scheme:
//   - leaf hash:  sha256(0x00 || leaf)
//   - inner hash: sha256(0x01 || left || right)
//
// The split point at each level is the largest power of two strictly
// less than len(items), matching cometbft's merkle.HashFromByteSlices.
func merkleRoot(items [][]byte) []byte {
	switch len(items) {
	case 0:
		empty := sha256.Sum256(nil)
		return empty[:]
	case 1:
		h := sha256.New()
		h.Write([]byte{0x00})
		h.Write(items[0])
		return h.Sum(nil)
	default:
		k := largestPow2LT(len(items))
		left := merkleRoot(items[:k])
		right := merkleRoot(items[k:])
		h := sha256.New()
		h.Write([]byte{0x01})
		h.Write(left)
		h.Write(right)
		return h.Sum(nil)
	}
}

// largestPow2LT returns the largest power of two strictly less than n,
// for n >= 2. Matches cometbft's getSplitPoint.
func largestPow2LT(n int) int {
	if n < 2 {
		return 0
	}
	k := 1
	for k < n {
		k <<= 1
	}
	return k >> 1
}
