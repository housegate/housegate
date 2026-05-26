// Package celestia is a thin JSON-RPC client over HTTP for the
// celestia-node light node. Only the methods used by the MVP
// (blob.Submit, blob.Get) are wrapped.
package celestia

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// Submit publishes data under namespace and returns the inclusion height.
func (c *Client) Submit(ctx context.Context, namespace, data []byte) (uint64, error) {
	b := blobJSON{
		Namespace:    base64.StdEncoding.EncodeToString(namespace),
		Data:         base64.StdEncoding.EncodeToString(data),
		ShareVersion: 0,
	}
	var height uint64
	if err := c.call(ctx, "blob.Submit", []interface{}{[]blobJSON{b}, nil}, &height); err != nil {
		return 0, err
	}
	return height, nil
}

// Get retrieves a blob previously submitted under namespace/commitment.
func (c *Client) Get(ctx context.Context, height uint64, namespace, commitment []byte) ([]byte, error) {
	params := []interface{}{
		height,
		base64.StdEncoding.EncodeToString(namespace),
		base64.StdEncoding.EncodeToString(commitment),
	}
	var b blobJSON
	if err := c.call(ctx, "blob.Get", params, &b); err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(b.Data)
}
