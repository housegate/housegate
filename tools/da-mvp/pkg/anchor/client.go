// Package anchor wraps the abigen-generated DAAnchor binding with
// ergonomic Go types. The Solidity contract is defined in
// tools/da-mvp/contracts/src/DAAnchor.sol; bindings live in
// tools/da-mvp/contracts/binding.
package anchor

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"housegate/housegate/tools/da-mvp/contracts/binding"
)

type Client struct {
	eth      *ethclient.Client
	contract *binding.DAAnchor
	chainID  *big.Int
	signer   *ecdsa.PrivateKey
	addr     common.Address
}

func NewClient(ctx context.Context, rpcURL, contractAddr string, signerKey *ecdsa.PrivateKey) (*Client, error) {
	ec, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("anchor: dial: %w", err)
	}
	chainID, err := ec.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("anchor: chainID: %w", err)
	}
	addr := common.HexToAddress(contractAddr)
	c, err := binding.NewDAAnchor(addr, ec)
	if err != nil {
		return nil, fmt.Errorf("anchor: bind: %w", err)
	}
	return &Client{eth: ec, contract: c, chainID: chainID, signer: signerKey, addr: addr}, nil
}

func (c *Client) txOpts(ctx context.Context) (*bind.TransactOpts, error) {
	opts, err := bind.NewKeyedTransactorWithChainID(c.signer, c.chainID)
	if err != nil {
		return nil, err
	}
	opts.Context = ctx
	return opts, nil
}

// PublishFirstChunk submits chunkIdx=0 and returns the allocated partSeq
// once the tx is mined.
func (c *Client) PublishFirstChunk(ctx context.Context, p PublishParams) (uint64, error) {
	opts, err := c.txOpts(ctx)
	if err != nil {
		return 0, err
	}
	tx, err := c.contract.Publish(opts, p.DBID, p.TableID, p.DAHeight, p.DACommitment,
		p.BlobSize, p.BlobHash, p.SchemaHash, 0, p.ChunkCount)
	if err != nil {
		return 0, fmt.Errorf("anchor: publish tx: %w", err)
	}
	receipt, err := bind.WaitMined(ctx, c.eth, tx)
	if err != nil {
		return 0, fmt.Errorf("anchor: wait mined: %w", err)
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return 0, fmt.Errorf("anchor: publish reverted")
	}
	for _, lg := range receipt.Logs {
		ev, err := c.contract.ParsePublished(*lg)
		if err == nil {
			return ev.PartSeq, nil
		}
	}
	return 0, fmt.Errorf("anchor: no Published event in receipt")
}

// PublishChunk submits chunkIdx > 0 reusing an existing partSeq.
func (c *Client) PublishChunk(ctx context.Context, p PublishParams, partSeq uint64, chunkIdx uint8) error {
	opts, err := c.txOpts(ctx)
	if err != nil {
		return err
	}
	tx, err := c.contract.PublishChunk(opts, p.DBID, p.TableID, partSeq, p.DAHeight, p.DACommitment,
		p.BlobSize, p.BlobHash, p.SchemaHash, chunkIdx, p.ChunkCount)
	if err != nil {
		return fmt.Errorf("anchor: publishChunk tx: %w", err)
	}
	if _, err := bind.WaitMined(ctx, c.eth, tx); err != nil {
		return err
	}
	return nil
}

// QueryPublished returns all Published events for (dbId, tableId) with
// partSeq >= since.
func (c *Client) QueryPublished(ctx context.Context, dbId, tableId [32]byte, since uint64) ([]PublishedEvent, error) {
	it, err := c.contract.FilterPublished(&bind.FilterOpts{Context: ctx, Start: 0}, [][32]byte{dbId}, [][32]byte{tableId})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []PublishedEvent
	for it.Next() {
		e := it.Event
		if e.PartSeq < since {
			continue
		}
		out = append(out, PublishedEvent{
			DBID:         e.DbId,
			TableID:      e.TableId,
			PartSeq:      e.PartSeq,
			DAHeight:     e.DaHeight,
			DACommitment: e.DaCommitment,
			BlobSize:     e.BlobSize,
			BlobHash:     e.BlobHash,
			SchemaHash:   e.SchemaHash,
			ChunkIdx:     e.ChunkIdx,
			ChunkCount:   e.ChunkCount,
		})
	}
	return out, it.Error()
}

type PublishParams struct {
	DBID         [32]byte
	TableID      [32]byte
	DAHeight     uint64
	DACommitment [32]byte
	BlobSize     uint32
	BlobHash     [32]byte
	SchemaHash   [32]byte
	ChunkCount   uint8
}

type PublishedEvent struct {
	DBID         [32]byte
	TableID      [32]byte
	PartSeq      uint64
	DAHeight     uint64
	DACommitment [32]byte
	BlobSize     uint32
	BlobHash     [32]byte
	SchemaHash   [32]byte
	ChunkIdx     uint8
	ChunkCount   uint8
}
