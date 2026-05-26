// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package binding

import (
	"errors"
	"math/big"
	"strings"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = errors.New
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
	_ = abi.ConvertType
)

// DAAnchorMetaData contains all meta data concerning the DAAnchor contract.
var DAAnchorMetaData = &bind.MetaData{
	ABI: "[{\"type\":\"function\",\"name\":\"publish\",\"inputs\":[{\"name\":\"dbId\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"tableId\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"daHeight\",\"type\":\"uint64\",\"internalType\":\"uint64\"},{\"name\":\"daCommitment\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"blobSize\",\"type\":\"uint32\",\"internalType\":\"uint32\"},{\"name\":\"blobHash\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"schemaHash\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"chunkIdx\",\"type\":\"uint8\",\"internalType\":\"uint8\"},{\"name\":\"chunkCount\",\"type\":\"uint8\",\"internalType\":\"uint8\"}],\"outputs\":[{\"name\":\"seq\",\"type\":\"uint64\",\"internalType\":\"uint64\"}],\"stateMutability\":\"nonpayable\"},{\"type\":\"function\",\"name\":\"publishChunk\",\"inputs\":[{\"name\":\"dbId\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"tableId\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"seq\",\"type\":\"uint64\",\"internalType\":\"uint64\"},{\"name\":\"daHeight\",\"type\":\"uint64\",\"internalType\":\"uint64\"},{\"name\":\"daCommitment\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"blobSize\",\"type\":\"uint32\",\"internalType\":\"uint32\"},{\"name\":\"blobHash\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"schemaHash\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"chunkIdx\",\"type\":\"uint8\",\"internalType\":\"uint8\"},{\"name\":\"chunkCount\",\"type\":\"uint8\",\"internalType\":\"uint8\"}],\"outputs\":[],\"stateMutability\":\"nonpayable\"},{\"type\":\"event\",\"name\":\"Published\",\"inputs\":[{\"name\":\"dbId\",\"type\":\"bytes32\",\"indexed\":true,\"internalType\":\"bytes32\"},{\"name\":\"tableId\",\"type\":\"bytes32\",\"indexed\":true,\"internalType\":\"bytes32\"},{\"name\":\"partSeq\",\"type\":\"uint64\",\"indexed\":false,\"internalType\":\"uint64\"},{\"name\":\"daHeight\",\"type\":\"uint64\",\"indexed\":false,\"internalType\":\"uint64\"},{\"name\":\"daCommitment\",\"type\":\"bytes32\",\"indexed\":false,\"internalType\":\"bytes32\"},{\"name\":\"blobSize\",\"type\":\"uint32\",\"indexed\":false,\"internalType\":\"uint32\"},{\"name\":\"blobHash\",\"type\":\"bytes32\",\"indexed\":false,\"internalType\":\"bytes32\"},{\"name\":\"schemaHash\",\"type\":\"bytes32\",\"indexed\":false,\"internalType\":\"bytes32\"},{\"name\":\"chunkIdx\",\"type\":\"uint8\",\"indexed\":false,\"internalType\":\"uint8\"},{\"name\":\"chunkCount\",\"type\":\"uint8\",\"indexed\":false,\"internalType\":\"uint8\"}],\"anonymous\":false}]",
}

// DAAnchorABI is the input ABI used to generate the binding from.
// Deprecated: Use DAAnchorMetaData.ABI instead.
var DAAnchorABI = DAAnchorMetaData.ABI

// DAAnchor is an auto generated Go binding around an Ethereum contract.
type DAAnchor struct {
	DAAnchorCaller     // Read-only binding to the contract
	DAAnchorTransactor // Write-only binding to the contract
	DAAnchorFilterer   // Log filterer for contract events
}

// DAAnchorCaller is an auto generated read-only Go binding around an Ethereum contract.
type DAAnchorCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DAAnchorTransactor is an auto generated write-only Go binding around an Ethereum contract.
type DAAnchorTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DAAnchorFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type DAAnchorFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DAAnchorSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type DAAnchorSession struct {
	Contract     *DAAnchor         // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// DAAnchorCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type DAAnchorCallerSession struct {
	Contract *DAAnchorCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts   // Call options to use throughout this session
}

// DAAnchorTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type DAAnchorTransactorSession struct {
	Contract     *DAAnchorTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts   // Transaction auth options to use throughout this session
}

// DAAnchorRaw is an auto generated low-level Go binding around an Ethereum contract.
type DAAnchorRaw struct {
	Contract *DAAnchor // Generic contract binding to access the raw methods on
}

// DAAnchorCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type DAAnchorCallerRaw struct {
	Contract *DAAnchorCaller // Generic read-only contract binding to access the raw methods on
}

// DAAnchorTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type DAAnchorTransactorRaw struct {
	Contract *DAAnchorTransactor // Generic write-only contract binding to access the raw methods on
}

// NewDAAnchor creates a new instance of DAAnchor, bound to a specific deployed contract.
func NewDAAnchor(address common.Address, backend bind.ContractBackend) (*DAAnchor, error) {
	contract, err := bindDAAnchor(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &DAAnchor{DAAnchorCaller: DAAnchorCaller{contract: contract}, DAAnchorTransactor: DAAnchorTransactor{contract: contract}, DAAnchorFilterer: DAAnchorFilterer{contract: contract}}, nil
}

// NewDAAnchorCaller creates a new read-only instance of DAAnchor, bound to a specific deployed contract.
func NewDAAnchorCaller(address common.Address, caller bind.ContractCaller) (*DAAnchorCaller, error) {
	contract, err := bindDAAnchor(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &DAAnchorCaller{contract: contract}, nil
}

// NewDAAnchorTransactor creates a new write-only instance of DAAnchor, bound to a specific deployed contract.
func NewDAAnchorTransactor(address common.Address, transactor bind.ContractTransactor) (*DAAnchorTransactor, error) {
	contract, err := bindDAAnchor(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &DAAnchorTransactor{contract: contract}, nil
}

// NewDAAnchorFilterer creates a new log filterer instance of DAAnchor, bound to a specific deployed contract.
func NewDAAnchorFilterer(address common.Address, filterer bind.ContractFilterer) (*DAAnchorFilterer, error) {
	contract, err := bindDAAnchor(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &DAAnchorFilterer{contract: contract}, nil
}

// bindDAAnchor binds a generic wrapper to an already deployed contract.
func bindDAAnchor(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := DAAnchorMetaData.GetAbi()
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, *parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_DAAnchor *DAAnchorRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _DAAnchor.Contract.DAAnchorCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_DAAnchor *DAAnchorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _DAAnchor.Contract.DAAnchorTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_DAAnchor *DAAnchorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _DAAnchor.Contract.DAAnchorTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_DAAnchor *DAAnchorCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _DAAnchor.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_DAAnchor *DAAnchorTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _DAAnchor.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_DAAnchor *DAAnchorTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _DAAnchor.Contract.contract.Transact(opts, method, params...)
}

// Publish is a paid mutator transaction binding the contract method 0xb3382dfb.
//
// Solidity: function publish(bytes32 dbId, bytes32 tableId, uint64 daHeight, bytes32 daCommitment, uint32 blobSize, bytes32 blobHash, bytes32 schemaHash, uint8 chunkIdx, uint8 chunkCount) returns(uint64 seq)
func (_DAAnchor *DAAnchorTransactor) Publish(opts *bind.TransactOpts, dbId [32]byte, tableId [32]byte, daHeight uint64, daCommitment [32]byte, blobSize uint32, blobHash [32]byte, schemaHash [32]byte, chunkIdx uint8, chunkCount uint8) (*types.Transaction, error) {
	return _DAAnchor.contract.Transact(opts, "publish", dbId, tableId, daHeight, daCommitment, blobSize, blobHash, schemaHash, chunkIdx, chunkCount)
}

// Publish is a paid mutator transaction binding the contract method 0xb3382dfb.
//
// Solidity: function publish(bytes32 dbId, bytes32 tableId, uint64 daHeight, bytes32 daCommitment, uint32 blobSize, bytes32 blobHash, bytes32 schemaHash, uint8 chunkIdx, uint8 chunkCount) returns(uint64 seq)
func (_DAAnchor *DAAnchorSession) Publish(dbId [32]byte, tableId [32]byte, daHeight uint64, daCommitment [32]byte, blobSize uint32, blobHash [32]byte, schemaHash [32]byte, chunkIdx uint8, chunkCount uint8) (*types.Transaction, error) {
	return _DAAnchor.Contract.Publish(&_DAAnchor.TransactOpts, dbId, tableId, daHeight, daCommitment, blobSize, blobHash, schemaHash, chunkIdx, chunkCount)
}

// Publish is a paid mutator transaction binding the contract method 0xb3382dfb.
//
// Solidity: function publish(bytes32 dbId, bytes32 tableId, uint64 daHeight, bytes32 daCommitment, uint32 blobSize, bytes32 blobHash, bytes32 schemaHash, uint8 chunkIdx, uint8 chunkCount) returns(uint64 seq)
func (_DAAnchor *DAAnchorTransactorSession) Publish(dbId [32]byte, tableId [32]byte, daHeight uint64, daCommitment [32]byte, blobSize uint32, blobHash [32]byte, schemaHash [32]byte, chunkIdx uint8, chunkCount uint8) (*types.Transaction, error) {
	return _DAAnchor.Contract.Publish(&_DAAnchor.TransactOpts, dbId, tableId, daHeight, daCommitment, blobSize, blobHash, schemaHash, chunkIdx, chunkCount)
}

// PublishChunk is a paid mutator transaction binding the contract method 0x6885f3e5.
//
// Solidity: function publishChunk(bytes32 dbId, bytes32 tableId, uint64 seq, uint64 daHeight, bytes32 daCommitment, uint32 blobSize, bytes32 blobHash, bytes32 schemaHash, uint8 chunkIdx, uint8 chunkCount) returns()
func (_DAAnchor *DAAnchorTransactor) PublishChunk(opts *bind.TransactOpts, dbId [32]byte, tableId [32]byte, seq uint64, daHeight uint64, daCommitment [32]byte, blobSize uint32, blobHash [32]byte, schemaHash [32]byte, chunkIdx uint8, chunkCount uint8) (*types.Transaction, error) {
	return _DAAnchor.contract.Transact(opts, "publishChunk", dbId, tableId, seq, daHeight, daCommitment, blobSize, blobHash, schemaHash, chunkIdx, chunkCount)
}

// PublishChunk is a paid mutator transaction binding the contract method 0x6885f3e5.
//
// Solidity: function publishChunk(bytes32 dbId, bytes32 tableId, uint64 seq, uint64 daHeight, bytes32 daCommitment, uint32 blobSize, bytes32 blobHash, bytes32 schemaHash, uint8 chunkIdx, uint8 chunkCount) returns()
func (_DAAnchor *DAAnchorSession) PublishChunk(dbId [32]byte, tableId [32]byte, seq uint64, daHeight uint64, daCommitment [32]byte, blobSize uint32, blobHash [32]byte, schemaHash [32]byte, chunkIdx uint8, chunkCount uint8) (*types.Transaction, error) {
	return _DAAnchor.Contract.PublishChunk(&_DAAnchor.TransactOpts, dbId, tableId, seq, daHeight, daCommitment, blobSize, blobHash, schemaHash, chunkIdx, chunkCount)
}

// PublishChunk is a paid mutator transaction binding the contract method 0x6885f3e5.
//
// Solidity: function publishChunk(bytes32 dbId, bytes32 tableId, uint64 seq, uint64 daHeight, bytes32 daCommitment, uint32 blobSize, bytes32 blobHash, bytes32 schemaHash, uint8 chunkIdx, uint8 chunkCount) returns()
func (_DAAnchor *DAAnchorTransactorSession) PublishChunk(dbId [32]byte, tableId [32]byte, seq uint64, daHeight uint64, daCommitment [32]byte, blobSize uint32, blobHash [32]byte, schemaHash [32]byte, chunkIdx uint8, chunkCount uint8) (*types.Transaction, error) {
	return _DAAnchor.Contract.PublishChunk(&_DAAnchor.TransactOpts, dbId, tableId, seq, daHeight, daCommitment, blobSize, blobHash, schemaHash, chunkIdx, chunkCount)
}

// DAAnchorPublishedIterator is returned from FilterPublished and is used to iterate over the raw logs and unpacked data for Published events raised by the DAAnchor contract.
type DAAnchorPublishedIterator struct {
	Event *DAAnchorPublished // Event containing the contract specifics and raw log

	contract *bind.BoundContract // Generic contract to use for unpacking event data
	event    string              // Event name to use for unpacking event data

	logs chan types.Log        // Log channel receiving the found contract events
	sub  ethereum.Subscription // Subscription for errors, completion and termination
	done bool                  // Whether the subscription completed delivering logs
	fail error                 // Occurred error to stop iteration
}

// Next advances the iterator to the subsequent event, returning whether there
// are any more events found. In case of a retrieval or parsing error, false is
// returned and Error() can be queried for the exact failure.
func (it *DAAnchorPublishedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(DAAnchorPublished)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}
	// Iterator still in progress, wait for either a data or an error event
	select {
	case log := <-it.logs:
		it.Event = new(DAAnchorPublished)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

// Error returns any retrieval or parsing error occurred during filtering.
func (it *DAAnchorPublishedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *DAAnchorPublishedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// DAAnchorPublished represents a Published event raised by the DAAnchor contract.
type DAAnchorPublished struct {
	DbId         [32]byte
	TableId      [32]byte
	PartSeq      uint64
	DaHeight     uint64
	DaCommitment [32]byte
	BlobSize     uint32
	BlobHash     [32]byte
	SchemaHash   [32]byte
	ChunkIdx     uint8
	ChunkCount   uint8
	Raw          types.Log // Blockchain specific contextual infos
}

// FilterPublished is a free log retrieval operation binding the contract event 0x7639e95e03fc54f889db2b769c08b81abca56905d7fd30a59bad1427921f46de.
//
// Solidity: event Published(bytes32 indexed dbId, bytes32 indexed tableId, uint64 partSeq, uint64 daHeight, bytes32 daCommitment, uint32 blobSize, bytes32 blobHash, bytes32 schemaHash, uint8 chunkIdx, uint8 chunkCount)
func (_DAAnchor *DAAnchorFilterer) FilterPublished(opts *bind.FilterOpts, dbId [][32]byte, tableId [][32]byte) (*DAAnchorPublishedIterator, error) {

	var dbIdRule []interface{}
	for _, dbIdItem := range dbId {
		dbIdRule = append(dbIdRule, dbIdItem)
	}
	var tableIdRule []interface{}
	for _, tableIdItem := range tableId {
		tableIdRule = append(tableIdRule, tableIdItem)
	}

	logs, sub, err := _DAAnchor.contract.FilterLogs(opts, "Published", dbIdRule, tableIdRule)
	if err != nil {
		return nil, err
	}
	return &DAAnchorPublishedIterator{contract: _DAAnchor.contract, event: "Published", logs: logs, sub: sub}, nil
}

// WatchPublished is a free log subscription operation binding the contract event 0x7639e95e03fc54f889db2b769c08b81abca56905d7fd30a59bad1427921f46de.
//
// Solidity: event Published(bytes32 indexed dbId, bytes32 indexed tableId, uint64 partSeq, uint64 daHeight, bytes32 daCommitment, uint32 blobSize, bytes32 blobHash, bytes32 schemaHash, uint8 chunkIdx, uint8 chunkCount)
func (_DAAnchor *DAAnchorFilterer) WatchPublished(opts *bind.WatchOpts, sink chan<- *DAAnchorPublished, dbId [][32]byte, tableId [][32]byte) (event.Subscription, error) {

	var dbIdRule []interface{}
	for _, dbIdItem := range dbId {
		dbIdRule = append(dbIdRule, dbIdItem)
	}
	var tableIdRule []interface{}
	for _, tableIdItem := range tableId {
		tableIdRule = append(tableIdRule, tableIdItem)
	}

	logs, sub, err := _DAAnchor.contract.WatchLogs(opts, "Published", dbIdRule, tableIdRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(DAAnchorPublished)
				if err := _DAAnchor.contract.UnpackLog(event, "Published", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// ParsePublished is a log parse operation binding the contract event 0x7639e95e03fc54f889db2b769c08b81abca56905d7fd30a59bad1427921f46de.
//
// Solidity: event Published(bytes32 indexed dbId, bytes32 indexed tableId, uint64 partSeq, uint64 daHeight, bytes32 daCommitment, uint32 blobSize, bytes32 blobHash, bytes32 schemaHash, uint8 chunkIdx, uint8 chunkCount)
func (_DAAnchor *DAAnchorFilterer) ParsePublished(log types.Log) (*DAAnchorPublished, error) {
	event := new(DAAnchorPublished)
	if err := _DAAnchor.contract.UnpackLog(event, "Published", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
