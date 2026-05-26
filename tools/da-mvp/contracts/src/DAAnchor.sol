// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @title DAAnchor — MVP commitment registry for housegate DA blobs.
/// @notice publish() allocates a new partSeq for chunk 0; subsequent
///         chunks of the same part use publishChunk(seq, ...).
contract DAAnchor {
    event Published(
        bytes32 indexed dbId,
        bytes32 indexed tableId,
        uint64  partSeq,
        uint64  daHeight,
        bytes32 daCommitment,
        uint32  blobSize,
        bytes32 blobHash,
        bytes32 schemaHash,
        uint8   chunkIdx,
        uint8   chunkCount
    );

    mapping(bytes32 => uint64) private _nextSeq;

    function publish(
        bytes32 dbId,
        bytes32 tableId,
        uint64  daHeight,
        bytes32 daCommitment,
        uint32  blobSize,
        bytes32 blobHash,
        bytes32 schemaHash,
        uint8   chunkIdx,
        uint8   chunkCount
    ) external returns (uint64 seq) {
        require(chunkIdx == 0, "MVP: use publishChunk for chunkIdx > 0");
        require(chunkCount >= 1, "chunkCount must be >= 1");
        bytes32 key = keccak256(abi.encode(dbId, tableId));
        seq = _nextSeq[key]++;
        emit Published(dbId, tableId, seq, daHeight, daCommitment,
                       blobSize, blobHash, schemaHash, chunkIdx, chunkCount);
    }

    function publishChunk(
        bytes32 dbId,
        bytes32 tableId,
        uint64  seq,
        uint64  daHeight,
        bytes32 daCommitment,
        uint32  blobSize,
        bytes32 blobHash,
        bytes32 schemaHash,
        uint8   chunkIdx,
        uint8   chunkCount
    ) external {
        require(chunkIdx > 0, "MVP: chunkIdx must be > 0 for publishChunk");
        require(chunkIdx < chunkCount, "chunkIdx out of range");
        emit Published(dbId, tableId, seq, daHeight, daCommitment,
                       blobSize, blobHash, schemaHash, chunkIdx, chunkCount);
    }
}
