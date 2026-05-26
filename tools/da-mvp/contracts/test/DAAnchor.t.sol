// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Test.sol";
import "../src/DAAnchor.sol";

contract DAAnchorTest is Test {
    DAAnchor anchor;
    bytes32 dbId   = keccak256("db");
    bytes32 tableId = keccak256("tbl");

    function setUp() public {
        anchor = new DAAnchor();
    }

    function test_FirstChunkAllocatesSeqZero() public {
        uint64 seq = anchor.publish(dbId, tableId, 1, bytes32(uint256(0xaa)), 100, bytes32(uint256(0xbb)), bytes32(uint256(0xcc)), 0, 1);
        assertEq(seq, 0);
    }

    function test_SecondPartIncrementsSeq() public {
        anchor.publish(dbId, tableId, 1, bytes32(uint256(1)), 10, bytes32(uint256(1)), bytes32(uint256(1)), 0, 1);
        uint64 seq = anchor.publish(dbId, tableId, 2, bytes32(uint256(2)), 10, bytes32(uint256(2)), bytes32(uint256(1)), 0, 1);
        assertEq(seq, 1);
    }

    function test_DifferentTableHasIndependentSeq() public {
        anchor.publish(dbId, tableId, 1, bytes32(0), 10, bytes32(0), bytes32(0), 0, 1);
        bytes32 otherTbl = keccak256("other");
        uint64 seq = anchor.publish(dbId, otherTbl, 1, bytes32(0), 10, bytes32(0), bytes32(0), 0, 1);
        assertEq(seq, 0);
    }

    function test_PublishWithChunkIdxNonZero_Reverts() public {
        vm.expectRevert();
        anchor.publish(dbId, tableId, 1, bytes32(0), 10, bytes32(0), bytes32(0), 1, 2);
    }

    function test_PublishChunkEmitsWithProvidedSeq() public {
        vm.recordLogs();
        anchor.publishChunk(dbId, tableId, 42, 1, bytes32(uint256(1)), 10, bytes32(uint256(1)), bytes32(0), 1, 2);
        Vm.Log[] memory logs = vm.getRecordedLogs();
        assertEq(logs.length, 1);
        // logs[0].topics[0] is the event sig; topics[1]=dbId, topics[2]=tableId.
        assertEq(logs[0].topics[1], dbId);
        assertEq(logs[0].topics[2], tableId);
    }

    function test_PublishChunkWithChunkIdxEqualChunkCount_Reverts() public {
        // chunkIdx == chunkCount is past-the-end; off-by-one guard must reject.
        vm.expectRevert();
        anchor.publishChunk(dbId, tableId, 0, 1, bytes32(0), 10, bytes32(0), bytes32(0), 2, 2);
    }
}
