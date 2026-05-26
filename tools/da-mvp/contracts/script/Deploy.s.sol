// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Script.sol";
import "../src/DAAnchor.sol";

contract Deploy is Script {
    function run() external returns (DAAnchor) {
        vm.startBroadcast();
        DAAnchor a = new DAAnchor();
        vm.stopBroadcast();
        return a;
    }
}
