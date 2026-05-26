package main

import "github.com/zeebo/blake3"

func blake3Sum256(b []byte) [32]byte { return blake3.Sum256(b) }
