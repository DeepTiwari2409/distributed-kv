# distributed-kv

A distributed fault-tolerant key-value store written in Go.

## Overview

This repository is the foundation for a production-quality distributed key-value store. The project is being built in careful phases, starting with the basic Go project structure.

## Implementation

- Language: Go
- Current phase: Phase 0 — Project Foundation
- Future consensus layer: Raft will eventually provide consensus for replication and fault tolerance
- Future features: replication, crash recovery, network partitions, snapshots, deterministic fault injection, linearizability testing, and benchmarking

## Status

This phase establishes project layout, module configuration, a minimal server entrypoint, and a basic test to verify the Go toolchain.
