# Distributed Fault-Tolerant Key-Value Store

A distributed, fault-tolerant key-value storage engine built from scratch in **Go**, using the **Raft consensus protocol** to provide leader election, replicated logs, fault tolerance, and consistent state-machine execution.

The system is designed as a small-scale distributed storage engine capable of continuing operation through node failures, network partitions, message loss, and leader changes while maintaining **linearizable semantics** for committed operations.

---

## Overview

Distributed storage systems must solve several problems simultaneously:

* How do multiple nodes agree on the same sequence of operations?
* What happens when the leader crashes?
* How does the cluster recover after a network partition?
* How are committed operations persisted across process crashes?
* How do nodes recover when their logs diverge?
* How can old log entries be compacted without losing state?
* How can the system be tested against failures that are difficult to reproduce?

This project addresses these problems by implementing a replicated key-value store around a **Raft-based replicated state machine**.

A client operation is first accepted by the Raft leader, appended to the replicated log, replicated to a majority of nodes, committed, and then applied to the key-value state machine.

```text
                     Client
                       │
                       │ PUT / GET / DELETE
                       ▼
                ┌───────────────┐
                │   gRPC API    │
                └───────┬───────┘
                        │
                        ▼
                ┌───────────────┐
                │ Raft Leader   │
                │               │
                │ Consensus     │
                │ Log           │
                │ Commit        │
                └───────┬───────┘
                        │
              Replicate │ to majority
            ┌───────────┼───────────┐
            ▼           ▼           ▼
         Node 2       Node 3      Node 4
            │           │           │
            └───────────┼───────────┘
                        │
                     Commit
                        │
                        ▼
                ┌───────────────┐
                │ KV State      │
                │ Machine       │
                └───────┬───────┘
                        │
                        ▼
                ┌───────────────┐
                │ WAL / Storage │
                └───────────────┘
```

---

# Key Features

* **Raft consensus** for distributed agreement
* **Leader election** with majority-based voting
* **Replicated write-ahead log**
* **Linearizable committed operations**
* **Fault-tolerant replicated state machine**
* **Automatic leader failover**
* **Network partition handling**
* **Crash recovery**
* **Persistent Raft state**
* **Log consistency and conflict resolution**
* **Snapshotting and log compaction**
* **Memory-mapped log storage**
* **gRPC + Protocol Buffers transport**
* **Deterministic fault injection**
* **Network message loss and reordering simulation**
* **Concurrent client operations**
* **Race-detector validation**
* **Linearizability testing**
* **Performance benchmarking**
* **Docker-based multi-node deployment**

---

# System Architecture

The system is organized into several layers.

```text
┌─────────────────────────────────────────────────────────────┐
│                         Client                              │
└────────────────────────────┬────────────────────────────────┘
                             │
                             │ gRPC
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                      RPC / API Layer                        │
│                                                             │
│              Protocol Buffers + gRPC                        │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                       Raft Node                             │
│                                                             │
│  ┌───────────────┐  ┌──────────────┐  ┌─────────────────┐ │
│  │ Leader        │  │ Replication  │  │ Commit Tracking │ │
│  │ Election      │  │ nextIndex    │  │ matchIndex      │ │
│  └───────────────┘  └──────────────┘  └─────────────────┘ │
│                                                             │
│                    Replicated Log                           │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                   State Machine                             │
│                                                             │
│                   PUT / GET / DELETE                        │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                   Persistent Storage                        │
│                                                             │
│        WAL + Log Segments + Snapshots                       │
│                                                             │
│              Memory-Mapped Storage                          │
└─────────────────────────────────────────────────────────────┘
```

---

# Replicated State Machine

The key-value store follows the replicated state-machine model.

Every write operation is represented as a command in the Raft log.

For example:

```text
PUT("user:42", "Deepankar")
```

becomes a Raft log entry:

```text
┌─────────┬─────────┬─────────────────────────┐
│ Index   │ Term    │ Command                 │
├─────────┼─────────┼─────────────────────────┤
│ 42      │ 7       │ PUT user:42 Deepankar   │
└─────────┴─────────┴─────────────────────────┘
```

The leader replicates the entry to followers.

Once the entry is safely committed by a majority:

```text
Raft Log
   │
   │ committed
   ▼
State Machine
   │
   ▼
Key-Value Store
```

All healthy replicas eventually apply the same sequence of committed commands and therefore converge to the same state.

---

# Raft Consensus

The system uses the Raft consensus algorithm to coordinate replicated state.

Each node maintains:

### Persistent state

```text
currentTerm
votedFor
log[]
```

### Volatile state

```text
commitIndex
lastApplied
```

### Leader state

```text
nextIndex[]
matchIndex[]
```

Nodes operate in three states:

```text
             ┌──────────┐
             │ Follower │
             └────┬─────┘
                  │
          election timeout
                  │
                  ▼
             ┌──────────┐
             │ Candidate│
             └────┬─────┘
                  │
            majority votes
                  │
                  ▼
             ┌──────────┐
             │  Leader  │
             └──────────┘
```

A leader periodically sends heartbeats to maintain authority and prevent unnecessary elections.

---

# Leader Election

When a follower does not receive valid leader communication within its election timeout, it becomes a candidate.

The candidate:

1. Increments its term.
2. Votes for itself.
3. Requests votes from other nodes.
4. Waits for a majority.
5. Becomes leader after receiving enough votes.

For a cluster containing `N` nodes:

```text
majority = floor(N / 2) + 1
```

Examples:

```text
3 nodes → 2 votes
5 nodes → 3 votes
7 nodes → 4 votes
```

The implementation handles:

* Stale terms
* Higher terms
* Duplicate requests
* One vote per term
* Split votes
* Election retries
* Candidate failures
* Network partitions
* Leader failures

---

# Log Replication

Once elected, the leader becomes the authoritative entry point for writes.

```text
Client
   │
   │ PUT
   ▼
Leader
   │
   │ append
   ▼
Local Log
   │
   ├──────────────► Follower 1
   │
   ├──────────────► Follower 2
   │
   └──────────────► Follower 3
                       │
                       ▼
                   Majority
                       │
                       ▼
                    Commit
```

Each log entry contains:

```text
Index
Term
Command
```

Followers verify that the previous log entry matches before accepting new entries.

If a follower's log diverges from the leader, the leader backs up the follower's replication position until a matching prefix is found and then repairs the follower's log.

This provides the core log-consistency property of Raft.

---

# Commit Semantics

A log entry becomes committed after the leader has replicated it to a majority of the cluster according to Raft's commitment rules.

The important indexes are:

```text
commitIndex
lastApplied
```

with the invariant:

```text
lastApplied <= commitIndex
```

Committed entries are applied to the state machine strictly in log order.

This ensures that all replicas execute the same committed command sequence.

---

# Key-Value API

The storage engine exposes basic key-value operations:

```text
PUT(key, value)
GET(key)
DELETE(key)
```

Keys and values are treated as byte sequences, allowing arbitrary binary data.

Example:

```text
PUT("user:100", "Deepankar")
GET("user:100")
DELETE("user:100")
```

Writes are routed through the Raft leader so that they participate in the replicated log.

Reads can be handled according to the consistency mechanism implemented by the server.

---

# Persistence

The system uses an append-only write-ahead log to persist important state.

The persistence layer is designed to provide:

* Sequential writes
* Ordered recovery
* Checksum validation
* Corruption detection
* Truncated-record detection
* Explicit synchronization
* Crash recovery

Conceptually:

```text
Operation
    │
    ▼
Raft Log
    │
    ▼
WAL
    │
    ▼
Disk
```

After a process restart:

```text
Disk
  │
  ▼
WAL Replay
  │
  ▼
Recover Raft State
  │
  ▼
Recover State Machine
```

This separates consensus correctness from the physical persistence mechanism.

---

# Snapshots and Log Compaction

A continuously growing Raft log is impractical for long-running systems.

The system therefore supports snapshot-based log compaction.

Without snapshots:

```text
Log:
1 2 3 4 5 6 7 8 9 10 ... 1,000,000
```

After compaction:

```text
Snapshot
──────────────
State at index 900,000

Remaining log
──────────────
900001
900002
900003
...
```

A snapshot contains the state-machine state at a particular committed log index.

Older log entries can then be safely discarded.

Snapshots also reduce recovery time because the node does not need to replay the entire historical log.

---

# Storage Architecture

The storage subsystem is designed around sequential persistence and efficient recovery.

```text
                 ┌─────────────────┐
                 │   Raft Log      │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ Log Manager     │
                 └────────┬────────┘
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
       ┌─────────────┐        ┌─────────────┐
       │ WAL Segment │        │ WAL Segment │
       └─────────────┘        └─────────────┘
              │
              ▼
       Memory-Mapped Files
              │
              ▼
             Disk
```

Memory mapping is used to reduce copying and system-call overhead for suitable storage paths.

---

# Fault Tolerance

The cluster is designed to tolerate failures as long as a majority of nodes remain available.

For a 5-node cluster:

```text
5 nodes → tolerate 2 failures
3 nodes → tolerate 1 failure
7 nodes → tolerate 3 failures
```

This follows the Raft majority requirement.

---

## Leader Failure

Example:

```text
Before:

        Node 1
        Leader
       /     \
      ▼       ▼
   Node 2   Node 3
   Follower Follower
```

If Node 1 crashes:

```text
Node 1
   X

Node 2 ───── Node 3
    \         /
     \       /
      Election
         │
         ▼
     New Leader
```

A new leader can be elected as long as a majority remains available.

---

# Network Partitions

The system is explicitly tested against network partitions.

Example:

```text
             PARTITION
                │
                X
                │

Node 1 ─── Node 2       Node 3 ─── Node 4 ─── Node 5
```

The minority side cannot elect a new leader because it lacks a majority.

The majority side can continue making progress.

When connectivity is restored, stale nodes reconcile their state through the Raft protocol.

---

# Fault Injection

Distributed systems are difficult to validate using only normal execution.

The project therefore includes deterministic fault injection.

Failures can include:

```text
Message loss
Message delay
Message duplication
Message reordering
Network partition
Node crash
Node restart
Disk corruption
```

Example:

```text
Leader
   │
   ├──────X──────► Follower
   │
   ├─────────────► Follower
   │
   └─────────────► Follower
```

The test framework can reproduce the same failure scenario using a deterministic seed.

This makes failures debuggable rather than dependent on timing luck.

---

# Linearizability

The final system is designed to provide linearizable semantics for committed operations.

A concurrent execution might look like:

```text
Client 1 ── PUT(A, 10) ────────────────┐
                                      │
Client 2 ─────── GET(A) ──────────────┤
                                      │
Client 3 ───────── DELETE(A) ─────────┘
```

The system records operation histories and verifies that the observed execution can be explained by some valid sequential ordering that respects real-time constraints.

Testing covers:

* Concurrent clients
* Leader changes
* Node crashes
* Network partitions
* Message delays
* Retries
* Concurrent reads and writes

---

# Testing Strategy

Correctness is validated at multiple levels.

## Unit Testing

Individual components are tested independently:

```text
KV Store
WAL
Raft Log
Raft State
Election
Replication
Snapshots
Transport
```

---

## Integration Testing

Multiple Raft nodes are executed together to validate:

* Leader election
* Replication
* Commit
* Recovery
* Failover
* Partition handling

---

## Fault Injection Testing

The cluster is deliberately subjected to failures:

```text
Normal
  │
  ├── Node crash
  ├── Leader crash
  ├── Message loss
  ├── Message delay
  ├── Network partition
  └── Restart
```

---

## Race Detection

Concurrency bugs are detected using Go's race detector:

```bash
go test -race ./...
```

---

## Repeated Testing

Tests are executed repeatedly to expose nondeterministic behavior:

```bash
go test -count=20 ./...
```

and:

```bash
go test -race -count=10 ./...
```

---

# Performance

The project is designed to benchmark a 5-node replicated cluster.

Target performance:

```text
Write throughput:
35,000+ requests/sec

p99 commit latency:
< 10 ms
```

These numbers are engineering targets and will only be reported as achieved after reproducible benchmarks.

Benchmark results will document:

* Hardware
* CPU
* Memory
* Go version
* Cluster size
* Request size
* Concurrency
* Replication factor
* Throughput
* p50 latency
* p95 latency
* p99 latency

---

# Docker Deployment

The final system can be deployed as a multi-node cluster using Docker.

Example topology:

```text
                 Docker Network
                       │
       ┌───────────────┼────────────────┐
       │               │                │
       ▼               ▼                ▼
   ┌────────┐      ┌────────┐      ┌────────┐
   │ Node 1 │      │ Node 2 │      │ Node 3 │
   └────────┘      └────────┘      └────────┘
       │               │                │
       └───────────────┼────────────────┘
                       │
                 ┌───────────┐
                 │ Node 4/5  │
                 └───────────┘
```

This allows the system to be tested as an actual multi-process distributed cluster rather than only as an in-process simulation.

---

# Technology Stack

| Component            | Technology              |
| -------------------- | ----------------------- |
| Language             | Go                      |
| Consensus            | Raft                    |
| RPC                  | gRPC                    |
| Serialization        | Protocol Buffers        |
| Storage              | Custom WAL              |
| Log Compaction       | Snapshots               |
| Storage Optimization | Memory-Mapped Files     |
| Testing              | Go Testing              |
| Race Detection       | Go Race Detector        |
| Fault Injection      | Deterministic Simulator |
| Deployment           | Docker                  |
| Version Control      | Git                     |

---

# Repository Structure

```text
.
├── cmd/
│   └── server/
│
├── kv/
│   ├── store.go
│   └── store_test.go
│
├── raft/
│   ├── node.go
│   ├── log.go
│   ├── election.go
│   ├── replication.go
│   ├── snapshot.go
│   └── *_test.go
│
├── storage/
│   ├── wal.go
│   ├── mmap.go
│   └── *_test.go
│
├── transport/
│   ├── grpc.go
│   ├── memory.go
│   └── *_test.go
│
├── proto/
│   └── raft.proto
│
├── test/
│   ├── fault_injection/
│   └── linearizability/
│
├── benchmarks/
│
├── Dockerfile
├── docker-compose.yml
├── go.mod
└── README.md
```

---

# Running the Project

Clone the repository:

```bash
git clone <repository-url>
cd distributed-kv
```

Run the test suite:

```bash
go test ./...
```

Run static analysis:

```bash
go vet ./...
```

Run the race detector:

```bash
go test -race ./...
```

Run repeated tests:

```bash
go test -count=20 ./...
```

Build the server:

```bash
go build ./cmd/server
```

Run locally:

```bash
go run ./cmd/server
```

Docker deployment:

```bash
docker compose up --build
```

---

# Design Goals

The project focuses on demonstrating practical distributed-systems engineering rather than simply exposing a key-value API.

The primary goals are:

* **Correctness** — preserve Raft safety invariants.
* **Fault tolerance** — continue operating through minority failures.
* **Persistence** — recover state after crashes.
* **Consistency** — provide linearizable committed writes.
* **Deterministic testing** — make distributed failures reproducible.
* **Concurrency safety** — avoid races under concurrent workloads.
* **Observability** — make protocol behavior and failures diagnosable.
* **Performance** — optimize storage and networking only after correctness is established.

---

# What This Project Demonstrates

This project combines several systems concepts in one implementation:

* Distributed consensus
* Replicated state machines
* Leader election
* Log replication
* Majority quorum
* Failure detection
* Network partitions
* Crash recovery
* Write-ahead logging
* Persistent storage
* Snapshotting
* Log compaction
* Memory-mapped I/O
* Concurrency
* gRPC
* Protocol Buffers
* Fault injection
* Linearizability
* Distributed-systems testing
* Performance engineering

The implementation intentionally avoids relying on an existing consensus framework so that the underlying mechanics of a replicated storage system are explicit and testable.

---

# Author

**Deepankar Tiwari**

Distributed systems project implemented in Go with a focus on Raft consensus, persistent storage, fault tolerance, deterministic failure testing, and high-performance replicated state management.
