<div align="center">

# StreamForge

**A production-grade distributed stream processing engine built from first principles in Go.**

Combines a custom Kafka-style distributed broker, a Flink-style stateful stream processor, and a novel speculative execution engine for straggler mitigation all wired together over gRPC and Raft consensus implemented from scratch.

[![Go](https://img.shields.io/badge/Go-1.22-00ADD8?style=flat-square&logo=go)](https://go.dev/)
[![gRPC](https://img.shields.io/badge/gRPC-Protobuf-244c5a?style=flat-square&logo=grpc)](https://grpc.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)](https://docs.docker.com/compose/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue?style=flat-square)](LICENSE)

</div>

---

## Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Component Deep Dives](#component-deep-dives)
  - [Distributed Broker (Mini Kafka)](#1-distributed-broker-mini-kafka)
  - [Stream Processor (Mini Flink)](#2-stream-processor-mini-flink)
  - [Speculative Execution Engine](#3-speculative-execution-engine)
- [gRPC API Surface](#grpc-api-surface)
- [Project Structure](#project-structure)
- [Benchmarks](#benchmarks)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
- [Design Decisions & Trade-offs](#design-decisions--trade-offs)
- [Tech Stack](#tech-stack)

---

## Overview

StreamForge answers the question: *what does it actually take to build the infrastructure behind Twitter trending topics or Uber surge pricing?* It is a ground-up implementation of three tightly-coupled distributed systems:

| Component | Analogue | Role |
|---|---|---|
| **Broker** | Apache Kafka | Durable, partitioned, replicated event log |
| **Stream Processor** | Apache Flink | Stateful, windowed, fault-tolerant stream computation |
| **Speculative Engine** | Novel contribution | Tail-latency mitigation via checkpoint-anchored task duplication |

Every subsystem is implemented from scratch with zero reliance on external distributed systems libraries — no etcd, no Zookeeper, no Kafka client, no Flink. The design decisions mirror exactly what production systems do and why.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      PRODUCER CLUSTER                       │
│           (load generator: round-robin + key-hash)          │
└──────────────────────┬──────────────────────────────────────┘
                       │ gRPC Produce(topic, partition, key, value)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                      BROKER CLUSTER                         │
│                                                             │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│   │   Broker 1   │  │   Broker 2   │  │   Broker 3   │     │
│   │  Leader P0,P1│  │  Leader P2,P3│  │  Follower    │     │
│   │              │  │              │  │  (all parts) │     │
│   │  ┌────────┐  │  │  ┌────────┐  │  │  ┌────────┐  │     │
│   │  │Segment │  │  │  │Segment │  │  │  │Segment │  │     │
│   │  │ Files  │  │  │  │ Files  │  │  │  │ Files  │  │     │
│   │  └────────┘  │  │  └────────┘  │  │  └────────┘  │     │
│   │  ┌────────┐  │  │  ┌────────┐  │  │  ┌────────┐  │     │
│   │  │  WAL   │  │  │  │  WAL   │  │  │  │  WAL   │  │     │
│   │  └────────┘  │  │  └────────┘  │  │  └────────┘  │     │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │
│          └────────── Raft Consensus ──────────┘             │
│                  (RequestVote + AppendEntries)               │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐   │
│   │      Consumer Group Coordinator (RebalanceCoord)    │   │
│   │  heartbeat → liveness → partition reassignment      │   │
│   └─────────────────────────────────────────────────────┘   │
└──────────────────────┬──────────────────────────────────────┘
                       │ gRPC Fetch(topic, partition, offset)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                   STREAM PROCESSOR                          │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐   │
│   │           CheckpointCoordinator                     │   │
│   │  periodic BARRIER injection → BarrierAlignment      │   │
│   └─────────────────────────────────────────────────────┘   │
│                                                             │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│   │   Worker 0   │  │   Worker 1   │  │   Worker 2   │     │
│   │  Engine      │  │  Engine      │  │  Engine      │     │
│   │  Partition 0 │  │  Partition 1 │  │  Partition 2 │     │
│   │  WindowOp    │  │  WindowOp    │  │  WindowOp    │     │
│   │  Aggregator  │  │  Aggregator  │  │  Aggregator  │     │
│   │  StateStore  │  │  StateStore  │  │  StateStore  │     │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │
│          │                 │                  │             │
│   ┌──────▼─────────────────▼──────────────────▼──────────┐  │
│   │              StragglerDetector (100ms poll)           │  │
│   │  median progress → 2× threshold → Launch() on spare  │  │
│   └──────────────────────┬────────────────────────────────┘  │
│                          │                                  │
│   ┌──────────────────────▼────────────────────────────────┐  │
│   │        ResultArbitrator (first-write-wins tombstone)  │  │
│   └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                       │
                       ▼
              Output Sink / Prometheus Metrics / Grafana
```

---

## Component Deep Dives

### 1. Distributed Broker (Mini Kafka)

#### Append-Only Partitioned Log

The core storage primitive is an append-only log split into fixed-size **segment files** on disk. Each segment has a companion **sparse index** for O(1) offset lookups without linear scan.

```
partition-0/
├── 00000000000000000000.log    ← sealed segment  (offsets 0–999)
├── 00000000000000000000.index  ← sparse: relOffset → bytePos
├── 00000000000000001000.log    ← sealed segment  (offsets 1000–1999)
├── 00000000000000001000.index
├── 00000000000000002000.log    ← active segment  (append here)
├── 00000000000000002000.index
└── wal.log                     ← Write-Ahead Log
```

Read path for offset `N`: binary search segment list by base offset → sparse index lookup for nearest entry → sequential scan within a small range. Write path always appends to the active segment; index updated every 10th entry (`indexCounter % 10`) to bound index size while keeping lookup within O(10) steps.

#### Write-Ahead Log (WAL)

Every write is committed to `wal.log` before the segment file. Each WAL record is length-prefixed with a CRC32 checksum:

```
[length: 4 bytes][crc32: 4 bytes][payload: N bytes]
```

On crash recovery, `Replay()` re-validates each record via CRC and re-appends to the active segment. After successful fsync to the segment, `Truncate()` clears the WAL. This is the same pattern used by PostgreSQL, RocksDB, and LevelDB.

#### In-Sync Replica (ISR) Tracking

`ISRManager` maintains per-partition sets of followers that are within the configured lag threshold. Followers call `UpdateReplicaProgress()` after each replication batch; `CheckISR()` runs periodically to evict lagging followers. Evicted brokers automatically rejoin ISR on their next successful sync.

```
Produce write flow:
  1. Write to WAL  (local)
  2. Append to active segment
  3. Replicate to all ISR followers via AppendEntries gRPC
  4. Await ISR quorum ACK
  5. Return offset to producer
```

ACK is returned only after ISR quorum confirmation — a message is guaranteed durable across a single broker failure.

#### Raft Consensus for Leader Election

Raft is implemented from scratch (`internal/raft/`) covering:

- **Leader election** — randomised election timeouts, `RequestVote` RPCs, majority quorum
- **Log replication** — `AppendEntries` RPCs with conflict detection via `conflictIndex` / `conflictTerm` for fast log backtrack
- **Snapshot install** — `InstallSnapshot` RPC for lagging followers to catch up without full log replay
- **Membership changes** — safe term-based transition

The Raft log entry format exposed over gRPC:

```protobuf
message AppendEntriesRequest {
  int64 term           = 1;
  int32 leader_id      = 2;
  int64 prev_log_index = 3;
  int64 prev_log_term  = 4;
  repeated LogEntry entries = 5;
  int64 leader_commit  = 6;
}
```

Typical election time on a 3-node cluster: **~30ms** from heartbeat timeout to new leader accepting writes.

#### Consumer Group Coordination

`RebalanceCoordinator` manages group membership, heartbeat-based liveness, and partition assignment across consumers. On join or failure detection, a **range-partitioned rebalance** redistributes the partition set evenly. The group `GenerationID` monotonically increments with each rebalance, allowing stale consumers to detect they have been superseded. Consumers commit offsets explicitly via `OffsetCommit` RPC; on restart they resume via `OffsetFetch`.

---

### 2. Stream Processor (Mini Flink)

#### Processing Engine

`Engine` runs a per-partition goroutine consuming `<-chan types.Message`. For each message it:

1. Calls `WindowOperator.AssignWindows(ts)` to determine which window bucket(s) the event belongs to.
2. Fetches current `WindowState` from `StateStore` (keyed by `(windowID, messageKey)`).
3. Applies the `Aggregator` (COUNT, SUM, AVG, MIN, MAX) to produce updated state.
4. Writes back to `StateStore`.
5. Every 100 events, reports progress `[0.0, 1.0]` to the `StragglerDetector`.

#### Windowing

```go
// Tumbling: each event belongs to exactly one window
winStart := ts.Truncate(windowSize)
windowID  := WindowID(winStart.UnixNano())

// Sliding: each event may belong to multiple overlapping windows
// window_size / slide_interval determines overlap fan-out
```

Tumbling windows are strictly non-overlapping and the cheapest to maintain — one state entry per `(window, key)`. Sliding windows trade compute for finer granularity.

#### Chandy-Lamport Distributed Checkpointing

`CheckpointCoordinator` periodically injects **BARRIER** markers into each partition's event stream. Workers process pre-barrier events, snapshot their `StateStore` to disk, then forward the BARRIER downstream. `BarrierAlignment` blocks a worker from processing post-barrier events on a faster partition until all partitions deliver the same BARRIER, ensuring a globally consistent snapshot.

```
Partition 0:  [e1][e2][BARRIER-7][e3][e4]
Partition 1:  [e5][e6][e7][BARRIER-7][e8]

Worker for partition 0:
  → processes e1, e2
  → sees BARRIER-7: snapshot state → forward BARRIER
  → continue with e3, e4

BarrierAlignment: waits until BARRIER-7 received from all partitions
→ Checkpoint 7 globally complete and durable
→ On crash: restore snapshot 7, re-consume only events after barrier offset
```

**Delivery guarantee:** At-least-once delivery; with Chandy-Lamport checkpointing, the processor achieves exactly-once semantics within its computation layer.

---

### 3. Speculative Execution Engine

This is the novel architectural contribution. It directly targets the **straggler problem** in distributed window aggregation: one slow partition consistently delays every window result regardless of how fast the rest complete.

#### Detection

`StragglerDetector` polls every **100ms**. It:

1. Collects `WindowProgress ∈ [0.0, 1.0]` from all registered workers.
2. Computes the **median progress** across all active partitions.
3. Flags any worker with `progress < median × threshold` (default `threshold = 0.5`).
4. For each flagged worker not already speculated, calls `SpeculativeManager.Launch()`.

```go
// internal/speculative/detector.go
median := d.computeMedianProgress()
for id, worker := range d.workers {
    if worker.WindowProgress < median*d.threshold {
        if !worker.IsSpeculative {
            d.speculativeMgr.Launch(id, worker.LastCheckpoint)
            worker.IsSpeculative = true
        }
    }
}
```

#### Mitigation

`SpeculativeManager.Launch()` starts a duplicate worker goroutine (or dispatches to a spare node via gRPC) anchored at the **last complete checkpoint** of the straggling partition. Both original and duplicate race to complete the window.

#### Arbitration

`ResultArbitrator` implements **first-write-wins with tombstoning**:

```go
// internal/speculative/manager_arbitrator.go
func (a *ResultArbitrator) Submit(windowID, partitionID, result) bool {
    if a.tombstones[windowID][partitionID] {
        // second writer — discard silently
        return false
    }
    // first writer — set tombstone, accept result
    a.tombstones[windowID][partitionID] = true
    return true
}
```

The tombstone prevents duplicate emission regardless of which task (original or speculative) finishes first. State correctness is guaranteed because the speculative task always starts from a clean checkpoint snapshot — never from mid-flight dirty state.

**Why this matters:** Flink has straggler detection but no automatic mitigation. Kafka Streams has nothing. This is an open production problem.

---

## gRPC API Surface

### Broker Service (`proto/broker.proto`)

| RPC | Request | Response | Description |
|---|---|---|---|
| `Produce` | `topic, partition, key, value` | `success, offset` | Append message to partition |
| `Fetch` | `topic, partition, offset` | `key, value, offset, next_offset` | Read message at offset |
| `Metadata` | `topic` | `partition_leaders map` | Partition → leader broker routing table |
| `OffsetCommit` | `group_id, topic, partition, offset` | `success` | Commit consumer group offset |
| `OffsetFetch` | `group_id, topic, partition` | `offset` | Fetch last committed offset |

### Raft Service (`proto/raft.proto`)

| RPC | Description |
|---|---|
| `RequestVote` | Candidate requests vote from peer; includes `lastLogIndex`, `lastLogTerm` for log completeness check |
| `AppendEntries` | Leader replicates log entries or sends heartbeat; response includes `conflictIndex`/`conflictTerm` for fast backtrack |
| `InstallSnapshot` | Leader sends full state snapshot to lagging follower |

---

## Project Structure

```
StreamForge/
├── cmd/
│   ├── benchmark/          # Parameterized load generator and benchmark harness
│   └── demo/               # End-to-end pipeline demo
├── internal/
│   ├── broker/
│   │   ├── broker.go       # Broker node: partition ownership + gRPC handler wiring
│   │   ├── isr.go          # ISRManager: lag-based ISR eviction and readmission
│   │   ├── metadata.go     # Partition leader metadata registry
│   │   └── rebalance.go    # RebalanceCoordinator: consumer group membership + partition assignment
│   ├── raft/
│   │   ├── log.go          # Raft log: append, truncate, EntriesFrom, term lookup
│   │   └── state.go        # Raft FSM: Follower / Candidate / Leader transitions
│   ├── processor/
│   │   ├── engine.go       # Core per-partition processing loop
│   │   ├── window.go       # Tumbling and sliding WindowOperator
│   │   ├── aggregator.go   # COUNT, SUM, AVG, MIN, MAX aggregation functions
│   │   ├── state.go        # In-memory StateStore: (windowID, key) → WindowState
│   │   ├── checkpoint.go   # CheckpointCoordinator + BarrierAlignment
│   │   └── recovery.go     # State restore from checkpoint snapshot on restart
│   ├── speculative/
│   │   ├── detector.go         # StragglerDetector: 100ms median-based monitoring
│   │   └── manager_arbitrator.go # SpeculativeManager + ResultArbitrator (tombstone)
│   └── storage/
│       ├── partition.go    # Partition: segment orchestration, WAL integration, read/write
│       ├── segment.go      # Segment: fixed-size append-only log file
│       ├── index.go        # Sparse index: relativeOffset → bytePosition
│       └── wal.go          # WAL: CRC32-checksummed length-prefixed records, replay, truncate
├── pkg/
│   ├── config/             # Cluster config structs and defaults
│   ├── log/                # Structured logger with component/partition context
│   └── types/              # Shared types: Message, Offset, WindowID, AggregatedResult
├── proto/
│   ├── broker.proto        # Broker service: Produce, Fetch, Metadata, OffsetCommit/Fetch
│   ├── consumer.proto      # Consumer group join/heartbeat/leave RPCs
│   ├── processor.proto     # Processor control-plane RPCs
│   └── raft.proto          # Raft: RequestVote, AppendEntries, InstallSnapshot
├── benchmarks/             # Go benchmark suites (testing.B)
├── docker-compose.yml      # 3 brokers + primary processor + spare + loadgen + Prometheus + Grafana
├── Makefile
└── Dockerfile
```

---

## Benchmarks

All benchmarks run on `darwin/arm64` (Apple M-series, 8 cores), Go 1.22.2.

### Broker: Throughput & Failover

| Metric | Result |
|---|---|
| Message throughput | **21,000 msg/sec** |
| WAL write throughput | **68 MB/s** |
| Write latency P50 | 34.0 µs |
| Write latency P99 | 4,200 µs |
| Heartbeat failure detection | 300 ms (3 × 100ms timeout) |
| Raft leader election | **30 ms** |
| **Total end-to-end failover** | **< 346 ms** |
| Messages lost during failover | **0** |
| Replication factor | 3 nodes |

### Broker: Consumer Group Rebalance

| Metric | Result |
|---|---|
| Heartbeat liveness detection | 153 ms |
| Rebalance time (failure recovery) | 0.04 ms |
| Rebalance time (new consumer join) | 0.01 ms |
| Rebalance P50 over 100 iterations | 0.009 ms |
| Rebalance P99 over 100 iterations | 0.026 ms |
| Messages lost during rebalance | **0** |

### Processor: Accuracy & Checkpoint Recovery

| Metric | Result |
|---|---|
| Processing throughput | **9,000,000 ops/sec** |
| Operation latency | **111 ns/op** |
| Aggregation accuracy (COUNT/SUM/AVG/MIN/MAX) | **100.0%** |
| Checkpoint snapshot + disk write | 0.06 ms |
| Checkpoint recovery P50 | 0.032 ms |
| Checkpoint recovery P99 | **0.334 ms** |
| Delivery guarantee | exactly-once (Chandy-Lamport) |

### Speculative Execution: Straggler Mitigation

*Config: 4 partitions, 500 windows, 8% straggler injection probability, 2× median threshold.*

| Metric | Baseline | Speculative | Δ |
|---|---|---|---|
| Window latency P50 | 10,046.6 ms | 10,046.6 ms | 0.0% |
| Window latency P95 | 10,884.4 ms | 10,148.5 ms | **−6.8%** |
| Window latency P99 | 10,932.7 ms | 10,742.9 ms | **−1.7%** |

| Operational Metric | Value |
|---|---|
| Speculative tasks launched | 143 / 2,000 (7.1%) |
| Speculative win rate | **100%** (143/143) |
| Duplicate compute overhead | 7.1% |
| Per-partition straggler latency reduction | **~75%** (500–950ms → 120–150ms) |

> The window-level P99 improvement is bounded by windows with *multiple concurrent stragglers*. At the per-partition level, every detected straggler was fully eliminated — a strict 0% miss rate under the test load.

---

## Quickstart

### Prerequisites

- Go 1.22+
- Docker & Docker Compose
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (for proto regeneration only)

### Run the Full Cluster

```bash
# Clone
git clone https://github.com/sagar0x0/StreamForge.git
cd StreamForge

# Start 3 brokers, primary processor, spare processor, load generator,
# Prometheus, and Grafana
docker-compose up -d

# Follow processor logs
docker-compose logs -f processor-primary

# Watch broker replication
docker-compose logs -f broker-1
```

### Grafana Dashboard

Navigate to `http://localhost:3000` (credentials: `admin` / `admin`).

Metrics exposed include:
- `streamforge_messages_produced_total` — producer throughput
- `streamforge_partition_lag` — per-partition consumer lag
- `streamforge_speculative_launched_total` — cumulative speculative task launches
- `streamforge_checkpoint_duration_ms` — checkpoint snapshot latency histogram

### Run Benchmarks

```bash
# Broker storage benchmarks
go test ./internal/storage/... -bench=. -benchmem -count=3

# Processor + checkpoint benchmarks
go test ./internal/processor/... -bench=. -benchmem -count=3

# Speculative execution benchmarks
go test ./internal/speculative/... -bench=. -benchmem -count=3

# End-to-end load benchmark (custom harness)
go run ./cmd/benchmark -partitions=4 -windows=500 -straggler-prob=0.08
```

### Simulate Broker Failover

```bash
# Kill the leader broker mid-write
docker-compose stop broker-1

# Observe Raft election in broker-2 logs (~30ms election, <350ms total)
docker-compose logs -f broker-2

# Restore the failed broker
docker-compose start broker-1
# broker-1 rejoins ISR automatically after catching up
```

---

## Configuration

All cluster parameters are passed via environment variables to the Docker containers.

| Variable | Default | Description |
|---|---|---|
| `NODE_ID` | — | Unique broker node identifier |
| `PEERS` | — | Comma-separated `host:port` of all Raft peers |
| `BROKERS` | — | Comma-separated broker addresses for processor/producer |
| `GROUP_ID` | `stream-engine` | Consumer group identifier |
| `ENABLE_SPECULATION` | `false` | Enable speculative execution on this processor node |
| `SPECULATION_THRESHOLD` | `0.5` | Fraction of median progress below which a partition is a straggler |
| `CHECKPOINT_INTERVAL` | `5s` | Chandy-Lamport BARRIER injection frequency |
| `SEGMENT_MAX_BYTES` | `67108864` | Max segment file size (64 MiB) before rotation |
| `ISR_LAG_THRESHOLD` | `500ms` | Max follower lag before ISR eviction |

---

## Design Decisions & Trade-offs

### Why Raft from scratch instead of etcd?
Delegating leader election to etcd would hide the hard part. The educational and interview value of this project is the direct, from-paper implementation of the consensus protocol. The `internal/raft/` package includes full log compaction, snapshot install, and the fast log backtrack optimization from the extended Raft paper.

### Why WAL before segment append?
Without the WAL, a crash between `Append()` starting and `fsync()` completing would silently lose the write — no error, no recovery. The WAL with CRC32 gives crash atomicity: either the payload survived the CRC check on replay, or it is discarded and the producer retries. Same as PostgreSQL's `pg_wal`.

### Why first-write-wins over a coordinator for speculation?
A central coordinator for arbitration is a single point of failure and an additional round trip on the critical path. Tombstone-based local arbitration is O(1), contention-free under the common case (only 7.1% of partition-windows ever speculate), and safe because speculative tasks only start after a checkpoint boundary — their initial state is always identical.

### At-least-once vs. exactly-once
The broker delivers at-least-once (producer retries on timeout; consumer may reprocess on restart before the offset commit). Within the processor, Chandy-Lamport checkpointing + offset-anchored replay achieves exactly-once computation semantics. End-to-end exactly-once would require idempotent output sinks — explicitly out of scope and documented as such.

### Sparse index trade-off
Writing a full index entry per message would enable true O(1) reads but at 2× write amplification and proportionally larger index files. Indexing every 10th entry bounds the linear scan to ≤10 records while keeping index size at ~10% of the log — the same strategy Kafka uses in production.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go 1.22 |
| Inter-node RPC | gRPC + Protocol Buffers |
| Consensus | Custom Raft (from Ongaro & Ousterhout, 2014) |
| Log storage | Append-only segment files + sparse index (local disk) |
| Crash recovery | Write-Ahead Log with CRC32 checksums |
| Stream state | In-memory `map[WindowID]map[string]*WindowState` + disk checkpoint |
| Observability | Prometheus client + Grafana |
| Containerization | Docker Compose (3 brokers + 2 processors + loadgen + monitoring) |
| Testing | `testing.B` benchmark suites, integration tests per package |

---

<div align="center">

Built as a demonstration of distributed systems fundamentals: consensus, replication, fault tolerance, stateful stream processing, and tail-latency engineering.

</div>
