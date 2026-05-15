# StreamForge: Distributed Stream Processing Engine — Complete Summary

---

## What You're Building

Three systems that work together as one complete pipeline:

**Part 1 — Mini Kafka (The Broker)** A distributed message broker that durably stores event streams in append-only logs, replicates data across nodes for fault tolerance, and delivers messages to consumers with at-least-once guarantees.

**Part 2 — Mini Flink (The Stream Processor)** A stateful stream processing engine sitting on top of the broker, consuming events continuously and computing real-time aggregations over time windows with full fault recovery via distributed checkpointing.

**Part 3 — Speculative Execution Engine (The Novel Contribution)** A straggler detection and mitigation layer that monitors partition processing speeds in real time, identifies slow partitions, and speculatively launches duplicate tasks on spare nodes to reduce tail latency — a problem Kafka and Flink still don't solve cleanly.

Together they form a production-grade stream processing pipeline equivalent in architecture to what powers Twitter trending topics, Uber surge pricing, and Google Analytics in real time.

---

## The Full Architecture

```
Producers (load generators)
    ↓  [publish events via gRPC]
┌──────────────────────────────────────────┐
│             BROKER CLUSTER               │
│                                          │
│  Topic: "orders" (4 partitions)          │
│                                          │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  │
│  │Broker 1 │  │Broker 2 │  │Broker 3 │  │
│  │Leader:  │  │Leader:  │  │Follower │  │
│  │P0, P1   │  │P2, P3   │  │all parts│  │
│  │WAL      │  │WAL      │  │WAL      │  │
│  └─────────┘  └─────────┘  └─────────┘  │
│       ↕ Raft consensus + ISR replication │
└──────────────────────────────────────────┘
    ↓  [consume events via consumer groups]
┌──────────────────────────────────────────┐
│          STREAM PROCESSOR                │
│                                          │
│  ┌──────────────────────────────────┐    │
│  │     Straggler Detector           │    │
│  │  monitors partition latencies    │    │
│  │  triggers speculative execution  │    │
│  └──────────────────────────────────┘    │
│                                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│  │Worker 0  │ │Worker 1  │ │Worker 2  │ │
│  │Partition │ │Partition │ │Partition │ │
│  │0 primary │ │1 primary │ │2 primary │ │
│  └──────────┘ └──────────┘ └──────────┘ │
│                                          │
│  ┌──────────────────────────────────┐    │
│  │   Window Aggregator + Merger     │    │
│  │   Chandy-Lamport Checkpointing   │    │
│  └──────────────────────────────────┘    │
└──────────────────────────────────────────┘
    ↓
Output sink (results, metrics, alerts)
```

---

## Part 1 — Mini Kafka: Complete Breakdown

### The Append-Only Log

Everything in the broker revolves around one idea — the log. A partition is a file you only ever append to, never modify or overwrite. Each message gets a monotonically increasing offset number. Consumers track which offset they have read up to and advance independently.

```
Partition 0 log:
Offset 0: {city: "NYC", amount: 250.0, ts: 1000}
Offset 1: {city: "LA",  amount: 120.0, ts: 1001}
Offset 2: {city: "NYC", amount: 890.0, ts: 1002}
Offset 3: {city: "CHI", amount: 45.0,  ts: 1003}
Offset 4: {city: "LA",  amount: 310.0, ts: 1004}

Consumer A last committed offset: 2 → next fetch from 3
Consumer B last committed offset: 0 → next fetch from 1
Consumer C last committed offset: 4 → caught up, waiting
```

Multiple consumer groups can read the same log independently without interfering with each other. A slow consumer doesn't block a fast one. This decoupling is the fundamental insight behind the entire design.

### Segment Files

You never maintain one infinite log file. You split each partition log into fixed-size segments on disk:

```
partition-0/
├── 00000000000000000000.log    ← segment 1 (offsets 0-999)
├── 00000000000000000000.index  ← sparse index for fast lookup
├── 00000000000000001000.log    ← segment 2 (offsets 1000-1999)
├── 00000000000000001000.index
├── 00000000000000002000.log    ← active segment (append here)
└── 00000000000000002000.index
```

The index file maps offset → byte position in the log file for O(1) random access. Old segments past the retention policy (time-based or size-based) are deleted automatically. This is how you handle infinite streams on finite disk.

When a consumer requests offset 1500, you binary search the segment list to find the right file, then use the index to jump directly to the byte position. No linear scan.

### Partitioning

A topic is split into N partitions for parallelism. Producers choose partitions either round-robin for even distribution or by hashing a message key for ordering guarantees:

```
Key-based routing:
"NYC" → hash("NYC") % 4 = partition 2
"LA"  → hash("LA")  % 4 = partition 0
"CHI" → hash("CHI") % 4 = partition 1

All NYC events always land on partition 2.
Ordering per city is preserved within a partition.
This ordering guarantee matters deeply for stateful processing downstream.
```

Without key-based routing, events for the same city could end up on different partitions, making stateful aggregation per city impossible without cross-partition joins.

### Replication and ISR

Each partition has one leader broker and N-1 follower brokers. All producer writes go exclusively to the leader. Followers continuously fetch new messages from the leader and replicate them locally. The In-Sync Replica set (ISR) contains only followers that are caught up within a configurable lag threshold.

```
Producer write flow:
1. Producer sends message to Partition 0 leader (Broker 1)
2. Broker 1 writes to local log and WAL
3. Broker 2 (follower) fetches and replicates
4. Broker 3 (follower) fetches and replicates
5. Both followers send acknowledgment to leader
6. Leader: all ISR members confirmed → send ACK to producer ✓

If Broker 3 falls 500ms behind:
   Broker 3 removed from ISR
   Writes only need Broker 2 confirmation
   Broker 3 catches up → rejoins ISR automatically
```

This is your durability guarantee. A message acknowledged to the producer is guaranteed to survive any single broker failure because at least one other ISR member has it.

### Write-Ahead Log

Before writing anything to the partition log, you write it to a WAL first. If the broker crashes between the WAL write and the partition log write, on restart you replay the WAL to recover. This is the same pattern PostgreSQL, RocksDB, and every serious storage system uses.

```
Write sequence:
1. Append to WAL                    ← crash here: replay WAL on restart
2. Append to partition log          ← crash here: WAL has the data
3. Update in-memory index
4. Send ACK to follower/producer    ← crash here: producer retries, idempotent
```

### Raft Leader Election

When a partition leader broker crashes, a new leader must be elected from the ISR members. You implement Raft consensus directly from the original Ongaro paper.

Raft guarantees exactly one leader at a time, and that the new leader has all committed messages. The election process:

```
Broker 1 (Partition 0 leader) crashes
    ↓
Brokers 2 and 3 miss 3 consecutive heartbeats (~300ms total)
    ↓
Both increment term number, transition to Candidate state
    ↓
First to broadcast RequestVote wins (usually ~50ms)
    ↓
Broker 2 wins majority vote, becomes new leader
    ↓
Broker 2 begins accepting writes for Partition 0
    ↓
Producers and consumers redirect to Broker 2
    ↓
Total time from crash to resumed operation: ~500ms
```

Implement Raft with these components: leader election, log replication, log commitment, and membership changes. Do not use a library — implementing it yourself is the point. Read the paper, not blog posts.

### Consumer Group Coordination

Multiple consumers form a group to read a topic in parallel. The Group Coordinator (running on one of the brokers) manages membership and partition assignment:

```
Topic: 4 partitions
Consumer Group "processors": 2 active consumers

Initial assignment:
  Consumer C1: partitions [0, 1]
  Consumer C2: partitions [2, 3]

C1 misses 3 heartbeats → declared dead
  Rebalance triggered
  C2 reassigned: partitions [0, 1, 2, 3]
  Rebalance time: ~180ms
  Messages lost: 0

New consumer C3 joins:
  Rebalance triggered
  C2: partitions [0, 1]
  C3: partitions [2, 3]
  Rebalance time: ~180ms
  Messages lost: 0
```

Each consumer commits its offset back to the coordinator after processing. On restart, a consumer fetches its last committed offset and resumes from there — no reprocessing from the beginning.

---

## Part 2 — Mini Flink: Complete Breakdown

### Continuous Query Over Infinite Stream

A stream processor runs forever, continuously consuming events and updating results. Unlike a database query that runs once and returns, a stream processor never stops:

```
Stream query: every 10 seconds, COUNT orders per city

t=10s output:  {NYC: 423,  LA: 218, CHI: 97}
t=20s output:  {NYC: 891,  LA: 445, CHI: 203}
t=30s output:  {NYC: 1205, LA: 634, CHI: 291}
... runs forever
```

### Windowing

Windows define how you group infinite events into finite batches for computation. You implement two types:

**Tumbling Window — fixed size, no overlap:**

```
[0s ────────── 10s][10s ────────── 20s][20s ────────── 30s]
     window 1            window 2            window 3

Each event belongs to exactly one window.
```

**Sliding Window — fixed size, overlapping:**

```
[0s ──── 10s]
      [5s ──── 15s]
            [10s ──── 20s]

Each event may belong to multiple windows.
More expensive — window_size / slide_interval times the work.
```

Start with tumbling. Sliding is a meaningful extension that shows you understand the tradeoff.

### Stateful Operators

Each operator maintains per-key per-window state in memory:

```go
type WindowState struct {
    Key        string     // "NYC"
    WindowID   int64      // which 10s window bucket
    Count      int64      // running count of events
    Sum        float64    // running sum of amounts
    Min        float64
    Max        float64
    LastUpdate time.Time
}

// In-memory state store
// map[windowID][key] → WindowState
type StateStore map[int64]map[string]*WindowState
```

When an event arrives for city "NYC" in window 3, you look up its state, increment count, add to sum, update the store. When the window closes, you emit the final aggregated result and clear that window's state.

### Chandy-Lamport Distributed Checkpointing

If the stream processor crashes mid-window, you cannot lose all accumulated state. Chandy-Lamport distributed snapshots solve this:

```
Checkpoint coordinator periodically injects BARRIER markers
into the event stream between normal events:

Partition 0 stream: [e1][e2][BARRIER-7][e3][e4][e5]
Partition 1 stream: [e6][e7][BARRIER-7][e8][e9]
Partition 2 stream: [e4][BARRIER-7][e5][e6][e7]

When a worker sees BARRIER-7:
  1. Finish processing all events before the barrier
  2. Snapshot current WindowState to disk
  3. Send BARRIER-7 downstream
  4. Continue processing events after barrier normally

When all workers have snapshotted:
  Checkpoint 7 is complete and durable

On crash and restart:
  Load last complete checkpoint (e.g., checkpoint 6)
  Re-consume only events after checkpoint 6's barrier offset
  State is exactly where it was at checkpoint 6
  No duplicates, no data loss
```

The subtle hard part: barriers from multiple partitions arrive at different times. A worker must pause processing from partitions where the barrier has already arrived while waiting for the barrier on other partitions. This barrier alignment ensures a consistent global snapshot.

### At-Least-Once vs Exactly-Once

At-least-once: events may be processed more than once after failure. Simpler to implement. Acceptable if your aggregation is idempotent.

Exactly-once: every event processed exactly once end-to-end. Requires checkpointing plus idempotent output writes. Much harder.

You implement at-least-once as the base. With Chandy-Lamport checkpointing added, you get exactly-once semantics within the processor. Be explicit about this distinction in your README — showing you understand the tradeoff is as impressive as implementing it.

---

## Part 3 — Speculative Execution: The Novel Contribution

### The Problem Nobody Solves Well

In any distributed window aggregation, one slow partition consistently delays the entire window result. This is called the straggler problem. If your window has 4 partitions and partition 2 takes 3x longer than the others, every window result is delayed waiting for it — regardless of how fast the other 3 are.

Flink has straggler detection but no automatic mitigation. Kafka Streams has nothing. This is a genuine open problem in production stream processing systems.

```
Window closes at t=10s

Partition 0 worker: finishes at t=10.08s  ✓ fast
Partition 1 worker: finishes at t=10.11s  ✓ fast
Partition 2 worker: finishes at t=10.09s  ✓ fast
Partition 3 worker: still running at t=10.9s  ← STRAGGLER

Everyone waits for partition 3.
Window result finally emits at t=10.95s
P99 latency dominated by one slow worker every time.
```

Causes of stragglers: garbage collection pauses, CPU contention, disk I/O spikes, network jitter, skewed key distribution causing one partition to have far more events.

### Your Solution — Speculative Duplicate Execution

When a partition worker is detected as a straggler, you launch an identical duplicate task on a spare node using the last checkpoint state as its starting point. Whichever finishes first — original or duplicate — provides the result. The other is cancelled.

```
Straggler Detection:
  Every 100ms, measure each partition's processing progress
  Compute median progress across all partitions
  If partition_progress < median * threshold (e.g., 0.5x):
      Mark as straggler → trigger speculative task

Speculative Execution Flow:
  Partition 3 detected as straggler at t=10.5s
      ↓
  Load last checkpoint state for partition 3
      ↓
  Launch duplicate worker on Spare Node A
  Duplicate starts from checkpoint, re-consumes events
      ↓
  Original worker finishes at t=10.95s
  Duplicate worker finishes at t=10.87s  ← faster this time
      ↓
  Duplicate result accepted
  Original task cancelled and cleaned up
      ↓
  Window emits at t=10.87s instead of t=10.95s
```

### The Hard Problem — State Correctness

The non-obvious difficulty: you cannot just clone a running stateful worker. If the original worker has partially updated state that is not checkpointed, the duplicate starting from the last checkpoint will diverge.

Your solution:

```
Rule 1: Only speculate after a checkpoint boundary
  Never launch a speculative task mid-checkpoint-interval
  Always start the duplicate from a clean checkpoint snapshot
  This guarantees both tasks start from identical state

Rule 2: First-write-wins with tombstoning
  Whichever task completes first writes its result
  It also writes a tombstone for this (windowID, partitionID)
  The slower task checks for tombstone before writing
  If tombstone exists: discard result, clean up silently

Rule 3: Conservative speculation threshold
  Only speculate when lag exceeds 2x median
  Not 1.5x — avoids unnecessary duplication under normal jitter
  Configurable per deployment
```

### Straggler Detector Implementation

```go
type StragglerDetector struct {
    workers         map[int]*WorkerStats
    threshold       float64        // 0.5 = 50% of median
    checkInterval   time.Duration  // 100ms
    speculativeMgr  *SpeculativeManager
    mu              sync.RWMutex
}

type WorkerStats struct {
    PartitionID      int
    EventsProcessed  int64
    WindowProgress   float64    // 0.0 to 1.0
    LastCheckpoint   time.Time
    IsSpeculative    bool
}

func (d *StragglerDetector) monitor() {
    ticker := time.NewTicker(d.checkInterval)
    for range ticker.C {
        median := d.computeMedianProgress()
        for id, worker := range d.workers {
            if worker.WindowProgress < median * d.threshold {
                if !worker.IsSpeculative {
                    d.speculativeMgr.launch(id, worker.LastCheckpoint)
                }
            }
        }
    }
}
```

### What This Gives You on Benchmarks

```
Without speculative execution:
  P50 window latency: 10.12s
  P95 window latency: 10.68s
  P99 window latency: 10.94s  ← dominated by stragglers

With speculative execution (threshold=2x):
  P50 window latency: 10.11s  (unchanged — most windows fine)
  P95 window latency: 10.31s  (37% reduction)
  P99 window latency: 10.64s  (39% reduction)

Speculative task launch rate: 3.2% of all partition-windows
Duplicate wasted work: 3.2% overhead (acceptable)
Net benefit: 39% P99 latency reduction for 3.2% extra compute
```

This is a clean engineering tradeoff story — exactly what Google interviewers love.

---

## Tech Stack

```
Language:          Go throughout
Transport:         gRPC/Protobuf for all inter-node communication
Consensus:         Raft — implemented from scratch
Log storage:       Append-only segment files on local disk
WAL:               Sequential write file per broker
State store:       In-memory hashmap + periodic disk checkpoint
Metrics:           Prometheus client library
Visualization:     Grafana dashboard
Containerization:  Docker Compose for local cluster simulation
Benchmarking:      Custom Go load generator
```

---

## What You Implement — Scoped Honestly

### Broker

- Append-only partitioned log with segment files and sparse index
- Producer with hash-key and round-robin partition routing
- In-Sync Replica tracking and follower lag monitoring
- Raft leader election on broker failure
- Consumer group coordination with heartbeat and rebalance
- Write-ahead log for crash recovery
- Offset commit and fetch per consumer group
- Segment retention and deletion policy

### Stream Processor

- Tumbling and sliding window operators
- Stateful per-key aggregation: COUNT, SUM, AVG, MIN, MAX
- Chandy-Lamport barrier-based distributed checkpointing
- At-least-once delivery with offset commit after processing
- Barrier alignment across multiple partitions
- State recovery from last complete checkpoint on restart

### Speculative Execution Engine

- Real-time per-partition progress monitoring every 100ms
- Median-based straggler detection with configurable threshold
- Checkpoint-anchored speculative task launch on spare nodes
- First-write-wins result arbitration with tombstone protocol
- Automatic cancellation of slower duplicate tasks
- Metrics: speculation rate, duplicate wasted work, P99 improvement

### Skip For Now

- Cross-partition joins
- Out-of-order event handling and watermarks
- Schema registry
- SSL/TLS encryption
- Multi-datacenter replication