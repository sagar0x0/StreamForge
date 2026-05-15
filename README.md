# StreamForge: Distributed Stream Processing Engine

StreamForge is a production-grade distributed stream processing pipeline integrating three tightly-coupled components:
1. **Mini Kafka**: An append-only partition log tracking messages with Raft coordination for Fault Tolerance.
2. **Mini Flink**: A continuous stream query processor featuring stateful accumulation (COUNT, SUM) anchored behind Chandy-Lamport snapshot checkpoints.
3. **Speculative Execution**: The engine dynamically detects partition stragglers tracking latencies below median margins via `internal/speculative/detector.go` and launches overlapping task replicas via a First-Write-Wins tombstone metric logic to dramatically reduce P99 window outputs.

## Architecture Highlights
- Uses explicit generic Raft state configurations built fully from scratch tracking candidates and mismatch back-tracks.
- O(1) fetch times off memory-mapped Segment structures utilizing Sparse Indices mirroring Kafka operations natively.
- Full At-Least-Once Delivery utilizing check-point barrier alignment blocks allowing state to map deterministically onto a timeline avoiding data-loss during cluster node crashes.

## Quickstart

```bash
# Bring up 3x brokers, 2x processors (1 primary + 1 spare for speculative launch)
docker-compose up -d

# Verify metrics on Grafana
localhost:3000
```
