# System Design: Real-Time Sliding Window Packet Aggregator

**Domain:** Streaming Data · Telemetry Pipeline  
**Difficulty:** Medium–Hard  
**Topics:** Ring-buffer, Windowing, Late-data handling, Horizontal scaling

---

## Problem Statement

Design a service that ingests a continuous stream of timestamped packets of the form `{"Val": 5}` and answers, in real time, the following query:

> **"What is the average value of all packets received in the last 60 seconds, excluding packets from the most recent 1 second?"**

The effective aggregation window is therefore **[T − 60s, T − 1s)**, where T is the current wall-clock time at query time.

---

## Functional Requirements (FR)

| # | Requirement |
|---|-------------|
| FR1 | Accept a real-time stream of packets `{"Val": number}` via an ingest API (HTTP or gRPC). |
| FR2 | Attach a **server-side ingestion timestamp** to each packet on arrival. |
| FR3 | Expose a **query endpoint** that returns the rolling average over `[T − 60s, T − 1s)` at the moment of the call. |
| FR4 | **Exclude** all packets arriving in the most recent 1-second "cooldown" zone to prevent partial-second skew. |
| FR5 | **Automatically evict** packets older than 60 seconds from aggregation state. |
| FR6 | Handle **out-of-order and late-arriving packets** within a configurable late-arrival tolerance window. |

---

## Non-Functional Requirements (NFR)

| # | Dimension | Target |
|---|-----------|--------|
| NFR1 | **Latency** | Query response ≤ 10 ms at p99 under sustained ingest load |
| NFR2 | **Throughput** | ≥ 100,000 events/sec per node without packet loss or back-pressure |
| NFR3 | **Memory** | Footprint must be O(W/B) where W = window size, B = bucket granularity — not O(events) |
| NFR4 | **Accuracy** | Average must be exact (not approximate) for all packets within the canonical window |
| NFR5 | **Fault tolerance** | Single-node failure must not cause data loss; replicated state or WAL required |
| NFR6 | **Scalability** | Ingest stream must be horizontally partitionable; aggregation must merge correctly across shards |

---

## Capacity Assumptions

| Parameter | Value |
|-----------|-------|
| Peak ingest rate | 100,000 packets/sec |
| Outer window size | 60 seconds |
| Leading exclusion zone | 1 second |
| Ring-buffer buckets (1 s each) | ~60 |
| Target query latency (p99) | < 10 ms |
| Memory complexity per shard | O(W/B) |

---

## Core Design Areas

### 1. Ring-buffer / time-bucket design

Maintain **60 one-second buckets** in a circular array. Each bucket stores a running `(sum, count)` pair rather than individual events.

- On **ingest**: compute `bucket_index = ingestion_timestamp % 60` and add to that bucket's `sum` and `count`.
- On **query**: skip the latest bucket (the 1-second exclusion zone), then sum the `(sum, count)` across the remaining 59 buckets and divide to get the average.
- On **eviction**: lazily zero out a bucket when its slot is overwritten by a new time-period.

```
Ring buffer (60 slots, each = 1 second)
┌──────┬──────┬──────┬─────┬──────┬──────┐
│ t-60 │ t-59 │ t-58 │ ... │ t-1  │  t   │
│(s,c) │(s,c) │(s,c) │ ... │(s,c) │(s,c) │  ← excluded
└──────┴──────┴──────┴─────┴──────┴──────┘
 ←────────── aggregate these 59 ────────→
```

### 2. Window boundary management

| Strategy | Pros | Cons |
|----------|------|------|
| **Lazy eviction** (skip stale on read) | No background threads; simple | May accumulate stale state between reads |
| **Time-triggered sweep** (background goroutine) | State always clean | Adds concurrency complexity |

Recommended: lazy eviction with a monotonic pointer for most low-write-rate cases; background sweep when write rate is very high.

### 3. Late-data handling

Define a **maximum lateness threshold** (e.g., 2 seconds):

- Packets within the threshold → placed in the correct historical bucket.
- Packets beyond the threshold → dropped or routed to a dead-letter queue for auditing.
- The 1-second exclusion zone effectively provides a built-in buffer against mild clock drift.

### 4. Horizontal partitioning

- Shard the ingest stream by packet source key (e.g., device ID, sensor ID).
- Each shard maintains its own ring-buffer aggregator.
- At query time, a **query-aggregation layer** collects `(sum, count)` from all shards and computes:

```
global_average = Σ(shard_sum) / Σ(shard_count)
```

### 5. Clock skew & NTP drift

- Use **server-assigned ingestion timestamps** rather than client-supplied timestamps to eliminate client clock skew.
- Use a **monotonic clock** for advancing the ring-buffer write pointer; use wall-clock only for bucket labeling.
- Monitor the delta between client timestamp and server timestamp to detect misbehaving producers.

### 6. Persistence & recovery

- Checkpoint the full ring-buffer state to a WAL or fast KV store (e.g., Redis Sorted Sets by timestamp).
- On restart: replay from the latest checkpoint and discard buckets whose time range has since expired.
- For in-memory deployments: replicate state to a hot standby; a leader-election mechanism (e.g., via ZooKeeper or etcd) promotes the standby on primary failure.

---

## What This Problem Tests

- **Windowing** — understanding sliding vs. tumbling windows and the semantics of open/closed interval boundaries.
- **Late-data handling** — defining watermarks and late-arrival policies in a streaming context.
- **Ring-buffer / time-bucket design** — the conceptual core of any telemetry or metrics pipeline.
- **Memory-bounded aggregation** — reasoning about O(events) vs. O(W/B) space complexity.
- **Distributed aggregation** — correctly merging partial aggregates (sum, count) across shards, which generalizes to any commutative, associative aggregation function.

---

*This problem maps directly to the design of systems like Prometheus (scrape windows), Datadog metrics, and any real-time anomaly-detection pipeline.*