# Real-Time View Counter — Hotstar-Scale System Design

Concurrent-viewer counting at tens of millions of simultaneous users.

---

## Functional Requirements

### FR-1 — Live viewer count
Display the number of concurrent viewers watching a stream, updating in near-real-time (≤ 5 s lag). Count must reflect active sessions, not just opens.

### FR-2 — View event ingestion
Accept a high-throughput stream of heartbeat events (join, keep-alive, leave) from every client session. Each event carries: stream ID, session ID, timestamp, client region.

### FR-3 — Aggregation windows
Support tumbling (1 min, 5 min) and sliding windows for aggregation. Expose current-window count and a rolling 60-minute trend timeline per stream.

### FR-4 — Per-stream and global counts
Counts must be queryable at two granularities: per individual stream (e.g. IPL Match 42) and platform-wide aggregate across all live streams simultaneously.

### FR-5 — Deduplication of sessions
A single user watching on multiple tabs or devices must be counted once per stream. Session-level dedup via session ID; user-level dedup is a best-effort approximation.

### FR-6 — Spike-resilient ingestion
During sudden traffic surges (e.g. a six-hit in a cricket match), the ingest pipeline must absorb spikes without dropping events or stalling downstream aggregators.

### FR-7 — Count read API
Expose a low-latency read endpoint (REST or WebSocket push) for product surfaces: scoreboard overlay, admin dashboard, CDN cache population.

### FR-8 — Historical playback counts
After a stream ends, persist per-minute aggregates so that post-match analytics can reconstruct the viewership curve and identify peak-viewer moments.

---

## Non-Functional Requirements

### NFR-1 — Scale: peak concurrency
Must support ≥ 50 million concurrent viewers across all streams, with individual high-profile streams reaching 25 M+ simultaneously (IPL finals baseline).

- 25 M+ per single stream
- 50 M platform-wide

### NFR-2 — Throughput: ingest rate
Heartbeat events arrive at ~1 event / 30 s per session. At 50 M sessions this is ≈ 1.67 M events/s steady-state, with 5–10× bursts during key moments.

- ~1.7 M events/s baseline
- 10–15 M events/s at spike

### NFR-3 — Read latency
P99 read latency for the count API must be ≤ 50 ms. Viewers see slightly stale counts (eventual consistency is acceptable); staleness window ≤ 5 s.

- p99 ≤ 50 ms
- Staleness ≤ 5 s

### NFR-4 — Accuracy: approximate is acceptable
Exact counts at 50 M scale are prohibitively expensive. Approximate counting (HyperLogLog, ±1–2% error) is acceptable for display. Exact counts required for billing/audit logs only.

- HyperLogLog ±2% for display
- Exact counts for audit only

### NFR-5 — Availability
The counter service must maintain 99.99% availability during live events. A stale count displayed is vastly preferable to an error. Design for graceful degradation over failure.

- 99.99% SLA during live events
- Stale count preferred over 5XX

### NFR-6 — Hot-key handling
A single viral stream is a hot partition. The system must prevent a single stream ID from bottlenecking one shard. Use write-sharding, local aggregation, and read fanout to distribute load.

- Hot partition risk on popular streams
- Shard fan-out required at read time

### NFR-7 — Write amplification control
Aggregation must not create O(n) downstream writes per ingest event. Use local buffering (edge counters) + periodic flush to avoid write amplification at 10 M events/s.

- Buffer and batch flush pattern
- No O(n) fan-out per event

### NFR-8 — Durability and recovery
Ingest events must be durable (Kafka replication factor ≥ 3). In the event of an aggregator crash, replay from the last committed offset. Lose at most 30 s of aggregation state on failure.

- Kafka RF ≥ 3
- Replay from last committed offset
- ≤ 30 s aggregation state loss on failure

---

## Key Design Tensions

### Exact counting vs. Approximate (HyperLogLog)

| | Exact counting | HyperLogLog |
|---|---|---|
| Accuracy | Perfect | ±1–2% error |
| Memory | Grows with cardinality | Fixed |
| Mergeability | Complex | Mergeable across shards |
| Feasibility at 25 M sessions | No | Yes |

### Per-event write vs. Local buffer + batch flush

| | Per-event write to DB | Local buffer + batch flush |
|---|---|---|
| Precision | Real-time | Slightly stale |
| Spike resilience | Bottlenecks under load | Absorbs 10× spikes gracefully |
| Hot-key risk | High (stream_id) | Mitigated |
| Write pattern | O(n) per event | Flush every 1–5 s to Redis |

---

## Spike Scenario — Unpredictable Traffic Surge

**Trigger:** A six-hit or wicket in an IPL match — within 2–3 seconds, 5–10 M additional heartbeat events arrive simultaneously across the ingest tier.

**Challenges created:**
- Kafka partition saturation if stream_id maps to one partition
- Aggregator lag spikes — consumers fall behind the head of the log
- Redis counter hot-key — all writes for one match_id land on one slot
- Thundering herd on the read API from front-end polling

**Constraints this imposes on the design:**
- Kafka keyed by `stream_id + shard_suffix` (virtual sharding)
- Aggregators must have auto-scaling triggers on consumer-lag metrics
- Redis counter must be sharded (N keys per stream, summed at read time)
- Read results cached at edge (CDN or API gateway) with a 2–3 s TTL to suppress thundering herd