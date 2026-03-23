# Behavioral Analytics Engine: Architecting Funnel and Path Analysis for Clickstream Data

----- 

## Orignal Problem Statement

In the modern digital economy, understanding the "how" and "why" of user journeys is more critical than simple metric aggregation. For a Staff or Principal Engineer, the challenge is designing a system that can reconstruct billions of temporal event sequences into coherent funnels and exploratory paths in near real-time. This requires navigating the trade-offs between the flexibility of raw event analysis and the performance constraints of high-cardinality, petabyte-scale datasets.

This problem statement focuses on building a scalable behavioral analytics platform that enables product teams to define complex event sequences and discover unexpected user flows.

## The Core Architectural Challenge: Temporal Sequence Matching at Scale
The fundamental technical hurdle in behavioral analytics is the Temporal Join Problem. Unlike standard OLAP queries that aggregate independent rows, funnel and path analysis require joining sequences of events belonging to the same user over time. At a Principal level, the system must move away from rigid pre-aggregation—which fails when a user wants to analyze a new, ad-hoc sequence—toward a Raw Event Query Model. This requires specialized indexing, user-centric data partitioning, and efficient state management to handle out-of-order events and high-cardinality identity resolution.

### High-Level Requirements

| **Requirement Type** | **Description** |
| --- | --- |
| **Functional** | Ad-hoc Funnel Builder: Support N-step funnels with "Specific Order" (Step A before B) or "Any Order" logic. |
| **Functional** | Conversion Controls: Enable user-defined conversion windows (e.g., 24h, 30 days) and exclusion steps that disqualify users. |
| **Functional** | Path Discovery: Automated discovery of frequent event sequences (Top-N flows) using Sequential Pattern Mining. |
| **Functional** | Serving & Visualization: High-performance API for rendering Sankey/Sunburst diagrams and conversion trend lines. |
| **Non-Functional** | Latency (Batch): P95 query response < 3 seconds for complex funnels over 30 days of data . |
| **Non-Functional** | Scalability: Horizontal scaling for ingestion (100k+ events/sec) and storage (petabytes) . |
| **Non-Functional** | Freshness: Real-time funnel availability within 1-5 minutes of event occurrence (optional requirement). |
| **Non-Functional** | User-Centric Isolation: Multi-tenant resource isolation to prevent "noisy neighbor" queries from impacting cluster stability. |

### Nuanced Considerations for Staff and Principal Engineers

1. The Behavioral OLAP Engine: Pinot vs. ClickHouse
A Principal-level design must evaluate the storage tier based on the query patterns. Apache Pinot is often superior for high-QPS, user-facing analytics via Star-Tree indexing. ClickHouse excels at deep scans and complex analytical joins through vectorized execution and high-efficiency columnar compression. The design should leverage specialized functions like windowFunnel to avoid expensive query-time self-joins.

2. Identity Resolution and "Profile Stitching"
One of the most complex Staff-level challenges is Identity Aliasing. A user may interact anonymously on mobile and later login on web. The system must "stitch" these timelines at query-time without rewriting historical immutable data. This involves maintaining an Identity Graph (e.g., in DynamoDB or FoundationDB) and performing a query-time mapping to merge event streams from multiple aliases.

3. Solving the "Small File Problem" in Lakehouses
Streaming clickstream data into a Lakehouse (Iceberg/Delta) often creates thousands of tiny files, which increases metadata overhead and query latency . A sophisticated design should implement Dynamic Time-Range Files and an asynchronous Compaction Service to merge small fragments into optimal sizes (256-512 MB) based on server upload time rather than just event time .

4. Sequential Pattern Mining for Path Analysis
For path discovery, standard SQL is insufficient. The architecture should support algorithms like PrefixSpan (Prefix-Projected Sequential Pattern Mining). This allows the system to recursively project sequence databases into smaller subsets to identify frequent paths without the exponential "candidate generation" bottleneck of older algorithms .

5. Probabilistic Data Structures for Interactive Exploration
To achieve sub-second latencies on high-cardinality cohorts, the system should utilize Probabilistic Data Structures. HyperLogLog (HLL) should be used for rapid unique visitor estimates, while Roaring Bitmaps or Theta Sketches are required for precise set operations (unions/intersections) during cohort-based funnel analysis .

6. Real-Time Stateful Processing (Additional Requirement)
To support real-time funnels, the design must incorporate a stateful stream processor like Apache Flink. Using the KeyedProcessFunction, the system can partition streams by user_id and maintain a local, fault-tolerant state for each user's current funnel progress, triggering updates the moment a completion event arrives.

-----

# Behavioral Analytics Engine: Funnel & Path Analysis — Architectural Design

---

## Phase 1: Scoping & Requirements

### Problem Restatement

Design a scalable behavioral analytics platform that ingests high-volume clickstream events (100k+ events/sec), stores them in a user-centric, petabyte-scale lakehouse, and serves ad-hoc N-step funnel queries and exploratory path analysis in near real-time (<3s P95). The core challenge is the **Temporal Join Problem**: reconstructing ordered event sequences per user without pre-aggregating into rigid schemas, while handling identity aliasing, out-of-order events, and multi-tenant query isolation.

---

### Functional Requirements

| # | Requirement |
|---|---|
| FR-1 | Ad-hoc N-step funnel builder: "Specific Order" (A → B → C) and "Any Order" modes |
| FR-2 | Conversion window controls (e.g., 24h, 7d, 30d) and exclusion steps |
| FR-3 | Path discovery: Top-N frequent event sequences via Sequential Pattern Mining |
| FR-4 | High-performance API for Sankey/Sunburst diagrams and conversion trend lines |
| FR-5 | Identity stitching: merge anonymous + authenticated timelines at query time |
| FR-6 | Real-time funnel availability within 1–5 minutes of event occurrence (optional) |

### Non-Functional Requirements

| Dimension | Target |
|---|---|
| Latency (batch) | P95 < 3s for complex funnels over 30 days |
| Latency (real-time) | Funnel state updated within 1–5 min of event |
| Throughput (ingestion) | 100k+ events/sec sustained |
| Storage | Petabyte-scale, horizontal |
| Availability | 99.99% for query API |
| Consistency | Eventual (AP system; funnel counts may lag by minutes) |
| Multi-tenancy | Resource isolation — noisy neighbor prevention |

---

## Phase 2: Back-of-Envelope Estimates

**Assumptions:**
- 100k events/sec ingestion peak; ~50k avg
- Average event payload: 500 bytes (compressed ~200 bytes)
- 30-day query window is the most common; 365-day max
- 10k distinct event types; 500M MAU

**Storage:**
```
50k events/sec × 86400 sec/day × 200 bytes = ~864 GB/day (compressed)
~315 TB/year raw compressed
With 2× replication in lakehouse → ~630 TB/year
```

**QPS (Query API):**
```
Assume 5,000 product analysts; avg 10 funnel queries/hour during business hours
= ~14 QPS sustained, ~100 QPS peak
Each query scans ~30 days × 864 GB/day = 25 TB logical scan
→ Must rely on partitioning + columnar pruning to scan <1% of data per query
```

**Kafka throughput:**
```
100k events/sec × 500 bytes = 50 MB/sec → ~180 GB/hr
32 partitions × ~1.5 MB/sec/partition → comfortable headroom
```

---

## Phase 3: High-Level Architecture

### Core Components

| Component | Technology | Role |
|---|---|---|
| Event Ingestion API | Go / Envoy | Stateless HTTP/gRPC endpoint; validates, batches, publishes |
| Message Bus | Apache Kafka | Durable, replayable event stream; partitioned by `user_id` |
| Stream Processor | Apache Flink | Real-time stateful funnel tracking; computes 1–5 min funnel state |
| Lakehouse Writer | Flink → Apache Iceberg on S3 | Micro-batch writes; handles compaction, partitioning |
| Compaction Service | Spark (scheduled) | Merges small files; rewrites into 256–512 MB optimal Parquet files |
| OLAP Engine | ClickHouse (primary) + Apache Pinot (optional) | Batch funnel queries; `windowFunnel` function; columnar scans |
| Identity Graph | Redis (hot) + PostgreSQL (cold) | Maps anonymous IDs → canonical user IDs |
| Query API | Python / FastAPI | Translates funnel DSL → SQL; fans out to ClickHouse; caches results |
| Result Cache | Redis | Caches funnel result sets by query fingerprint (TTL 5 min) |
| Path Mining Service | Spark (PrefixSpan) | Offline sequential pattern mining; writes top-N paths to ClickHouse |
| Metadata Store | PostgreSQL | Funnel definitions, saved queries, tenant configs |
| Observability | Prometheus + Grafana + OpenTelemetry | Golden signals across all services |

---

### Data Flow: Happy Path (Batch Funnel Query)

```
1. User defines funnel: [page_view → add_to_cart → purchase] within 7 days
2. Query API receives request → resolves tenant, validates DSL
3. Identity resolution: fetch canonical_user_id mappings from Redis/Postgres
4. Query API generates ClickHouse SQL using windowFunnel() with conversion window
5. ClickHouse scans Iceberg-backed MergeTree table, pruned by (date, tenant_id) partition
6. ClickHouse returns per-step counts + drop-off rates
7. Query API enriches with cohort metadata, caches result in Redis (TTL 5 min)
8. Response returned to client → rendered as Sankey diagram
```

### Data Flow: Real-Time Funnel (Flink Path)

```
1. Event arrives at Ingestion API → published to Kafka topic `events` (key=user_id)
2. Flink KeyedProcessFunction consumes, keyed by user_id
3. For each active funnel definition (broadcast state), Flink checks if event advances user's funnel state
4. On completion or timeout, Flink emits a FunnelCompletionEvent to Kafka topic `funnel_results`
5. A sink consumer writes aggregated real-time counts to ClickHouse `funnel_realtime` table
6. Query API merges batch + real-time counts for freshness within 1–5 min
```

---

### Architecture Diagram

```mermaid
flowchart TD
    subgraph Ingestion
        SDK[Client SDKs\nWeb / Mobile / Server]
        API[Ingestion API\nGo + Envoy]
        KAFKA[Apache Kafka\n32 partitions, keyed by user_id]
    end

    subgraph Stream Processing
        FLINK[Apache Flink\nKeyedProcessFunction\nStateful Funnel Tracker]
        FLINK_SINK[Kafka: funnel_results]
    end

    subgraph Lakehouse
        ICEBERG[Apache Iceberg on S3\nPartitioned: tenant_id / date / hour]
        COMPACT[Compaction Service\nSpark — 256-512 MB files]
    end

    subgraph OLAP
        CH[ClickHouse Cluster\nMergeTree + windowFunnel\nReplicated, Sharded by tenant_id]
        PINOT[Apache Pinot\nOptional: Star-Tree Index\nHigh-QPS user-facing]
    end

    subgraph Identity
        REDIS_ID[Redis\nHot ID Mappings]
        PG_ID[PostgreSQL\nCanonical Identity Graph]
    end

    subgraph Query Layer
        QAPI[Query API\nFastAPI]
        RCACHE[Redis\nResult Cache TTL 5m]
        PATHSVC[Path Mining Service\nSpark PrefixSpan]
    end

    subgraph Observability
        PROM[Prometheus]
        GRAF[Grafana]
    end

    SDK --> API
    API --> KAFKA
    KAFKA --> FLINK
    KAFKA --> ICEBERG
    FLINK --> FLINK_SINK
    FLINK_SINK --> CH
    ICEBERG --> COMPACT
    COMPACT --> ICEBERG
    ICEBERG --> CH
    ICEBERG --> PATHSVC
    PATHSVC --> CH
    QAPI --> REDIS_ID
    REDIS_ID --> PG_ID
    QAPI --> CH
    QAPI --> RCACHE
    QAPI --> PINOT
```

---

## Phase 4: Deep Dive — Data Model & Storage

### 4.1 Raw Event Schema (Iceberg + ClickHouse)

```sql
-- Iceberg / ClickHouse MergeTree table
CREATE TABLE events (
    event_id        UUID,
    tenant_id       String,           -- partition key (multi-tenant)
    canonical_uid   String,           -- resolved user ID post-identity-stitch
    anonymous_id    String,           -- original device/session ID
    event_name      LowCardinality(String),  -- e.g. 'page_view', 'add_to_cart'
    event_time      DateTime64(3),    -- millisecond precision
    server_time     DateTime64(3),    -- for out-of-order detection
    session_id      String,
    properties      String,           -- JSON blob (sparse attributes)
    platform        LowCardinality(String),  -- web/ios/android
    geo_country     LowCardinality(String),
    date            Date MATERIALIZED toDate(event_time)  -- partition pruning
)
ENGINE = MergeTree()
PARTITION BY (tenant_id, toYYYYMMDD(date))
ORDER BY (tenant_id, canonical_uid, event_time)   -- sort key = temporal join key
SETTINGS index_granularity = 8192;
```

**Why `ORDER BY (tenant_id, canonical_uid, event_time)`**: ClickHouse's MergeTree physically co-locates all events for a user within a partition. The `windowFunnel` function then scans a contiguous block per user — no shuffle, no self-join.

### 4.2 Identity Graph Schema

```sql
-- PostgreSQL (cold store)
CREATE TABLE identity_aliases (
    anonymous_id    TEXT NOT NULL,
    canonical_uid   TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    first_seen      TIMESTAMPTZ,
    last_seen       TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, anonymous_id)
);
CREATE INDEX ON identity_aliases (tenant_id, canonical_uid);
```

Redis hot layer: `HSET tenant:{tenant_id}:alias:{anonymous_id} canonical_uid {uid}` — O(1) lookup, TTL 24h.

### 4.3 Funnel Definition Schema (PostgreSQL)

```sql
CREATE TABLE funnel_definitions (
    funnel_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       TEXT NOT NULL,
    name            TEXT,
    steps           JSONB NOT NULL,   -- [{event_name, filters}, ...]
    order_mode      TEXT CHECK (order_mode IN ('strict', 'any')),
    conversion_window_seconds BIGINT,
    exclusion_steps JSONB,
    created_by      TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);
```

### 4.4 Partitioning Strategy

| Layer | Partition Key | Sort/Cluster Key | Rationale |
|---|---|---|---|
| Iceberg (S3) | `tenant_id / date / hour` | — | Prune by tenant + time range; hour-level for compaction granularity |
| ClickHouse | `(tenant_id, toYYYYMMDD(event_time))` | `(tenant_id, canonical_uid, event_time)` | Co-locate user events; enable `windowFunnel` without shuffle |
| Kafka | `user_id` hash | — | Ordered delivery per user to Flink; avoids cross-partition state |

### 4.5 Hot / Warm / Cold Storage

| Tier | Storage | Retention | Access Pattern |
|---|---|---|---|
| Hot (real-time) | ClickHouse in-memory + NVMe | Last 7 days | Sub-second funnel queries |
| Warm (batch) | ClickHouse MergeTree on SSD | 7–90 days | <3s P95 funnel queries |
| Cold (archive) | Iceberg on S3 Standard-IA | 90 days – 2 years | Ad-hoc Spark/Trino queries |
| Glacier | S3 Glacier | 2+ years | Compliance / audit only |

### 4.6 Caching Strategy

- **Result cache (Redis)**: Funnel query results keyed by `SHA256(tenant_id + funnel_definition + time_range + cohort_filters)`. TTL 5 min for real-time, 30 min for historical. Invalidated on funnel definition change.
- **Identity cache (Redis)**: Anonymous → canonical ID mapping. TTL 24h, write-through on new alias events.
- **No query-level row cache in ClickHouse**: ClickHouse's internal mark cache + OS page cache is sufficient; application-level Redis cache handles repeat identical queries.

---

## Phase 5: Funnel Query Execution

### ClickHouse `windowFunnel` Pattern

```sql
-- 3-step funnel: page_view → add_to_cart → purchase within 7 days
SELECT
    level,
    count() AS users
FROM (
    SELECT
        canonical_uid,
        windowFunnel(604800)(  -- 7 days in seconds
            event_time,
            event_name = 'page_view',
            event_name = 'add_to_cart',
            event_name = 'purchase'
        ) AS level
    FROM events
    WHERE
        tenant_id = 'acme'
        AND date BETWEEN '2026-02-21' AND '2026-03-23'
        AND event_name IN ('page_view', 'add_to_cart', 'purchase')
    GROUP BY canonical_uid
)
GROUP BY level
ORDER BY level;
```

`windowFunnel` is O(n) per user — it scans the sorted event stream once, tracking the furthest step reached within the conversion window. No self-join, no subquery explosion.

### Identity Resolution at Query Time

```
1. Query API receives funnel request with tenant_id
2. If cohort filter includes anonymous_id ranges → batch lookup Redis
3. Redis miss → PostgreSQL fallback → write back to Redis
4. Inject resolved canonical_uid set as IN-clause or temp table into ClickHouse query
5. ClickHouse filters on canonical_uid; events from all aliases are co-located by sort key
```

This avoids rewriting historical events when a new alias is discovered — the identity graph is the source of truth, not the event table.

---

## Phase 6: Path Analysis — Sequential Pattern Mining

### PrefixSpan on Spark

Path discovery runs as an **offline Spark job** (hourly or daily), not at query time:

```python
# PySpark pseudocode
from pyspark.ml.fpm import PrefixSpan

# Build sequences per user: list of event_name ordered by event_time
sequences = events_df \
    .filter(col("tenant_id") == tenant_id) \
    .filter(col("date") >= lookback_date) \
    .groupBy("canonical_uid") \
    .agg(sort_array(collect_list(struct("event_time", "event_name"))).alias("seq")) \
    .select(col("seq.event_name").alias("items"))

ps = PrefixSpan(minSupport=0.01, maxPatternLength=5)
frequent_paths = ps.findFrequentSequentialPatterns(sequences)

# Write top-N paths to ClickHouse `path_patterns` table
frequent_paths.orderBy(col("freq").desc()).limit(1000).write \
    .format("jdbc").option("url", clickhouse_url).save()
```

**Why PrefixSpan over Apriori/GSP**: PrefixSpan avoids candidate generation by projecting the sequence database recursively. At petabyte scale, candidate generation is O(2^n) — infeasible. PrefixSpan is O(n × avg_seq_length) in practice.

---

## Phase 7: Real-Time Stateful Funnel (Flink)

### KeyedProcessFunction Design

```
State per user (RocksDB state backend):
  - Map<funnel_id, FunnelProgress>
    - FunnelProgress: { current_step: int, step_timestamps: List<long>, started_at: long }

On each event:
  1. For each active funnel definition (broadcast state):
     a. Check if event_name matches expected next step
     b. Check conversion window: event_time - started_at < window
     c. Check exclusion steps: if event matches exclusion → reset progress
     d. If step matches → advance current_step
     e. If current_step == total_steps → emit FunnelCompletionEvent
  2. Register event-time timer for conversion window expiry → emit partial completion on timeout
```

**State backend**: RocksDB (incremental checkpointing to S3). Avoids JVM heap pressure for 500M user states.

**Funnel definition broadcast**: Product team updates funnel definitions via API → published to Kafka `funnel_config` topic → Flink BroadcastProcessFunction updates all parallel task instances atomically.

---

## Phase 8: Trade-offs & Justification

### ClickHouse vs. Apache Pinot

| Dimension | ClickHouse | Apache Pinot |
|---|---|---|
| Funnel queries (deep scan) | **Winner** — vectorized execution, `windowFunnel`, high compression | Slower on full-table scans |
| High-QPS point lookups | Adequate | **Winner** — Star-Tree index, sub-100ms at 10k QPS |
| Operational complexity | Lower (single binary) | Higher (Helix, ZooKeeper, Controller, Broker, Server) |
| Our choice | **Primary OLAP engine** | Optional: add Pinot only if user-facing dashboards need <100ms at >5k QPS |

**Decision**: Start with ClickHouse only. Pinot is an optional future addition for user-facing real-time leaderboards. Similar to how Mixpanel migrated from MySQL to ClickHouse for funnel performance (Mixpanel Engineering Blog, 2021).

### Kafka vs. Kinesis

Kafka wins on: replayability, partition-level ordering (critical for per-user event ordering to Flink), open-source operational control, and no per-shard cost model that penalizes high-cardinality user keys.

### Iceberg vs. Delta Lake

Both are viable. Chose Iceberg because:
- Vendor-neutral (no Databricks dependency)
- Native Flink sink support (Iceberg Flink sink with exactly-once semantics)
- Hidden partitioning + partition evolution without rewriting data
- Trino/Spark/ClickHouse all read Iceberg natively

### Consistency vs. Availability

This system is **AP** (Availability + Partition Tolerance). Funnel counts may be eventually consistent (lag up to 5 min for real-time path, up to compaction delay ~1h for batch). This is acceptable — product analytics is not a financial ledger. We prioritize query availability over strict consistency.

### Push vs. Pull (Ingestion)

**Push model** (clients push to Ingestion API): chosen because:
- Clients (web/mobile) cannot run a pull-based consumer
- Ingestion API can validate, enrich (server_time, geo), and batch before Kafka publish
- Simpler client SDK surface area

---

## Phase 9: Reliability, Scaling & Operations

### Bottlenecks & Mitigations

| Bottleneck | Mitigation |
|---|---|
| Kafka hot partition (viral user_id) | Consistent hash + salting for extreme outliers; monitor partition lag per key |
| ClickHouse query fan-out on large tenants | Per-tenant query quotas (`max_concurrent_queries`, `max_memory_usage` per user); query queue |
| Small file problem in Iceberg | Compaction Service (Spark) runs every 15 min; merges files < 32 MB into 256–512 MB targets; uses `server_time` as compaction boundary (not event_time) to avoid reprocessing late arrivals |
| Flink state explosion (500M users × N funnels) | RocksDB state backend with TTL eviction (evict inactive user state after conversion_window + 1h); incremental checkpoints to S3 |
| Identity graph lookup latency | Redis cluster (read replicas per AZ); PostgreSQL as fallback; pre-warm cache on tenant login |

### Failure Handling

| Failure | Recovery |
|---|---|
| Ingestion API node crash | Stateless; load balancer routes to healthy instances; Kafka retains events |
| Kafka broker failure | Replication factor 3; ISR ≥ 2; producer `acks=all` |
| Flink job failure | Checkpoint every 30s to S3; restart from last checkpoint; exactly-once via Kafka transactions |
| ClickHouse node failure | ReplicatedMergeTree with 2 replicas per shard; queries route to healthy replica |
| S3 region outage | Cross-region replication for Iceberg metadata + critical partitions; Iceberg snapshots enable point-in-time recovery |
| Bad deployment (poison pill events) | Dead Letter Queue (DLQ) Kafka topic; Flink deserialization errors → DLQ → alert; schema registry (Confluent) enforces Avro schema at producer |

### Edge Cases

- **Out-of-order events**: Flink uses event-time watermarks (5 min allowed lateness). Late events beyond watermark go to a side output → reprocessed as a micro-batch correction to ClickHouse via `ALTER TABLE ... UPDATE` or insert + deduplication.
- **Identity merge after funnel completion**: When two anonymous IDs are merged post-fact, historical funnel counts are not retroactively corrected (too expensive). A nightly reconciliation job re-runs affected funnels for the prior 7 days and updates a `funnel_corrections` table.
- **Exclusion step race**: If exclusion event and funnel step arrive out of order → Flink's event-time processing with watermark handles this correctly; server_time used as tiebreaker.
- **Traffic spike (10× burst)**: Ingestion API auto-scales (Kubernetes HPA on CPU + RPS). Kafka absorbs burst. Flink scales via reactive mode (auto-parallelism). ClickHouse query queue + circuit breaker in Query API returns 429 with Retry-After.

### Observability

**Golden Signals:**

| Signal | Metric | Alert Threshold |
|---|---|---|
| Latency | `query_api_p95_latency_ms` | > 3000ms for 5 min |
| Traffic | `kafka_consumer_lag_by_partition` | > 100k events lag |
| Errors | `ingestion_api_5xx_rate` | > 0.1% |
| Saturation | `clickhouse_memory_usage_ratio` | > 80% |
| Freshness | `flink_watermark_lag_seconds` | > 300s |

**SLOs:**

| SLO | Target |
|---|---|
| Query API availability | 99.99% (< 52 min downtime/year) |
| Funnel query P95 latency | < 3s over 30-day window |
| Real-time funnel freshness | < 5 min end-to-end |
| Ingestion durability | 99.999% (no event loss after Kafka ack) |

**Health Checks:**
- Synthetic funnel query runs every 60s against a canary tenant; alerts if P95 > 2s
- Flink checkpoint age monitored; alert if last successful checkpoint > 5 min ago
- Kafka consumer group lag dashboard per service

---

## Phase 10: Staff-Level Considerations

### Cost

| Component | Cost Driver | Optimization |
|---|---|---|
| S3 (Iceberg) | Storage volume | Parquet + Zstd compression (~4–6× reduction); lifecycle policies to IA/Glacier |
| ClickHouse | Compute (query CPU) | Per-tenant query quotas; result caching eliminates repeat scans; tiered storage (SSD hot, HDD warm) |
| Kafka | Broker storage | Retention 7 days (events are in Iceberg for long-term); log compaction for config topics |
| Flink | Parallelism × instance size | RocksDB state TTL reduces state size; scale down during off-peak |
| Spark (compaction + PrefixSpan) | Spot instances | Run on spot/preemptible; Iceberg ACID ensures safe retries |

Rough estimate at 100k events/sec: ~$150–250k/month fully loaded (compute + storage + network), dominated by ClickHouse and Kafka infrastructure.

### Security

- **PII handling**: `properties` JSON blob is encrypted at the field level for PII columns (AES-256 via application-layer encryption before Kafka publish). ClickHouse stores encrypted blobs; decryption only in Query API layer after authz check.
- **Encryption at rest**: S3 SSE-S3 for Iceberg; ClickHouse disk encryption enabled.
- **Encryption in transit**: TLS 1.3 everywhere (Kafka, ClickHouse, internal gRPC).
- **Multi-tenant isolation**: `tenant_id` is a mandatory partition key and query filter; enforced at Query API layer (JWT claim → tenant_id binding). ClickHouse row-level policies as defense-in-depth.
- **Audit log**: All funnel definition changes and query executions logged to append-only PostgreSQL audit table.

### Evolution: 10× Scale

| Current | 10× Challenge | Solution |
|---|---|---|
| 100k events/sec | 1M events/sec | Kafka: scale to 256 partitions; Flink: increase parallelism to 512; Ingestion API: horizontal pod autoscaling |
| 315 TB/year | 3.15 PB/year | Iceberg partition evolution (add `hour` sub-partition); ClickHouse tiered storage + object storage backend (S3-backed MergeTree) |
| 500M MAU | 5B MAU | Identity graph: migrate from PostgreSQL to FoundationDB or Cassandra for horizontal key-value scale |
| 14 QPS queries | 140 QPS | Add Pinot for high-QPS user-facing queries; expand Redis result cache cluster; read replicas per AZ |
| Single region | Multi-region | Iceberg cross-region replication; ClickHouse geo-distributed replicas; Kafka MirrorMaker 2 for cross-region replication |

---

## Summary Architecture Decision Record

| Decision | Choice | Rejected Alternatives |
|---|---|---|
| OLAP Engine | ClickHouse | Druid (higher ops overhead), Redshift (proprietary, slow for user-centric queries) |
| Lakehouse Format | Apache Iceberg | Delta Lake (Databricks-centric), Hudi (complex write path) |
| Stream Processor | Apache Flink | Spark Streaming (micro-batch latency too high for 1-min freshness), Kafka Streams (limited state backend options) |
| Message Bus | Apache Kafka | AWS Kinesis (vendor lock-in, per-shard cost), Pulsar (less mature ecosystem) |
| Identity Store (hot) | Redis | Memcached (no hash type), DynamoDB (proprietary) |
| Path Mining | PrefixSpan on Spark | GSP (candidate generation explosion), standard SQL recursive CTEs (not scalable) |
| State Backend (Flink) | RocksDB | JVM heap (GC pressure at 500M user states) |
