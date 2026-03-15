# Real-Time Experimentation Platforms and Analytics Pipelines

-----

## Original Problem Statement

Tech giants like Netflix and Pinterest rely on A/B testing as a fundamental element of product development, requiring infrastructure that can handle trillions of rows of events while delivering sub-second insights into user behavior. The challenge is building a pipeline that can detect statistically significant changes in real-time to allow for rapid mitigation of harmful experiments.

### The Core Architectural Challenge

A real-time experimentation pipeline must handle massive event volume (over 2 million events per second) while isolating issues that may only affect specific app versions or geographic regions. The system needs to perform complex de-duplication, sessionization, and windowed aggregations without introducing excessive latency or data lag.

### High-Level Requirements

| **Requirement Type** | **Description** |
| --- | --- |
| **Functional** | Capture and filter millions of user events per second to extract business-critical signals. |
| **Functional** | Real-time activation tracking to identify which users are triggered into which experiments. |
| **Functional** | Automated de-duplication of events based on user ID, event type, and timestamps. |
| **Functional** | Support for controlled rollouts with automatic rollbacks if regressions are detected. |
| **Non-Functional** | Latency: Sub-second querying across trillions of rows to gain instant visibility into metrics. |
| **Non-Functional** | Granularity: Ability to tag every measurement with device, region, and version metadata. |
| **Non-Functional** | Accuracy: Processing time tumbling windows (e.g., 15 minutes) for efficient count aggregation. |
| **Non-Functional** | Reliability: 100% uptime during peak global streaming windows. |

### Nuanced Considerations for Staff Engineers

Using Apache Flink for real-time stream processing is a common choice, but it requires careful state management. For instance, de-duplication can be implemented as a KeyedProcessFunction where events are cached for a short period (e.g., five minutes) to discard duplicates. Similarly, finding the first trigger time for a user in an experiment requires state persistence for at least the duration of the experiment ramp-up (e.g., three days). For efficient storage, counts should be aggregated in tumbling windows before being sent to an analytical store like Apache Druid, which is optimized for sub-second slicing and dicing of massive datasets. Anomaly detection models can then be layered on top to flag deviations in system metrics, such as success rates or CPU utilization, notifying on-call engineers immediately.

-----


# Architectural Design: Real-Time Experimentation Platform

## Phase 1: Scoping & Requirements

### Problem Restatement

Design a real-time experimentation (A/B testing) platform capable of ingesting millions of user events per second, enriching them with experiment metadata, performing de-duplication and windowed aggregation, and serving sub-second analytical queries across trillions of rows. The platform must automatically detect regressions in experiments and trigger rollbacks to protect user experience — similar to what Netflix (Interleaving/XP platform) and Pinterest (Rapid Experimentation) operate at scale.

### Functional Requirements

| # | Requirement |
|---|---|
| FR1 | Ingest and filter 2M+ user events/sec from client SDKs (mobile, web, server-side) |
| FR2 | Real-time activation tracking — identify which users are triggered into which experiment variant |
| FR3 | Automated de-duplication of events keyed on (user_id, event_type, timestamp) within a configurable TTL window |
| FR4 | Pre-aggregate metrics in tumbling time windows (e.g., 15 min) per experiment × variant × dimension |
| FR5 | Sub-second OLAP queries with slicing by device, region, app version, experiment, variant |
| FR6 | Controlled rollouts with automatic rollback when statistically significant regressions are detected |
| FR7 | Experiment lifecycle management — create, configure targeting rules, ramp, pause, conclude |

### Non-Functional Requirements

| Attribute | Target |
|---|---|
| **Throughput** | 2M events/sec sustained, 5M burst |
| **Event-to-Insight Latency** | < 60 seconds (P99) from event emission to queryable in dashboard |
| **Query Latency** | < 500ms (P95) for dashboard OLAP queries across trillions of pre-aggregated rows |
| **Anomaly Detection Latency** | < 5 minutes from regression onset to alert/rollback trigger |
| **Availability** | 99.99% for ingestion pipeline; 99.95% for dashboard reads |
| **Consistency** | Eventual consistency acceptable for analytics; strong consistency for experiment config |
| **Durability** | Zero event loss — at-least-once delivery with idempotent downstream processing |
| **Data Retention** | Hot: 7 days, Warm: 90 days, Cold/Archival: 2+ years in object storage |

---

## Phase 2: High-Level Design

### Back-of-Envelope Math

```
Events/sec:          2,000,000
Avg event size:      ~500 bytes (JSON, compressed ~200 bytes on wire)

Ingestion bandwidth: 2M × 500B = 1 GB/s raw (~400 MB/s compressed)
Events/day:          2M × 86,400 = ~173 billion
Storage/day (raw):   173B × 500B = ~86 TB/day

Pre-aggregated rows (15-min windows):
  Assume 1,000 experiments × 3 variants × 50 metrics × 96 windows/day × 100 dim combos
  = ~1.44 billion rows/day (~200 bytes each) = ~288 GB/day in Druid

Query load:
  ~500 concurrent dashboard users + automated anomaly detection
  ~1,000 QPS analytical queries against Druid

Kafka cluster sizing:
  2M msgs/sec ÷ ~100K msgs/sec/partition ≈ 20 partitions minimum for raw topic
  In practice: 64-128 partitions for parallelism headroom
  Brokers: 9-12 brokers (3 racks, RF=3, min.ISR=2)

Flink cluster sizing:
  ~64-128 TaskManager slots aligned with Kafka partitions
  State size: de-dup state ~50 GB (5-min TTL × 2M/sec × 16 bytes key)
              activation state ~200 GB (100M active users × 2 KB per user-experiment)
  Checkpoint interval: 60 seconds, incremental, to S3
```

### High-Level Components

| Component | Technology | Purpose |
|---|---|---|
| Event Ingestion API | Envoy/NGINX → gRPC service | Accept events from client SDKs, validate schema, produce to Kafka |
| Event Bus | Apache Kafka | Decouple ingestion from processing; buffer backpressure; enable replay |
| Stream Processor | Apache Flink | De-duplication, activation tracking, windowed aggregation |
| Experiment Config Service | PostgreSQL + Redis cache | CRUD for experiment definitions, targeting rules, variant allocations |
| Analytical Store | Apache Druid | Sub-second OLAP on pre-aggregated experiment metrics |
| Anomaly Detection Service | Python/Go service + statistical models | Monitor metric streams, detect regressions, trigger rollbacks |
| Rollback / Decision Service | Go service | Execute experiment pause/rollback based on anomaly signals |
| Dashboard & Query API | React + GraphQL / REST API over Druid | Interactive exploration of experiment results |
| Raw Event Archive | S3 (Parquet via Flink sink) | Long-term archival for ad-hoc deep dives via Spark/Trino |

### Data Flow (Happy Path)

```
1. Client SDK emits event → HTTPS/gRPC → Load Balancer → Ingestion API
2. Ingestion API validates schema, enriches with server timestamp → produces to Kafka [raw-events] topic
3. Flink Job 1 (De-duplication):
   - Consumes [raw-events], keyed by (user_id, event_type, truncated_timestamp)
   - KeyedProcessFunction with ValueState + 5-min TTL
   - Deduped events → Kafka [deduped-events]
4. Flink Job 2 (Activation Tracking):
   - Consumes [deduped-events], joins with experiment config (from Redis-backed AsyncIO)
   - Determines variant assignment (hash-based bucketing on user_id + experiment_salt)
   - Tracks first-trigger-time per (user_id, experiment_id) using MapState (TTL = experiment duration)
   - Emits activation records → Kafka [activations]
5. Flink Job 3 (Windowed Aggregation):
   - Consumes [deduped-events] + [activations]
   - Groups by (experiment_id, variant_id, metric_name, device, region, app_version)
   - 15-minute tumbling window with event-time semantics, watermark = max_out_of_orderness(30s)
   - Emits pre-aggregated counts/sums/HLL sketches → Kafka [aggregated-metrics]
6. Druid Real-Time Ingestion:
   - Kafka indexing service consumes [aggregated-metrics]
   - Creates segments with HOUR granularity, partitioned by experiment_id
7. Anomaly Detection Service:
   - Polls Druid every 60s for critical metrics (error rate, crash rate, latency percentiles)
   - Also receives pushed alerts from Flink for catastrophic signals (e.g., crash rate > 5%)
   - Runs sequential probability ratio test (SPRT) or CUSUM for early stopping
   - Flags regression → calls Rollback Service
8. Rollback Service:
   - Pauses experiment allocation in Config Service
   - Pushes updated config to Redis → Flink picks up within seconds
   - Notifies on-call via PagerDuty/Slack
9. Dashboard:
   - Analysts query Druid via Query API for experiment results
   - Supports drill-down by device/region/version, time range, metric
```

> **Q**: Provide an example of how a raw event would look like and how does it appear after activation tracking flink job?
>
>> **A:** A user taps "Buy Now" on the iOS app. The SDK emits a raw event that hits Kafka `[raw-events]`:
>>
>> **Raw event (after ingestion + de-duplication):**
>>
>> ```json
>> {
>>   "event_id": "01956a3b-7c2e-7d4a-b1ef-3a8c9f012d45",
>>   "user_id": "u_abc123",
>>   "event_type": "purchase_click",
>>   "client_timestamp": "2026-02-23T14:32:10.221Z",
>>   "server_timestamp": "2026-02-23T14:32:10.298Z",
>>   "device_type": "ios",
>>   "app_version": "14.2.1",
>>   "region": "us-west-2",
>>   "session_id": "sess_8f3a1b",
>>   "payload": {
>>     "element_id": "buy_button_v2",
>>     "page": "/product/sku-9981",
>>     "price_cents": 2999
>>   }
>> }
>> ```
>>
>> The Activation Tracking Flink job (Job 2) consumes this event and does three things:
>>
>> 1. **Looks up experiment config** from Redis — finds that experiment `exp_checkout_redesign` is running, targeting `region=us-west-2` and `app_version>=14.0`.
>> 2. **Computes variant assignment**: `murmur3(user_id + salt) % 100` → bucket 37 → falls in `treatment_a` (buckets 0-49).
>> 3. **Checks activation state** (MapState keyed by `(user_id, experiment_id)`): first time seeing this user in this experiment → records first trigger time.
>>
>> It then emits two outputs:
>>
>> **Enriched event (back to Kafka `[deduped-events]` with experiment annotations — consumed by Job 3 for aggregation):**
>>
>> ```json
>> {
>>   "event_id": "01956a3b-7c2e-7d4a-b1ef-3a8c9f012d45",
>>   "user_id": "u_abc123",
>>   "event_type": "purchase_click",
>>   "client_timestamp": "2026-02-23T14:32:10.221Z",
>>   "server_timestamp": "2026-02-23T14:32:10.298Z",
>>   "device_type": "ios",
>>   "app_version": "14.2.1",
>>   "region": "us-west-2",
>>   "session_id": "sess_8f3a1b",
>>   "experiment_id": "exp_checkout_redesign",
>>   "variant_id": "var_treatment_a",
>>   "payload": {
>>     "element_id": "buy_button_v2",
>>     "page": "/product/sku-9981",
>>     "price_cents": 2999
>>   }
>> }
>> ```
>>
>> **Activation record (to Kafka `[activations]` — only emitted on first trigger):**
>>
>> ```json
>> {
>>   "user_id": "u_abc123",
>>   "experiment_id": "exp_checkout_redesign",
>>   "variant_id": "var_treatment_a",
>>   "first_trigger_time": "2026-02-23T14:32:10.221Z",
>>   "device_type": "ios",
>>   "app_version": "14.2.1",
>>   "region": "us-west-2"
>> }
>> ```
>>
>> Key differences: the raw event has **no experiment context** — it's just "something happened." After Job 2, the event is tagged with `experiment_id` + `variant_id`, and a separate activation record captures the **first moment** this user entered the experiment. Subsequent events from the same user in the same experiment get enriched but don't emit a new activation record (the MapState check short-circuits).

> **Q:** Why can't client devices not send experiment identifier in event payload? The active experiments for a user will impact user experience, UI flows etc. Also consider cases where a multiple experiments are active for a user which is usually the case.
>
>> **A:** You're absolutely right — and in practice, **both approaches coexist**. Let me clarify the architecture should actually be a **hybrid model**:
>>
>> **What the client DOES know:** The client SDK calls an **Assignment Service** (or embedded SDK logic) at session start / feature-flag check time to get its active experiment assignments. The response looks like:
>>
>> ```json
>> {
>>   "assignments": [
>>     { "experiment_id": "exp_checkout_redesign", "variant_id": "var_treatment_a" },
>>     { "experiment_id": "exp_search_ranking_v3", "variant_id": "var_control" },
>>     { "experiment_id": "exp_onboarding_flow", "variant_id": "var_treatment_b" }
>>   ]
>> }
>> ```
>>
>> The client uses these to render the right UI/flow. So yes, the client **already has** experiment context and **should stamp it on events**. A click on the redesigned checkout button should carry `exp_checkout_redesign` in its payload.
>>
>> **Why server-side activation tracking is still needed:**
>>
>> 1. **Not all events are attributable client-side.** A client emits a generic `page_view` or `app_open` event — it doesn't know which of its 15 active experiments "caused" that event. Server-side enrichment attributes the event to **all applicable experiments** based on targeting rules. The client only knows "I'm in experiment X," but the analytics pipeline needs "this event counts toward experiments X, Y, and Z."
>>
>> 2. **Multi-experiment attribution.** A single `purchase_click` event may be relevant to `exp_checkout_redesign` (UI change) AND `exp_pricing_model_v2` (backend pricing) AND `exp_search_ranking_v3` (how they found the product). The client SDK shouldn't need to reason about this fan-out — that's business logic that belongs server-side.
>>
>> 3. **Source of truth for activation time.** Even if the client sends `experiment_id`, the server-side Flink job is the **canonical record** of "when was this user first exposed to this experiment." Client clocks drift, SDKs have caching bugs, and offline-first mobile apps may batch events with stale assignment data. The server-side MapState is the durable, consistent activation record.
>>
>> 4. **Server-side experiments.** Not all experiments have client-side UI impact — backend A/B tests (ranking algorithms, recommendation models, API timeout tuning) are invisible to the client. These need pure server-side activation tracking.
>>
>> **Revised data flow for the hybrid model:**
>>
>> - **Client-attributed events** (e.g., user clicked the treatment-A checkout button): Client stamps `experiment_id` + `variant_id` in the event payload. Flink Job 2 **validates** the assignment (guards against stale/spoofed client data) and still records activation state, but trusts the client's attribution as a hint.
>> - **Unattributed events** (e.g., generic `session_start`, `crash`, `latency_metric`): No experiment context from client. Flink Job 2 enriches by looking up **all active experiments** this user is assigned to and fans the event out to each.
>>
>> Good catch — the document's original framing was too server-centric. The architecture should show the Assignment Service as a first-class component that the client SDK calls, with Flink acting as the **enrichment + validation + fan-out** layer rather than the sole source of experiment attribution.

### Architecture Diagram

```
┌──────────────┐
│  Client SDKs │  (Mobile, Web, Server)
│  (2M evt/s)  │
└──────┬───────┘
       │ HTTPS / gRPC
       ▼
┌──────────────┐     ┌──────────────────────┐
│   Envoy LB   │────▶│   Ingestion API       │
└──────────────┘     │  (Schema Validation)  │
                     └──────────┬───────────┘
                                │ Produce
                                ▼
                     ┌──────────────────────┐
                     │    Apache Kafka       │
                     │                      │
                     │  [raw-events]        │  64-128 partitions
                     │  [deduped-events]    │  RF=3, min.ISR=2
                     │  [activations]       │
                     │  [aggregated-metrics]│
                     │  [dead-letter]       │
                     └──┬───┬───┬───────────┘
                        │   │   │
           ┌────────────┘   │   └────────────┐
           ▼                ▼                ▼
┌─────────────────┐ ┌────────────────┐ ┌─────────────────────┐
│  Flink Job 1    │ │  Flink Job 2   │ │  Flink Job 3        │
│  De-duplication │ │  Activation    │ │  Windowed Aggregation│
│  (KeyedProcess) │ │  Tracking      │ │  (15-min tumbling)   │
│                 │ │                │ │                     │
│  State: 50 GB   │ │  State: 200 GB │ │  HLL sketches       │
│  TTL: 5 min     │ │  TTL: exp dur  │ │  count/sum/min/max  │
└────────┬────────┘ └───────┬────────┘ └──────────┬──────────┘
         │                  │                     │
         │    ┌─────────────┘     ┌───────────────┘
         │    │                   │
         │    │  ┌────────────────┘
         │    │  │
         ▼    ▼  ▼                          ┌──────────────────┐
┌──────────────────────┐                    │  Experiment      │
│    Apache Druid      │◀───── config ─────▶│  Config Service  │
│                      │                    │  (PostgreSQL +   │
│  Real-time ingestion │                    │   Redis Cache)   │
│  from Kafka          │                    └────────┬─────────┘
│                      │                             │
│  Hot: SSD (7d)       │                             │
│  Warm: HDD (90d)     │                    ┌────────▼─────────┐
│  Cold: S3 deep store │                    │  Rollback /      │
└──────────┬───────────┘                    │  Decision Service│
           │                                └────────▲─────────┘
           │ Query                                   │ Trigger
           ▼                                         │
┌──────────────────────┐                    ┌────────┴─────────┐
│  Query API (REST/GQL)│                    │  Anomaly         │
│  + Dashboard (React) │                    │  Detection Svc   │
└──────────────────────┘                    │  (SPRT / CUSUM)  │
                                            └──────────────────┘

   ┌──────────────────────┐
   │  S3 (Parquet)        │  Raw event archival
   │  Queryable via       │  for ad-hoc analysis
   │  Spark / Trino       │
   └──────────────────────┘
```

---

## Phase 3: Deep Dive — Data & Storage

### Data Model

#### Raw Event (Kafka + S3 Parquet)

```json
{
  "event_id": "uuid-v7",
  "user_id": "u_abc123",
  "event_type": "click",
  "client_timestamp": "2026-02-23T10:15:32.456Z",
  "server_timestamp": "2026-02-23T10:15:32.512Z",
  "device_type": "ios",
  "app_version": "14.2.1",
  "region": "us-west-2",
  "session_id": "sess_xyz",
  "payload": {
    "element_id": "buy_button",
    "page": "/product/123"
  }
}
```

#### Experiment Configuration (PostgreSQL)

```sql
CREATE TABLE experiments (
    experiment_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    description     TEXT,
    status          TEXT NOT NULL CHECK (status IN ('draft','running','paused','concluded')),
    owner_team      TEXT NOT NULL,
    targeting_rules JSONB NOT NULL DEFAULT '{}',
    salt            TEXT NOT NULL,                -- for deterministic hashing
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE variants (
    variant_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment_id   UUID REFERENCES experiments(experiment_id),
    name            TEXT NOT NULL,               -- e.g., "control", "treatment_a"
    allocation_pct  NUMERIC(5,2) NOT NULL,       -- e.g., 50.00
    is_control      BOOLEAN DEFAULT false
);

CREATE TABLE rollback_thresholds (
    experiment_id   UUID REFERENCES experiments(experiment_id),
    metric_name     TEXT NOT NULL,               -- e.g., "crash_rate", "latency_p99"
    operator        TEXT NOT NULL,               -- "gt", "lt", "pct_change_gt"
    threshold_value NUMERIC NOT NULL,
    PRIMARY KEY (experiment_id, metric_name)
);
```

#### Activation Record (Flink State → Kafka → Druid)

```json
{
  "user_id": "u_abc123",
  "experiment_id": "exp_001",
  "variant_id": "var_treatment_a",
  "first_trigger_time": "2026-02-23T10:15:32.456Z",
  "device_type": "ios",
  "app_version": "14.2.1",
  "region": "us-west-2"
}
```

#### Pre-Aggregated Metric Row (Druid)

```json
{
  "__time": "2026-02-23T10:00:00.000Z",
  "experiment_id": "exp_001",
  "variant_id": "var_treatment_a",
  "metric_name": "click_count",
  "device_type": "ios",
  "app_version": "14.2.1",
  "region": "us-west-2",
  "count": 14523,
  "sum_value": 14523.0,
  "unique_users_hll": "<base64-encoded HyperLogLog sketch>"
}
```

### Druid Ingestion Spec (Key Fields)

```json
{
  "dataSchema": {
    "dataSource": "experiment_metrics",
    "timestampSpec": { "column": "__time", "format": "iso" },
    "dimensionsSpec": {
      "dimensions": [
        "experiment_id", "variant_id", "metric_name",
        "device_type", "app_version", "region"
      ]
    },
    "metricsSpec": [
      { "type": "longSum", "name": "count", "fieldName": "count" },
      { "type": "doubleSum", "name": "sum_value", "fieldName": "sum_value" },
      { "type": "HLLSketchMerge", "name": "unique_users_hll", "fieldName": "unique_users_hll", "lgK": 14 }
    ],
    "granularitySpec": {
      "segmentGranularity": "HOUR",
      "queryGranularity": "FIFTEEN_MINUTE",
      "rollup": true
    }
  }
}
```

### Storage Strategy

| Layer | Technology | Data | Retention | Rationale |
|---|---|---|---|---|
| **Hot** | Druid (SSD-backed historical nodes) | Pre-aggregated metrics | 7 days | Sub-second queries for active experiments |
| **Warm** | Druid (HDD-backed historical nodes) | Pre-aggregated metrics | 90 days | Slower but still queryable for concluded experiments |
| **Cold** | S3 (Druid deep storage) | Druid segments | 2+ years | Reloadable on-demand for historical analysis |
| **Archive** | S3 (Parquet, partitioned by date/hour) | Raw events | 2+ years | Ad-hoc deep dives via Spark or Trino |
| **Config** | PostgreSQL | Experiment definitions | Indefinite | Strong consistency, relational integrity |
| **Cache** | Redis (cluster mode) | Experiment configs, variant assignments | TTL 60s | Look-aside cache; Flink reads config here via AsyncIO |

### Partitioning & Sharding

- **Kafka**: Partition by `murmur3(user_id) % num_partitions` — ensures all events for a user land on the same partition (ordering guarantee needed for de-duplication and activation tracking).
- **Druid**: Segments partitioned by time (HOUR) with secondary partitioning by `experiment_id` hash — colocates data for the most common query pattern (filter by experiment + time range).
- **PostgreSQL**: No sharding needed — experiment config is small (~10K rows). Single-primary with read replicas.
- **S3 Parquet**: Partitioned by `date/hour/region` — Hive-compatible layout for Spark/Trino queries.

### Caching Strategy

- **Redis (Look-aside)**: Flink jobs fetch experiment config from Redis. Config Service writes-through to Redis on every update. TTL = 60s as a safety net. This avoids Flink hitting PostgreSQL at 2M events/sec.
- **Druid Query Cache**: Druid's built-in segment-level cache for repeated dashboard queries. Broker-level result cache for identical queries within a TTL.
- **Dashboard CDN**: Static assets via CDN; API responses are not cached (freshness matters for experiment dashboards).

---

## Phase 4: Trade-offs & Justification

### Apache Kafka (Event Bus)

| | |
|---|---|
| **Why Kafka** | Durable, replayable log with partition-level ordering. Handles 2M msgs/sec with ease on a modest cluster. Native integration with Flink (FlinkKafkaConsumer/Producer). Tiered storage support moves older segments to S3 automatically. |
| **Why not Pulsar** | Pulsar's tiered storage is appealing, but Kafka's ecosystem maturity and Flink connector stability are stronger. Operational knowledge is more widely available. |
| **Why not Kinesis** | Shard limits (1 MB/s per shard) would require 1,000+ shards at this scale. Costly, no replay beyond 7 days without re-architecture, vendor lock-in. |

### Apache Flink (Stream Processor)

| | |
|---|---|
| **Why Flink** | True event-time processing with watermarks. Exactly-once via Kafka transactions + checkpointing. Rich stateful primitives (KeyedProcessFunction, MapState) for de-duplication and activation tracking. Netflix, Pinterest, and Uber all use Flink for experimentation pipelines. |
| **Why not Spark Structured Streaming** | Micro-batch model adds 1-10s latency per batch. State management is less flexible — no TTL on state, no ProcessFunction equivalent. |
| **Why not Kafka Streams** | Viable for simpler use cases, but lacks Flink's windowing primitives, watermark handling, and RocksDB-backed state with TTL. Also harder to operate separate scaling of processing from Kafka brokers. |

### Apache Druid (Analytical Store)

| | |
|---|---|
| **Why Druid** | Purpose-built for sub-second OLAP on time-series data with high-cardinality dimensions. Native Kafka ingestion (real-time + batch compaction). Built-in approximate algorithms (HyperLogLog for count-distinct, quantile sketches). Segment-level caching. Netflix uses Druid as the backend for their experimentation dashboards. |
| **Why not ClickHouse** | Excellent query engine, but real-time ingestion from Kafka is less mature (requires Kafka Engine tables with MergeTree, which can have deduplication quirks). Less battle-tested for this exact use case at Netflix/Pinterest scale. |
| **Why not Pinot** | LinkedIn's equivalent — strong contender. Slightly smaller open-source community and fewer production references for experimentation specifically. Would be a valid alternative. |
| **Why not raw queries on S3 (Trino/Athena)** | Too slow for sub-second interactive dashboards. Good for ad-hoc but not real-time. Used as complementary cold-tier query engine. |

### Pre-aggregation in Flink vs. Raw Ingestion into Druid

| | |
|---|---|
| **Why pre-aggregate** | Reduces Druid ingestion rate from 2M rows/sec to ~100K rows/sec (per window flush). Smaller segments, faster queries, lower Druid cost. Enables HLL sketch computation at the Flink layer where we have the full event stream. |
| **Trade-off** | Loses per-event granularity in Druid. Mitigated by archiving raw events to S3 Parquet for ad-hoc deep dives. |

### Consistency Model

| Concern | Choice | Rationale |
|---|---|---|
| **Experiment Config** | Strong consistency (PostgreSQL) | Incorrect config could assign users to wrong variants. Config changes are infrequent (~10 writes/min). |
| **Event Pipeline** | Eventual consistency | Analytics can tolerate seconds of lag. Exactly-once semantics in Flink ensure no double-counting once data lands in Druid. |
| **Anomaly Detection** | Eventual (1-5 min lag) | Acceptable trade-off — detecting a regression within 5 minutes is fast enough to prevent widespread harm. |

### Push vs. Pull

| Path | Model | Rationale |
|---|---|---|
| SDK → Ingestion API | **Push** | Clients push events as they occur. Minimizes client-side buffering. |
| Kafka → Flink | **Pull** (consumer poll) | Flink consumers pull from Kafka at their own pace; backpressure is natural. |
| Flink → Kafka (downstream topics) | **Push** (produce) | Processed results pushed immediately for low latency. |
| Kafka → Druid | **Pull** (Kafka indexing service) | Druid's native Kafka supervisor pulls and manages offsets. |
| Anomaly Detection ← Druid | **Pull** (poll every 60s) | Decoupled; anomaly service queries Druid for latest windows. |
| Catastrophic Alerts | **Push** (Flink → anomaly svc) | Flink side-output pushes critical signals (crash rate spike) directly to anomaly service for <30s detection. |

---

## Phase 5: Reliability, Scaling & Operations

### Bottlenecks & Mitigation

| Bottleneck | Risk | Mitigation |
|---|---|---|
| **Flink activation state** | State can reach 200+ GB for 100M users across active experiments | RocksDB state backend with incremental checkpointing. State TTL = experiment duration. Purge concluded experiment state via savepoint + state cleanup job. |
| **Kafka partition skew** | Power users or bot traffic on a few user_ids | Monitor partition lag skew. Use sub-key (user_id + event_type) if needed. Rate-limit per user_id at ingestion layer. |
| **Druid hot segments** | All dashboards query the same recent time range | Druid's segment-level cache handles this well. Add broker replicas for read scaling. Pre-warm segments on historical nodes. |
| **Experiment config fan-out** | 2M events/sec all need config lookup | Redis cluster (6+ nodes) with read replicas. Flink AsyncIO for non-blocking lookups. Local LRU cache in Flink operators (TTL = 10s). |
| **S3 checkpoint storm** | 128 subtasks checkpointing 250 GB concurrently | Incremental checkpoints (only delta). Stagger checkpoint barriers. Dedicated S3 bucket with high request rate prefix hashing. |

### Failure Handling

| Failure | Recovery Strategy |
|---|---|
| **Flink TaskManager crash** | Flink JobManager detects via heartbeat timeout. Restores from last checkpoint (60s RPO). Affected Kafka partitions re-assigned to surviving or new TaskManagers. |
| **Flink JobManager crash** | HA via ZooKeeper or Kubernetes leader election. Standby JobManager takes over, restores job graph from ZooKeeper. |
| **Kafka broker failure** | RF=3, min.ISR=2 ensures no data loss. Controller re-assigns partition leadership. Producers retry with `acks=all`. |
| **Druid historical node crash** | Segment replication factor = 2. Coordinator reassigns segments to surviving nodes. Re-downloads from deep storage if needed. |
| **Druid real-time ingestion lag** | Kafka indexing supervisor automatically resets and catches up. Alert if lag > 5 minutes. |
| **Region outage** | Active-passive Kafka MirrorMaker 2 to DR region. Flink jobs in DR region consume from mirrored topics. Druid in DR serves reads from replicated deep storage. DNS failover for dashboard. |
| **Bad deployment (Flink job)** | Canary deploys: new job version processes shadow traffic from a separate consumer group. Compare output before cutover. Rollback via savepoint restore. |
| **Anomaly detection false positive** | Rollback decision requires statistical significance (p < 0.01) AND magnitude threshold. Human-in-the-loop confirmation for experiments below 5% allocation. |

### Edge Cases

| Edge Case | Handling |
|---|---|
| **Poison pill event** | Flink try-catch around deserialization. Malformed events routed to `[dead-letter]` Kafka topic. Alert if DLQ rate > 0.01%. |
| **Clock skew / late arrivals** | Event-time processing with `BoundedOutOfOrdernessWatermarks(30s)`. Allowed lateness = 1 hour. Late events update previously emitted windows via Druid's reindexing compaction. |
| **Traffic spike (10x burst)** | Kafka absorbs burst (disk-backed, minutes of buffer at 5M/sec). Flink backpressure slows consumers gracefully. Ingestion API rate-limits at 5M/sec with 429 responses; client SDKs buffer and retry with exponential backoff. |
| **Experiment targeting change mid-flight** | Config Service publishes change event. Flink picks up new config from Redis within 60s. Activation records after the change use new targeting. Historical activations remain immutable. |
| **Hash collision in variant assignment** | Use MurmurHash3 with 32-bit output space — collision probability negligible at 100M users. Salt per experiment ensures independence across experiments. |

### Observability

#### Golden Signals

| Signal | Metric | Source | Alert Threshold |
|---|---|---|---|
| **Latency** | Event-to-Druid end-to-end latency (P99) | Synthetic events measured in Druid | > 120 seconds |
| **Latency** | Druid query P95 latency | Druid broker metrics | > 1 second |
| **Traffic** | Kafka ingestion rate (events/sec) | Kafka broker JMX | < 500K/sec (below expected baseline) |
| **Errors** | Flink job restart count | Flink REST API / Prometheus | > 2 restarts in 10 min |
| **Errors** | Dead-letter queue rate | Kafka consumer lag on DLQ topic | > 0.01% of ingestion rate |
| **Saturation** | Flink checkpoint duration | Flink metrics | > 5 minutes (indicates state bloat) |
| **Saturation** | Kafka consumer lag (records) | Burrow / Kafka consumer group offsets | > 1M records for any consumer group |
| **Saturation** | Druid segment build queue depth | Druid coordinator metrics | > 100 pending segments |

#### SLAs & SLOs

| SLI | SLO | SLA (external) |
|---|---|---|
| Ingestion availability | 99.99% measured over rolling 30 days | 99.95% |
| Event-to-insight latency | P99 < 60 seconds | P99 < 120 seconds |
| Dashboard query latency | P95 < 500ms | P95 < 2 seconds |
| Anomaly detection time-to-alert | < 5 minutes from regression onset | < 15 minutes |
| Data completeness | > 99.99% of events queryable within 1 hour | > 99.9% |

#### Health Checks

- **Synthetic Canary Events**: Ingestion API emits synthetic events every 10 seconds with a known marker. Druid query checks for their arrival. If a canary is missing for > 2 minutes, page the on-call.
- **Flink Job Health**: Monitor via Flink REST API — check `RUNNING` status, checkpoint success rate, backpressure indicators.
- **Kafka Health**: Burrow for consumer lag monitoring. Under-replicated partitions alert. ISR shrink alert.
- **Druid Health**: Coordinator API for segment availability. Broker health endpoint for query serving.
- **End-to-End Smoke Test**: Every 5 minutes, a test harness creates a synthetic experiment, triggers activation events, and verifies the aggregated result appears in Druid within the SLO.

---

## Phase 6: Staff-Level Considerations

### Cost Analysis

| Component | Estimated Monthly Cost | Notes |
|---|---|---|
| **Kafka** (12 brokers, i3.2xlarge equiv.) | ~$15K | NVMe storage, tiered storage to S3 for retention > 24h |
| **Flink** (128 slots, m5.4xlarge equiv.) | ~$25K | RocksDB state on local NVMe, incremental checkpoints to S3 |
| **Druid** (20 historical + 4 broker + 2 coordinator) | ~$20K | Hot tier on SSD (i3), warm tier on HDD (d3) |
| **S3** (raw archival + checkpoints + deep storage) | ~$8K | ~86 TB/day raw, compressed to ~20 TB/day Parquet, lifecycle policy to Glacier after 90d |
| **Redis** (6-node cluster) | ~$3K | r6g.xlarge equivalent, cluster mode |
| **PostgreSQL** (primary + 2 replicas) | ~$2K | Small instance, experiment config is tiny |
| **Networking / Misc** | ~$5K | Cross-AZ transfer, DNS, monitoring infra |
| **Total** | **~$78K/month** | Self-managed on Kubernetes; managed services would be 2-3x more |

**Cost Optimization Levers:**
- Kafka tiered storage moves 90%+ of data to S3 at 1/10th the cost of broker disk.
- Flink incremental checkpoints reduce S3 PUT costs by 80% vs. full checkpoints.
- Druid auto-compaction merges small real-time segments, reducing query overhead and segment storage.
- Spot/preemptible instances for Flink TaskManagers (checkpointing makes this safe).

### Security

| Concern | Approach |
|---|---|
| **PII (user_id)** | Pseudonymize at ingestion: `user_id → HMAC-SHA256(user_id, rotating_key)`. Original mapping stored in encrypted vault (HashiCorp Vault). Druid and Flink only see pseudonymized IDs. |
| **Encryption in transit** | TLS 1.3 everywhere: SDK → Ingestion API, Kafka inter-broker, Flink → Kafka, Druid → S3. |
| **Encryption at rest** | S3: SSE-S3 or SSE-KMS. Druid local segments: dm-crypt. PostgreSQL: pgcrypto for sensitive fields. |
| **Access control** | RBAC on experiment config (team-level ownership). Druid data-source-level ACLs. S3 bucket policies + IAM roles. |
| **Audit logging** | All experiment config changes logged with actor, timestamp, diff. Queryable in a separate audit table. |
| **GDPR / Data Deletion** | User deletion request → mark user_id for purge → batch job rewrites affected S3 Parquet partitions excluding user. Druid segments reindexed. Pseudonymization makes this easier since we only need to delete the HMAC mapping to make data unlinkable. |

### Evolution Path (10x Scale → 20M events/sec)

| Layer | Current | 10x Strategy |
|---|---|---|
| **Kafka** | 12 brokers, 128 partitions | Scale to 40+ brokers, 512 partitions. Enable tiered storage to keep broker disk manageable. Consider Kafka on dedicated hardware (bare metal) for cost efficiency at this scale. |
| **Flink** | 128 TaskManager slots | Scale to 500+ slots. Split monolithic jobs into smaller, independently scalable pipelines (de-dup, activation, aggregation as separate Flink applications). Consider Flink's reactive scaling mode. |
| **Druid** | 20 historical nodes | Scale to 60+ historical nodes. Increase segment granularity to 15-MIN for hot tier to reduce segment count. Consider Druid's multi-stage query engine for complex joins. |
| **Architecture** | Single region | Active-active multi-region with Kafka MirrorMaker 2 (async replication). Region-local Flink + Druid clusters. Global experiment config via CockroachDB or Spanner for strong cross-region consistency. |
| **Query Layer** | Druid only | Add a materialized view layer (e.g., Cube.js or custom pre-computation) for the top 100 most-queried experiment dashboards. Reduces Druid load by 80%. |
| **Storage** | S3 Parquet | Migrate to Apache Iceberg on S3 for ACID transactions, schema evolution, and time-travel queries. Use Trino/Starburst for federated queries across Iceberg + Druid. |

### Architectural Decision Records (ADR) Summary

| Decision | Status | Context |
|---|---|---|
| Use Kafka over Kinesis | **Accepted** | Replayability, cost, partition-level ordering |
| Use Flink over Spark Streaming | **Accepted** | Event-time semantics, stateful processing, low latency |
| Use Druid over ClickHouse | **Accepted** | Native Kafka ingestion, HLL sketches, Netflix precedent |
| Pre-aggregate in Flink before Druid | **Accepted** | 20x reduction in Druid ingestion rate, faster queries |
| Pseudonymize user_id at ingestion | **Accepted** | GDPR compliance, defense in depth |
| Separate Flink jobs vs. single job | **Accepted** | Independent scaling, fault isolation, easier upgrades |
| SPRT for early stopping | **Accepted** | Faster regression detection than fixed-horizon tests |
| Redis for config fan-out to Flink | **Accepted** | Avoids PostgreSQL at 2M QPS; 60s TTL acceptable |

### References

- Netflix XP Platform: ["It's All A/B Testing" (Netflix Tech Blog)](https://netflixtechblog.com/its-all-a-bout-testing-the-netflix-experimentation-platform-4e1ca458c15)
- Pinterest Real-Time Experimentation: ["Building Pinterest's Real-Time Experimentation Platform"](https://medium.com/pinterest-engineering)
- Apache Flink Stateful Processing: [Flink Documentation — Process Function](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/process_function/)
- Apache Druid Architecture: [Druid Design Documentation](https://druid.apache.org/docs/latest/design/architecture.html)
- Sequential Probability Ratio Test: Wald, A. (1945). "Sequential Tests of Statistical Hypotheses"
- Kafka Tiered Storage: [KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)

---

## Appendix: Why Druid for Pre-Aggregated Experimentation Metrics — Deep Comparison

The Phase 4 trade-off section gave a brief rationale. This appendix provides a rigorous, dimension-by-dimension comparison of **Apache Druid** vs. **ClickHouse**, **Apache Pinot**, and **StarRocks** for this specific workload: **pre-aggregated experiment metrics with sub-second slice-and-dice queries**.

### Workload Profile Recap

Before comparing, let's pin the workload characteristics:

| Property | Value |
|---|---|
| Ingestion pattern | Flink emits pre-aggregated rows every 15 minutes (burst of ~100K rows per flush) |
| Row shape | Narrow — 6 dimensions, 3-4 metrics (count, sum, HLL sketch) |
| Rollup | Yes — identical dimension combos merge at ingestion time |
| Query pattern | Filter by `experiment_id` + time range, GROUP BY variant + 1-2 dimensions |
| Concurrency | ~500 dashboard users + automated anomaly detection ≈ 1,000 QPS |
| Latency target | P95 < 500ms |
| Approximate algorithms needed | HyperLogLog (count-distinct users), quantile sketches |
| Data mutability | Append-only. No updates, no deletes (pre-aggregated windows are immutable) |
| Retention tiers | Hot (7d SSD) → Warm (90d HDD) → Cold (S3 deep storage) |

### Dimension-by-Dimension Comparison

#### 1. Native Kafka Ingestion

| System | Mechanism | Maturity | Verdict |
|---|---|---|---|
| **Druid** | Kafka Indexing Service (supervisor) — first-class citizen. Manages offsets, creates segments directly, handles exactly-once via segment transaction protocol. | Production-proven at Netflix, Airbnb, Pinterest for 5+ years. | **Best-in-class** |
| **Pinot** | Real-time server with Kafka consumer — also first-class. Segment completion protocol similar to Druid's. | Production at LinkedIn, Uber, Stripe. Comparable maturity. | **Comparable** |
| **ClickHouse** | Kafka Engine table + Materialized View → MergeTree. Works, but is a bolt-on. Offset management is fragile — consumer group rebalances can cause duplicates or gaps. Requires manual dedup via `ReplacingMergeTree` or application-level idempotency. | Improved significantly since 2023, but still not native-feeling. | **Weaker** |
| **StarRocks** | Routine Load (Kafka consumer → internal tables). Decent but less battle-tested at extreme scale. Alternatively, Stream Load via HTTP for Flink-to-StarRocks direct push. | Growing adoption but fewer public references at >1M rows/sec sustained. | **Adequate** |

**Why this matters for us:** Our pipeline emits pre-aggregated rows from Flink to Kafka. We want the OLAP store to consume these autonomously without a separate ETL job or connector to manage. Druid and Pinot both do this natively. ClickHouse requires more glue.

#### 2. Rollup at Ingestion Time

| System | Rollup Support | Notes |
|---|---|---|
| **Druid** | **Native rollup** — dimensions specified in `dimensionsSpec`, metrics in `metricsSpec`. Identical dimension combos are merged during segment creation. This is Druid's raison d'être. | Reduces storage 10-100x for our workload. A 15-min window with 1,000 experiments × 3 variants × 50 metrics × 100 dim combos → merged into far fewer rows than raw events. |
| **Pinot** | **Star-tree index** — pre-aggregation at query time via pre-built aggregation trees. Not true ingestion-time rollup, but achieves similar query performance. | Different approach — stores more data but queries are still fast via star-tree. Trade-off: more storage, less ingestion complexity. |
| **ClickHouse** | `SummingMergeTree` / `AggregatingMergeTree` — merges happen **asynchronously in background**. Until merge completes, duplicate dimension combos exist and queries may see partial rollup. Requires `FINAL` modifier or application-level awareness. | The async merge model means query results can be subtly wrong if you query before merge completes. For experimentation dashboards where analysts need consistent numbers, this is a footgun. |
| **StarRocks** | Aggregate data model — supports rollup at ingestion. Conceptually similar to ClickHouse's AggregatingMergeTree but with a more predictable merge schedule. | Decent, but the aggregate model is less flexible than Druid's native approach when you want to add new metrics retroactively. |

**Why this matters for us:** We're sending **pre-aggregated** data from Flink. Druid's rollup is a natural fit — it collapses the data further at segment creation time (e.g., merging two 15-min flushes that happened to have overlapping dimension combos). ClickHouse's async merge means we can't trust the numbers until compaction finishes, which is unacceptable for an experimentation dashboard where a PM is watching metrics in real-time.

#### 3. Approximate Algorithms (HyperLogLog, Quantile Sketches)

| System | HLL | Theta/Quantile Sketches | Notes |
|---|---|---|---|
| **Druid** | **First-class** — `HLLSketchBuild`, `HLLSketchMerge` as native metric types. Merges HLL sketches during rollup and at query time. Also supports Theta sketches for set operations (intersection, difference). | `quantilesDoublesSketch` built-in. | Druid was designed with sketches as a core concept. Netflix uses HLL in Druid for unique-user counting across experiments. |
| **Pinot** | Supported via `DISTINCTCOUNTHLL`. Also has Theta sketch support. | Percentile estimation via `PERCENTILEEST`. | Comparable to Druid. LinkedIn uses this heavily for metrics. |
| **ClickHouse** | `uniqHLL12` aggregate function. Works, but HLL is a function, not a stored column type. You can't store a pre-computed HLL sketch in a column and merge it at query time without `AggregatingMergeTree` + `AggregateFunction(uniqHLL12, ...)` column type — which adds schema complexity. | `quantileTDigest`. | Possible but clunkier. The sketch-as-stored-type pattern is less ergonomic than Druid/Pinot. |
| **StarRocks** | `approx_count_distinct` (HLL-based). Supports `HLL` column type in aggregate model. | `percentile_approx`. | Workable. Less mature ecosystem of sketch types compared to Druid. |

**Why this matters for us:** Counting unique users per experiment variant is the **core metric** of any experimentation platform. We compute HLL sketches in Flink and need the OLAP store to store, merge, and query them natively. Druid treats sketches as first-class stored metric types that participate in rollup — exactly what we need. ClickHouse can do it but requires the more complex `AggregatingMergeTree` with `AggregateFunction` column types.

#### 4. Tiered Storage (Hot / Warm / Cold)

| System | Built-in Tiering | Deep Storage | Notes |
|---|---|---|---|
| **Druid** | **Native** — Historical nodes configured in tiers (`_default_tier` on SSD, `_warm` on HDD). Coordinator rules auto-migrate segments between tiers based on age. Deep storage (S3) is always the source of truth; historical nodes are a cache. | Segments in S3 by default. Can re-load any segment on-demand. | This is a core architectural feature. Losing a historical node is a non-event — segments are re-fetched from S3. |
| **Pinot** | Tiered storage via server tags (similar to Druid's tier concept). Deep storage on S3/HDFS for segment backup. | Comparable architecture to Druid. | Equally strong. |
| **ClickHouse** | **Storage policies** with volume rules (e.g., hot SSD → cold S3 via `s3` disk type). Works, but it's a single-node or replicated-pair concept. No equivalent of Druid's "segment re-download from deep storage" — if a node dies, you rely on replicas, not a shared deep storage. | S3-backed `MergeTree` exists but performance on cold reads is significantly worse. | Weaker for our 2+ year retention requirement. ClickHouse's shared-nothing architecture means cold data access requires either keeping replicas alive or accepting slow S3 reads. |
| **StarRocks** | Supports storage-compute separation with S3 as primary storage (StarRocks "shared-data" mode). Relatively new feature. | Growing maturity. | Promising but less proven in production at our scale. |

**Why this matters for us:** We need 7-day hot, 90-day warm, 2+ year cold. Druid's architecture was designed for this — historical nodes are stateless caches over S3 deep storage. Scaling reads means adding more historical nodes that pull segments from S3. ClickHouse's architecture treats local disk as the source of truth, making tiered storage an afterthought.

#### 5. Query Concurrency

| System | Concurrency Model | Notes |
|---|---|---|
| **Druid** | **Scatter-gather** — broker fans out to historical nodes, each scans its segments in parallel, results merged at broker. Designed for high concurrency (1,000+ QPS is normal). Segment-level and result-level caching at broker. | Purpose-built for dashboard workloads where many users hit similar queries. |
| **Pinot** | Similar scatter-gather via broker → server. Also designed for high concurrency. Uber runs Pinot at 100K+ QPS. | Arguably even stronger than Druid for extreme concurrency. |
| **ClickHouse** | **Single-node query execution** (or distributed across shards). Each query can consume significant CPU (vectorized execution). High concurrency (>100 QPS on complex queries) often requires read replicas or a query routing layer. | ClickHouse excels at throughput per query (fast full scans) but struggles with high concurrency. 1,000 QPS of analytical queries would require significant over-provisioning. |
| **StarRocks** | MPP engine, scatter-gather across BEs. Better concurrency than ClickHouse, designed for interactive analytics. | Handles concurrency better than ClickHouse but less proven than Druid/Pinot at 1,000+ QPS. |

**Why this matters for us:** 500 dashboard users + automated anomaly detection = ~1,000 QPS. Druid and Pinot handle this natively. ClickHouse would need a caching layer (e.g., Cube.js, Materialized Views) or 3-4x the hardware to sustain this concurrency without query queuing.

#### 6. Operational Complexity

| System | Ops Burden | Notes |
|---|---|---|
| **Druid** | **High** — many moving parts: Coordinator, Overlord, Broker, Historical, MiddleManager/Indexer, ZooKeeper, metadata store (MySQL/Postgres). Kubernetes operators exist but are complex. | This is Druid's main weakness. Netflix, Airbnb, etc. have dedicated Druid SRE teams. |
| **Pinot** | **High** — similar component count: Controller, Broker, Server, Minion, ZooKeeper. | Comparable ops burden to Druid. |
| **ClickHouse** | **Moderate** — simpler architecture (server nodes + ZooKeeper/ClickHouse Keeper for replication). Fewer components. `clickhouse-operator` for Kubernetes is mature. | Significantly easier to operate than Druid. This is ClickHouse's biggest advantage. |
| **StarRocks** | **Moderate** — FE (frontend/coordinator) + BE (backend/compute). Simpler than Druid/Pinot. | Easy to deploy, good K8s operator. |

#### 7. SQL Support & Developer Experience

| System | Query Language | Joins | Notes |
|---|---|---|---|
| **Druid** | Native JSON query API + SQL (via Apache Calcite). SQL is a translation layer, not native — some SQL patterns don't map well. | Limited join support (broadcast joins only, added recently). | For our use case (simple filter + group by), Druid SQL is sufficient. But engineers used to full SQL find it frustrating. |
| **Pinot** | SQL (PQL deprecated, moved to Calcite-based SQL). Similar limitations to Druid. | Limited join support. | Comparable to Druid. |
| **ClickHouse** | **Full ANSI SQL** — native SQL engine. JOINs, subqueries, CTEs, window functions all work. | Full JOIN support, including distributed joins. | **Strongest SQL story by far.** If your workload ever needs joins (e.g., join experiment config with metrics), ClickHouse wins. |
| **StarRocks** | Full SQL — MySQL-compatible wire protocol. | Full JOIN support, including cross-cluster joins. | Comparable to ClickHouse. Analyst-friendly. |

### Summary Matrix

| Dimension | Druid | Pinot | ClickHouse | StarRocks | **Weight for This Workload** |
|---|---|---|---|---|---|
| Kafka native ingestion | ★★★★★ | ★★★★★ | ★★★☆☆ | ★★★★☆ | **Critical** |
| Ingestion-time rollup | ★★★★★ | ★★★★☆ | ★★★☆☆ | ★★★★☆ | **Critical** — pre-aggregated data |
| HLL / sketch as stored type | ★★★★★ | ★★★★★ | ★★★☆☆ | ★★★★☆ | **Critical** — unique user counts |
| Tiered storage (hot/warm/cold) | ★★★★★ | ★★★★★ | ★★★☆☆ | ★★★★☆ | **High** — 2+ year retention |
| Query concurrency (1K+ QPS) | ★★★★★ | ★★★★★ | ★★★☆☆ | ★★★★☆ | **High** — dashboard + anomaly detection |
| Operational simplicity | ★★★☆☆ | ★★★☆☆ | ★★★★★ | ★★★★☆ | Medium |
| SQL & developer experience | ★★★☆☆ | ★★★☆☆ | ★★★★★ | ★★★★★ | Low for this workload (simple queries) |
| Production references for A/B testing | ★★★★★ | ★★★★☆ | ★★★☆☆ | ★★☆☆☆ | Medium |

### Verdict

**Druid wins on the dimensions that matter most for this workload** — native Kafka ingestion with exactly-once semantics, ingestion-time rollup that matches our pre-aggregation model, first-class sketch support, and tiered storage as a core architectural concept rather than a bolt-on.

**Pinot is the closest alternative** and would be an equally valid choice. The decision between Druid and Pinot often comes down to organizational familiarity and ecosystem. If the team has Pinot expertise (e.g., ex-LinkedIn/Uber engineers), Pinot is a fine pick.

**ClickHouse would be the better choice IF:**
- The workload evolved to need complex JOINs (e.g., joining metrics with user cohort tables)
- Query concurrency was lower (< 200 QPS) but query complexity was higher (window functions, subqueries)
- Operational simplicity was the top priority (small SRE team)
- The data was NOT pre-aggregated (ClickHouse's raw scan speed on columnar data is unmatched)

**StarRocks is the rising contender** — it combines ClickHouse-class SQL with Pinot/Druid-class concurrency. If building this system from scratch in 2026 with no legacy, StarRocks deserves serious evaluation. Its shared-data architecture on S3 is particularly compelling for cost optimization. The main risk is a thinner production reference base for experimentation-scale workloads.

> **TL;DR:** For append-only pre-aggregated metrics with sketch merging, high concurrency, and tiered storage — Druid/Pinot's segment-based architecture is purpose-built. ClickHouse's column-oriented, scan-heavy architecture shines for different workloads (ad-hoc analytics, complex SQL, lower concurrency). StarRocks is converging on both, but bet on proven for a system where "wrong experiment results" = shipping harmful product changes.

-----