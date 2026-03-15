# Comprehensive Big Data Observability and Anomaly Detection

-----

## Original Problem Statement

In a petabyte-scale environment, identifying the root cause of failures or performance degradations is exceptionally difficult due to the high cardinality of metrics across diverse compute engines (Spark, Presto, Flink) and storage solutions (HDFS, S3).^34^ The system design problem is to build an observability and attribution platform that provides transparency into costs, resource usage, and application-level performance.^35^

### The Core Architectural Challenge

The system must track all data I/O patterns across hybrid cloud architectures to power insights into network egress attribution, cross-zone traffic monitoring, and strategic dataset placement.^34^ This involves client-side instrumentation---such as enhancing HDFS or GCS clients---to track call counts and latencies without requiring job code changes.^34^ Handling the sheer volume of metrics---potentially tens of billions of events daily---requires high-throughput aggregation pipelines like Uber's HiCam.^34^

### High-Level Requirements

| **Requirement Type** | **Description** |
| --- | --- |
| **Functional** | Real-time ingestion of query-level metadata and resource usage from diverse compute engines.^34^ |
| **Functional** | Automated anomaly detection for multi-dimensional time series using unsupervised models.^30^ |
| **Functional** | Root cause analysis service that surfaces actionable fixed for engine-level and file-system errors.^35^ |
| **Functional** | Chargeback system that provides high transparency into costs across business dimensions.^34^ |
| **Non-Functional** | Efficiency: Observability platform must maintain low overhead to avoid overwhelming system resources.^34^ |
| **Non-Functional** | Scalability: Capable of handling peak throughput of hundreds of thousands of RPS for engine metrics.^35^ |
| **Non-Functional** | Availability: Resilient architecture with high availability via Zookeeper discovery.^34^ |
| **Non-Functional** | Latency: End-to-end delay from metric emission to analytics visibility within 1 to 5 minutes.^34^ |

### Nuanced Considerations for Staff Engineers

Building a model-agnostic anomaly detection platform is essential for handling distinct seasonality patterns and diverse metrics (e.g., CPU vs. success rates).^30^ Such a system, like Uber's uVitals, passing through a multi-stage pipeline: a Profiler for identifying gaps, a Seasonality Detector for selecting appropriate models, and a Significance Scorer to rank anomalies for alerting.^31^ To ensure backward and forward compatibility, a \"impute-and-inject\" pattern can be used for forecast results stored in historical databases.^30^ Ultimately, this observability should be exposed via aggregated dashboards where engineers can \"slice and dice\" data read/write patterns, top applications causing high egress, and exact network throughput on each dimension.^34^

-----

# Big Data Observability & Anomaly Detection Platform — Architectural Design

---

## 1. Problem Restatement & Scoping

We need to build an **observability, anomaly detection, and cost-attribution platform** for a petabyte-scale data infrastructure spanning multiple compute engines (Spark, Presto, Flink) and storage layers (HDFS, S3/GCS). The platform must ingest tens of billions of telemetry events daily from client-side instrumentation, perform real-time aggregation, detect anomalies across high-cardinality multi-dimensional time series, surface root causes, and expose transparent chargeback dashboards—all with sub-5-minute end-to-end latency and minimal overhead on production workloads.

Reference architecture: Uber's Mastermind observability stack (HiCam aggregation, uVitals anomaly detection, Crane cost attribution).

---

## 2. Requirements

### 2.1 Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-1 | **Metric Ingestion** — Real-time ingestion of query-level metadata, resource usage, and I/O patterns from Spark, Presto, Flink, HDFS, and S3/GCS clients. |
| FR-2 | **High-Throughput Aggregation** — Multi-dimensional roll-up of raw events into pre-aggregated time-series at 1-min, 5-min, 1-hr, and 1-day granularities. |
| FR-3 | **Anomaly Detection** — Automated, model-agnostic detection over multi-dimensional time series using unsupervised methods (statistical + ML). |
| FR-4 | **Root Cause Analysis** — Attribution of anomalies to specific engines, jobs, queries, file-system errors, or infrastructure changes. |
| FR-5 | **Chargeback & Cost Attribution** — Transparent cost breakdown by org, team, project, and job across compute, storage, and network egress dimensions. |
| FR-6 | **Network Egress Attribution** — Track cross-zone / cross-region data movement, attribute traffic to specific datasets and applications. |
| FR-7 | **Interactive Dashboards** — Slice-and-dice analytics on read/write patterns, top applications, network throughput, with drill-down to individual jobs. |

### 2.2 Non-Functional Requirements

| Property | Target |
|----------|--------|
| **Throughput** | 500K–1M RPS sustained ingestion; 10–50 billion events/day |
| **Latency** | End-to-end metric emission → dashboard visibility: **< 5 min** (p99), **< 1 min** (p50) |
| **Availability** | 99.95% for ingestion pipeline; 99.9% for dashboards |
| **Durability** | Zero data loss for billing/chargeback metrics; at-least-once for operational metrics |
| **Overhead** | Instrumentation CPU overhead < 2% on production jobs |
| **Consistency** | Eventual consistency acceptable for dashboards; strong consistency for chargeback ledger |
| **Retention** | Hot: 7 days, Warm: 90 days, Cold: 3 years |

---

## 3. Back-of-Envelope Estimation

```
Events/day:        30 billion (midpoint)
Avg event size:    ~500 bytes (structured, after client-side batching)
Raw daily volume:  30B × 500B = 15 TB/day
Peak RPS:          30B / 86400 ≈ 350K avg → ~1M peak (3x burst factor)

Kafka throughput:  1M msgs/s × 500B = 500 MB/s sustained → ~1.5 GB/s peak
                   Replication factor 3 → ~4.5 GB/s disk write throughput

Storage (raw):     15 TB/day × 365 = ~5.5 PB/year (before compression)
Storage (compressed): Parquet + Zstd → ~5:1 ratio → ~1.1 PB/year
Pre-aggregated:    ~2% of raw volume → ~30 TB/year (hot tier)

Anomaly detection: ~10M distinct time-series (high cardinality from
                   engine × job × cluster × metric × dimension combos)
                   Each series scored every 5 min → ~33K series/sec
```

---

## 4. High-Level Architecture

### 4.1 Component Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        INSTRUMENTATION LAYER                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────┐ ┌──────────┐ │
│  │Spark Hook│  │Presto    │  │Flink     │  │HDFS Client│ │GCS/S3    │ │
│  │(Plugin)  │  │EventLsnr │  │MetricRptr│  │Interceptor│ │Interceptor│ │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └─────┬─────┘ └─────┬────┘ │
│       │              │              │              │             │      │
│       └──────────────┴──────┬───────┴──────────────┴─────────────┘      │
│                             │ (batched, async, protobuf)                │
│                        ┌────▼─────┐                                     │
│                        │ Local    │  Ring buffer + flush every 10s      │
│                        │ Agent    │  or 10K events, whichever first     │
│                        └────┬─────┘                                     │
└─────────────────────────────┼───────────────────────────────────────────┘
                              │ gRPC / HTTP2
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        COLLECTION TIER                                  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                  Apache Kafka  (3 clusters)                      │   │
│  │                                                                  │   │
│  │  Topics:                                                         │   │
│  │   • raw-metrics          (partitioned by cluster_id, 256 parts)  │   │
│  │   • raw-io-events        (partitioned by job_id, 256 parts)      │   │
│  │   • cost-events          (partitioned by org_id, 64 parts)       │   │
│  │   • anomaly-alerts       (partitioned by metric_id, 32 parts)    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌────────────────────┐                                                 │
│  │ Schema Registry    │  (Protobuf schemas, backward-compatible)       │
│  │ (Buf / Confluent)  │                                                 │
│  └────────────────────┘                                                 │
└─────────────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────────┐
              ▼               ▼                   ▼
┌───────────────────┐ ┌──────────────┐  ┌─────────────────────┐
│  STREAM PROCESSOR │ │ BATCH LAYER  │  │ ANOMALY DETECTION   │
│  (Apache Flink)   │ │ (Spark)      │  │ PIPELINE (Flink)    │
│                   │ │              │  │                     │
│ • 1-min rollups   │ │ • Hourly/    │  │ • Profiler          │
│ • Sessionization  │ │   daily      │  │ • Seasonality Det.  │
│ • Real-time egress│ │   compaction │  │ • Model Selector    │
│   attribution     │ │ • ML model   │  │ • Significance      │
│ • Dedup & enrich  │ │   training   │  │   Scorer            │
│                   │ │ • Backfill   │  │ • Alert Router      │
└───────┬───────────┘ └──────┬───────┘  └──────────┬──────────┘
        │                    │                     │
        ▼                    ▼                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        STORAGE TIER                                     │
│                                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ ┌─────────────┐  │
│  │ ClickHouse   │  │ Apache       │  │ PostgreSQL   │ │ Redis       │  │
│  │ (Hot)        │  │ Iceberg on   │  │ (Chargeback  │ │ Cluster     │  │
│  │              │  │ S3/HDFS      │  │  Ledger +    │ │ (Cache +    │  │
│  │ 7-day window │  │ (Warm/Cold)  │  │  Metadata)   │ │  Rate Limit)│  │
│  │ Pre-agg +    │  │              │  │              │ │             │  │
│  │ raw samples  │  │ 90d warm     │  │ Strong       │ │ Dashboard   │  │
│  │ Real-time    │  │ 3yr cold     │  │ consistency  │ │ query cache │  │
│  │ analytics    │  │ Partitioned  │  │ for billing  │ │ Anomaly     │  │
│  │              │  │ by date+eng  │  │              │ │ state       │  │
│  └──────────────┘  └──────────────┘  └──────────────┘ └─────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ M3DB / VictoriaMetrics  — time-series store for anomaly models  │   │
│  │ Stores: forecasts, baselines, anomaly scores, thresholds        │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        SERVING & PRESENTATION                           │
│                                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ ┌─────────────┐  │
│  │ Query API    │  │ Anomaly API  │  │ Chargeback   │ │ Alert       │  │
│  │ (GraphQL +   │  │ (gRPC)       │  │ API (REST)   │ │ Manager     │  │
│  │  REST)       │  │              │  │              │ │ (PagerDuty/ │  │
│  │              │  │ RCA drill-   │  │ Cost reports │ │  OpsGenie)  │  │
│  │ Dashboards   │  │ down         │  │ Invoice gen  │ │             │  │
│  └──────────────┘  └──────────────┘  └──────────────┘ └─────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ Grafana (operational) + Custom React UI (chargeback/analytics)  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Data Flow — Happy Path

```
1. Spark job executes on cluster-A
     │
2. SparkListenerPlugin captures task-level metrics (CPU, memory, shuffle,
   I/O bytes, duration) + HDFS Client Interceptor captures read/write
   call counts, latencies, bytes transferred, remote datacenter flag
     │
3. Local Agent batches events (protobuf), applies local sampling
   for high-frequency metrics (reservoir sampling, 10% for DEBUG-level),
   flushes to Kafka via async gRPC → Collection Gateway (stateless,
   auto-scaled on K8s)
     │
4. Kafka raw-metrics topic receives events; Schema Registry validates
     │
5. Flink Streaming Job #1 (HiCam-style aggregator):
   ├─ Keyed by (cluster, engine, job_id, metric_name)
   ├─ Tumbling 1-min windows with allowed lateness = 30s
   ├─ Emits pre-aggregated records → ClickHouse (hot)
   └─ Emits raw events → Iceberg sink (warm/cold)
     │
6. Flink Streaming Job #2 (Anomaly Pipeline):
   ├─ Reads 1-min aggregates from internal Kafka topic
   ├─ Profiler: checks data completeness, fills gaps (impute-and-inject)
   ├─ Seasonality Detector: classifies series (daily, weekly, none)
   ├─ Model Selector: routes to appropriate model
   │   ├─ STL + 3-sigma for strongly seasonal
   │   ├─ DBSCAN for multi-dimensional cluster-based
   │   └─ Isolation Forest for high-cardinality sparse series
   ├─ Significance Scorer: ranks anomaly severity, deduplicates
   └─ Writes anomaly events → anomaly-alerts topic + M3DB
     │
7. Alert Manager picks up critical anomalies → PagerDuty/OpsGenie
     │
8. Spark Batch (hourly): compacts 1-min aggregates into 1-hr/1-day
   rollups, retrains ML models on 90-day history, computes chargeback
   ledger entries → PostgreSQL
     │
9. Dashboard user opens Grafana or custom UI:
   ├─ Query API reads from ClickHouse (last 7d real-time)
   ├─ Falls back to Iceberg via Trino for historical (>7d)
   ├─ Redis caches frequent dashboard queries (TTL 60s)
   └─ Chargeback API reads from PostgreSQL
```

---

## 5. Deep Dive — Data Model & Storage

### 5.1 Raw Metric Event (Protobuf)

```protobuf
message MetricEvent {
  string   event_id       = 1;   // UUID v7 (time-sortable)
  int64    timestamp_ms   = 2;
  string   cluster_id     = 3;
  string   engine         = 4;   // SPARK | PRESTO | FLINK
  string   job_id         = 5;
  string   query_id       = 6;
  string   user_id        = 7;
  string   org_id         = 8;
  string   metric_name    = 9;
  double   metric_value   = 10;
  map<string, string> dimensions = 11;  // arbitrary k/v tags
  IODetail io_detail      = 12;
}

message IODetail {
  string storage_type   = 1;   // HDFS | S3 | GCS
  string path           = 2;
  int64  bytes          = 3;
  int32  call_count     = 4;
  int32  latency_p99_us = 5;
  string src_zone       = 6;
  string dst_zone       = 7;
}
```

### 5.2 ClickHouse Schema (Hot Tier)

```sql
CREATE TABLE metric_aggregates ON CLUSTER '{cluster}'
(
    ts              DateTime64(3) CODEC(DoubleDelta),
    cluster_id      LowCardinality(String),
    engine          LowCardinality(String),
    job_id          String,
    org_id          LowCardinality(String),
    metric_name     LowCardinality(String),
    -- pre-aggregated per 1-min window
    sum_value       Float64     CODEC(Gorilla),
    count           UInt64      CODEC(T64),
    min_value       Float64     CODEC(Gorilla),
    max_value       Float64     CODEC(Gorilla),
    p50             Float64     CODEC(Gorilla),
    p99             Float64     CODEC(Gorilla),
    -- network egress attribution
    src_zone        LowCardinality(String),
    dst_zone        LowCardinality(String),
    egress_bytes    UInt64      CODEC(T64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/metric_aggregates/{shard}', '{replica}')
PARTITION BY toDate(ts)
ORDER BY (cluster_id, engine, metric_name, ts)
TTL ts + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;
```

**Why ClickHouse over Druid:**
- Native SQL support simplifies integration with Grafana and ad-hoc queries.
- Superior compression (Gorilla + DoubleDelta) for time-series numeric data—critical at 15 TB/day.
- Materialized views for automatic roll-up from 1-min → 5-min → 1-hr without separate Spark jobs.
- Vectorized execution engine handles high-cardinality GROUP BY (10M+ distinct series) efficiently.
- **Why not Druid:** Druid's real-time ingestion requires Kafka indexing service with more operational overhead; query flexibility is limited (no JOINs). At Uber's scale, their shift from Druid to ClickHouse-like AresDB corroborates this trade-off.

### 5.3 Apache Iceberg on S3 (Warm/Cold Tier)

```
Iceberg Table: observability.raw_events
├── Partition: date(ts), engine
├── Sort Order: cluster_id, job_id, ts
├── File Format: Parquet + Zstd (level 3)
├── Compaction: hourly minor, daily major
└── TTL: 90 days warm (NVMe-backed S3 Express), 3 years cold (S3 Glacier IR)
```

**Why Iceberg over Delta Lake / Hudi:**
- Engine-agnostic: works with Spark, Flink, Trino, and ClickHouse (via Iceberg connector).
- Hidden partitioning avoids user-facing partition column pollution.
- Time-travel enables auditable chargeback queries ("what was the cost on March 1st at the time of invoice?").
- Partition evolution allows changing partition schemes without rewriting data as cardinality patterns shift.

### 5.4 PostgreSQL (Chargeback Ledger)

```sql
CREATE TABLE chargeback_ledger (
    id              BIGSERIAL PRIMARY KEY,
    period_start    TIMESTAMPTZ NOT NULL,
    period_end      TIMESTAMPTZ NOT NULL,
    org_id          TEXT NOT NULL,
    team_id         TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    engine          TEXT NOT NULL,
    cost_dimension  TEXT NOT NULL,  -- COMPUTE | STORAGE | EGRESS
    usage_units     NUMERIC(20,4) NOT NULL,
    unit_price      NUMERIC(12,6) NOT NULL,
    total_cost      NUMERIC(20,4) NOT NULL,
    currency        TEXT DEFAULT 'USD',
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_chargeback_period_org ON chargeback_ledger (period_start, org_id);
```

**Why PostgreSQL for chargeback:** Strong consistency guarantees, ACID transactions for financial-grade ledger entries, rich query capabilities for ad-hoc cost analysis. Volume is manageable (~millions of rows/month after aggregation).

### 5.5 M3DB (Anomaly Time-Series Store)

Stores forecasted baselines, anomaly scores, and model metadata per time-series. Chosen over Prometheus for its native distributed architecture, multi-tenancy, and long-term retention without federation hacks. VictoriaMetrics is a viable alternative if operational simplicity is preferred (single binary, less cluster complexity).

### 5.6 Caching Strategy

| Layer | Technology | Pattern | TTL |
|-------|-----------|---------|-----|
| Dashboard queries | Redis Cluster | Look-aside (cache-aside) | 60s |
| Anomaly state (per series) | Redis | Write-through from Flink | 5 min |
| Metadata (cluster/job info) | Local in-process (Caffeine) | Look-aside | 5 min |
| Chargeback reports | Redis | Cache-aside, invalidate on ledger write | 1 hr |

---

## 6. Anomaly Detection Pipeline — Deep Dive

This follows a multi-stage architecture inspired by Uber's uVitals.

```
                    ┌──────────────────────────┐
                    │   1-min Aggregated Stream │
                    │   (from HiCam/Flink)      │
                    └────────────┬───────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │   PROFILER                │
                    │                          │
                    │ • Detect gaps in series   │
                    │ • Impute missing points   │
                    │   (last-value or linear)  │
                    │ • Tag data quality score  │
                    └────────────┬───────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │   SEASONALITY DETECTOR    │
                    │                          │
                    │ • FFT-based periodogram   │
                    │ • Classify: daily/weekly/ │
                    │   none/multi-seasonal     │
                    │ • Cache classification    │
                    │   (recompute weekly)      │
                    └────────────┬───────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │   MODEL SELECTOR          │
                    │                          │
                    │ Seasonal → STL decomp +  │
                    │   3-sigma / MSTL         │
                    │ Non-seasonal → Isolation  │
                    │   Forest / DBSCAN        │
                    │ Low-volume → Simple       │
                    │   threshold rules        │
                    │ GPU cluster for heavy     │
                    │   models (Prophet, LSTM)  │
                    │   invoked via async RPC   │
                    └────────────┬───────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │   SIGNIFICANCE SCORER     │
                    │                          │
                    │ • Anomaly score 0–100     │
                    │ • Business impact weight  │
                    │   (revenue, SLA-critical) │
                    │ • Dedup: suppress repeat  │
                    │   alerts within 30-min    │
                    │   window per series       │
                    │ • Correlation: group      │
                    │   co-occurring anomalies  │
                    └────────────┬───────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │   ROOT CAUSE ANALYZER     │
                    │                          │
                    │ • Trace anomaly to:       │
                    │   - Specific job/query    │
                    │   - Node / rack / zone    │
                    │   - Recent deployment     │
                    │   - Resource contention   │
                    │ • Uses dimensional drill- │
                    │   down (top-k contributors│
                    │   via subtractive method) │
                    │ • Enriches with deploy    │
                    │   changelog (git/CD API)  │
                    └────────────┬───────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │   ALERT ROUTER            │
                    │                          │
                    │ Score ≥ 80 → PagerDuty   │
                    │ Score 50-79 → Slack      │
                    │ Score < 50 → Dashboard   │
                    │ All → anomaly-alerts topic│
                    └──────────────────────────┘
```

### Model-Agnostic Design

The pipeline treats models as pluggable strategies behind an interface:

```python
class AnomalyModel(Protocol):
    def fit(self, series: TimeSeries, config: ModelConfig) -> FittedModel: ...
    def predict(self, fitted: FittedModel, window: TimeSeries) -> AnomalyResult: ...
    def serialize(self, fitted: FittedModel) -> bytes: ...
```

New models (e.g., a transformer-based detector) can be registered in a model registry (backed by PostgreSQL metadata + S3 artifacts) and activated per-metric-class via feature flags without pipeline redeployment. This is critical for handling distinct metric behaviors—CPU utilization has different seasonality patterns than query success rates.

### Impute-and-Inject Pattern

When forecast results are stored to M3DB, we use a dual-write with versioned schema:

```
write(series_id, timestamp, {
    "v": 2,
    "forecast": 142.5,
    "upper_bound": 165.0,
    "lower_bound": 120.0,
    "model": "mstl_v3",
    "score": null  // injected by scorer later
})
```

This ensures backward compatibility when models change output schemas—old readers ignore unknown fields, new readers handle missing fields with defaults.

---

## 7. Network Egress Attribution — Deep Dive

### 7.1 Client-Side Instrumentation

The HDFS and GCS/S3 clients are enhanced at the FileSystem abstraction layer (Hadoop `FileSystem.open()` / `FileSystem.create()`):

```java
public class InstrumentedHdfsClient extends DistributedFileSystem {
    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        long start = System.nanoTime();
        FSDataInputStream stream = super.open(f, bufferSize);
        long latency = System.nanoTime() - start;

        MetricEvent event = MetricEvent.newBuilder()
            .setMetricName("hdfs.read.open")
            .setCallCount(1)
            .setLatencyUs(latency / 1000)
            .setSrcZone(localZone)
            .setDstZone(resolveZone(f))
            .build();

        localAgent.emit(event);  // non-blocking, ring-buffer backed
        return new InstrumentedInputStream(stream, localAgent, f);
    }
}
```

**Key design decisions:**
- Wraps at the `FileSystem` abstraction, not at individual RPC level — captures all I/O regardless of higher-level library (Parquet, ORC, Avro readers).
- `localAgent.emit()` is non-blocking with a bounded ring buffer (128K slots). Overflow = drop + increment a counter metric. This ensures instrumentation never blocks the data path.
- Zone resolution is cached in a local lookup table refreshed every 60s from a metadata service.

### 7.2 Egress Cost Computation (Flink)

```
Cross-zone traffic detected when src_zone ≠ dst_zone

egress_cost = bytes_transferred × zone_pair_price_per_GB

zone_pair_price table maintained in PostgreSQL, updated monthly.
Flink job enriches each I/O event with zone-pair price via
broadcast state (pushed from a Kafka topic on price changes).
```

---

## 8. Trade-offs & Justification

### 8.1 Apache Kafka — Message Bus

| Aspect | Decision |
|--------|----------|
| **Why Kafka** | Decouples producers from consumers. Replay capability enables reprocessing after bug fixes. Handles 1M+ msgs/s proven at LinkedIn/Uber/Netflix scale. Partitioned log provides ordering within partition (keyed by job_id). |
| **Why not Pulsar** | Kafka's ecosystem maturity (Connect, Streams, Schema Registry) is deeper. Pulsar's tiered storage is compelling but Iceberg sink handles our cold-tier needs. |
| **Why not direct gRPC streaming** | Kafka provides durable buffering during consumer downtime. Without it, a Flink restart means data loss during the recovery window. |

### 8.2 Apache Flink — Stream Processing

| Aspect | Decision |
|--------|----------|
| **Why Flink** | True event-time processing with watermarks handles out-of-order events from distributed clusters. Exactly-once semantics with Kafka connector. Savepoints enable zero-downtime upgrades. |
| **Why not Spark Structured Streaming** | Spark SS micro-batch model adds latency (100ms–seconds per batch). For sub-minute aggregation windows, Flink's per-event model is more natural. Flink also has lower memory footprint for stateful windowing. |
| **Why not Kafka Streams** | Kafka Streams lacks native windowed aggregation with allowed-lateness semantics. Also, we need the Flink ecosystem for ML model serving (ONNX runtime integration). |

### 8.3 ClickHouse — Hot Analytics Store

| Aspect | Decision |
|--------|----------|
| **Why ClickHouse** | Column-oriented, vectorized execution, exceptional compression for time-series. Handles high-cardinality GROUP BY that Druid struggles with. Native Kafka engine for direct ingestion. Materialized views for automatic rollup. |
| **Why not TimescaleDB** | TimescaleDB (Postgres-based) doesn't match ClickHouse's raw scan throughput at our data volume (15 TB/day). |
| **Why not Elasticsearch** | ES is optimized for full-text search, not numerical aggregation. Storage overhead 2-3x higher for structured metrics. |

### 8.4 Push vs. Pull Ingestion

**Decision: Push model with client-side batching.**

- Agents push to collection gateways via gRPC. At 1M RPS from thousands of nodes, a pull model (like Prometheus scraping) would require a massive scraper fleet and creates scheduling complexity.
- Pull is viable for a small number of exporters; at our cardinality (tens of thousands of compute nodes), push with client-side batching is the only practical approach. This aligns with how Uber's HiCam and Meta's Scuba handle ingestion.

### 8.5 CAP Trade-offs

| Subsystem | Choice | Rationale |
|-----------|--------|-----------|
| Metric ingestion pipeline | **AP** (Available + Partition-tolerant) | During network partitions, it's better to keep ingesting (possibly with duplicates) than to reject metrics. Flink deduplicates downstream. |
| Chargeback ledger | **CP** (Consistent + Partition-tolerant) | Financial data requires strong consistency. PostgreSQL with synchronous replication. Briefly unavailable during partition is acceptable for billing. |
| Dashboard queries | **AP** | Stale data (up to 60s via Redis cache) is acceptable. ClickHouse replicas serve reads even if one replica lags. |

---

## 9. Reliability, Scaling & Operations

### 9.1 Bottleneck Analysis & Mitigation

| Bottleneck | Risk | Mitigation |
|------------|------|------------|
| **Kafka broker hot partition** | Skewed partition key (one large cluster produces 50% of traffic) | Use composite partition key `hash(cluster_id + metric_name)` to distribute. Monitor partition lag, rebalance with cruise-control. |
| **ClickHouse write amplification** | MergeTree compaction during peak ingestion | Separate ingestion replicas from query replicas (read/write split). Use `Buffer` engine as ingestion buffer. |
| **Flink state size** | 10M keyed time-series × window state = multi-TB RocksDB state | Use incremental checkpointing. Tune RocksDB block cache. Consider state TTL to evict inactive series. |
| **Anomaly model scoring throughput** | 33K series/sec scoring | Horizontally scale Flink anomaly job. Use lightweight models (STL + 3-sigma) for 95% of series; only route complex series to GPU inference. |

### 9.2 Failure Handling

**Kafka broker failure:**
- Replication factor 3, min ISR 2. Automatic leader election. Producers retry with idempotent=true. Zero data loss.

**Flink job failure:**
- Exactly-once via Kafka transactional producer + Flink checkpointing (every 60s, incremental). Recovery time: ~30s from last checkpoint. Savepoints taken before any deployment.

**ClickHouse node failure:**
- ReplicatedMergeTree with 2 replicas per shard. ZooKeeper (or ClickHouse Keeper) coordinates replication. Automatic repair from replica. Load balancer health-checks route reads away from failed nodes.

**Region outage:**
- Kafka MirrorMaker 2 replicates critical topics to DR region. Iceberg data is on S3 (cross-region replication enabled for chargeback data). ClickHouse DR cluster in standby with async replication.

**Poison pill / corrupt events:**
- Schema Registry rejects malformed messages at ingestion.
- Flink dead-letter queue (DLQ) topic for events that fail processing. DLQ consumer retries with backoff, alerts after 3 failures.
- Circuit breaker pattern on anomaly model scoring: if a model consistently errors, fall back to simple threshold detection and flag for investigation.

### 9.3 Backpressure & Rate Limiting

```
Collection Gateway:
├── Per-cluster rate limit: 100K RPS (token bucket)
├── Global rate limit: 1.5M RPS
├── Backpressure signal: HTTP 429 → agent backs off (exponential, max 30s)
└── Priority lanes: chargeback events > anomaly metrics > debug traces

Flink:
├── Backpressure propagates naturally via Flink's credit-based flow control
├── If consumer lag > 10 min → scale out Flink parallelism (KEDA autoscaler)
└── If lag > 30 min → trigger alert, consider dropping low-priority metrics
```

### 9.4 Observability of the Observability Platform (Meta-Monitoring)

| Signal | Metric | SLO | Alert |
|--------|--------|-----|-------|
| **Latency** | p99 ingestion-to-ClickHouse latency | < 5 min | > 10 min for 5 consecutive minutes |
| **Traffic** | Events/sec into Kafka | Baseline ± 30% | Sudden drop > 50% (indicates agent failure) |
| **Errors** | DLQ rate | < 0.01% of total events | > 0.1% for 5 min |
| **Saturation** | Kafka partition lag (max) | < 5 min | > 15 min |
| **Saturation** | ClickHouse merge queue depth | < 100 | > 500 |
| **Saturation** | Flink checkpoint duration | < 30s | > 120s |

**Health checks:**
- Synthetic transaction: every 60s, inject a canary event through the full pipeline. Measure round-trip time from emission to ClickHouse query visibility. Alert if > 10 min.
- Heartbeat: each agent emits a heartbeat every 30s. Missing heartbeats for > 5 min trigger an agent-down alert.

### 9.5 SLAs / SLOs

| Service | SLO | SLA (external) |
|---------|-----|-----------------|
| Metric ingestion | 99.95% availability, < 5 min e2e latency (p99) | 99.9% |
| Anomaly detection | 99.9% availability, < 10 min detection latency | 99.5% |
| Dashboard queries | 99.9% availability, < 2s query latency (p95) | 99.5% |
| Chargeback reports | 99.99% data accuracy, daily report delivery by 06:00 UTC | 99.9% |

---

## 10. Staff-Level Considerations

### 10.1 Cost Analysis

```
Component             Monthly Estimate (at scale)
────────────────────────────────────────────────
Kafka (36 brokers)    ~$50K  (i3en.2xlarge or equivalent)
Flink (24 TMs)        ~$30K  (m5.4xlarge, spot for batch)
ClickHouse (12 nodes) ~$40K  (r5d.4xlarge, NVMe)
S3 / Iceberg          ~$25K  (1.1 PB/yr, mostly Glacier IR)
M3DB (6 nodes)        ~$8K
PostgreSQL (HA)       ~$3K
Redis Cluster         ~$5K
Compute (agents)      ~$0   (amortized into existing fleet)
────────────────────────────────────────────────
Total                 ~$161K/month (~$1.9M/year)
```

**Cost optimization levers:**
- **Tiered storage on Kafka:** Move segments older than 2 hours to S3 (Confluent Tiered Storage or Redpanda). Reduces broker storage by 70%.
- **Aggressive rollup:** Keep 1-min granularity for 24h, 5-min for 7d, 1-hr for 90d. Reduces ClickHouse storage by 10x.
- **Spot instances:** Flink TaskManagers and Spark batch jobs run on spot (with checkpointing for Flink, retry for Spark). Saves ~60% on compute.
- **Sample high-volume debug metrics:** 10% sampling for trace-level data. Full fidelity only for SLA-critical and chargeback metrics.

### 10.2 Security

| Concern | Implementation |
|---------|----------------|
| **Encryption in transit** | mTLS between all services. Kafka uses SASL/SCRAM + TLS. |
| **Encryption at rest** | S3 SSE-S3. ClickHouse disk encryption. PostgreSQL TDE. |
| **PII handling** | Metric events should not contain PII by design. Query text (which may contain PII) is hashed before storage. Opt-in full-text storage with 30-day TTL and RBAC. |
| **Access control** | RBAC on dashboards. Chargeback data scoped by org_id. ClickHouse users with row-level security policies. |
| **Audit** | All chargeback ledger mutations logged in an append-only audit table. |

### 10.3 Evolution — Scaling 10x

| Dimension | Current | 10x Strategy |
|-----------|---------|--------------|
| **Events/day** | 30B → 300B | Add Kafka clusters (federated). Flink scales horizontally. ClickHouse adds shards. Iceberg handles unlimited S3 scale. |
| **Time-series cardinality** | 10M → 100M | Shard M3DB by metric namespace. Consider switching to VictoriaMetrics (better high-cardinality handling). Pre-filter low-value series with adaptive sampling. |
| **Query load** | 100 QPS → 1K QPS | ClickHouse read replicas behind load balancer. Materialized views for top-N dashboards. Redis cache hit rate target > 80%. |
| **Multi-region** | Single region → Multi-region | Kafka MirrorMaker 2 for cross-region replication. Regional ClickHouse clusters with global query federation via ClickHouse Distributed engine. Iceberg on S3 with cross-region replication for cold data. |
| **ML models** | Statistical → Deep Learning | GPU inference cluster (NVIDIA Triton) behind async gRPC. Flink routes complex series to GPU pool, falls back to CPU-based models under load. |

### 10.4 Organizational Considerations

- **Team structure:** Platform team owns ingestion + storage. ML team owns anomaly pipeline. Product team owns dashboards + chargeback. SRE owns meta-monitoring.
- **Rollout strategy:** Dark-launch anomaly detection alongside existing monitoring. Shadow-mode for chargeback (show but don't bill) for 2 months. Gradual agent rollout cluster-by-cluster with kill switch.
- **Schema evolution:** Protobuf with field numbering ensures wire-compatible evolution. Schema Registry enforces backward compatibility checks in CI.

---

## 11. Architecture Decision Records (Summary)

| ADR | Decision | Status |
|-----|----------|--------|
| ADR-001 | Use Kafka over Pulsar for message bus | Accepted |
| ADR-002 | Use Flink over Spark Streaming for real-time aggregation | Accepted |
| ADR-003 | Use ClickHouse over Druid for hot analytics | Accepted |
| ADR-004 | Use Iceberg over Delta Lake for cold storage | Accepted |
| ADR-005 | Push model with client-side agents over pull-based scraping | Accepted |
| ADR-006 | Model-agnostic anomaly pipeline with pluggable strategies | Accepted |
| ADR-007 | PostgreSQL for chargeback ledger (strong consistency) | Accepted |
| ADR-008 | M3DB for anomaly time-series over Prometheus | Accepted |
| ADR-009 | Protobuf over Avro for event serialization (performance + strict typing) | Accepted |

---

## References

- Uber Engineering: "Crane: Uber's Next-Gen Infrastructure Cost Control Platform" (2023)
- Uber Engineering: "uVitals: Anomaly Detection for Uber's Real-Time Monitoring" (2022)
- Uber Engineering: "HiCam: High-Throughput Aggregation at Uber" (2021)
- ClickHouse Inc.: "ClickHouse for Observability" benchmark papers
- Netflix Tech Blog: "Lessons from Building Observability Tools at Netflix" (2023)
- Apache Iceberg documentation: Partition Evolution and Hidden Partitioning
- Meta Engineering: "Scuba: Diving into Data at Facebook" (VLDB 2013)
