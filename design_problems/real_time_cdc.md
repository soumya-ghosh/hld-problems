# Real-Time Change Data Capture and Synchronization at Enterprise Scale

-----

## Original Problem Statement

Organizations increasingly require their analytical stores, caches, and search indexes to reflect the current state of their production databases in near real-time. Change Data Capture (CDC) is the design pattern that enables the capture of database modifications (inserts, updates, deletes) and the streaming of these events to downstream consumers. For a Senior Engineer, the challenge is not just capturing these changes but doing so without degrading the performance of high-throughput transactional systems.

### The Core Architectural Challenge

The most efficient method of CDC is log-based, which involves reading the database's transaction logs (e.g., MySQL binlog or PostgreSQL WAL) to identify changes. This approach avoids the high latency and read-locks associated with trigger-based or timestamp-polling methods. However, implementing log-based CDC at scale introduces complex issues such as managing log rotation (where logs might be deleted before the consumer catches up), handling schema evolution (where adding a column might break downstream consumers), and ensuring exactly-once delivery semantics to prevent data duplication.

### High-Level Requirements

| **Requirement Type** | **Description** |
| --- | --- |
| **Functional** | Capture all DML operations from source systems with millisecond-level propagation delay. |
| **Functional** | Maintain transactional consistency by preserving transaction boundaries in the event stream. |
| **Functional** | Automatic propagation of schema changes to downstream systems or integration with a schema registry. |
| **Functional** | Support for multiple target sinks including Elasticsearch, Redis, and analytical warehouses. |
| **Non-Functional** | Ordering: Strict sequence guarantees for events per primary key to prevent state corruption. |
| **Non-Functional** | Reliability: Resumability from exact log offsets after system restarts or network failures. |
| **Non-Functional** | Performance: Minimal impact on the source database's write throughput and CPU utilization. |
| **Non-Functional** | Scale: Capability to handle up to 10 million events per minute across global deployments. |

### Nuanced Considerations for Staff Engineers

In high-throughput environments, a common pitfall is the lack of a schema registry, which results in downstream breakage when source field names are modified. A resilient design utilizes a message broker like Apache Kafka organized into topics mapped to unique source tables, enabling services to subscribe to changes asynchronously. Furthermore, to handle the challenges of eventual consistency in microservices, CDC can be paired with the Outbox Pattern or event sourcing to provide a reliable audit trail and decouple service updates from database writes. For zero-downtime migrations, CDC serves as the bridge that synchronizes the old database with a new cloud-native instance, allowing for a seamless switch-over once the systems are in parity.

-----

## Architectural Design Document

### Phase 1: Scoping & Requirements

#### Problem Restatement

We need to build a real-time Change Data Capture pipeline that reads transaction logs (WAL/binlog) from production OLTP databases, streams change events through a durable message backbone, and materializes them into multiple heterogeneous sinks (Elasticsearch, Redis, analytical warehouses) — all with millisecond-class propagation, strict per-key ordering, and zero measurable impact on source write paths.

#### Functional Requirements

| # | Requirement |
|---|-------------|
| F1 | Capture all DML operations (INSERT, UPDATE, DELETE) from MySQL binlog and PostgreSQL WAL. |
| F2 | Preserve transaction boundaries — events within a single transaction must be consumable as an atomic unit. |
| F3 | Propagate DDL / schema changes automatically with backward-compatible evolution via a Schema Registry. |
| F4 | Fan-out to multiple sink types: Elasticsearch (search), Redis (cache invalidation), ClickHouse / data lakehouse (analytics). |
| F5 | Support initial snapshot for bootstrapping a new sink or re-syncing after prolonged downtime. |

#### Non-Functional Requirements

| Attribute | Target |
|-----------|--------|
| End-to-end latency (p99) | < 2 seconds source-commit → sink-visible |
| Throughput | 10 M events/min (~166 K events/sec) across all sources |
| Availability | 99.95 % for the CDC pipeline; source DB availability unaffected |
| Ordering | Strict per-primary-key ordering; causal ordering within a transaction |
| Durability | Zero data loss — every committed WAL entry must reach Kafka |
| Resumability | Restart from exact LSN / binlog position after crash or rolling deploy |
| Source impact | < 2 % additional CPU, < 5 % additional IOPS on source DB |

---

### Phase 2: High-Level Design

#### Back-of-Envelope Math

```
Events/sec         = 10 M / 60         ≈ 166 K events/sec
Avg event size     = ~1 KB (before + after image, metadata, Avro envelope)
Raw throughput     = 166 K × 1 KB      ≈ 166 MB/s
Daily volume       = 166 MB/s × 86 400 ≈ 14 TB/day
Kafka retention    = 7 days            ≈ 100 TB (uncompressed)
  with LZ4 (3:1)                       ≈ 33 TB on-disk
Yearly sink volume = 14 TB × 365       ≈ 5 PB (before compaction/aggregation)
```

At 166 K events/sec, a single Debezium task can handle ~30-50 K events/sec per source, so we need 4-6 parallel connector tasks distributed across a Kafka Connect cluster.

#### Core Components

| Component | Technology | Role |
|-----------|-----------|------|
| Source DBs | PostgreSQL (WAL), MySQL (binlog) | OLTP systems of record |
| CDC Connector | **Debezium** (on Kafka Connect) | Reads WAL/binlog, emits change events |
| Message Backbone | **Apache Kafka** | Durable, ordered, partitioned event log |
| Schema Registry | **Apicurio** (or Confluent Schema Registry) | Avro schema storage, compatibility enforcement |
| Stream Processor | **Apache Flink** (optional) | Enrichment, filtering, routing, deduplication |
| Sink Connectors | Kafka Connect Sinks / Flink Sinks | Materialize events into target stores |
| Search Sink | **Elasticsearch** | Full-text search index |
| Cache Sink | **Redis** | Low-latency key-value cache invalidation |
| Analytics Sink | **ClickHouse** (or S3 → Iceberg → Trino) | OLAP queries on change history |
| Orchestration | **Kubernetes** | Container scheduling, auto-scaling |
| Observability | **Prometheus + Grafana + OTel** | Metrics, alerting, traces |

#### Data Flow (Happy Path)

```
1. Application commits a transaction to PostgreSQL.
2. PostgreSQL appends WAL records for the committed DML.
3. Debezium's logical replication slot reads the WAL stream.
4. Debezium serializes each change into an Avro envelope, registers/validates
   the schema with the Schema Registry, and produces to Kafka.
   - Topic naming: `cdc.<database>.<schema>.<table>`
   - Partition key: hash(primary_key) → guarantees per-PK ordering.
5. Kafka brokers replicate the message (RF=3, min.insync.replicas=2),
   acknowledge the producer.
6. Debezium advances its stored offset (LSN) in a Kafka offset topic.
7. Downstream consumers (Kafka Connect sinks or Flink jobs) poll from Kafka:
   a. Elasticsearch Sink → upsert doc by PK, delete on tombstone.
   b. Redis Sink → SET key with new value, DEL on delete event.
   c. ClickHouse Sink → append to ReplacingMergeTree (dedup on PK + version).
8. Each sink connector commits its consumer offset only after successful write.
```

#### Architecture Diagram

```
┌──────────────┐   ┌──────────────┐
│ PostgreSQL   │   │   MySQL      │
│  (WAL)       │   │  (binlog)    │
└──────┬───────┘   └──────┬───────┘
       │  logical repl.    │  binlog reader
       ▼                   ▼
┌─────────────────────────────────────┐
│       Debezium (Kafka Connect)      │
│  ┌─────────┐ ┌─────────┐ ┌──────┐  │
│  │ Task 1  │ │ Task 2  │ │ ...  │  │
│  └─────────┘ └─────────┘ └──────┘  │
└──────────────────┬──────────────────┘
                   │  Avro + Schema ID
                   ▼
          ┌─────────────────┐
          │  Schema Registry │
          │  (Apicurio)      │
          └─────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│          Apache Kafka               │
│  ┌───────────────────────────────┐  │
│  │ cdc.orders_db.public.orders   │  │
│  │  [P0] [P1] [P2] ... [P63]    │  │
│  ├───────────────────────────────┤  │
│  │ cdc.users_db.public.users     │  │
│  │  [P0] [P1] ... [P31]         │  │
│  └───────────────────────────────┘  │
│  RF=3, min.insync.replicas=2       │
└──────┬──────────┬──────────┬────────┘
       │          │          │
       ▼          ▼          ▼
┌───────────┐ ┌────────┐ ┌───────────────────┐
│Elasticsearch│ │ Redis  │ │ ClickHouse /      │
│  Sink      │ │ Sink   │ │ S3+Iceberg Sink   │
│ (upsert)   │ │ (SET/  │ │ (append +         │
│            │ │  DEL)  │ │  ReplacingMerge)   │
└───────────┘ └────────┘ └───────────────────┘

        ┌────────────────────────┐
        │ Prometheus + Grafana   │
        │ (lag, throughput, err) │
        └────────────────────────┘
```

---

### Phase 3: Deep Dive — Data Model & Storage

#### CDC Event Envelope (Avro)

```json
{
  "source": {
    "connector": "postgresql",
    "db": "orders_db",
    "schema": "public",
    "table": "orders",
    "txId": 58923471,
    "lsn": "0/16B3748",
    "ts_ms": 1708700000000
  },
  "op": "u",
  "before": {
    "id": 42,
    "status": "pending",
    "amount": 10000,
    "updated_at": "2026-02-22T10:00:00Z"
  },
  "after": {
    "id": 42,
    "status": "shipped",
    "amount": 10000,
    "updated_at": "2026-02-23T08:15:00Z"
  },
  "ts_ms": 1708700000123,
  "transaction": {
    "id": "58923471",
    "total_order": 3,
    "data_collection_order": 2
  }
}
```

- `op` values: `c` (create), `u` (update), `d` (delete), `r` (snapshot read), `t` (truncate).
- `transaction` block enables consumers to reconstruct transaction boundaries (Debezium `provide.transaction.metadata=true`).
- `before` image is populated for updates and deletes (`REPLICA IDENTITY FULL` on PostgreSQL).

#### Kafka Topic Design

| Topic | Partition Key | Partitions | Retention |
|-------|--------------|------------|-----------|
| `cdc.<db>.<schema>.<table>` | `hash(PK)` | 64–256 (scaled to throughput) | 7 days (raw) |
| `cdc.schema-changes` | `db.schema.table` | 8 | 30 days |
| `cdc.transaction-metadata` | `txId` | 16 | 3 days |
| `connect-offsets` | connector name | 25 | compacted, infinite |
| `connect-configs` | connector name | 1 | compacted, infinite |

**Partition key = hash(primary_key)** ensures all mutations for a given row land in the same partition, preserving strict per-key ordering. Kafka guarantees ordering within a partition.

#### Sink-Specific Storage Strategy

**Elasticsearch**
- Index-per-table, alias-based rollover.
- Document `_id` = source PK → upserts are idempotent.
- Deletes: Debezium tombstone → delete-by-id.
- Mapping derived from Schema Registry (automated via Kafka Connect SMTs or Flink).

**Redis**
- Key pattern: `<table>:<pk>` → e.g., `orders:42`.
- Value: serialized JSON of the `after` image.
- Delete events → `DEL` key.
- TTL optional for cache-only use cases.
- Pipeline batching for throughput (MULTI/EXEC for atomicity within a transaction boundary).

**ClickHouse (Analytical)**
- Engine: `ReplacingMergeTree(updated_at)` — deduplicates on PK, keeps latest version.
- Partition by: `toYYYYMM(event_time)` for efficient historical queries.
- Order by: `(table_name, pk, event_time)`.
- Alternative path: Kafka → S3 (Parquet via Kafka Connect S3 Sink) → Apache Iceberg → Trino/Spark for a lakehouse approach.

#### Hot / Warm / Cold Tiering

| Tier | Store | Retention | Access Pattern |
|------|-------|-----------|----------------|
| Hot | Kafka (NVMe) | 7 days | Real-time consumers |
| Warm | S3 / MinIO (Parquet) | 90 days | Ad-hoc analytical queries |
| Cold | S3 Glacier / Deep Archive | 7 years | Compliance, audit |

Kafka Tiered Storage (KIP-405, GA in Kafka 3.6+) offloads segments older than 1 day to S3 automatically, reducing broker disk costs by ~70%.

---

### Phase 4: Trade-offs & Justification

#### Debezium vs. Alternatives

| | Debezium | Maxwell | AWS DMS | Custom WAL Reader |
|---|---------|---------|---------|-------------------|
| DB support | PG, MySQL, Mongo, Oracle, SQL Server | MySQL only | Broad (managed) | One DB at a time |
| Schema evolution | Built-in history topic + registry integration | Limited | Opaque | Manual |
| Snapshot support | Yes (initial + incremental) | Yes | Yes | Manual |
| Community / ecosystem | Largest OSS CDC community | Smaller | Proprietary | N/A |
| Operational model | Kafka Connect (distributed, scalable) | Standalone JVM | Managed | Custom |

**Decision**: Debezium. Most mature open-source option, native Kafka Connect integration, handles snapshots and schema history. Similar to how LinkedIn and Uber standardized on Debezium for internal CDC pipelines.

**Rejected**: Maxwell (MySQL-only, weaker schema handling). AWS DMS (vendor lock-in, limited observability, no replayability). Custom WAL reader (high engineering cost, maintenance burden).

#### Kafka vs. Alternatives

| | Kafka | Redpanda | Pulsar | Kinesis |
|---|-------|----------|--------|---------|
| Throughput | Very high (MB/s per partition) | Higher (no JVM GC) | High | Limited (1 MB/s per shard) |
| Ordering | Per-partition | Per-partition | Per-key (subscription ordering) | Per-shard |
| Ecosystem | Massive (Connect, Streams, Flink) | Kafka-compatible | Growing | AWS-only |
| Tiered storage | KIP-405 (GA) | Yes | Built-in | Managed |
| Replayability | Full (offset-based) | Full | Full | 7-day max |
| Operational maturity | Battle-tested at every major tech co | Younger | Complex (ZK + BookKeeper) | Managed |

**Decision**: Kafka. The ecosystem (Kafka Connect for Debezium, Schema Registry, exactly-once support) is unmatched. Tiered storage addresses the historical cost concern.

**Rejected**: Redpanda is compelling (lower tail latency, no JVM) but its Kafka Connect compatibility has edge cases at this scale. Pulsar's architecture (separate serving and storage) adds operational complexity. Kinesis has hard throughput limits per shard and no offset-based replay.

#### Serialization: Avro vs. Protobuf vs. JSON

| | Avro | Protobuf | JSON |
|---|------|----------|------|
| Schema evolution | Excellent (reader/writer schema resolution) | Good (field numbers) | None enforced |
| Size | Compact (no field names in payload) | Compact | 3-5x larger |
| Schema Registry | Native integration | Supported | Manual |
| Debezium support | First-class | Supported | Supported |

**Decision**: Avro with Schema Registry in `BACKWARD_TRANSITIVE` compatibility mode. Downstream consumers can always read events produced by older schemas. This is the standard at Confluent, LinkedIn, and most Kafka-centric orgs.

#### Push vs. Pull

The pipeline is **pull-based at every boundary**:
- Debezium pulls from WAL/binlog (logical replication slot — DB pushes into the slot, Debezium pulls at its own pace).
- Kafka consumers pull from brokers (long-poll with `fetch.min.bytes` / `fetch.max.wait.ms` tuning).

Pull gives natural **backpressure**: if a sink is slow, it simply falls behind on its consumer group offset. No data loss, no OOM — just increased lag, which we alert on.

---

### Phase 5: Reliability, Scaling & Operations

#### Exactly-Once Delivery Semantics

End-to-end exactly-once is achieved in layers:

| Layer | Mechanism |
|-------|-----------|
| Source → Kafka | Debezium reads WAL monotonically. If connector crashes, it restarts from the last committed LSN stored in `connect-offsets`. Duplicate events are possible during recovery → Kafka idempotent producer (`enable.idempotence=true`) deduplicates at the broker. |
| Within Kafka | Kafka transactions ensure atomic writes across topic partitions. |
| Kafka → Sink | Idempotent writes: Elasticsearch upsert-by-PK, Redis SET-by-key, ClickHouse `ReplacingMergeTree`. Even if a message is delivered twice, the result is the same. This gives **effectively-once** semantics. |

#### Failure Scenarios

| Failure | Impact | Recovery |
|---------|--------|----------|
| **Debezium task crash** | CDC pauses for that source | Kafka Connect auto-restarts the task (configurable restart policy). Resumes from stored LSN. |
| **WAL/binlog rotated past offset** | Cannot resume incrementally | Debezium triggers an automatic **snapshot** of the affected tables, then resumes streaming. Alert fires for operator awareness. |
| **Kafka broker loss** | 1 of 3 replicas down | ISR shrinks, no data loss. Producer retries to remaining ISR. Broker replacement via K8s StatefulSet auto-healing. |
| **Kafka partition leader election** | Brief (~ms) write unavailability | Producer retries with `acks=all`. Consumer re-fetches from new leader transparently. |
| **Sink (e.g., ES) outage** | Consumer stops committing offsets | Events buffer in Kafka (7-day retention). Sink connector pauses, resumes when sink recovers. No data loss. |
| **Schema-incompatible change** | Schema Registry rejects the write | DDL blocked from propagating. Alert fires. Engineer must register a compatible schema or bump the subject compatibility level intentionally. |
| **Poison pill message** | Single message fails deserialization | Dead Letter Queue (DLQ) topic. Message routed to `cdc.dlq.<table>`, consumer continues. DLQ monitored + drained by ops. |
| **Network partition (cross-DC)** | MirrorMaker 2 replication paused | Consumers in remote DC read stale data. MM2 catches up once partition heals. RPO = duration of partition. |

#### Handling Log Rotation (Critical Edge Case)

This is the #1 operational risk in log-based CDC. Mitigation:

1. **PostgreSQL**: Use logical replication slots. The slot prevents WAL segments from being recycled until Debezium has consumed them. Risk: unbounded WAL growth if Debezium is down for too long → set `max_slot_wal_keep_size` (PG 13+) as a safety valve, and alert when slot lag exceeds threshold.
2. **MySQL**: Set `binlog_expire_logs_seconds` to at least 7 days. Monitor Debezium's `binlog.position` vs. `SHOW MASTER STATUS`. If the gap exceeds a threshold, trigger a re-snapshot.
3. **Heartbeat table**: Debezium writes a heartbeat to a dedicated table every 10 seconds. This advances the replication slot even during periods of no real traffic, preventing WAL bloat on quiet databases.

#### Scaling Strategy

| Component | Scaling Approach |
|-----------|-----------------|
| Debezium | Horizontal: one connector per source DB, multiple tasks per connector (for MySQL; PG is single-task per slot but can use multiple slots on read replicas). |
| Kafka | Add brokers + reassign partitions. Increase partition count for hot topics. Tiered storage for cost. |
| Flink (if used) | Adjust parallelism per operator. Checkpoint to S3. |
| Sink connectors | Increase `tasks.max` for Kafka Connect sinks. Each task consumes a subset of partitions. |
| Elasticsearch | Add data nodes, increase shard count. Use ILM for rollover. |
| ClickHouse | Add shards (distributed table). Rebalance with `ATTACH PARTITION`. |

#### Observability

**Golden Signals for CDC**

| Signal | Metric | Source | Alert Threshold |
|--------|--------|--------|----------------|
| **Latency** | `debezium_metrics_MilliSecondsBehindSource` | Debezium JMX → Prometheus | > 30 s for 5 min |
| **Latency** | `end_to_end_lag_ms` (event ts vs. sink write ts) | Custom metric in sink connector | > 5 s (p99) |
| **Traffic** | `kafka_server_BrokerTopicMetrics_MessagesInPerSec` | Kafka JMX | Drop > 50% from baseline |
| **Errors** | `kafka_connect_connector_status` (FAILED) | Kafka Connect REST API | Any FAILED status |
| **Errors** | DLQ topic message count | Kafka consumer lag on DLQ topic | > 0 |
| **Saturation** | `kafka_log_Log_Size` per broker | Kafka JMX | > 80% disk |
| **Saturation** | PG replication slot lag bytes | `pg_stat_replication` | > 1 GB |

**SLAs / SLOs**

| SLO | Target |
|-----|--------|
| CDC pipeline availability | 99.95 % (monthly) |
| End-to-end latency (source commit → sink visible) | p50 < 500 ms, p99 < 2 s |
| Data completeness | 100 % of committed transactions captured (zero loss) |
| DLQ drain time | < 4 hours (manual triage SLA) |

**Health Checks**
- Synthetic transaction: a cron job writes a canary row to a monitored table every 60 s. A downstream consumer verifies it arrives in Elasticsearch/Redis within the latency SLO. If not → PagerDuty alert.
- Kafka consumer group lag monitored via Burrow or built-in `kafka-consumer-groups.sh`.

---

### Phase 6: Staff-Level Considerations

#### Outbox Pattern Integration

For microservices that need guaranteed event publishing alongside local DB writes, the **Transactional Outbox** pattern is the recommended approach:

1. Service writes the business entity + an event row into an `outbox` table within the same DB transaction.
2. Debezium captures the outbox row from the WAL.
3. A Debezium SMT (`outbox.EventRouter`) extracts the event payload, routes it to the correct Kafka topic, and discards the outbox wrapper.
4. A periodic job or DB trigger truncates the outbox table to prevent unbounded growth.

This eliminates dual-write problems (writing to DB + Kafka separately) and is the pattern used at companies like Wepay and Zalando.

#### Schema Evolution Workflow

```
Developer adds column
        │
        ▼
CI/CD runs schema compatibility check
against Schema Registry (BACKWARD_TRANSITIVE)
        │
   ┌────┴────┐
   │ Pass    │ Fail → Block merge, notify developer
   ▼         │
Deploy DDL   │
   │         │
   ▼         │
Debezium captures DDL, new Avro schema
auto-registered in Schema Registry
   │
   ▼
Downstream consumers read with old schema →
Avro reader/writer resolution fills new field
with default value. Zero downtime.
```

Breaking changes (renaming a field, changing a type) require a **two-phase migration**: add new field → backfill → deprecate old field → remove.

#### Multi-Region / Global Deployment

For global deployments, use **Kafka MirrorMaker 2** (or Confluent Cluster Linking) to replicate CDC topics across regions:

- Active-passive: CDC runs in the primary region. MM2 replicates to DR region with ~100-500 ms additional latency.
- Active-active: Each region has its own CDC pipeline for local sources. Cross-region topics replicated for global materialized views. Conflict resolution handled at the application layer (last-writer-wins with vector clocks, or CRDTs for specific data types).

#### Zero-Downtime Database Migration

CDC is the backbone for live migrations (e.g., PostgreSQL 14 → 16, or on-prem → cloud):

1. Set up Debezium on the old DB, snapshot + stream to Kafka.
2. A sink connector writes to the new DB.
3. Application dual-reads from both DBs (shadow traffic / feature flag).
4. Once replication lag ≈ 0 and validation passes, cut over writes to the new DB.
5. Reverse CDC from new → old for rollback safety.
6. Decommission old DB after bake period.

This is the approach used at Shopify (ghostferry), GitHub (gh-ost for schema changes), and Stripe for online migrations.

#### Cost Analysis

| Component | Monthly Cost Estimate (at 166K events/sec) |
|-----------|---------------------------------------------|
| Kafka cluster (9 brokers, i3en.xlarge, 3 AZs) | ~$8,100 |
| Kafka tiered storage (S3, ~100 TB warm) | ~$2,300 |
| Kafka Connect cluster (6 nodes, m5.2xlarge) | ~$3,300 |
| Schema Registry (3 nodes, t3.large) | ~$300 |
| Flink cluster (if used, 12 task slots) | ~$2,600 |
| Elasticsearch (6 data nodes, r5.2xlarge) | ~$5,400 |
| ClickHouse (3 shards × 2 replicas) | ~$4,200 |
| Monitoring (Prometheus + Grafana) | ~$500 |
| **Total** | **~$26,700/month** |

Tiered storage reduces Kafka broker cost by ~60-70% vs. keeping all data on NVMe. At 10x scale, Kafka and sinks scale linearly; the biggest lever is compaction and filtering early in the pipeline (via Flink) to reduce downstream volume.

#### Security

| Concern | Mitigation |
|---------|------------|
| Data in transit | TLS 1.3 for all Kafka connections (inter-broker, client-broker). mTLS for Debezium → Kafka. |
| Data at rest | Kafka broker disks encrypted (LUKS or cloud KMS). S3 SSE-S3 or SSE-KMS for tiered storage. |
| Authentication | SASL/SCRAM-SHA-512 for Kafka clients. Service accounts per connector with least-privilege ACLs. |
| PII handling | Field-level encryption via Debezium SMT (`io.debezium.transforms.Filter` + custom SMT for tokenization). PII fields (email, SSN) encrypted before they hit Kafka. Decryption keys managed in HashiCorp Vault, accessible only to authorized sinks. |
| Audit | Kafka's authorizer logs all topic access. CDC events themselves serve as an immutable audit trail. |
| Network | Kafka brokers in private subnets. VPC peering or PrivateLink for cross-account access. No public endpoints. |

#### Evolution — Scaling 10x (1.66 M events/sec)

| Challenge | Strategy |
|-----------|----------|
| Kafka throughput | Add brokers, increase partitions (256–1024 per hot topic), enable LZ4 batch compression. |
| Debezium bottleneck | Shard large tables across multiple logical replication slots (PG) or use read replicas as CDC sources. |
| Sink write amplification | Micro-batch writes (Elasticsearch bulk API, ClickHouse async inserts). Buffer in Flink with windowed flushes. |
| Cost | Aggressive Kafka tiered storage. Filter/aggregate in Flink before sinking (e.g., skip unchanged columns, collapse rapid updates on the same key within a window). |
| Operational complexity | Move to a platform team model. GitOps for connector configs (Kafka Connect REST API driven by ArgoCD). Self-service topic provisioning with guardrails. |

---

### Summary

This design uses **Debezium** on **Kafka Connect** to read database transaction logs, **Apache Kafka** as the durable ordered event backbone, **Apicurio Schema Registry** for schema evolution, and **Kafka Connect sink connectors** (with optional **Flink** enrichment) to materialize changes into Elasticsearch, Redis, and ClickHouse. Per-primary-key ordering is guaranteed by Kafka's partitioning model. Exactly-once semantics are achieved through idempotent producers, WAL-based offset tracking, and idempotent sink writes. The architecture handles log rotation, schema evolution, and multi-region replication, and scales to 10x by adding partitions, sharding CDC sources, and leveraging tiered storage.

**Key References:**
- Debezium architecture: [debezium.io/documentation/reference](https://debezium.io/documentation/reference)
- Kafka Tiered Storage (KIP-405): confluent.io/blog/kafka-tiered-storage
- LinkedIn's CDC at scale: engineering.linkedin.com/blog/2019/brooklin-open-source
- Wepay's Outbox Pattern with Debezium: debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern
- Uber's Schemaless CDC: eng.uber.com/schemaless-rewrite
