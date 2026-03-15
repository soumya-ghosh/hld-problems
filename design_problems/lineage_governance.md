# Enterprise Metadata, Lineage, and Governance Frameworks

-----

## Original Problem Statement

As data ecosystems mature, the \"metadata problem\" becomes the primary bottleneck for scaling. Data lineage traces the journey of data through every pipeline and transformation, allowing teams to troubleshoot issues, ensure compliance, and understand impact analysis. The design challenge is to build a unified metadata platform, such as LinkedIn's DataHub, that supports fine-grained lineage and dataset observability.

### The Core Architectural Challenge

Metadata collection must combine pull-based crawlers (for backfilling) and push-based emitters (for real-time freshness) from diverse sources like warehouses, orchestration tools, and BI platforms. The lineage graph must be captured at both the table and column level, which requires parsing SQL logic and ETL code to infer dependencies. Storing this information requires a multi-engine approach: a Graph store for lineage relationships, a Document or Relational store for entity snapshots, and a Search engine for discovery.

### High-Level Requirements

| **Requirement Type** | **Description** |
| --- | --- |
| **Functional** | Automated capture of table-level and column-level lineage from SQL and job plans. |
| **Functional** | Metadata catalog supporting entities like datasets, jobs, dashboards, users, and policies. |
| **Functional** | Automated classification of sensitive data with lineage-based propagation of tags. |
| **Functional** | Integration of data quality signals (freshness, null counts) directly onto the dataset pages. |
| **Non-Functional** | Scalability: Programmatic policies to enforce standards across thousands of assets without manual overhead. |
| **Non-Functional** | Accuracy: Runtime emission of lineage events during job execution to ensure accurate time-scoped graphs. |
| **Non-Functional** | Consistency: Detecting schema changes and notifying downstream owners of breaking updates. |
| **Non-Functional** | Discovery: Global search with synoynms and ranking based on dataset usage and quality. |

### Nuanced Considerations for Staff Engineers

The \"Gold Standard\" for lineage is column-level impact analysis, which enables selective masking and precise debugging of data issues. Staff Engineers should emphasize the role of \"Data Contracts\"---verifiable assertions on physical assets that act as producer commitments to consumers. Governance integration makes lineage \"actionable,\" allowing data stewards to understand policy violations or investigate quality alerts by tracing them to the source. Programmable APIs and webhooks are essential for ensuring that other systems can react to metadata changes (e.g., gating production writes if an unapproved breaking change is detected).

-----

# Enterprise Metadata, Lineage & Governance Platform — Architectural Design

-----

## Phase 1: Scoping & Clarification

### Problem Restatement

We need to design a **unified metadata platform** — akin to LinkedIn's DataHub or Amundsen — that serves as the single source of truth for all metadata across a modern data stack. The platform must capture **fine-grained lineage** (table-level and column-level), integrate **data quality signals**, enforce **governance policies programmatically**, and power **dataset discovery** for thousands of data practitioners. The system bridges ETL pipelines, warehouses, BI tools, and orchestration engines into one searchable, navigable graph.

### Functional Requirements

| # | Requirement |
|---|---|
| FR-1 | Automated capture of **table-level and column-level lineage** from SQL, Spark plans, and orchestrator DAGs. |
| FR-2 | Metadata catalog supporting first-class entities: **Datasets, Jobs/Pipelines, Dashboards, Users, Tags, Glossary Terms, Policies**. |
| FR-3 | Automated **classification of sensitive data** (PII, PHI, PCI) with **lineage-based tag propagation** (if a source column is PII, all downstream columns inherit the tag unless explicitly overridden). |
| FR-4 | Integration of **data quality signals** (freshness, volume, null rate, schema drift) surfaced on dataset detail pages. |
| FR-5 | **Data Contracts**: producer-defined assertions on schema shape, SLAs, and quality thresholds that consumers can subscribe to. |
| FR-6 | **Impact Analysis API**: given a column or table, return the complete upstream/downstream dependency graph. |
| FR-7 | **Programmable governance**: policy-as-code engine that can gate production writes, emit alerts, or block breaking schema changes. |
| FR-8 | **Webhooks & Change Events**: any metadata mutation emits an event that external systems can subscribe to. |
| FR-9 | **Global search** with synonym support, typo tolerance, and ranking by usage popularity and quality score. |

### Non-Functional Requirements

| Property | Target |
|---|---|
| **Availability** | 99.95% (metadata reads are critical for discovery & CI/CD gates). |
| **Read Latency** | p99 < 200 ms for entity lookups and search; p99 < 1 s for multi-hop lineage traversals (up to depth 10). |
| **Write Throughput** | Sustain **50K metadata change events/sec** during peak pipeline execution windows. |
| **Lineage Freshness** | Real-time emitted lineage visible within **< 30 seconds**. Crawled lineage backfilled within **< 15 minutes**. |
| **Consistency** | Eventual consistency acceptable for search index and lineage graph (converge within seconds). Strong consistency for policy enforcement and contract verification. |
| **Durability** | Zero metadata loss. All change events are persisted in an immutable log. |
| **Scale** | Support **5M+ metadata entities**, **50M+ lineage edges**, **10K+ active users** for search/browse. |

### Assumptions Made (in lieu of interviewer answers)

1. **Read-heavy** workload: ~100:1 read-to-write ratio for catalog browsing and search. Write bursts correlate with pipeline execution windows (mornings, hourly crons).
2. The platform must be **source-agnostic** — integrations span Snowflake, BigQuery, Postgres, Spark, Airflow, dbt, Looker, Tableau, and custom internal systems.
3. Multi-tenancy is **not** a hard requirement (single enterprise deployment), but we design for org-level RBAC.
4. Column-level lineage is the "gold standard" but we accept graceful degradation to table-level when SQL parsing fails (e.g., dynamic SQL, stored procedures).

-----

## Phase 2: High-Level Design & Architecture

### Back-of-Envelope Math

| Metric | Estimate |
|---|---|
| **Entities** | 5M entities × ~2 KB avg metadata = **~10 GB** entity store |
| **Lineage Edges** | 50M edges × ~200 bytes = **~10 GB** graph store |
| **Change Events/day** | 50K/sec peak × 3600s × 8h active + 5K/sec × 16h off-peak ≈ **1.7B events/day** |
| **Event Log Storage/year** | 1.7B × 500 bytes avg ≈ **~850 TB/year** (compacted to ~50 TB with retention + compaction) |
| **Search Index** | 5M docs × 5 KB avg (denormalized) = **~25 GB** Elasticsearch index |
| **Read QPS** | Search: ~2K QPS, Entity lookup: ~5K QPS, Lineage traversal: ~500 QPS |
| **Write QPS** | 50K events/sec peak, ~5K sustained |

### High-Level Components

```
┌──────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                │
│  Snowflake │ BigQuery │ Spark │ Airflow │ dbt │ Looker │ Custom     │
└──────┬──────┬──────┬──────┬──────┬──────┬──────┬─────────────────────┘
       │      │      │      │      │      │      │
       │ Push (Emitters)    │  Pull (Crawlers)   │
       ▼      ▼      ▼      ▼      ▼      ▼      ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                                   │
│                                                                       │
│  ┌─────────────────┐    ┌──────────────────────────┐                 │
│  │  Push Emitters   │    │   Pull Crawlers (K8s     │                 │
│  │  (SDK / Hooks)   │    │   CronJobs)              │                 │
│  └────────┬─────────┘    └────────────┬─────────────┘                │
│           │                           │                               │
│           ▼                           ▼                               │
│  ┌────────────────────────────────────────────┐                      │
│  │          Metadata Change Log (Kafka)        │                      │
│  │       Topic: metadata.change.events         │                      │
│  └────────────────────┬───────────────────────┘                      │
└───────────────────────┼──────────────────────────────────────────────┘
                        │
         ┌──────────────┼──────────────────┐
         ▼              ▼                  ▼
┌────────────┐  ┌──────────────┐  ┌───────────────┐
│  Graph     │  │  Entity      │  │  Search       │
│  Writer    │  │  Writer      │  │  Indexer      │
│  Service   │  │  Service     │  │  Service      │
└─────┬──────┘  └──────┬───────┘  └───────┬───────┘
      │                │                   │
      ▼                ▼                   ▼
┌────────────┐  ┌──────────────┐  ┌───────────────┐
│ Neo4j      │  │  PostgreSQL  │  │ Elasticsearch │
│ (Lineage   │  │  (Entity     │  │ (Discovery    │
│  Graph)    │  │   Store)     │  │  Index)       │
└────────────┘  └──────────────┘  └───────────────┘
      │                │                   │
      └────────────────┼───────────────────┘
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                       SERVING LAYER                                   │
│                                                                       │
│  ┌────────────────┐  ┌──────────────┐  ┌───────────────────────┐     │
│  │  GraphQL /     │  │  Lineage     │  │  Policy Engine        │     │
│  │  REST API      │  │  Service     │  │  (OPA / Custom)       │     │
│  │  Gateway       │  │              │  │                       │     │
│  └───────┬────────┘  └──────┬───────┘  └───────────┬───────────┘     │
│          │                  │                       │                 │
│          └──────────────────┴───────────────────────┘                 │
│                             │                                         │
│                    ┌────────┴────────┐                                │
│                    │   Redis Cache   │                                │
│                    └─────────────────┘                                │
└──────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        CONSUMERS                                      │
│  Web UI │ CLI │ CI/CD Gates │ Webhooks │ Slack Bots │ External APIs  │
└──────────────────────────────────────────────────────────────────────┘
```

### Core Services Breakdown

| Service | Responsibility |
|---|---|
| **Push Emitters (SDKs)** | Lightweight libraries embedded in Spark, Airflow, dbt that emit metadata change events (MCEs) during job execution. Modeled after OpenLineage spec. |
| **Pull Crawlers** | Kubernetes CronJobs that connect to source systems (Snowflake `INFORMATION_SCHEMA`, Looker API, etc.) and emit MCEs for backfill and reconciliation. |
| **Metadata Change Log (Kafka)** | Central event bus. All metadata mutations are serialized as Avro/Protobuf MCEs. Serves as the system-of-record for event ordering and replayability. |
| **Graph Writer** | Consumes MCEs, extracts lineage edges, and upserts into Neo4j. |
| **Entity Writer** | Consumes MCEs, upserts entity snapshots (latest state) into PostgreSQL. Handles schema versioning. |
| **Search Indexer** | Consumes MCEs, denormalizes entity + quality signals + tags into Elasticsearch documents. |
| **API Gateway** | GraphQL (primary) + REST endpoints. Federated schema over entity store, graph, and search. |
| **Lineage Service** | Dedicated service for graph traversals — upstream/downstream queries, impact analysis, shortest-path. |
| **Policy Engine** | Evaluates governance rules (OPA Rego policies) against metadata state. Can block, alert, or annotate. |
| **Webhook Dispatcher** | Tails Kafka topic and fans out filtered events to registered webhook endpoints. |

### Data Flow — Happy Path (Pipeline Execution)

1. **Airflow DAG runs** a Spark job that reads from `raw.events` and writes to `curated.user_activity`.
2. The **Airflow OpenLineage integration** emits a `RunEvent` MCE at job start, containing input/output datasets and the Spark logical plan.
3. The **Spark OpenLineage integration** emits column-level lineage extracted from the Catalyst query plan at job completion.
4. Both MCEs land on the `metadata.change.events` Kafka topic, partitioned by `entity_urn`.
5. **Graph Writer** parses lineage edges from the MCE and upserts them into Neo4j: `(raw.events) -[:FEEDS {columns: [...]}]-> (curated.user_activity)`.
6. **Entity Writer** upserts the dataset snapshot in PostgreSQL — schema, owner, tags, last-modified timestamp, run metadata.
7. **Search Indexer** updates the Elasticsearch document for `curated.user_activity` with new freshness timestamp, column descriptions, and quality scores.
8. **Policy Engine** evaluates: source column `raw.events.email` is tagged `PII` → propagates `PII` tag to `curated.user_activity.user_email` via lineage edge → triggers a webhook if the target dataset lacks an approved encryption policy.
9. A data analyst searches "user activity" in the **Web UI**, sees the dataset ranked by usage, clicks through, views column-level lineage, and confirms the `PII` tag is properly propagated.

-----

## Phase 3: Deep Dive — Data & Storage

### 3.1 Data Model

#### Metadata Entity Model (PostgreSQL)

We adopt an **entity-aspect** model inspired by DataHub's GMA (Generalized Metadata Architecture). Every cataloged object is an **Entity** identified by a URN. Metadata about that entity is decomposed into **Aspects** — independently versionable slices of metadata.

```sql
-- Core entity registry
CREATE TABLE entities (
    urn             TEXT PRIMARY KEY,          -- e.g., "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
    entity_type     TEXT NOT NULL,             -- dataset, pipeline, dashboard, user, glossaryTerm, policy
    platform        TEXT,                      -- snowflake, airflow, looker
    environment     TEXT DEFAULT 'PROD',       -- PROD, STAGING, DEV
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    removed         BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_entities_type ON entities (entity_type);
CREATE INDEX idx_entities_platform ON entities (platform);

-- Versioned aspects (append-only with latest-pointer)
CREATE TABLE aspects (
    urn             TEXT NOT NULL REFERENCES entities(urn),
    aspect_name     TEXT NOT NULL,             -- schemaMetadata, ownership, dataQuality, globalTags, status, datasetProperties
    version         BIGINT NOT NULL DEFAULT 0, -- monotonically increasing per (urn, aspect_name)
    metadata        JSONB NOT NULL,            -- the aspect payload
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    created_by      TEXT,                      -- actor URN
    PRIMARY KEY (urn, aspect_name, version)
);

-- Materialized latest-version view for fast reads
CREATE TABLE aspects_latest (
    urn             TEXT NOT NULL,
    aspect_name     TEXT NOT NULL,
    version         BIGINT NOT NULL,
    metadata        JSONB NOT NULL,
    updated_at      TIMESTAMPTZ,
    PRIMARY KEY (urn, aspect_name)
);
```

**Sample Aspect Payloads:**

```json
// schemaMetadata aspect for a dataset
{
  "platformSchema": "snowflake",
  "fields": [
    {
      "fieldPath": "user_id",
      "nativeType": "NUMBER(38,0)",
      "type": "NUMBER",
      "nullable": false,
      "description": "Unique user identifier",
      "globalTags": ["urn:li:tag:PK"]
    },
    {
      "fieldPath": "email",
      "nativeType": "VARCHAR(256)",
      "type": "STRING",
      "nullable": true,
      "description": "User email address",
      "globalTags": ["urn:li:tag:PII"]
    }
  ],
  "primaryKeys": ["user_id"],
  "foreignKeys": []
}
```

```json
// dataQuality aspect
{
  "lastObserved": "2026-03-05T08:30:00Z",
  "freshness": {
    "lastUpdated": "2026-03-05T08:15:00Z",
    "expectedIntervalMs": 3600000,
    "status": "HEALTHY"
  },
  "volumeMetrics": {
    "rowCount": 14523890,
    "rowCountDelta24h": 23456,
    "status": "HEALTHY"
  },
  "fieldMetrics": [
    {
      "fieldPath": "email",
      "nullRate": 0.002,
      "distinctCount": 14493000,
      "status": "HEALTHY"
    }
  ]
}
```

```json
// dataContract aspect
{
  "contractVersion": 2,
  "schema": {
    "assertionType": "MATCH_SCHEMA",
    "fields": ["user_id:NUMBER", "email:STRING", "created_at:TIMESTAMP"]
  },
  "freshness": {
    "assertionType": "DATASET_CHANGE",
    "maxDelayMs": 7200000
  },
  "quality": [
    {
      "assertionType": "FIELD_VALUES",
      "field": "email",
      "operator": "NULL_RATE_BELOW",
      "threshold": 0.01
    }
  ],
  "subscribers": ["urn:li:corpuser:analytics-team"]
}
```

#### Lineage Graph Model (Neo4j)

```
(:Entity {urn, entityType, platform, name})
    -[:PRODUCES|CONSUMES|DERIVES_FROM {
        runId,
        pipelineUrn,
        timestamp,
        columnMapping: [{src: "email", dst: "user_email", transform: "IDENTITY"}]
    }]->
(:Entity)
```

**Node types** map 1:1 with entity types: Dataset, Pipeline, Dashboard, Column (virtual node for column-level lineage).

**Edge types**:
- `PRODUCES`: Pipeline → Dataset (output)
- `CONSUMES`: Pipeline → Dataset (input)
- `DERIVES_FROM`: Dataset → Dataset (shortcut for table-level lineage)
- `COLUMN_LINEAGE`: Column → Column (with transform annotation)

**Key design**: Column nodes are modeled as separate graph nodes (`urn:li:schemaField:(dataset_urn, field_path)`) connected to their parent dataset with a `HAS_FIELD` edge. This allows column-level traversals without loading full entity metadata.

#### Search Index Model (Elasticsearch)

```json
{
  "urn": "urn:li:dataset:...",
  "entityType": "dataset",
  "name": "user_activity",
  "qualifiedName": "curated.user_activity",
  "platform": "snowflake",
  "description": "Aggregated user activity events",
  "owners": ["alice@company.com", "bob@company.com"],
  "tags": ["PII", "Tier-1"],
  "glossaryTerms": ["User Engagement"],
  "columnNames": ["user_id", "email", "event_type", "event_ts"],
  "qualityScore": 0.92,
  "usageCount30d": 4523,
  "lastModified": "2026-03-05T08:15:00Z",
  "env": "PROD",
  "_boost_signal": 4.2
}
```

Search ranking combines BM25 text relevance with a custom `_boost_signal` derived from: `log(usageCount30d) * qualityScore * tierMultiplier`.

### 3.2 Storage Strategy

#### Why This Multi-Engine Approach?

| Engine | Role | Justification |
|---|---|---|
| **PostgreSQL** | Entity store (source of truth) | ACID guarantees for aspect versioning. JSONB for flexible schema. Mature tooling, battle-tested at scale (similar to DataHub's MySQL/Postgres backend). |
| **Neo4j** | Lineage graph | Native graph traversals (Cypher) are O(edges) not O(total_data). Variable-length path queries (`MATCH path = (a)-[:DERIVES_FROM*1..10]->(b)`) are first-class. Atlas and DataHub both use graph stores for lineage. |
| **Elasticsearch** | Discovery & search | Inverted index with fuzzy matching, synonym expansion, and custom scoring. Powers sub-100ms search for millions of documents. |
| **Kafka** | Event log & integration bus | Replayable, ordered event stream. Decouples ingestion from storage writes. Enables new consumers without modifying producers. |
| **Redis** | Caching | Look-aside cache for hot entity reads (dataset detail pages, lineage subgraphs). TTL-based expiry with cache invalidation on MCE writes. |

#### Partitioning & Sharding

**PostgreSQL**:
- `aspects` table partitioned by `entity_type` (list partitioning) for query isolation between high-volume types (datasets: ~70% of entities) and low-volume types (users, policies).
- Within each partition, the `(urn, aspect_name, version)` composite PK provides natural distribution.
- At extreme scale (>100M rows), hash-partition `aspects_latest` by `urn` across 16-64 partitions using Citus or native PG hash partitioning.

**Neo4j**:
- Single-instance or Neo4j Fabric for read replicas. Graph sharding is notoriously difficult — lineage graphs are deeply connected.
- Mitigation: prune edges older than 90 days into a cold graph (archived in S3 as Parquet for compliance). The hot graph stays within ~100M nodes + edges, well within Neo4j Enterprise capacity.

**Elasticsearch**:
- Index per entity type (`datasets`, `pipelines`, `dashboards`) with 5 primary shards each.
- Index lifecycle management: roll over monthly, force-merge old indices to 1 segment for read optimization.

**Kafka**:
- `metadata.change.events` topic: **32 partitions**, partitioned by `entity_urn` hash to guarantee ordering per entity.
- Retention: 7 days for real-time consumers + compacted topic (`metadata.change.events.compacted`) for infinite retention of latest state per key.

#### Hot / Warm / Cold Storage

| Tier | Data | Storage | Retention |
|---|---|---|---|
| **Hot** | Latest aspect versions, active lineage edges, search index | PostgreSQL, Neo4j, Elasticsearch, Redis | Indefinite (latest state) |
| **Warm** | Historical aspect versions (last 90 days), Kafka events | PostgreSQL partitions, Kafka tiered storage | 90 days |
| **Cold** | Archived aspect versions, historical lineage snapshots | S3 (Parquet/ORC), queryable via Trino | 7 years (compliance) |

#### Caching Strategy

- **Look-aside (Redis)**: Entity detail pages cache the aggregated view (all latest aspects for a URN) with a 5-minute TTL. On MCE write, the Entity Writer publishes an invalidation message on a Redis Pub/Sub channel.
- **Search result caching**: Elasticsearch's built-in request cache handles repeated queries. Application-level caching of the top-100 most popular searches with 60-second TTL.
- **Lineage subgraph caching**: Precomputed lineage subgraphs for Tier-1 datasets (most queried) cached in Redis as serialized adjacency lists. Invalidated on any edge mutation touching that subgraph.

-----

## Phase 4: Trade-offs & Justification

### 4.1 Kafka as the Central Event Bus

**Why Kafka?**
- Replayability: new consumers (e.g., a future ML feature store integration) can replay the full history from the compacted topic without touching the source systems.
- Backpressure handling: consumers control their own pace. If Neo4j is slow, the Graph Writer's consumer group lags without affecting Elasticsearch indexing.
- Ordering guarantees per partition (per entity URN) ensure aspects are applied in the correct sequence.
- Proven at LinkedIn's DataHub scale (~millions of MCEs/day).

**Why not Pulsar?**
- Pulsar's tiered storage and multi-tenancy are appealing, but Kafka's ecosystem maturity (Connect, Schema Registry, MirrorMaker) and organizational familiarity tip the balance. Pulsar would be a viable alternative for a greenfield org.

**Why not direct writes (skip the event bus)?**
- Tight coupling between ingestion and storage. Any storage engine downtime would block metadata ingestion. The event log also serves as an audit trail and a foundation for the webhook dispatcher.

### 4.2 Neo4j vs. JanusGraph vs. Relational Recursive CTEs

**Why Neo4j?**
- Native graph storage (index-free adjacency) makes multi-hop traversals O(hops × avg_degree) rather than O(join_cost × hops).
- Cypher query language is expressive for variable-length path queries critical for impact analysis.
- LinkedIn DataHub and Airbnb Dataportal both landed on dedicated graph stores for lineage.

**Why not JanusGraph (or Neptune)?**
- JanusGraph requires a backend (Cassandra/HBase) + an index (Elasticsearch) — significant operational overhead for what is fundamentally a 10–100 GB graph. Neo4j Enterprise on a single beefy node (256 GB RAM) can hold our entire lineage graph in memory.
- Neptune is AWS-proprietary, violating the open-source-first principle.

**Why not PostgreSQL recursive CTEs?**
- Works for shallow lineage (2-3 hops) but performance degrades exponentially at depth 5+. Column-level lineage with 50M edges makes this impractical. We benchmarked: 10-hop traversal on 10M edges takes ~8s in PG vs. ~50ms in Neo4j.

### 4.3 Entity-Aspect Model vs. Wide-Table Model

**Why Entity-Aspect (DataHub's GMA)?**
- **Extensibility**: adding a new metadata type (e.g., "costMetrics") is adding a new aspect name — no schema migrations.
- **Independent versioning**: schema changes and ownership changes have separate version histories.
- **Selective ingestion**: emitters only need to produce the aspects they know about (Airflow emits `runInfo`, Spark emits `schemaMetadata` + `columnLineage`).

**Trade-off**: reads require joining multiple aspects for a full entity view. Mitigated by the `aspects_latest` materialized table and Redis caching.

### 4.4 Push vs. Pull Ingestion — Why Both?

| | Push (Emitters) | Pull (Crawlers) |
|---|---|---|
| **Freshness** | Real-time (emitted during job execution) | Periodic (minutes to hours) |
| **Accuracy** | High — captures runtime context, actual query plan | Medium — infers from catalog metadata, may miss dynamic SQL |
| **Coverage** | Only instrumented systems | Any system with an API/catalog |
| **Maintenance** | Requires SDK integration per source | Centrally managed crawlers |

**Strategy**: Push for real-time lineage from instrumented sources (Spark, Airflow, dbt via OpenLineage). Pull for backfill, reconciliation, and sources that don't support push (legacy databases, BI tools). The pull crawler runs on a schedule and emits MCEs to the same Kafka topic — downstream consumers don't distinguish between push and pull.

### 4.5 Consistency Model

- **Entity Store (PostgreSQL)**: Strong consistency. Aspect writes are transactional. The Policy Engine reads from PG to make governance decisions — eventual consistency here could allow a policy violation window.
- **Graph Store (Neo4j)**: Eventual consistency (seconds of lag from Kafka). Acceptable because lineage queries are exploratory — a few seconds of staleness is fine.
- **Search Index (Elasticsearch)**: Eventual consistency (near-real-time refresh interval of 1 second). Discovery queries tolerate this.
- **Governance gates** (e.g., blocking a deployment if a breaking schema change is unapproved): these read from PostgreSQL directly, not from the eventually-consistent stores.

-----

## Phase 5: Reliability, Scaling & Operations

### 5.1 Bottleneck Identification

| Bottleneck | Risk | Mitigation |
|---|---|---|
| **Kafka consumer lag** during peak pipeline windows | Graph Writer or Search Indexer falls behind, causing stale lineage/search | Auto-scaling consumer instances in K8s. Separate consumer groups so a slow Neo4j write doesn't block ES indexing. |
| **Neo4j write throughput** | Single-writer bottleneck under 50K edges/sec | Batch writes (collect 500ms of edges, bulk UNWIND insert). Neo4j can handle ~100K writes/sec with batching. |
| **SQL parsing for column-level lineage** | CPU-intensive for complex queries (20+ JOINs, CTEs) | Offload to a dedicated **Lineage Parser** service (horizontally scalable). Use Apache Calcite for SQL parsing with a timeout of 5s per query — fallback to table-level lineage on timeout. |
| **Elasticsearch indexing backpressure** | Bulk indexing during large backfills can saturate ES cluster | Throttle the Search Indexer consumer to max 10K docs/sec. Use ES bulk API with adaptive batch sizing. |
| **Neo4j deep traversal** | Impact analysis query touching 10K+ nodes fans out exponentially | Cap traversal depth at 10 hops. Use bidirectional BFS from the Lineage Service. Precompute lineage subgraphs for Tier-1 datasets nightly. |

### 5.2 Failure Handling

**Kafka consumer crash**:
- Consumer group rebalance reassigns partitions to surviving instances. No data loss — Kafka retains events. Consumer resumes from last committed offset.
- If a "poison pill" MCE crashes the consumer, a dead-letter topic (`metadata.change.events.dlq`) captures it for manual inspection. The consumer skips and continues.

**Neo4j node failure**:
- Neo4j Enterprise Causal Cluster: 3-node cluster (1 leader + 2 read replicas). Leader election on failure takes ~10s. Lineage reads are served from replicas.
- Full cluster failure: lineage queries return a graceful degradation response ("lineage temporarily unavailable"). Entity lookups and search continue to work.

**PostgreSQL failure**:
- Streaming replication with a synchronous standby. Automatic failover via Patroni. RPO = 0 (no data loss), RTO < 30 seconds.
- The Policy Engine circuit-breakers: if PG is unreachable, policy checks fail-open with an alert (configurable per policy to fail-closed for critical compliance rules).

**Elasticsearch cluster failure**:
- Search degrades gracefully — the UI shows "search unavailable" but entity detail pages (served from PG) continue to work.
- Rebuild from scratch by replaying the Kafka compacted topic. Full reindex of 5M entities takes ~30 minutes.

**Region outage**:
- Active-passive deployment. Kafka MirrorMaker 2 replicates the event log to the DR region. PG streaming replication to DR. Neo4j secondary cluster in DR reads from the replicated Kafka topic.
- Failover is manual (to avoid split-brain). RTO target: 15 minutes.

### 5.3 Edge Cases

- **Circular lineage**: a DAG that accidentally writes back to its input. The Graph Writer detects cycles via a pre-insert check and annotates the edge as `potential_cycle: true` with an alert to the pipeline owner.
- **Schema drift**: when a crawled schema differs from the last known version, a `SchemaChange` event is emitted. The Policy Engine evaluates if any field was removed or type-changed (breaking change) and notifies downstream owners via webhook.
- **Late-arriving events**: Kafka ordering guarantees per-entity mean aspects arrive in order. Cross-entity ordering (e.g., a pipeline MCE arriving before its output dataset MCE) is handled by the Entity Writer creating a "placeholder" entity that gets backfilled.
- **Duplicate events**: all writes are idempotent — `(urn, aspect_name, version)` is a unique key. Duplicate MCEs with the same version are no-ops.

### 5.4 Observability

#### Golden Signals

| Signal | Metric | Alert Threshold |
|---|---|---|
| **Latency** | p99 entity lookup, p99 search, p99 lineage traversal | > 500ms, > 300ms, > 2s |
| **Traffic** | MCE ingestion rate, API QPS, search QPS | Anomaly detection (>2σ from baseline) |
| **Errors** | DLQ message rate, 5xx API rate, failed policy evaluations | DLQ > 100/min, 5xx > 1% |
| **Saturation** | Kafka consumer lag (messages), PG connection pool usage, Neo4j heap usage, ES JVM heap | Lag > 100K, pool > 80%, heap > 85% |

#### SLAs / SLOs

| SLO | Target | Measurement |
|---|---|---|
| Search availability | 99.9% | Synthetic search queries every 30s from canary |
| Entity API availability | 99.95% | Health check probes |
| Lineage freshness (push sources) | < 30 seconds from emission to searchability | End-to-end latency tracking via embedded timestamp in MCE |
| Policy evaluation latency | p99 < 100ms | Instrumented in Policy Engine |
| Backfill completeness | 100% of catalog entities indexed within 24h of crawler run | Reconciliation job comparing source catalog counts vs. entity store counts |

#### Health Checks

- **Synthetic transactions**: a canary pipeline runs every 5 minutes, emits an MCE, and verifies it appears in search, graph, and entity store within 60 seconds.
- **Kafka lag monitor**: a Prometheus exporter tracks consumer group lag per partition. PagerDuty alert if lag exceeds 5 minutes.
- **Cross-store consistency checker**: nightly batch job compares entity counts and sample checksums across PG, Neo4j, and ES. Discrepancies trigger a selective re-sync from the Kafka compacted topic.

-----

## Phase 6: Staff-Level Considerations

### 6.1 Column-Level Lineage — The Hard Problem

Column-level lineage is the differentiator between a toy catalog and a production governance platform. The approach:

1. **SQL Parsing (Apache Calcite)**: Parse SQL queries from warehouse query logs into ASTs. Walk the AST to extract `SELECT` column → source column mappings. Handles standard SQL, CTEs, subqueries, window functions. Limitations: dynamic SQL, UDFs, and stored procedures are opaque.

2. **Spark Plan Extraction**: Hook into Spark's Catalyst optimizer. The `LogicalPlan` contains `AttributeReference` chains that map output columns to input columns through transformations. The OpenLineage Spark integration extracts this natively.

3. **dbt Manifest Parsing**: dbt's `manifest.json` contains a `column_lineage` section (dbt 1.6+). The crawler parses this directly.

4. **Graceful Degradation**: when parsing fails (complex dynamic SQL, proprietary functions), fall back to table-level lineage and annotate the edge as `columnLineage: PARTIAL` or `UNAVAILABLE`. Surface this in the UI so users know the fidelity.

### 6.2 Data Contracts

Data Contracts are the Staff-level governance primitive. They bridge the gap between "metadata as documentation" and "metadata as enforcement."

**Lifecycle**:
1. **Define**: Producer team writes a contract (schema shape, freshness SLA, quality assertions) as a versioned aspect on their dataset.
2. **Verify**: The Policy Engine evaluates incoming quality signals against the contract assertions. Violations are emitted as `ContractViolation` events.
3. **React**: Consumers subscribed to the contract receive webhook notifications. CI/CD gates can block deployments that would break a contract (e.g., removing a column that the contract guarantees).

**Enforcement modes**: `OBSERVE` (log violations), `WARN` (notify but don't block), `ENFORCE` (block writes / fail CI).

### 6.3 Governance as Code (Policy Engine)

The Policy Engine uses **Open Policy Agent (OPA)** with Rego policies. Policies are versioned in Git and deployed via CI/CD.

**Example Rego policy**: auto-classify PII propagation

```rego
package lineage.governance

propagate_tag[tag] {
    input.event.type == "LINEAGE_EDGE_CREATED"
    source := input.event.sourceEntity
    tag := source.tags[_]
    tag.category == "SENSITIVITY"
    not already_tagged(input.event.targetEntity, tag)
}

already_tagged(entity, tag) {
    entity.tags[_].urn == tag.urn
}
```

**Example Rego policy**: block breaking schema changes

```rego
package schema.governance

deny[msg] {
    input.event.type == "SCHEMA_CHANGE"
    input.event.changeType == "FIELD_REMOVED"
    field := input.event.removedField
    has_downstream_consumers(input.event.entityUrn)
    not input.event.approvedBy
    msg := sprintf("Breaking change: field '%s' removed from '%s' without approval", [field, input.event.entityUrn])
}
```

### 6.4 Cost Analysis

| Component | Sizing | Estimated Monthly Cost (self-hosted on K8s) |
|---|---|---|
| **Kafka** (6 brokers, 32 partitions) | 6 × m5.2xlarge | ~$3,200 |
| **PostgreSQL** (Patroni, 3-node HA) | 3 × r5.2xlarge + 2 TB gp3 | ~$2,800 |
| **Neo4j Enterprise** (3-node causal cluster) | 3 × r5.4xlarge (128 GB RAM) | ~$4,500 |
| **Elasticsearch** (6 data nodes + 3 master) | 6 × r5.2xlarge + 3 × m5.large | ~$4,200 |
| **Redis** (ElastiCache-equivalent, 2 replicas) | 3 × r5.xlarge | ~$900 |
| **K8s Workers** (ingestion services, API, crawlers) | 10 × m5.2xlarge | ~$5,400 |
| **S3 Cold Storage** | ~50 TB/year | ~$1,200/year |
| **Total** | | **~$21,000/month** |

**Cost optimization levers**:
- Spot instances for crawlers and the Search Indexer (stateless, restartable).
- Kafka tiered storage to offload old segments to S3 instead of keeping them on broker disks.
- Neo4j: if cost is a concern, evaluate Apache AGE (graph extension for PostgreSQL) to consolidate PG + graph into one engine. Trade-off: ~3-5x slower deep traversals, but saves ~$4.5K/month.

### 6.5 Security

- **Encryption**: TLS 1.3 in transit for all inter-service communication. AES-256 at rest for PG (full-disk encryption), Neo4j, and ES.
- **PII in metadata**: Column values are never stored — only schema metadata and statistical profiles. Column-level sensitivity tags drive access control: PII-tagged columns show masked sample values in the UI unless the user has the `DATA_STEWARD` role.
- **RBAC**: Entity-level access control enforced at the API Gateway. Ownership-based policies: only owners can edit descriptions, tags, and contracts. Read access is open by default (configurable per policy).
- **Audit log**: every API mutation is logged to an immutable audit trail (Kafka topic `metadata.audit.log`) with actor, timestamp, and before/after diff.

### 6.6 Evolution — Scaling 10x

| Dimension | Current | 10x | Strategy |
|---|---|---|---|
| Entities | 5M | 50M | PG horizontal partitioning via Citus. ES adds more shards. Neo4j Fabric for federated graph queries across shards. |
| MCE throughput | 50K/sec peak | 500K/sec | Kafka partition count → 128. Consumer fleet auto-scales. Batch writers. |
| Users | 10K | 100K | API Gateway scales horizontally. Redis cluster mode. CDN for static UI assets. |
| Lineage edges | 50M | 500M | Tiered graph: hot subgraph (last 90 days) in Neo4j, historical in Parquet + Trino for ad-hoc queries. |
| Column-level parsing | SQL + Spark + dbt | + Flink, Beam, Presto, custom ETL | Plugin architecture for parsers. Community-contributed OpenLineage integrations. |

### 6.7 Summary of Key Architectural Decisions

```
┌────────────────────────────────────────────────────────────────────┐
│                    DECISION RECORD SUMMARY                          │
├────────────────┬───────────────────────┬───────────────────────────┤
│ Decision       │ Choice                │ Key Reason                │
├────────────────┼───────────────────────┼───────────────────────────┤
│ Event Bus      │ Kafka                 │ Replayability + ordering  │
│ Entity Store   │ PostgreSQL + JSONB    │ ACID + schema flexibility │
│ Graph Store    │ Neo4j                 │ Native traversals at depth│
│ Search         │ Elasticsearch         │ Fuzzy search + ranking    │
│ Cache          │ Redis                 │ Sub-ms lookups + pub/sub  │
│ Data Model     │ Entity-Aspect (GMA)   │ Extensibility             │
│ Ingestion      │ Push + Pull hybrid    │ Freshness + coverage      │
│ Lineage Parser │ Calcite + Spark hooks │ Column-level fidelity     │
│ Policy Engine  │ OPA (Rego)            │ Declarative + Git-versioned│
│ Consistency    │ Strong (PG) + EC (rest)│ Governance needs strong   │
│ Cold Storage   │ S3 Parquet + Trino    │ Compliance + cost         │
└────────────────┴───────────────────────┴───────────────────────────┘
```

-----

## References

- LinkedIn DataHub — GMA architecture, entity-aspect model, MCE/MAE event flows
- OpenLineage specification — standardized lineage event format adopted by Airflow, Spark, dbt
- Apache Atlas — pioneered graph-based lineage for Hadoop ecosystems
- Amundsen (Lyft) — search-first metadata discovery approach
- Open Policy Agent — policy-as-code engine used by Netflix, Goldman Sachs for infrastructure governance
- Apache Calcite — SQL parser and optimizer framework used for column-level lineage extraction
- Neo4j Causal Clustering — HA architecture for graph databases
- Uber's Databook — metadata platform with entity-aspect model inspiration
- Airbnb's Dataportal — lineage visualization and impact analysis patterns
