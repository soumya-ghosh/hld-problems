# System Design Problem Statement: Distributed Event Streaming Platform (Kafka-like)

---

## Problem Statement

Design a **distributed, fault-tolerant, high-throughput event streaming platform** — similar to Apache Kafka — that enables real-time data pipelines and stream processing at scale. The system must allow producers to publish messages to named topics and consumers to subscribe and process those messages in a durable, ordered, and reliable manner.

This platform will serve as the central nervous system for large-scale distributed applications — decoupling microservices, enabling event sourcing, powering analytics pipelines, and supporting real-time monitoring and alerting systems.

---

## Background & Motivation

Modern distributed systems generate enormous volumes of events — user actions, transaction logs, IoT sensor data, application metrics, and more. Traditional message queues (like RabbitMQ) are not designed for:

- **Replay** of historical events
- **Multi-consumer** fan-out without message deletion
- **Massive throughput** at sub-millisecond latency
- **Long-term retention** of event logs as a source of truth

The system to be designed addresses these gaps and must be operable at internet scale.

---

## Scope

Design the **core distributed log and messaging infrastructure**, including:

- Topic and partition management
- Producer publishing pipeline
- Consumer group processing model
- Broker cluster coordination
- Replication and fault tolerance
- Offset management and message retention

**Out of Scope (for this design round):**
- Stream processing engine (Kafka Streams equivalent)
- Schema Registry
- KSQL / declarative query interface
- Kafka Connect (connector framework)

---

## Functional Requirements (FR)

### 1. Topic Management
- Producers must be able to **create topics** with a configurable number of **partitions** and **replication factor**.
- Topics must support **time-based** and **size-based retention policies**.
- Topics must be **deletable** and **configurable** at runtime without downtime.

### 2. Message Publishing (Producer)
- Producers must be able to **publish messages** (key-value pairs with optional headers) to a named topic.
- Messages must be **routed to partitions** based on:
  - A hash of the message key (default)
  - Round-robin (if no key is provided)
  - Custom partitioner logic
- Producers must support three **delivery acknowledgement modes**:
  - `acks=0`: Fire and forget
  - `acks=1`: Leader acknowledgement only
  - `acks=all`: Full ISR (In-Sync Replica) acknowledgement

### 3. Message Consumption (Consumer)
- Consumers must be able to **subscribe to one or more topics**.
- Consumers must be organized into **consumer groups** where each partition is assigned to exactly one consumer in a group, enabling **parallel consumption**.
- Multiple independent consumer groups must be able to **read from the same topic** without interference.
- Consumers must be able to **replay messages** from any offset (beginning, end, or a specific offset/timestamp).

### 4. Offset Management
- The system must **track per-partition offsets** for each consumer group.
- Offsets must be **committed explicitly** (auto-commit configurable).
- Consumers must be able to **seek** to arbitrary offsets for replay or recovery.

### 5. Message Ordering & Delivery Guarantees
- Message **ordering must be guaranteed within a partition**.
- The system must support configurable delivery semantics:
  - **At-most-once** (fire and forget)
  - **At-least-once** (default; possible duplicates on retry)
  - **Exactly-once** (via idempotent producers and transactional APIs)

### 6. Replication & Leader Election
- Each partition must have one **leader broker** and zero or more **follower replicas**.
- Followers must continuously **replicate from the leader** and maintain an **In-Sync Replica (ISR)** list.
- On leader failure, the system must **automatically elect a new leader** from the ISR.

### 7. Consumer Group Rebalancing
- When consumers join or leave a group, the system must trigger a **partition rebalancing** to redistribute partition ownership.
- The system should support multiple rebalance strategies: **eager** (stop-the-world) and **cooperative/incremental**.

### 8. Retention & Log Compaction
- The system must support **time-based** (e.g., 7 days) and **size-based** (e.g., 100 GB per partition) retention.
- The system must support **log compaction** — retaining only the latest message per key for stateful use cases (e.g., change data capture).

### 9. Admin & Observability APIs
- The system must expose APIs for:
  - Listing/describing topics and partitions
  - Viewing consumer group lag
  - Reassigning partition leadership
  - Triggering leader rebalance

---

## Non-Functional Requirements (NFR)

### 1. Performance & Throughput
| Metric | Target |
|---|---|
| Write throughput | ≥ 1 million messages/sec per broker |
| End-to-end latency (P99) | < 10 ms under normal load |
| Read throughput | ≥ 2 million messages/sec (leveraging zero-copy) |
| Batch size | Configurable (up to 1 MB per batch) |

### 2. Scalability
- Must support **horizontal scaling** — adding brokers should linearly increase throughput.
- Must support **millions of topics** and **tens of thousands of partitions** per cluster.
- Partition leadership must be **evenly distributed** across brokers.

### 3. Durability
- Messages must be **persisted to disk** before acknowledgement (with `acks=all`).
- Data must be replicated across **at least 3 brokers** (default replication factor = 3) to tolerate `N-1` broker failures.
- No committed message should be lost even during broker crashes or network partitions.

### 4. Availability
- The system must achieve **99.99% uptime** (≤ 52 minutes downtime/year).
- Leader election and partition reassignment must complete within **30 seconds** of a broker failure.
- The system must tolerate **simultaneous failure of up to `(RF-1)` replicas** per partition without data loss.

### 5. Fault Tolerance
- The system must handle **broker failures**, **network partitions**, and **disk failures** gracefully.
- A broker restart must **recover its state** from the local log without data loss.
- The cluster must remain operational during **rolling upgrades**.

### 6. Consistency
- Within a partition, messages must be **totally ordered** (guaranteed by sequential offset assignment).
- Consumers in a group must never observe **out-of-order delivery** within a partition.
- Exactly-once semantics must hold across producer retries and consumer re-processing.

### 7. Maintainability & Operability
- The system must support **zero-downtime rolling deployments**.
- Configuration changes (retention, replication factor, partition count) must be **applied without restart**.
- The system must expose **Prometheus-compatible metrics** for monitoring lag, throughput, error rates, and disk usage.

### 8. Security
- Must support **TLS encryption** for all client-broker and broker-broker communication.
- Must support **SASL/SCRAM and mTLS** for authentication.
- Must support **topic-level ACLs** for authorization (read, write, describe, delete).
- Must support **audit logging** for all admin operations.

### 9. Multi-Tenancy
- Must support **logical isolation** of topics via namespaces or prefixes.
- Quota enforcement per producer/consumer client:
  - Produce/consume **byte-rate quotas**
  - **Request-rate throttling**

### 10. Geo-Replication (Stretch Goal)
- Must support **active-passive replication** of topics across data centers or cloud regions for disaster recovery.
- Cross-cluster replication lag must be **< 5 seconds** under normal conditions.

---

## Capacity Estimation (Baseline)

| Parameter | Value |
|---|---|
| Producers | 10,000 |
| Consumers | 50,000 |
| Topics | 100,000 |
| Avg message size | 1 KB |
| Peak write throughput | 10 GB/s across cluster |
| Retention period | 7 days |
| Storage per day | ~860 TB |
| Total cluster storage (7d) | ~6 PB (with replication factor 3) |

---

## Key Design Challenges

1. **Log Segmentation & Indexing** — Efficiently storing append-only logs with fast offset lookups.
2. **Zero-Copy I/O** — Using `sendfile()` syscall to transfer data from disk to network without copying to userspace.
3. **ISR Management** — Deciding when a follower falls out of sync and shouldn't participate in leader election.
4. **Consumer Group Coordination** — Distributed coordination of partition assignment without a single point of failure.
5. **Exactly-Once Semantics** — Idempotent producers (sequence numbers) + transactional APIs + atomic offset commits.
6. **Cluster Metadata Management** — Replacing ZooKeeper with a self-managed quorum (KRaft-style) using Raft consensus.
7. **Back-Pressure & Flow Control** — Handling slow consumers without starving fast producers.

---

## Candidate System Components

```
┌─────────────┐     ┌──────────────────────────────────────────┐
│  Producers  │────▶│              Broker Cluster               │
└─────────────┘     │  ┌─────────┐ ┌─────────┐ ┌─────────┐   │
                    │  │Broker 1 │ │Broker 2 │ │Broker 3 │   │
┌─────────────┐     │  │(Leader) │ │(Follwr) │ │(Follwr) │   │
│  Consumers  │◀────│  └─────────┘ └─────────┘ └─────────┘   │
└─────────────┘     │         ↕ Replication ↕                  │
                    │  ┌─────────────────────────────────┐     │
┌─────────────┐     │  │   Cluster Metadata Quorum       │     │
│ Admin Client│────▶│  │   (Raft / ZooKeeper / KRaft)    │     │
└─────────────┘     │  └─────────────────────────────────┘     │
                    │  ┌─────────────────────────────────┐     │
                    │  │   Distributed Log Storage        │     │
                    │  │   (Partitioned, Segmented)       │     │
                    │  └─────────────────────────────────┘     │
                    └──────────────────────────────────────────┘
```

---

## Success Criteria

- A producer can publish 1M messages/sec per broker with P99 latency < 10ms.
- No data loss occurs when up to `RF-1` brokers crash simultaneously.
- Consumer groups can rebalance partition ownership within 30 seconds of a member joining or leaving.
- The system can recover from a complete broker restart and rejoin the cluster within 60 seconds.
- Exactly-once semantics are validated under simulated producer retries and consumer crashes.

---

*This problem statement is intended for a system design interview or architectural planning session. Candidates are expected to deep-dive into one or more of the key design challenges listed above.*