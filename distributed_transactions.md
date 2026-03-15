# Distributed Transactions

## Part 1: Transactions Across Heterogeneous Storage Systems

The scenario: a single logical operation must atomically write to **two databases**, **update a Redis cache**, and **produce a message to a queue**. None of these systems share a transaction log. This is the hardest class of distributed transaction.

---

### 1. Two-Phase Commit (2PC)

The classical approach. A **coordinator** orchestrates all participants through two rounds.

**Phase 1 — Prepare (Vote)**
- Coordinator sends `PREPARE` to every participant (DB-A, DB-B, Redis, MQ).
- Each participant durably writes a **prepare record** to its WAL/journal and replies `YES` or `NO`.
- If a participant votes `YES`, it **promises** it can commit — the data is locked.

**Phase 2 — Commit / Abort**
- If **all** vote `YES` → coordinator writes `COMMIT` to its own durable log, then sends `COMMIT` to all.
- If **any** vote `NO` or timeout → coordinator sends `ABORT` to all.

```
Coordinator          DB-A         DB-B         Redis        MQ
    |---PREPARE------->|            |            |            |
    |---PREPARE---------------------------->|            |            |
    |---PREPARE---------------------------------------->|            |
    |---PREPARE------------------------------------------------------>|
    |<--YES------------|            |            |            |
    |<--YES-----------------------------|            |            |
    |<--YES--------------------------------------------|            |
    |<--YES--------------------------------------------------------------|
    |---COMMIT-------->|            |            |            |
    |---COMMIT---------------------------->|            |            |
    |---COMMIT---------------------------------------->|            |
    |---COMMIT------------------------------------------------------>|
```

**Problems:**
- **Blocking protocol**: if the coordinator crashes after sending `PREPARE` but before sending `COMMIT`, all participants hold locks indefinitely waiting for the decision.
- **Latency**: two synchronous network round-trips plus forced durable writes at every participant.
- **Heterogeneous support**: Redis and Kafka don't natively support XA/2PC. You'd need custom resource managers wrapping them, which is fragile.
- **Coordinator is a SPOF**: must be made HA itself (replicated log).

**XA Protocol** is the standardized 2PC interface (X/Open XA). Java's JTA/JTS implements it. Works well when all participants are XA-compliant RDBMSes. Falls apart with NoSQL stores, caches, and message brokers.

---

### 2. Three-Phase Commit (3PC)

Adds a **pre-commit** phase between prepare and commit to reduce the blocking window.

| Phase | What happens |
|---|---|
| **CanCommit** | Coordinator asks "can you commit?" — lightweight check, no locks yet |
| **PreCommit** | If all say yes, coordinator sends `PRE-COMMIT`. Participants acquire locks and durably prepare. |
| **DoCommit** | Coordinator sends `COMMIT`. Participants finalize. |

The key difference: if a participant is in the `PRE-COMMIT` state and the coordinator dies, it can **unilaterally decide to commit** after a timeout (because reaching pre-commit implies all participants agreed). This makes it **non-blocking** under certain failure models.

**In practice**: rarely used. The extra round-trip adds latency, and it still doesn't handle network partitions cleanly (a partitioned participant might commit while others abort). 2PC with a replicated coordinator log is preferred.

---

### 3. Saga Pattern

Instead of one atomic transaction, decompose it into a **sequence of local transactions**, each with a **compensating transaction** to undo its effect on failure.

```
T1: Write to DB-A          C1: Delete from DB-A
T2: Write to DB-B          C2: Delete from DB-B
T3: Update Redis cache     C3: Revert Redis cache
T4: Produce to MQ          C4: Produce compensating message / dead-letter
```

**Orchestration-based Saga:**
A central **saga orchestrator** (a stateful service or workflow engine) drives the sequence:

```
Orchestrator
    |--T1--> DB-A     ✓
    |--T2--> DB-B     ✓
    |--T3--> Redis    ✗ (fails)
    |--C2--> DB-B     (compensate)
    |--C1--> DB-A     (compensate)
```

**Choreography-based Saga:**
Each participant publishes an event on completion; the next participant listens and acts. No central coordinator. Harder to reason about, easier to decouple.

```
DB-A commits → publishes "DB-A-Done" →
    DB-B listens, commits → publishes "DB-B-Done" →
        Redis listens, updates → publishes "Redis-Done" →
            MQ producer listens, produces message
```

On failure, reverse events trigger compensations.

**Trade-offs:**
- **No atomicity**: intermediate states are visible (you get **eventual consistency**, not strict ACID). Between T1 and T2, DB-A has the write but DB-B doesn't.
- **Compensations are hard**: some actions are inherently non-compensable (sending an email, charging a credit card). You need **semantic undo**, not log-level undo.
- **Idempotency is mandatory**: compensations and retries can fire multiple times.
- **Observability**: must track saga state (pending, compensating, completed, failed) for debugging. Saga orchestrators typically persist this to a durable store.

**When to use**: most microservice architectures. This is the dominant pattern because it doesn't require cross-system locking.

---

### 4. Transactional Outbox Pattern

Solves a narrower but extremely common problem: **atomically write to a DB and publish a message/event**.

Instead of writing to the DB and the MQ in the same transaction (impossible without 2PC), you:

1. Write the **business data** and an **outbox record** to the same database in a single local ACID transaction.
2. A separate **relay process** (poller or CDC-based) reads the outbox table and publishes to the MQ.
3. After successful publish, mark the outbox record as sent (or delete it).

```
┌─────────────────────────────────────┐
│  DB-A (single ACID transaction)     │
│                                     │
│  INSERT INTO orders (...)           │
│  INSERT INTO outbox (event_payload) │
│                                     │
└─────────────────────────────────────┘
            │
            ▼
   ┌─────────────────┐
   │  Outbox Relay    │  (polls outbox table OR uses CDC / WAL tailing)
   │  (Debezium, etc) │
   └────────┬────────┘
            │
            ▼
   ┌─────────────────┐
   │  Message Queue   │
   └─────────────────┘
```

**CDC-based variant (log tailing):**
Instead of polling the outbox table, use **Change Data Capture** (Debezium on Postgres WAL, MySQL binlog, etc.) to stream changes directly from the DB's transaction log to Kafka. This eliminates the outbox table entirely — the DB's own WAL *is* the outbox.

**Guarantees:**
- **At-least-once delivery** to the MQ (relay might crash after publishing but before marking sent → re-publish on restart).
- Consumers must be **idempotent**.
- No distributed locks, no 2PC.

**Extending to our scenario (2 DBs + Redis + MQ):**
- Use outbox in DB-A. The relay publishes events.
- Consumers of those events write to DB-B, update Redis, etc.
- Each downstream consumer uses its own local transaction + idempotency key.
- This is essentially a **saga driven via the outbox pattern**.

---

### 5. Listen-to-Yourself / Event-Driven State Machine

Variant of the outbox: the service **never writes directly to the second DB or cache**. Instead:

1. Write to primary DB (with outbox).
2. Publish event.
3. A consumer (could be the same service) reacts to the event by writing to DB-B.
4. Another consumer updates Redis.
5. Another consumer produces the downstream MQ message.

Each step is a **local transaction** triggered by an event. The event log (Kafka) acts as the durable backbone. Failures trigger retries.

This is the **event-sourcing-adjacent** approach and is how most modern systems achieve cross-store consistency without 2PC.

---

### 6. TCC (Try-Confirm-Cancel)

Application-level protocol — essentially 2PC pushed up from the infrastructure to the business logic layer.

| Phase | Meaning |
|---|---|
| **Try** | Reserve resources. DB-A: insert row with `status=PENDING`. DB-B: same. Redis: set a tentative value. MQ: don't produce yet. |
| **Confirm** | Finalize. DB-A: update `status=COMMITTED`. DB-B: same. Redis: finalize value. MQ: produce message. |
| **Cancel** | Release. DB-A: delete or mark `status=CANCELLED`. DB-B: same. Redis: revert. MQ: no-op (nothing was produced). |

**Advantages over 2PC**: no infrastructure-level distributed locks; each system just does local operations. The "locks" are semantic (pending status).

**Disadvantages**: every participant must implement Try/Confirm/Cancel logic. Business logic becomes significantly more complex. Still has the coordinator problem (who drives Confirm/Cancel?).

---

### Comparison Matrix

| Approach | Consistency | Complexity | Latency | Heterogeneous Support | Blocking? |
|---|---|---|---|---|---|
| 2PC/XA | Strong (atomic) | Medium | High (2 sync RTTs) | Poor (needs XA) | Yes |
| 3PC | Strong | High | Higher (3 RTTs) | Poor | No (in theory) |
| Saga | Eventual | Medium-High | Low per step | Excellent | No |
| Outbox + CDC | At-least-once | Medium | Low (async) | Excellent | No |
| TCC | Strong (if all confirm) | High | Medium | Good | No |

---

## Part 2: Distributed Transactions Within a Single Distributed Database

Here the problem is different: a **single logical database** (e.g., CockroachDB, Spanner, YugabyteDB, TiDB, Vitess/sharded MySQL) is spread across multiple nodes. A transaction like:

```sql
BEGIN;
INSERT INTO orders (id, user_id, ...) VALUES (...);   -- shard/node A
UPDATE inventory SET qty = qty - 1 WHERE item_id = X;  -- shard/node B
INSERT INTO payments (order_id, ...) VALUES (...);      -- shard/node C
COMMIT;
```

...touches three different nodes. How does the database provide ACID?

---

### 1. Internal 2PC (Percolator-style)

Used by: **TiDB**, **CockroachDB** (variant), **Google Percolator**

This is 2PC but **entirely internal** to the database, not exposed to the application. The database's transaction coordinator (usually the node that received the query or a dedicated transaction manager) runs 2PC across the involved nodes/regions.

**Percolator Protocol (simplified):**

1. **Prewrite phase**: For each key being written, pick one as the **primary lock**. Write a tentative value + lock to each key's region.
   - Primary key: written with a lock pointing to itself.
   - Secondary keys: written with a lock pointing to the primary.
2. **Commit phase**: Once all prewrites succeed, commit the primary key (write the commit timestamp, remove lock). Then asynchronously commit all secondaries.
3. **Crash recovery**: Any reader encountering a lock checks the primary. If the primary is committed → roll forward the secondary. If the primary's lock is still there and has expired → roll back (clean up).

The key insight: **only the primary's commit is synchronous**. Secondary commits are async and resolved lazily. This reduces the blocking window to a single key.

---

### 2. Spanner's TrueTime + 2PC

Used by: **Google Spanner**, **CockroachDB** (adapted as HLC)

Spanner uses 2PC for cross-shard transactions but solves the **timestamp ordering** problem with **TrueTime** — a globally synchronized clock with bounded uncertainty (±ε, typically a few ms thanks to GPS + atomic clocks in every datacenter).

**How it works:**

1. Client sends transaction to a **coordinator** (one of the participant leaders).
2. **Prepare phase**: each participant leader acquires locks, prepares, and reports a **prepare timestamp**.
3. Coordinator picks a **commit timestamp** ≥ all prepare timestamps and ≥ `TT.now().latest` (ensures the timestamp is definitely in the past from every node's perspective after a small wait).
4. **Commit-wait**: coordinator waits until `TT.now().earliest > commit_timestamp`. This guarantees **external consistency** — if transaction T1 commits before T2 starts, T1's timestamp < T2's timestamp, globally. This wait is bounded by the clock uncertainty (typically 1-7ms).
5. **Commit phase**: coordinator tells all participants to commit at the chosen timestamp.

**Why this matters**: Spanner provides **strict serializability** (linearizability + serializability) across the entire planet. No other system provides this guarantee at this scale with the same confidence.

**CockroachDB's adaptation**: uses **Hybrid Logical Clocks (HLC)** instead of TrueTime. HLCs combine physical time with a logical counter. CockroachDB doesn't have GPS/atomic clock infrastructure, so it handles clock skew through a **read uncertainty interval** — if a read encounters a value with a timestamp within the uncertainty window, it restarts the transaction at a higher timestamp. This gives serializable isolation but not strict external consistency like Spanner.

---

### 3. Calvin / Deterministic Ordering

Used by: **FaunaDB** (formerly), **SLOG**, academic systems

Instead of coordinating *during* execution, **pre-order all transactions deterministically** before execution.

1. Transactions are submitted to a **sequencing layer** (replicated log like Raft/Paxos).
2. The sequencer assigns a **global order** to all transactions.
3. Every node receives the ordered log and executes transactions in that order.
4. Because the order is deterministic and known ahead of time, nodes can **execute without coordination** — no locks, no 2PC.

**Advantages:**
- No runtime coordination overhead for cross-shard transactions.
- Multi-master replication without conflict resolution.

**Disadvantages:**
- Requires **deterministic execution** (no external calls, no random(), no NOW() during transaction).
- The sequencer is on the critical path — throughput is bounded by sequencer capacity.
- Transactions that need to *read* before deciding what to write (dependent transactions) require a **reconnaissance phase**, adding latency.
- Aborts/retries are expensive — the entire batch may need re-sequencing.

---

### 4. Parallel Commits (CockroachDB)

An optimization over standard 2PC. In regular 2PC:

```
Client → Coordinator: COMMIT
Coordinator → Participants: PREPARE (round 1)
Participants → Coordinator: PREPARED
Coordinator writes commit record (durable)
Coordinator → Participants: COMMIT (round 2)
Coordinator → Client: OK
```

That's **2 network round trips** before the client gets an acknowledgement.

**Parallel Commits** reduce this to **1 round trip**:

1. The coordinator sends the final batch of writes (the last statement before COMMIT) **and** the `PREPARE` messages **in parallel**.
2. Each participant writes its intent and responds.
3. The coordinator can immediately respond to the client once all participants respond, because the commit record is **implicitly durable** — it's the conjunction of all the participants' prepared intents. A separate async process finalizes the commit record.
4. If the coordinator crashes, any other node can determine the transaction's status by checking whether **all** intents were written (→ committed) or not (→ aborted).

This is how CockroachDB achieves **single round-trip cross-shard commits** in the common case.

---

### 5. Raft/Paxos Consensus Per Shard + 2PC Across Shards

This is the layered architecture used by most NewSQL databases:

```
                    ┌─────────────────────────────┐
                    │     Transaction Layer        │
                    │  (2PC across shard groups)   │
                    └──────────┬──────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
    ┌─────▼──────┐      ┌─────▼──────┐      ┌─────▼──────┐
    │ Shard A     │      │ Shard B     │      │ Shard C     │
    │ (Raft Group)│      │ (Raft Group)│      │ (Raft Group)│
    │ Node1(L)    │      │ Node4(L)    │      │ Node7(L)    │
    │ Node2(F)    │      │ Node5(F)    │      │ Node8(F)    │
    │ Node3(F)    │      │ Node6(F)    │      │ Node9(F)    │
    └─────────────┘      └─────────────┘      └─────────────┘
```

- **Within a shard**: Raft/Paxos replicates every write to a quorum of replicas. This gives durability and availability within the shard.
- **Across shards**: 2PC (or a variant like Parallel Commits) coordinates the transaction.
- A single-shard transaction requires **zero 2PC** — just a Raft consensus round within that shard.
- Cross-shard transactions pay the 2PC cost but each prepare/commit is itself replicated via Raft for durability.

Used by: **CockroachDB**, **TiDB**, **YugabyteDB**, **Spanner**

---

### 6. MVCC + Timestamp Ordering

Almost every distributed database uses **Multi-Version Concurrency Control (MVCC)** as the backbone for isolation:

- Every write creates a **new version** of the row, tagged with a timestamp.
- Reads see a **consistent snapshot** at a chosen timestamp — they ignore versions written after their snapshot.
- Conflicts are detected by checking if a concurrent transaction has written a version in the reader's timestamp range.

**Snapshot Isolation** (SI): each transaction reads from a consistent snapshot. Write-write conflicts are detected and one transaction is aborted. Doesn't prevent write skew.

**Serializable Snapshot Isolation** (SSI): extends SI by tracking read-write dependencies. If a potential serialization anomaly is detected (rw-antidependency cycle), one transaction is aborted. Used by CockroachDB and PostgreSQL.

The timestamp assignment strategy is critical:
- **Spanner**: TrueTime (physical time, globally synchronized)
- **CockroachDB**: HLC (hybrid logical clock)
- **TiDB**: TSO (Timestamp Oracle — a centralized service that hands out monotonically increasing timestamps via Raft-replicated counter)

---

### 7. Epoch-Based / Batched Transactions

Used in some streaming/analytical systems (e.g., **FoundationDB**, **Anna**).

1. Time is divided into **epochs** (e.g., 50ms windows).
2. All transactions within an epoch are collected and committed as a **batch**.
3. Conflict detection happens within the batch.
4. The entire batch is either committed or some transactions within it are aborted and retried in the next epoch.

FoundationDB specifically uses a **sequencer** (similar to Calvin) that resolves conflicts for each batch. Combined with an **ordered key-value store** layer and a **log-structured storage engine**, this gives serializable ACID transactions with high throughput.

---

### Summary: When to Use What

| Scenario | Recommended Approach |
|---|---|
| Microservices writing to different databases + caches + queues | Saga + Outbox pattern |
| Single write to DB + publish event | Transactional Outbox (CDC) |
| Strong consistency required across heterogeneous stores | TCC or 2PC (if all support XA) |
| Single distributed SQL database (CockroachDB, Spanner, TiDB) | Database handles it internally (2PC + Raft + MVCC) |
| Financial/banking cross-system transfers | Saga with careful idempotency + compensation OR TCC |
| Global-scale, strict external consistency | Spanner (TrueTime) or CockroachDB (HLC with caveats) |

---

### Key Principles

1. **You cannot have atomic cross-system transactions without coordination** — the question is where you pay the cost (infrastructure 2PC vs. application-level sagas/TCC).
2. **Exactly-once is impossible in general** — design for at-least-once + idempotent consumers.
3. **The outbox pattern is the pragmatic workhorse** — it decouples the hardest part (atomic DB write + event publish) and lets you build everything else on top of events.
4. **Single distributed databases hide the complexity** — but they still run 2PC internally. The advantage is they control the entire stack (clocks, replication, conflict detection) so they can optimize aggressively (parallel commits, pipelining, deterministic ordering).
5. **Clock synchronization is the secret weapon** — the tighter your clocks, the less coordination you need. Spanner's TrueTime is the extreme; HLC and TSO are pragmatic alternatives.
