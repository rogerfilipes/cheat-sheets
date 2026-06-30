# Cassandra 5.0 Cheatsheet

## Contents

- [Architecture (ring, vnodes, gossip)](#architecture-ring-vnodes-gossip)
- [Data modeling (query-first)](#data-modeling-query-first)
- [Partition keys vs clustering keys](#partition-keys-vs-clustering-keys)
- [CQL basics](#cql-basics)
- [Data types and collections](#data-types-and-collections)
- [Secondary and SASI/SAI indexes](#secondary-and-sasisai-indexes)
- [Consistency levels and quorum math](#consistency-levels-and-quorum-math)
- [Tunable consistency vs CAP](#tunable-consistency-vs-cap)
- [Lightweight transactions (LWT / Paxos)](#lightweight-transactions-lwt--paxos)
- [TTL, tombstones, compaction](#ttl-tombstones-compaction)
- [Materialized views](#materialized-views)
- [Java driver 4.x](#java-driver-4x)
- [Modeling: time-series + multi-tenant](#modeling-time-series--multi-tenant)
- [nodetool quick reference](#nodetool-quick-reference)

---

## Architecture (ring, vnodes, gossip)

What it is: masterless peer-to-peer DB (Dynamo replication + Bigtable LSM storage). Every node is identical; any node can be the coordinator.

- Data placed by a token ring. Default partitioner `Murmur3Partitioner`, 64-bit tokens (`-2^63` to `2^63-1`).
- Partition key hashed to a token; owning node holds primary replica, `RF-1` successors hold the rest (placement via `NetworkTopologyStrategy` + snitch).
- Vnodes: each node owns many small token ranges. Default `num_tokens: 16` (old default 256). Faster bootstrap/decommission, better balance; cost is more ranges per range-scan and harder repair.

```text
  [Client] --hash(partition key)--> [Coordinator = any node]
                                          |
                          RF=3 replicas around the ring
                  +-----------+-----------+-----------+
                  v           v           v
            +--------+   +--------+   +--------+   +--------+
            | Node A |   | Node B |   | Node C |   | Node D |
            | tokens |   | tokens |   | tokens |   | tokens |
            +--------+   +--------+   +--------+   +--------+
   Coordinator routes to N1=A, N2=B, N3=C
```

- Gossip: each node gossips with up to 3 peers/sec, exchanging EndpointState (heartbeat + status, schema version, tokens, DC/rack). Drives membership, schema agreement, failure detection.
- Failure detection: Phi Accrual Failure Detector, not a fixed timeout. Node marked DOWN when phi exceeds threshold (default 8); phi is a probability score from heartbeat inter-arrival times.
- Write path: coordinator -> replicas (sync up to CL) -> commitlog + memtable; flush to SSTable; background compaction.
- Read path: coordinator picks fastest replica (snitch + dynamic snitch), reads memtable + SSTables (bloom filters narrow candidates), merges by timestamp, may trigger read repair.

Gotchas:
- No first-party web UI; `cassandra-web` is community-only, not authoritative.
- `SimpleStrategy` ignores DC/rack and is unsafe for multi-DC. Use `NetworkTopologyStrategy` from day one.
- Schema disagreement (multiple `schema_version` in `describecluster`) causes silent prepared-statement failures and DDL hangs. Check it in CI before DDL.
- Gossip is eventually consistent; transient UP/DOWN flaps are normal, do not over-react.
- Keep `num_tokens` consistent cluster-wide; mismatch breaks load balancing.
- Misconfigured `dynamic_snitch_badness_threshold` can pin reads to a slow replica.

---

## Data modeling (query-first)

What it is: the query is the schema. Enumerate access patterns first; design one table per query pattern; denormalize and keep tables in sync from the app/CDC.

Why: no joins, no ad-hoc filtering (only `ALLOW FILTERING`, a full scan), no cheap cross-partition ops. A partition is the unit of locality, replication, and "this read is fast." Every fast query touches exactly one partition (or a bounded `IN` scatter).

Pipeline:

1. Conceptual: entities and relationships.
2. Logical: list every query (key, ordering, selectivity), map each to a table. Choose partition key (equality only) and clustering keys (range + order).
3. Physical: types, partition-size estimate, row size, write pattern (TTL/counter/collection), compaction strategy.

Partition-size budget: aim 10-100 MB and < 100,000 cells per partition. Bucket by time/tenant to bound, e.g. `((sensor_id, day_bucket))` not `((sensor_id))`.

Gotchas:
- SQL-style modeling + `ALLOW FILTERING` is not a fix; it is a full scan.
- `JOIN` does not exist; "join in the app" usually means N+1 round-trips.
- `((tenant_id))` alone becomes a hot, unbounded partition within months.
- Secondary indexes do not solve cross-partition queries.
- Clustering columns fix on-disk order; you cannot reorder at query time.
- Reusing one table for two access patterns by piling on clustering columns usually means you needed two tables.

---

## Partition keys vs clustering keys

What it is: the primary key has two roles - the partition key routes/locates data; clustering keys sort rows within a partition.

| Role | Partition key | Clustering key(s) |
|---|---|---|
| Purpose | Hashed (Murmur3) to token -> routes to node; determines locality + replica set | On-disk sort order within a partition |
| Allowed in WHERE | Equality only: `=`, `IN` | Equality + range (`<`, `<=`, `>`, `>=`) + ordered slicing |
| Required in WHERE | All components, always | Prefix-first (cannot restrict c2 without c1) |
| ORDER BY | No | Yes, must match or reverse declared order |
| Part of PK syntax | `((a, b))` = composite tuple | trailing cols after partition key |

Compound vs composite:
- `PRIMARY KEY (a, b, c)` -> partition key is `a`; `b`, `c` are clustering (compound).
- `PRIMARY KEY ((a, b), c)` -> partition key is tuple `(a, b)`; `c` clustering (composite). The double parens matter.

Query legality rules:
1. Restrict all partition-key components with `=` or `IN`.
2. Clustering keys restricted prefix-first.
3. A range on a clustering key terminates the prefix: equality `c1` then range `c2` is OK; range `c1` then anything on `c2` is not.
4. `ORDER BY` only on clustering columns, matching or reversing declared order.

```sql
-- Bucketed partition to bound size
CREATE TABLE events_by_user_day (
  user_id  uuid, day date, event_at timestamp, event_id timeuuid,
  type text, payload text,
  PRIMARY KEY ((user_id, day), event_at, event_id)
) WITH CLUSTERING ORDER BY (event_at DESC, event_id ASC);

-- LEGAL: full partition key + range on first clustering col
SELECT * FROM events_by_user_day
  WHERE user_id = ? AND day = ? AND event_at >= ?;

-- ILLEGAL: partial partition key (user_id without day)
-- ILLEGAL: skip clustering prefix (event_id restricted, event_at not)
```

Gotchas:
- Forgetting double parens: `(a, b, c)` is NOT `((a, b), c)`.
- Low-cardinality field (`status`, `country`) as sole partition key -> hot partition.
- Monotonic value as partition key with no bucket -> unbounded growth.
- `IN` with hundreds of partition values -> scatter across many nodes, coordinator memory pressure.
- Counter columns cannot be part of the primary key.

---

## CQL basics

What it is: SQL-looking but not SQL. No joins, sub-selects, arbitrary WHERE, foreign keys, or multi-row transactions.

```sql
CREATE KEYSPACE app WITH replication = {
  'class':'NetworkTopologyStrategy','datacenter1':1
} AND durable_writes = true;

CREATE TABLE app.user (
  id uuid PRIMARY KEY, email text, created_at timestamp
) WITH default_time_to_live = 0
   AND gc_grace_seconds = 864000;
```

Key semantics:
- INSERT vs UPDATE: identical (both upsert). INSERT does not fail on existing row; UPDATE creates the row if missing.
- DELETE is a write: it writes a tombstone with a timestamp. Row gone only after compaction past `gc_grace_seconds` (default 10 days).
- Last-write-wins (LWW): every cell has a microsecond write timestamp; highest wins, ties resolved by value. `USING TIMESTAMP` overrides it (dangerous).
- TTL is per-cell: `INSERT ... USING TTL 3600`.
- BATCH is not a transaction: atomic + isolated on a single partition only. Cross-partition batches are logged, slower, for keeping denormalized tables consistent.
- Counters: special table type, every non-PK column must be `counter`, no TTL, not idempotent.

```sql
UPDATE app.user SET email = ? WHERE id = ?;       -- upsert, creates if missing
INSERT INTO app.user (id, email) VALUES (?, ?) IF NOT EXISTS;  -- LWT, expensive
SELECT id, WRITETIME(email), TTL(email) FROM app.user;        -- inspect

CREATE TABLE counts (id uuid PRIMARY KEY, hits counter);
UPDATE counts SET hits = hits + 1 WHERE id = ?;   -- counters only
```

Introspection: `system_schema.tables`, `system_schema.columns`. cqlsh: `DESCRIBE KEYSPACES`, `DESCRIBE TABLE`, `TRACING ON`.

Gotchas:
- INSERT does NOT fail on duplicate unless you add `IF NOT EXISTS` (LWT).
- `USING TIMESTAMP` with a timestamp higher than a tombstone resurrects deleted data (zombie rows).
- Mass `DELETE` over a range degrades reads until compaction (tombstones).
- Counter writes are not idempotent; retry after timeout may double-count.
- Inserting `NULL` is a delete (creates a tombstone) - omit the column instead.
- Client clock skew wrecks LWW resolution.

---

## Data types and collections

Common types: `uuid`, `timeuuid`, `text`, `int`, `bigint`, `double`, `boolean`, `timestamp`, `date`, `blob`, `decimal`, `inet`, `counter`.

Collections: `list<T>`, `set<T>`, `map<K,V>`, and UDT (struct). Designed for small, bounded collections; soft warning around 64k elements, degrades well before.

| Aspect | Non-frozen (`set<text>`) | Frozen (`frozen<set<text>>`) |
|---|---|---|
| Storage | N cells, one per element | Single opaque blob, 1 cell |
| Mutation | Per-element add/remove | Whole value only |
| In primary key | No | Yes (comparable) |
| Read | Returns the entire collection | Returns the entire blob |

```sql
-- Safe: set/map merges, no read-before-write
UPDATE article SET tags = tags + {'cassandra'} WHERE id = ?;
UPDATE article SET tags = tags - {'nosql'} WHERE id = ?;
UPDATE article SET authors[?] = 'Alice' WHERE id = ?;

-- DANGEROUS: list append/prepend = read-before-write on coordinator
UPDATE article SET history = history + ['v2'] WHERE id = ?;

-- UDT (almost always frozen)
CREATE TYPE address (street text, city text, zip text);
ALTER TABLE article ADD billing frozen<address>;
ALTER TYPE address ADD country text;  -- existing rows return country=null
```

Gotchas:
- `list` append/prepend and remove-by-index need read-before-write: breaks idempotency, retries can duplicate, latency unbounded. Prefer `set`/`map` (CRDT-like merges, no read-before-write).
- Unbounded non-frozen collection: every read returns the whole thing, inflates p99.
- For high-cardinality "set of N things tied to a row," use a side table keyed by `((parent_id), child_id)`, not a collection.
- Non-frozen UDTs are rare (schema-evolution caveats); use `frozen<>`.
- You cannot drop a UDT field in use; renames are partial. Adding a field returns `null` for old rows.

---

## Secondary and SASI/SAI indexes

What it is: Cassandra secondary indexes (2i) are LOCAL per-node, not a global B-tree. A query by an indexed column without the partition key fans out to every node (scatter-gather, O(N) nodes).

```text
   Client: SELECT * FROM users WHERE email = ?
                       |
                  [Coordinator]  <--- merges replies
        +----------+----+----+----------+
        v          v         v          v
   [Node1 2i]  [Node2 2i] [Node3 2i] [Node4 2i]   (each scans local index)
        +----------+----+----+----------+
                       v
              Merge, page back to client
```

When 2i is acceptable (all of these):
- Query ALSO restricts the partition key (lookup local to that partition's replicas).
- Moderate cardinality (not 1 row/value, not 1 value for whole table).
- Rarely updated column (updates rewrite index, deletes create index tombstones).
- Not on the hot path (ops/admin queries OK; user-facing not).

Default answer: a denormalized lookup table.

```sql
CREATE TABLE app.user_by_email (email text PRIMARY KEY, user_id uuid);
-- write both tables; one round-trip lookup, deterministic latency
```

- SASI (SSTable-Attached): supports LIKE/range, still experimental + flagged, inherits local-scatter + memory pressure. Avoid for new deployments.
- SAI (Storage-Attached, 5.0): modern alternative, better operational profile, still local-per-node. Benchmark vs denormalization.

Gotchas:
- High-cardinality 2i: looks fine in a demo, terrible at scale (one row matches, still hit every node).
- Low-cardinality 2i: returns most of the table, kills the coordinator.
- 2i without a partition-key predicate: every query is a cluster-wide scatter.
- 2i on heavily-mutated columns: tombstone storms in index SSTables.
- No composite indexes like SQL; you cannot index `(status, region)` as one ordered index.
- Run `nodetool rebuild_index` after bulk loads; the index lags otherwise.

---

## Consistency levels and quorum math

What it is: RF = replicas per partition per DC. CL = how many must ack before the coordinator returns success.

Strong-consistency (read-your-writes) rule on a single DC:

```text
R + W > RF   -> strong consistency on that key
```

With RF=3:
- W=QUORUM(2) + R=QUORUM(2): 2+2 > 3 -> strong.
- W=ONE + R=ALL: 1+3 > 3 -> strong, but read fails if any replica down.
- W=ONE + R=ONE: 1+1 = 2 <= 3 -> eventual.

| CL | Replicas required | Notes |
|---|---|---|
| ANY | 1, even just a hint on coordinator | Weakest; almost never used |
| ONE / LOCAL_ONE | 1 replica | Lowest latency, weakest consistency |
| TWO / THREE | 2 / 3 replicas | Fixed counts |
| QUORUM | (RF/2)+1 across ALL DCs | Cross-DC latency every op |
| LOCAL_QUORUM | (RF_local/2)+1 in client's local DC | Right multi-DC default |
| EACH_QUORUM | quorum in EVERY DC | Survive DC loss with strong reads |
| ALL | all replicas all DCs | One down node = failed query |
| SERIAL / LOCAL_SERIAL | Paxos quorum | LWT reads (see LWT section) |

Quorum math: QUORUM count = floor(RF/2) + 1.
- RF=3 -> QUORUM=2; RF=5 -> QUORUM=3.
- Multi-DC, DC1 RF=3 + DC2 RF=3: QUORUM = (6/2)+1 = 4 across both DCs; LOCAL_QUORUM = (3/2)+1 = 2 per DC.
- "Survive DC failure with strong reads": W=EACH_QUORUM, R=LOCAL_QUORUM.

Convergence mechanisms:
- Hinted handoff: coordinator stores a hint for a down replica; replays on recovery. Has a TTL (`max_hint_window_in_ms`, default 3h). NOT a substitute for repair.
- Read repair: digest reads detect divergence during a query; merged row pushed to lagging replicas.
- Anti-entropy repair: `nodetool repair` (Merkle-tree compare + stream). Run on a cadence shorter than `gc_grace_seconds` or deleted data resurrects.

```sql
CONSISTENCY LOCAL_QUORUM;   -- cqlsh session-level
```
```java
SimpleStatement.builder("SELECT * FROM k.t WHERE id=?")
    .addPositionalValue(id)
    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    .build();
```

Gotchas:
- R=ONE + W=ONE is the default in many samples: fast in dev, stale reads in prod.
- QUORUM in multi-DC crosses DCs (WAN RTT every op); use LOCAL_QUORUM.
- ALL: any down node fails the query.
- Hints expire; only repair guarantees convergence. Skipping repair past `gc_grace_seconds` resurrects deleted data.
- Mixed CLs across services on the same data produce surprising stale/ghost reads.

---

## Tunable consistency vs CAP

What it is: Cassandra is AP under CAP, but exposes per-operation tunable consistency, so the C/A trade is per request, not per cluster.

PACELC: PA/EL. During Partition, prefer Availability over Consistency. Else, prefer Latency over Consistency (until you raise CL).

- `R + W > RF` gives linearizable-on-a-key behavior (true compare-and-set linearizability needs LWT/Paxos).
- `R + W <= RF` gives faster, eventually-consistent reads.

Partition scenarios (RF=3 unless noted):

| Scenario | Outcome |
|---|---|
| 1 replica down, CL=QUORUM | Reads and writes succeed (2 of 3); hints buffer the down replica |
| 2 replicas down, CL=QUORUM | Writes fail `UnavailableException`; does NOT silently drop to ONE |
| DC partition, CL=LOCAL_QUORUM | Both DCs keep serving locally; converge on heal via hints + repair (LWW by timestamp) |
| DC partition, CL=EACH_QUORUM | Writes fail on the side that cannot reach the other DC |
| Coordinator dies mid-write, CL=QUORUM | Client gets `WriteTimeoutException`; write may or may not have applied |

Key truth: `WriteTimeoutException` does NOT mean "did not write." It means "not enough acks before timeout." Idempotent writes (no counters, no lists) are safe to retry; otherwise reconcile via a read.

Gotchas:
- "Cassandra is AP so just retry" is marketing-wrong: you can get strong-on-key with `R+W>RF`, but only if you also repair.
- Assuming `WriteTimeoutException` means the write failed.
- Treating higher CL as free: more acks = more latency + lower availability under failure.
- LWT and counters break the `R+W>RF` heuristic.
- Repair is the convergence mechanism; skip it and "eventual" never arrives.

---

## Lightweight transactions (LWT / Paxos)

What it is: compare-and-set on a single partition via Paxos. Any mutation with an `IF` clause is an LWT.

```sql
INSERT ... IF NOT EXISTS
UPDATE ... IF col = ?
UPDATE ... IF EXISTS
DELETE ... IF col = ?
```

Protocol: roughly 4 round-trips (Prepare/Promise, Read to evaluate IF, Propose/Accept, Commit).

Cost:
- About 4x the latency of a normal QUORUM write at best; worse under contention.
- Per-partition serialization: concurrent LWTs on the same partition queue, collide, and cause `CasWriteTimeout`.
- Returns `[applied]` plus the previous/conflicting row; client MUST check `[applied]`.
- Reads must use `SERIAL` (Paxos across all DCs, cross-DC RTT) or `LOCAL_SERIAL` (local DC only) to see in-flight Paxos state.

Not a multi-key transaction: no BEGIN/COMMIT across rows or tables. Need that? Use an external coordinator (saga, outbox) or a different DB.

```sql
INSERT INTO user_by_email (email, user_id) VALUES (?, ?) IF NOT EXISTS;
UPDATE orders SET status='SHIPPED' WHERE id=? IF status='PAID';   -- state guard
```
```java
BoundStatement bs = ps.bind("a@b.com", id)
    .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);   // commit phase
boolean applied = session.execute(bs).one().getBoolean("[applied]");
```

When justified: unique constraints, idempotency-token claims, state-machine guards, lock/leader patterns with TTL fencing.

Gotchas:
- "I'll add IF EXISTS to be safe": turns every UPDATE into Paxos, about 4x latency.
- `IF NOT EXISTS` on a hot partition: concurrent inserters serialize and time out.
- Forgetting `LOCAL_SERIAL` reads: can see stale data mid-commit.
- Retrying a non-idempotent LWT after timeout blindly can re-evaluate the condition wrongly.
- Using `SERIAL` when `LOCAL_SERIAL` suffices pays cross-DC RTT per LWT.
- `system.paxos` needs its own repair; abandoned ballots leak and block later LWTs.

---

## TTL, tombstones, compaction

What it is: LSM storage. Writes -> commitlog + memtable -> immutable SSTables; compaction merges SSTables to reclaim space and bound the SSTables a read touches.

Tombstones: every delete (and expired TTL, and inserted NULL) is a write marking data deleted with a timestamp. Removed only after `gc_grace_seconds` elapses AND compaction merges it. Default `gc_grace_seconds = 864000` (10 days): gives repair a window to propagate the tombstone, preventing deleted-data resurrection.

- TTL is a deferred tombstone: after expiry reads ignore it; becomes a tombstone at next compaction, removed `gc_grace_seconds` later.
- Read amplification: tombstones count against `tombstone_warn_threshold` (1000) and `tombstone_failure_threshold` (100000). Hitting failure throws `TombstoneOverwhelmingException` and aborts the read.

| Strategy | Best for | How it works | Trade-off |
|---|---|---|---|
| STCS (Size-Tiered) | Write-heavy, low read | Merges SSTables of similar size | Read amplification grows; big temp disk spike on major compaction |
| LCS (Leveled) | Read-heavy, predictable latency | Fixed-size SSTables in levels, each 10x previous | More write amplification; better read tail |
| TWCS (Time-Window) | Time-series with TTL | Buckets SSTables by time window; drops whole windows on TTL | Excellent for time-series; bad if you delete/update old data |
| UCS (Unified, 5.0) | General-purpose, adaptive | Tunable, spans STCS+LCS | Newer; benchmark before adopting |

```sql
CREATE TABLE sensor_reading (
  sensor_id uuid, bucket date, ts timestamp, value double,
  PRIMARY KEY ((sensor_id, bucket), ts)
) WITH default_time_to_live = 2592000        -- 30 days
   AND gc_grace_seconds = 86400              -- shorter when no manual deletes
   AND compaction = {
     'class':'TimeWindowCompactionStrategy',
     'compaction_window_unit':'DAYS','compaction_window_size':'1'
   };
```

Gotchas:
- Mass `DELETE WHERE pk IN (...)` over a hot range -> `TombstoneOverwhelmingException`.
- TWCS with frequent updates/deletes to old data: old windows cannot drop, lose the main benefit.
- Lowering `gc_grace_seconds` to 0 to clear tombstones faster -> deleted data resurrects.
- Inserting `NULL` creates a tombstone; omit the column instead.
- LCS on a write-heavy table: write amplification up to 10x.
- Prefer one range delete (`pk=? AND clustering >= ? AND clustering < ?`) over many per-row deletes.
- `TRUNCATE` writes a metadata marker, not row tombstones; correct tool to empty a table fast.

---

## Materialized views

What it is: a server-maintained denormalized copy of a base table, keyed by a different PK. Server intercepts base writes and applies them to each MV.

```sql
CREATE MATERIALIZED VIEW user_by_email AS
  SELECT * FROM user
  WHERE email IS NOT NULL AND id IS NOT NULL
  PRIMARY KEY (email, id);
```

Rules: MV PK must include EVERY base PK column; at most ONE new (lifting) column; all MV PK columns must be `IS NOT NULL`.

| Aspect | MV | App-maintained table |
|---|---|---|
| Consistency | Async, server-batched, can diverge | App-controlled (batch or LWT-protected) |
| Write amplification | Server-side | App-side but explicit |
| Schema evolution | Constrained (PK rules) | Free |
| Failure mode | Opaque (batchlog, view rebuild) | Explicit (idempotent writer, reconciliation) |
| Observability | Limited | Full app metrics |
| Maturity | Improving; historically risky | Well-understood |

Default to app-maintained denormalization. Use MV only for internal/analytics tables where lag is acceptable, single-team-owned write paths, low write rate + low key cardinality.

Gotchas:
- MV writes are NOT atomic with the base write; views can lag and diverge.
- Coordinator dying after base write but before MV update -> divergence (batchlog + per-view repair try to converge, but it can persist).
- MV repair has caveats and was historically broken; `nodetool rebuild_view` is a full base-table re-scan and blocks compaction throughput.
- Write amplification: each base write = 1 + N view writes with replica fan-out.
- Range deletes through the MV path have been a known correctness issue.
- `IS NOT NULL` constraint means rows with NULL in the lifting column never appear.
- MV is NOT a fix for a secondary index; counter columns cannot be in MVs.

---

## Java driver 4.x

What it is: DataStax java-driver 4.17 (Java 21). Config-driven (HOCON `application.conf`), async-first (`CompletionStage`), `CqlSession` replaces `Session`.

- `CqlSession` is thread-safe + heavy: ONE per application, never per request.
- Prepared statements: prepare every repeated query once at startup. Enables metadata cache, skips per-call parsing, enables token-aware routing (driver routes to a replica directly).
- Paging: results paged by `page_size` (default 5000); each next page is a server round-trip. Iterator auto-fetches; hold `PagingState` to resume.
- Async: `executeAsync` returns `CompletionStage<AsyncResultSet>`. Bound in-flight with a semaphore (often 256-1024); unbounded fan-out OOMs the driver queue.
- Load balancing: `DefaultLoadBalancingPolicy` is DC-aware + token-aware. `local-datacenter` is required.
- Retries: `DefaultRetryPolicy` retries ReadTimeout (if enough replicas responded), WriteTimeout only on BATCH_LOG, Unavailable on next host. Driver will not retry statements it considers non-idempotent.
- Idempotency: mark explicitly with `setIdempotent(true)`. Do NOT mark counter writes, list appends, or LWT.

```hocon
datastax-java-driver {
  basic {
    contact-points = [ "127.0.0.1:9042" ]
    load-balancing-policy { local-datacenter = "datacenter1" }
    request {
      timeout = 2 seconds
      consistency = LOCAL_QUORUM
      page-size = 500
      default-idempotence = false
    }
  }
  advanced.connection.pool.local.size = 1
}
```
```java
PreparedStatement ps = session.prepare("SELECT id, email FROM app.user WHERE id = ?");
Row r = session.execute(ps.bind(id).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)).one();

// bounded async fan-out
Semaphore inFlight = new Semaphore(512);
for (UUID id : ids) {
    inFlight.acquire();
    futures.add(dao.upsertAsync(id).whenComplete((x, e) -> inFlight.release()));
}
```

Gotchas:
- `CqlSession` per request -> connection storms, schema-version churn.
- `prepare()` per call -> "Re-preparing statement" log spam + latency tax.
- `executeAsync` with no semaphore -> queue overflow, rejections, OOM.
- Marking non-idempotent statements idempotent -> silent double-application.
- Forgetting `LOCAL_QUORUM` and silently getting `ONE` from an unreviewed config.
- Iterating large `SELECT *` without paging awareness -> miss rows or OOM the client.
- Too-low `request.timeout` -> cascading retries during normal GC pauses.
- Schema disagreement during deploys -> prepared statements blocked until convergence.

---

## Modeling: time-series + multi-tenant

What it is: worked design for a multi-tenant SaaS metrics-ingest. Ties together the partition-sizing, consistency, TTL/compaction, and LWT rules above.

Sizing: 86,400 readings/sensor/day x 200 bytes = ~17 MB/day raw -> bucket by `day` keeps a partition under the 100 MB budget. Largest tenant 10k sensors, so partition must include `sensor_id` (not tenant alone).

```sql
-- Raw readings (live view, time range, idempotent ingest)
CREATE TABLE readings_raw (
  tenant_id uuid, sensor_id uuid, day date,
  ts timestamp, reading_id timeuuid, value double, meta frozen<map<text,text>>,
  PRIMARY KEY ((tenant_id, sensor_id, day), ts, reading_id)
) WITH CLUSTERING ORDER BY (ts DESC, reading_id ASC)
   AND default_time_to_live = 7776000        -- 90 days
   AND gc_grace_seconds = 86400
   AND compaction = {'class':'TimeWindowCompactionStrategy',
       'compaction_window_unit':'DAYS','compaction_window_size':'1'};

-- Idempotency claim (TTL dedup window, not per-reading LWT)
CREATE TABLE ingest_claim (
  reading_id timeuuid PRIMARY KEY, claimed_at timestamp
) WITH default_time_to_live = 600;           -- 10 min
```

Key choices:
- `((tenant_id, sensor_id, day))`: bounds each partition ~17 MB, co-locates a sensor's day.
- `ts DESC` clustering: newest-first live view, no reverse-scan cost.
- `reading_id timeuuid` clustering tiebreaker: uniqueness within a millisecond without LWT.
- TWCS daily for raw / weekly for hourly rollups: whole windows drop on TTL.
- LCS for `sensor_by_tenant` (read-heavy admin).

Write path: do NOT `INSERT ... IF NOT EXISTS` on every reading (Paxos serializes per partition, throughput collapses). Instead claim once via `INSERT INTO ingest_claim ... IF NOT EXISTS` (`LOCAL_SERIAL` + `LOCAL_QUORUM`); if applied, write the raw reading at normal `LOCAL_QUORUM`.

Consistency: raw writes `LOCAL_QUORUM`; live-view reads `LOCAL_ONE` (OK to miss last 100ms); billing/audit reads `LOCAL_QUORUM`.

Gotchas:
- `tenant_id` alone as partition key: biggest tenant dominates one replica set.
- Forgetting the `day` bucket: 90-day per-sensor partitions blow the 100 MB budget.
- LWT on every reading: ingest throughput collapses.
- MV for rollups: divergence + write amp; use a batch consumer + app-maintained table.
- Non-TWCS for raw readings: old days never drop cleanly.
- `gc_grace_seconds = 0` "because TTL handles it": data resurrects after a node outage.

---

## nodetool quick reference

| Command | Use |
|---|---|
| `nodetool status` | Per-node UP/DOWN (UN/DN/UJ), ownership % |
| `nodetool ring` | Token ranges |
| `nodetool gossipinfo` | Gossip application states |
| `nodetool describecluster` | Schema versions (divergence = trouble) |
| `nodetool tpstats` | Thread-pool backpressure (Pending/Blocked) |
| `nodetool cfstats <ks>.<tbl>` | Partition sizes, tombstones per slice |
| `nodetool compactionstats` | Running compactions |
| `nodetool sstablemetadata <file>` | Per-SSTable tombstone histograms |
| `nodetool repair [ks]` | Anti-entropy repair (run < gc_grace_seconds) |
| `nodetool statushandoff` | Hinted handoff state |
| `nodetool rebuild_index <ks> <tbl> <idx>` | Rebuild a 2i after bulk load |
| `nodetool rebuild_view <ks> <tbl> <view>` | Rebuild an MV (full base scan) |
| `nodetool viewbuildstatus <ks> <view>` | MV build progress |
