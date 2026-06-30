# Kafka + Avro Cheatsheet

## Contents

Part 1: Kafka Core
- [Brokers / Topics / Partitions / Replicas](#brokers--topics--partitions--replicas)
- [Producer API (acks / idempotence)](#producer-api-acks--idempotence)
- [Consumer API & Consumer Groups](#consumer-api--consumer-groups)
- [Offset Management](#offset-management)
- [Delivery Semantics](#delivery-semantics)
- [Partitioner & Keying](#partitioner--keying)
- [Retention, Compaction, Tombstones](#retention-compaction-tombstones)
- [Transactions & EOS](#transactions--eos)
- [Kafka Streams Primer](#kafka-streams-primer)
- [Kafka Connect](#kafka-connect)
- [Monitoring (JMX, Lag)](#monitoring-jmx-lag)
- [Security (SASL / SSL)](#security-sasl--ssl)
- [Performance Tuning](#performance-tuning)
- [Failure Modes](#failure-modes)

Part 2: Avro / Schema Registry
- [Why a Schema Registry](#why-a-schema-registry)
- [Avro Format & Schema Language](#avro-format--schema-language)
- [Generating Java from .avsc](#generating-java-from-avsc)
- [Producer/Consumer with KafkaAvroSerializer](#producerconsumer-with-kafkaavroserializer)
- [Schema Evolution & Compatibility](#schema-evolution--compatibility)
- [Subject Naming Strategies](#subject-naming-strategies)
- [Specific vs Generic Records](#specific-vs-generic-records)
- [Schema References](#schema-references)
- [Avro vs Protobuf vs JSON Schema](#avro-vs-protobuf-vs-json-schema)
- [Schema Registry REST API](#schema-registry-rest-api)
- [Schema Registry Security & ACLs](#schema-registry-security--acls)
- [Testing with MockSchemaRegistryClient](#testing-with-mockschemaregistryclient)
- [Migrating a Live Topic](#migrating-a-live-topic)

---

# Part 1: Kafka Core

## Brokers / Topics / Partitions / Replicas

What it is: a distributed, partitioned, replicated commit log. Topic = named bundle of append-only partitions; offset is per-partition and monotonic. Ordering is guaranteed only within a partition.

Glossary:

| Term | Meaning |
|------|---------|
| partition | append-only log; unit of parallelism, ordering, replication |
| offset | position of a record within a partition |
| broker | machine storing partitions |
| replica | copy of a partition on another broker |
| leader | the replica clients read/write |
| follower / ISR | followers caught up within `replica.lag.time.max.ms` (default 30s) |
| key | decides partition: `partition = murmur2(key) % N` |

Durability triangle: `acks` (producer) + `min.insync.replicas` (topic) + ISR membership (broker). Standard durable config: RF=3, `min.insync.replicas=2`, `acks=all` (survives one broker loss).

KRaft (Confluent 7.6 default): Raft controller quorum (3 or 5 nodes) replaces ZooKeeper; metadata lives in `__cluster_metadata` topic.

Gotchas:
- `unclean.leader.election.enable=true` trades silent committed-data loss for availability; default false, keep it.
- Partition count: can grow, never shrink. Adding partitions breaks `hash(key) % N` keyed ordering (keys re-map).
- `min.insync.replicas = RF` means any single replica down blocks all writes; leave a cushion.
- RF=1 outside dev = permanent loss on one disk failure.
- Internal topics (`__cluster_metadata`, `__consumer_offsets`, `__transaction_state`) are real topics; keep them healthy.
- KRaft combined mode (controller+broker same node) is dev only.

CLI:

```bash
kafka-topics --bootstrap-server kafka:9092 --create --topic orders \
  --partitions 3 --replication-factor 1 --config min.insync.replicas=1
kafka-topics --bootstrap-server kafka:9092 --describe --topic orders
kafka-metadata-quorum --bootstrap-server kafka:9092 describe --status
```

## Producer API (acks / idempotence)

What it is: async `send()` returns `Future<RecordMetadata>`; the Sender thread batches per partition and ships when `batch.size` is full or `linger.ms` elapses. Brokers store bytes only, so `key.serializer` and `value.serializer` are mandatory (no default).

acks matrix:

| acks | Semantics | Failure mode |
|------|-----------|--------------|
| `0` | fire and forget, no wait | silent loss on network/broker failure |
| `1` | leader wrote its log | loss if leader dies before followers replicate |
| `all` / `-1` | all ISR members appended | slowest; durable with `min.insync.replicas` |

Idempotence (`enable.idempotence=true`, default since 3.0): per-partition, per-session dedup via `(PID, epoch, sequence)`. Requires:
- `acks=all`
- `retries > 0` (default `Integer.MAX_VALUE`)
- `max.in.flight.requests.per.connection <= 5`

Does NOT span partitions atomically and does NOT survive producer restart (PID resets). For atomic cross-partition + offset commit, use transactions.

Key producer configs:

| Config | Default | Purpose |
|--------|---------|---------|
| `acks` | `all` (client default varies) | durability level |
| `enable.idempotence` | `true` | dedup retries per partition/session |
| `retries` | `Integer.MAX_VALUE` | bounded by `delivery.timeout.ms` |
| `max.in.flight.requests.per.connection` | 5 | concurrency; <=5 keeps idempotent ordering |
| `delivery.timeout.ms` | 120000 | total time send() spends retrying |
| `request.timeout.ms` | 30000 | single in-flight request cap |
| `max.block.ms` | 60000 | how long send() blocks for buffer space |
| `linger.ms` | 0 | wait to fill a batch |
| `batch.size` | 16384 | per-partition batch buffer bytes |
| `buffer.memory` | 33554432 | total accumulator memory |
| `compression.type` | none | `lz4` / `zstd` recommended |

Gotchas:
- `acks=0` for "performance" turns off durability.
- `send().get()` per record serializes the pipeline; kills throughput.
- No `flush()`/`close()` before JVM exit: buffered records lost.
- `enable.idempotence=true` with `max.in.flight > 5`: 3.0+ throws.
- `retries=0` to "fail fast" silently disables idempotence.
- Retriable errors (`NotEnoughReplicas`, `NotLeaderForPartition`, transient net) auto-retried; terminal errors (`RecordTooLarge`, serialization, auth) never retried. Callback fires only terminally.

## Consumer API & Consumer Groups

What it is: consumers sharing a `group.id` form a group; the coordinator assigns partitions so each partition has exactly one consumer in the group. Different groups each see every record. One partition is the ceiling on useful consumers per group.

`poll(Duration)` does three jobs: fetch records, allow background heartbeats, reset the processing deadline. Two eviction timers:
- `session.timeout.ms` (45s): no heartbeat (JVM dead / GC-frozen).
- `max.poll.interval.ms` (5 min): app thread too slow between polls.

Assignment strategies:

| Strategy | Behavior |
|----------|----------|
| Range | contiguous per topic; eager (stop-the-world) |
| RoundRobin | spreads across all; eager |
| Sticky | preserves locality; eager |
| CooperativeSticky | default since 3.0; revokes only moving partitions |

Key consumer configs:

| Config | Default | Purpose |
|--------|---------|---------|
| `group.id` | (none) | group membership |
| `enable.auto.commit` | `true` | timer-based commit (often turn off) |
| `auto.offset.reset` | `latest` | `earliest`/`latest`/`none`, only when no committed offset |
| `max.poll.records` | 500 | records per poll() |
| `max.poll.interval.ms` | 300000 | processing deadline |
| `session.timeout.ms` | 45000 | heartbeat deadline |
| `fetch.min.bytes` | 1 | broker waits for this much data |
| `fetch.max.wait.ms` | 500 | cap on that wait |
| `isolation.level` | `read_uncommitted` | use `read_committed` for EOS reads |
| `partition.assignment.strategy` | CooperativeSticky | rebalance protocol |

Gotchas:
- `KafkaConsumer` is NOT thread-safe; only `wakeup()` is safe from another thread (clean shutdown).
- Fix `max.poll.interval.ms` evictions by lowering `max.poll.records`, NOT by raising the interval (that disables death detection).
- `auto.offset.reset=latest` on a fresh group silently skips the backlog.
- Mixing eager (Range/RoundRobin) and cooperative members in one group breaks rebalances; live migration needs a documented two-rolling-restart.
- `assign()` skips the group entirely: no rebalance, you own offsets.

CLI:

```bash
kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group orders-processor
kafka-consumer-groups --bootstrap-server kafka:9092 --group orders-processor \
  --topic orders --reset-offsets --to-earliest --execute
```

## Offset Management

What it is: a committed offset (the NEXT offset to read, `lastConsumed + 1`) is persisted to the compacted internal topic `__consumer_offsets`, keyed `(group, topic, partition)`. Only the latest survives compaction.

Commit modes:
- Auto (`enable.auto.commit=true`): commits current position every `auto.commit.interval.ms` (5s), evaluated inside `poll()`. Position advances when records are RETURNED, not processed: gives neither clean at-least nor at-most once. Canonical data-loss path.
- `commitSync()`: blocking, retries, throws on terminal failure. Use after a processed batch.
- `commitAsync(cb)`: fire-and-return; faster. Finish shutdown with a `commitSync()` so the last commit is durable.

Commit timing -> semantics:

| Semantics | Order | Crash effect |
|-----------|-------|--------------|
| At-least-once | process then commit | re-deliver, duplicates (need idempotent processing) |
| At-most-once | commit then process | lose in-flight record |
| Exactly-once | produce + commit in one txn | no dup/loss (transactions) |

Gotchas:
- Commit `offset + 1`, not last consumed; otherwise reprocess one record every restart.
- `commitAsync()`-only with no final `commitSync()` is a shutdown bug.
- `CommitFailedException` is a rebalance signal (partition revoked mid-batch), not a transient error; do not blindly retry.
- Thread-confine the consumer; commit only on the polling thread.
- Parallel per-key processing: commit only the contiguous prefix, never past a hole.
- External-DB offsets need the offset write in the SAME transaction as the data (outbox); otherwise it is bad 2PC.
- `OffsetOutOfRangeException`: lagged past `retention.ms`; `auto.offset.reset` kicks in.

Use `ConsumerRebalanceListener.onPartitionsRevoked` to flush/commit before partitions move; `onPartitionsAssigned` to `seek()` for external-store offsets.

## Delivery Semantics

What it is: an emergent property of producer config plus the order of process-vs-commit on the consumer.

| Semantic | Producer | Consumer |
|----------|----------|----------|
| At-most-once | `acks=0/1`, no retries | commit before process |
| At-least-once | `acks=all`, retries/idempotent | process then commit |
| Exactly-once (in Kafka) | idempotent + transactional | `read_committed`, offsets in txn |

Exactly-once = effectively-once WITHIN Kafka. Composes from: idempotent producer (dedup retries) + transactions (atomic multi-partition write AND offset commit) + consumer `isolation.level=read_committed` (skips aborted records). `processing.guarantee=exactly_once_v2` in Streams packages it.

Gotchas:
- EOS stops at the Kafka boundary. External sink (DB/S3/REST) needs idempotent writes, outbox, or sink-side 2PC.
- The atomic unit is "output records + offset commit". `sendOffsetsToTransaction` is load-bearing; replacing with `commitSync()` breaks EOS.
- `read_committed` is not free: consumer can only read up to the Last Stable Offset (LSO); a long open txn stalls all `read_committed` readers (LSO lag).
- Side effects inside an EOS loop (REST/email) run even if the txn aborts, and may run twice on replay.

## Partitioner & Keying

What it is: the rule mapping a record to a partition. Precedence:
1. Explicit partition in `ProducerRecord` -> used, partitioner skipped.
2. Key present -> `partition = murmur2(keyBytes) % N` (Java client default).
3. No key -> sticky/adaptive: fill one partition's batch, then switch.

Same key -> same partition -> per-key ordering (while N is stable). Sticky (KIP-480) fixed the round-robin small-batch pathology; adaptive (KIP-794, 3.3+) biases toward fast brokers. Plug custom logic via `partitioner.class`.

Gotchas:
- Hot keys are the #1 pathology (one broker pegged, others idle). Mitigate with composite key `tenant:bucket`, salting (`user_42:0`, `user_42:1`), or coarser key granularity.
- Adding partitions re-runs `hash(key) % N`: keys move, ordering and stateful consumers break. One-way (grow only).
- `murmur2` is the Java hash; non-Java clients (librdkafka: Python/Go/C#) may hash differently. Cross-client keyed reads not co-located.
- Sticky + low rate + small `linger.ms` can pile a burst on one partition (looks like imbalance).
- A custom partitioner that throws fails the whole `send()`; `keyBytes` can be null.
- Partition order is per-partition append order, not global event-time order.

## Retention, Compaction, Tombstones

What it is: per-topic cleanup policy. `delete` evicts whole old segments; `compact` keeps the latest value per key forever.

| | `delete` | `compact` |
|--|----------|-----------|
| Rule | drop closed segments older than `retention.ms` / over `retention.bytes` | keep latest value per key |
| Granularity | whole segment | per key |
| Use | event streams (orders, clicks) | state/changelog (latest profile, balance) |

Tombstone: a record with key + `null` value marks a key for deletion. After compaction it lingers `delete.retention.ms` (24h) so readers see the delete, then the marker is removed.

Key configs: `cleanup.policy` (`delete` / `compact` / `compact,delete`), `retention.ms`, `retention.bytes`, `segment.bytes` (~1 GB), `segment.ms`, `min.cleanable.dirty.ratio` (0.5), `delete.retention.ms` (24h), `min.compaction.lag.ms`, `max.compaction.lag.ms`.

Gotchas:
- The ACTIVE segment is never compacted. High `segment.bytes` + low write rate = segments never roll = nothing ever compacts (duplicates forever).
- `retention.ms` on a compact-ONLY topic is a no-op for compaction; use `cleanup.policy=compact,delete` for both.
- Consumers MUST handle `null` values; a deserializer that throws on null = crash loop on every tombstone.
- Compaction is per partition; rebalanced changelog readers re-read each partition's history.
- Log cleaner is memory-bound (dedupe buffer); huge key cardinality slows it.
- `min.compaction.lag.ms` gives consumers a window to read history before collapse; `max.compaction.lag.ms` forces eventual compaction (GDPR-style).

## Transactions & EOS

What it is: makes a set of writes plus consumer-offset commits atomic across topic-partitions; `read_committed` consumers see all or none. The consume-transform-produce loop is the canonical pattern.

Mechanism:
- Transaction coordinator: broker chosen by `hash(transactional.id) % numPartitions(__transaction_state)`; state in `__transaction_state`.
- Transactional producer: stable `transactional.id`, gets PID + epoch on `initTransactions()`; epoch bump fences old instances.
- Commit/abort markers: control records; records below the partition LSO stay invisible to `read_committed`.

API: `initTransactions()` once; per batch `beginTransaction()` -> `send(...)` -> `sendOffsetsToTransaction(offsets, consumer.groupMetadata())` -> `commitTransaction()` (or `abortTransaction()`). Consumer: `enable.auto.commit=false`, `isolation.level=read_committed`. Idempotence implied once `transactional.id` is set.

Key configs: `transactional.id` (stable, instance-unique), `transaction.timeout.ms` (60s, capped by broker `transaction.max.timeout.ms` 15 min).

Gotchas:
- `transactional.id` stable across restarts AND unique per instance. Random UUID per startup defeats fencing -> duplicates. Non-unique across live instances -> they fence each other.
- Offsets MUST go through `sendOffsetsToTransaction`, not `consumer.commitSync()` (correctness bug, not style).
- Downstream MUST use `read_committed` or it sees aborted records.
- Fence exceptions (`ProducerFencedException`, `OutOfOrderSequenceException`, `AuthorizationException`) are fatal: close and restart. Only retriable `KafkaException` -> `abortTransaction()` + re-poll.
- High `transaction.timeout.ms` -> open txns block LSO -> `read_committed` lag grows.
- One-record-per-transaction in a hot path: marker overhead (tens of ms); batch larger.
- `__transaction_state` mistuned (low RF/ISR) -> stuck transactions; keep RF=3 / minISR=2.

CLI:

```bash
kafka-transactions --bootstrap-server kafka:9092 list
kafka-transactions --bootstrap-server kafka:9092 describe --transactional-id enricher-instance-1
```

## Kafka Streams Primer

What it is: a LIBRARY (not a cluster) you add to a Java app; reads from Kafka, computes, writes back. Scale by running more app copies (ordinary consumer-group protocol).

Stream-table duality:
- KStream = unbounded sequence of events (the edits); same key reappears, all real.
- KTable = latest value per key (the materialized state); same key replaces; `null` deletes.
- GlobalKTable = full table replicated to every instance; no co-partitioning needed for joins, but holds whole table in memory.

Runtime: DSL builds a `Topology`; cut into subtopologies at key-change (repartition) boundaries; one TASK per input partition. Stream threads (`num.stream.threads`, default 1) run tasks; instances run threads. Stateful ops use RocksDB on local disk, backed by a compacted CHANGELOG topic; standby replicas (`num.standby.replicas`) keep warm copies.

Key configs: `application.id`, `processing.guarantee=exactly_once_v2` (default-recommended), `num.stream.threads`, `num.standby.replicas`, windowing `grace.ms`, `commit.interval.ms` (100ms under EOS).

Gotchas:
- Joins (KStream-KStream windowed, KStream-KTable) need CO-PARTITIONING: same partition count and keying. Mismatched counts silently fail to match (no error).
- `selectKey`/`map`/`groupBy` force a repartition topic (shuffle); `mapValues` does not.
- Late records past `grace.ms` are silently dropped (only the dropped-records metric shows it).
- Long blocking I/O in `transform` blows `max.poll.interval.ms` -> cascading rebalances.
- State on local disk: stateless container loses state on restart; use persistent volumes + standbys.
- Changing topology without `kafka-streams-application-reset` -> state/topology mismatch.

## Kafka Connect

What it is: no-code pipes in/out of Kafka. POST a JSON config to the REST API (port 8083); Connect handles offsets, scaling, retries, schema, DLQ.

| Term | Meaning |
|------|---------|
| worker | JVM process running pipes |
| connector | one configured pipe |
| tasks | parallel slices, up to `tasks.max` |
| converter | record <-> bytes (JSON / Avro / Protobuf) |
| transform / SMT | per-record tweak (InsertField, MaskField, RegexRouter) |
| source | reads external -> produces to Kafka |
| sink | consumes Kafka -> writes external |

Modes: standalone (one process, no fault tolerance, dev); distributed (workers in a group, state in `connect-configs` / `connect-offsets` / `connect-statuses`, prod default).

REST endpoints: `GET /connector-plugins`, `GET /connectors`, `POST /connectors`, `GET /connectors/{n}/status`, `PUT /connectors/{n}/pause`|`/resume`.

Offsets: source offsets in `connect-offsets` (keyed by source partition, at-least-once by default); sink offsets are ordinary consumer offsets in `__consumer_offsets`.

Gotchas:
- `errors.tolerance=all` WITHOUT `errors.deadletterqueue.topic.name` silently drops bad records (no failure, no audit). With `none` (default) the task goes FAILED on first poison record.
- Schema/converter mismatch is the #1 silent failure; key and value converters are independent.
- Source EOS needs `exactly.once.source.support=enabled` on the worker AND a connector that implements it (KIP-618, 3.3+). Sinks get EOS only via sink-side idempotence.
- Editing a connector config restarts all its tasks (visible gap).
- `tasks.max` above source parallelism: extra tasks idle but show healthy.
- Heavy work belongs in Streams/ksqlDB, not SMTs (they run in the worker JVM).

## Monitoring (JMX, Lag)

What it is: `lag(partition) = log-end-offset (LEO) - committed-offset`. High-but-flat lag is fine; GROWING lag is the fire.

Broker JMX (must-haves):
- `ReplicaManager,name=UnderReplicatedPartitions` -> should be 0.
- `ReplicaManager,name=UnderMinIsrPartitionCount` -> produce-blocking.
- `IsrShrinksPerSec` / `IsrExpandsPerSec` -> ISR churn.
- `KafkaController,name=ActiveControllerCount` -> exactly 1 cluster-wide.
- `BrokerTopicMetrics`: `BytesInPerSec` / `BytesOutPerSec` / `MessagesInPerSec`.

Producer: `record-send-rate`, `record-error-rate`, `record-retry-rate`, `batch-size-avg`, `buffer-available-bytes` (falling = back-pressure), `compression-rate-avg`.

Consumer: `records-lag-max`, `records-lag`, `fetch-latency-avg`; coordinator `commit-rate`, `rebalance-rate-per-hour`, `last-rebalance-seconds-ago`.

Lag two ways: live from consumer (`records-lag`, freezes if poll() is wedged) vs external AdminClient/CLI (`listConsumerGroupOffsets` minus `listOffsets(OffsetSpec.latest())`). External is the source of truth.

```bash
kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group orders-processor --offsets
```

Gotchas:
- Track PER-PARTITION (max), not aggregate; a hot partition averages away.
- Alert on lag GROWTH (`increase(lag[5m]) > 0`), not a static `lag > X` (over-fires on backfill, under-fires on slow leaks).
- Stale group (no members): CLI reports lag at last commit, frozen.
- A rebalance resets `records-lag` and `last-rebalance-seconds-ago`; correlate before paging.
- `ActiveControllerCount > 1` cluster-wide = split-brain; page immediately.
- Lag is offsets, not time or correctness; zero lag with a slow downstream still means slow end-to-end.

## Security (SASL / SSL)

What it is: three independent, per-listener dimensions: encryption in transit (TLS), authentication (who), authorization (ACLs: what).

`security.protocol`:

| Value | Encryption | Auth |
|-------|-----------|------|
| `PLAINTEXT` | none | none |
| `SSL` | TLS | optional mTLS (`ssl.client.auth=required`) |
| `SASL_PLAINTEXT` | none | SASL mechanism |
| `SASL_SSL` | TLS | SASL mechanism (standard prod) |

SASL mechanisms: PLAIN (password sent, safe only inside TLS), SCRAM-SHA-256/512 (challenge-response, password never on wire, rotates live), OAUTHBEARER (JWT/IdP), GSSAPI/Kerberos (heavy). mTLS is not SASL: cert subject becomes the principal.

ACLs: stored in `__cluster_metadata` (KRaft authorizer `StandardAuthorizer`); additive ALLOW, no default DENY. `super.users` bypass entirely. Set `allow.everyone.if.no.acl.found=false` in prod.

Client SASL props: `security.protocol`, `sasl.mechanism`, `sasl.jaas.config` (ScramLoginModule with username/password, ends with `;`), `ssl.truststore.location` / `.password`.

Gotchas:
- SASL/PLAIN over `SASL_PLAINTEXT` puts the password on the wire in clear; PLAIN must travel inside `SASL_SSL`.
- `ssl.endpoint.identification.algorithm=` (empty) disables hostname verification -> MITM; never in prod.
- A consumer needs ACLs on BOTH topic (`Read`+`Describe`) AND group (`Read`); missing the group ACL causes `TopicAuthorizationException` on poll().
- Cert rotation is the real TLS ops cost; SCRAM rotates without broker restart.
- Map mTLS DN to short names via `ssl.principal.mapping.rules`.
- Securing only brokers leaves back doors: Schema Registry / Connect / REST proxy have their own auth.

```bash
kafka-configs --bootstrap-server kafka:9092 --alter \
  --add-config 'SCRAM-SHA-256=[password=app-secret]' --entity-type users --entity-name app-orders
kafka-acls --bootstrap-server kafka:9092 --add --allow-principal User:app-orders \
  --operation Write --operation Describe --topic orders
```

## Performance Tuning

What it is: throughput is mostly batching; latency is mostly how long you wait before flushing; compression trades CPU for network/disk.

Producer knobs:

| Config | Default | Effect |
|--------|---------|--------|
| `batch.size` | 16384 | per-partition batch; bigger = throughput + memory |
| `linger.ms` | 0 | wait to fill batch; 5-20 ms typical for throughput |
| `compression.type` | none | per-batch; `lz4`/`zstd` recommended |
| `buffer.memory` | 33554432 | accumulator; full -> send() blocks `max.block.ms` |
| `max.in.flight.requests.per.connection` | 5 | with idempotence, 5 preserves order at ~5x throughput |

Compression codecs:

| Codec | CPU | Ratio | Use |
|-------|-----|-------|-----|
| none | 0 | 1.0 | small payloads, CPU-bound |
| lz4 | very low | ~2-3x | high-throughput sweet spot |
| snappy | low | ~2x | similar to lz4 |
| zstd | medium | ~3-5x | best ratio, modern CPU-friendly |
| gzip | high | ~3-4x | rarely best anymore (zstd wins) |

Consumer knobs: `fetch.min.bytes` (1; raise to 1 MB+ for batch), `fetch.max.wait.ms` (500), `max.poll.records` (500), `max.partition.fetch.bytes` (1 MB), `fetch.max.bytes` (~50 MB).

Broker: ride the OS PAGE CACHE, not JVM heap (typical ~6 GB heap, rest for cache); `num.network.threads` (3), `num.io.threads` (8); `compression.type=producer` (default) stores as-sent, avoids recompression.

Gotchas:
- Tune to an SLO and MEASURE; never "for maximum X" in the abstract.
- Compression is coupled to batch size: tiny batches barely compress and add CPU. Turn batching on first.
- `linger.ms` is a latency floor, not a constant tax: under load the batch fills before the timer.
- `max.in.flight=1` for "ordering" is a myth once idempotence is on (5 is safe and ~5x faster).
- Broker-side `compression.type` different from producer forces recompression (CPU).
- Most RAM to JVM heap starves the page cache (reads hit disk).

```bash
kafka-producer-perf-test --topic perf --num-records 1000000 --record-size 1024 \
  --throughput -1 --producer-props bootstrap.servers=kafka:9092 acks=all \
  linger.ms=20 batch.size=65536 compression.type=lz4
```

## Failure Modes

What it is: most outages are one of a few recurring patterns with a log fingerprint.

| Pattern | What happens | First check |
|---------|--------------|-------------|
| Rebalance storm | evict -> rejoin -> evict loop | slow processing / GC; `rebalance-rate-per-hour`, `last-rebalance-seconds-ago` |
| ISR shrinking | follower drops out of in-sync set | broker disk I/O, GC, `num.replica.fetchers` |
| Offset-out-of-range | committed offset older than earliest log | retention vs downtime; `auto.offset.reset` |
| Controller split-brain | `ActiveControllerCount > 1` | quorum health `kafka-metadata-quorum ... describe --status` |

Fingerprints: rebalance = repeated "Revoking previously assigned partitions" / "(Re-)joining group". ISR = "Shrinking ISR for partition ... from [1,2,3] to [1,2]" + `UnderReplicatedPartitions > 0`. Offset = "Fetch position ... is out of range; resetting offset".

Gotchas:
- Raising `max.poll.interval.ms` to "stop rebalances" disables liveness detection, does not fix slow processing.
- `unclean.leader.election.enable=true` trades silent committed-data loss for availability.
- `OffsetOutOfRangeException` is a data-management problem, not transient; retrying loops.
- Static membership (`group.instance.id`, KIP-345) skips rebalance on a reconnect within `session.timeout.ms` (good for rolling restarts); must be STABLE per instance (pod ordinal), not random UUID.
- Adding partitions does NOT fix a hot partition (the hot key still hashes to one).
- Commit in `onPartitionsRevoked` (before move), not `onPartitionsAssigned` (too late). `onPartitionsLost` (cooperative): partitions already gone, do not commit.

---

# Part 2: Avro / Schema Registry

## Why a Schema Registry

What it is: decouples the contract from the message. The wire payload carries a 5-byte prefix (`0x00` magic + int32 schema ID); the schema lives in a versioned central store. Producers register under a SUBJECT (`<topic>-value` / `<topic>-key`); consumers fetch the writer schema by ID and reconcile with the reader schema.

The registry enforces what the broker will not: compatibility (checked at registration time, not 3am), identity (global unique int ID per (subject,version)), discoverability (REST + UI).

Wire format: `[0x00][int32 schema id][avro binary]`. ID is global and immutable; version is subject-scoped and monotonic.

Gotchas:
- The registry is a HARD runtime dependency. Down registry: no new schema registrations, no cold-start consumers. Producers CACHE schemas after first lookup, so a warm cache keeps producing.
- `auto.register.schemas=true` in prod is a footgun: any local typo registers a permanent version. Disable in prod; register via CI/CD.
- The 5-byte prefix means raw non-Avro tools (kafkacat-on-raw) read garbage; it is NOT an Avro Object Container File.
- Schema ID 1 on one cluster != ID 1 on another; do not snapshot IDs across environments.
- "Schema in topic name" (`orders.v2`) bypasses the registry's whole point.

```bash
curl -s http://localhost:8081/subjects
curl -s http://localhost:8081/config | jq .   # {"compatibilityLevel":"BACKWARD"}
```

## Avro Format & Schema Language

What it is: schema-first, schema-on-write binary format. Schema required to read and write; that is why it pairs with a registry.

Types:
- Primitives: `null`, `boolean`, `int` (zig-zag varint), `long` (zig-zag varint), `float` (4 bytes), `double` (8 bytes), `bytes`, `string` (UTF-8, length-prefixed).
- Complex: `record`, `enum`, `array`, `map`, `union`, `fixed`.
- Logical: `decimal` (on bytes/fixed, with precision+scale), `uuid` (on string), `date` / `time-millis` / `timestamp-millis` / `timestamp-micros` (on int/long).

Binary encoding has NO field names and NO tags: fields are positional. That is why the schema is mandatory at decode and why renaming is "free" on the wire but catastrophic without aliases.

Nullable field = union, e.g. `["null", "string"]` with `default: null`. Compact because: no field names, varints, length-prefixed strings, no whitespace (typically 3-10x smaller than JSON).

Gotchas:
- Union order matters: `["null","string"]` with `default: null` is valid; `["string","null"]` with `default: null` is INVALID (default must match the first branch).
- Adding a field WITHOUT a default breaks backward compatibility immediately.
- `decimal` on bytes is two's-complement big-endian; wrong precision/scale silently corrupts amounts.
- Enums are closed: adding a symbol is backward-compatible only with a `default` on the enum (Avro 1.9+).
- Avro `null` is a TYPE, not a value; a field of type `"null"` only holds null.
- `fixed` has a hard byte size; changing it is a new type.

## Generating Java from .avsc

What it is: `avro-maven-plugin` (Apache, not Confluent) binds to `generate-sources` and turns each `.avsc`/`.avdl` under `src/main/avro` into a `SpecificRecord` class in `target/generated-sources/avro/`. Compile-time safety, IDE autocomplete, ~10-20% faster than GenericRecord (index access).

Generated class extends `SpecificRecordBase`, has `SCHEMA$`, typed getters/setters, a `Builder`.

Key plugin config: `<stringType>String</stringType>` (else CharSequence), `<enableDecimalLogicalType>true</enableDecimalLogicalType>`, `<customConversions>` for DecimalConversion / TimestampMillisConversion / DateConversion. Needs the Confluent Maven repo for `kafka-avro-serializer`.

```bash
mvn -q generate-sources
```

Gotchas:
- Logical conversions are OFF by default: `timestamp-millis` -> `Long` not `Instant`, `decimal` -> `ByteBuffer` not `BigDecimal`, `uuid` -> `String` not `UUID`. Always enable them.
- Plugin only regenerates if outputs are older/missing; `mvn clean` to clear stale CI caches.
- Generated classes are in `target/` (not Git); new devs hit "class not found" until they run generate-sources.
- Schema `namespace` becomes the Java package; changing it is a breaking change for consumers casting to the class.

## Producer/Consumer with KafkaAvroSerializer

What it is: `KafkaAvroSerializer` (accepts Specific or Generic) extracts the writer schema, computes the subject, looks up/registers -> schema ID, writes `0x00 + id + avro`. `KafkaAvroDeserializer` reads magic + ID, fetches writer schema (cached), decodes; with `specific.avro.reader=true` projects onto the generated class.

Key configs:

| Config | Default | Purpose |
|--------|---------|---------|
| `schema.registry.url` | (none) | registry URLs |
| `auto.register.schemas` | `true` | register on first produce; set FALSE in prod |
| `use.latest.version` | `false` | use latest registered as writer (projects record) |
| `specific.avro.reader` | `false` | deserialize to generated class vs GenericRecord |
| `value.subject.name.strategy` | TopicNameStrategy | subject derivation |

Prod combo: `auto.register.schemas=false` + `use.latest.version=true` (producers only emit pre-registered schemas, projected onto latest).

Gotchas:
- `specific.avro.reader=false` (default) returns `GenericRecord`; casting to your class -> `ClassCastException`.
- `specific.avro.reader=true` without the generated class on the classpath -> `ClassNotFoundException` at deserialize.
- Cold start with registry down halts consumption (deserializer LRU cache is empty after restart).
- `use.latest.version=true` projects the record onto the latest schema; if incompatible -> runtime exception.
- `SerializationException` mid-poll is fatal by default; wrap with an error-handling/DLQ deserializer in prod.
- First record after process start pays the registry lookup + connection cost.

## Schema Evolution & Compatibility

What it is: a subject (or global) property constraining what a new version V+1 may look like vs V (or all prior, for `_TRANSITIVE`). Answers "who can stay on the old schema without breaking?"

| Mode | New reads old | Old reads new | Upgrade first |
|------|---------------|---------------|---------------|
| BACKWARD (default) | OK | not required | Consumers |
| BACKWARD_TRANSITIVE | OK vs all prior | not required | Consumers |
| FORWARD | not required | OK | Producers |
| FORWARD_TRANSITIVE | not required | OK vs all prior | Producers |
| FULL | OK | OK | Either |
| FULL_TRANSITIVE | OK vs all | OK vs all | Either |
| NONE | unchecked | unchecked | Manual |

Change-by-mode matrix:

| Change | BACKWARD | FORWARD | FULL |
|--------|----------|---------|------|
| Add field WITH default | yes | yes | yes |
| Add field WITHOUT default | no | yes | no |
| Remove field WITH default | yes | yes | yes |
| Remove field WITHOUT default | yes | no | no |
| Rename (no alias) | no | no | no |
| Rename WITH `aliases` | yes | no | no |
| int -> long (widen) | yes | no | no |
| long -> int (narrow) | no | yes | no |
| Add union branch | no | yes | no |
| Remove union branch | yes | no | no |

Mnemonic: BACKWARD = new code reads old data, so deploy CONSUMERS first then producers. FORWARD = old code reads new data, deploy PRODUCERS first.

Gotchas:
- "BACKWARD" does NOT mean roll-forward freely; producer deploys SECOND.
- Adding a required (no-default) field is FORWARD-compatible but breaks BACKWARD (counter-intuitive).
- Removing a no-default field is BACKWARD-compatible but breaks FORWARD.
- `_TRANSITIVE` can lock you in: V1 constrains everything until you hard-delete history.
- Renaming without `aliases` looks like remove+add; aliases work only BACKWARD.
- Add a required field with no downtime: first add as nullable-with-default-null, populate, then flip the contract.

```bash
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @new.json http://localhost:8081/compatibility/subjects/orders-value/versions/latest
```

## Subject Naming Strategies

What it is: how the subject string (the unit a compatibility mode applies to) is derived from `(topic, record)`.

| Strategy | Subject | Topic holds | Compat scope |
|----------|---------|-------------|--------------|
| TopicNameStrategy (default) | `<topic>-key` / `<topic>-value` | one schema | per topic |
| RecordNameStrategy | `<fqcn.RecordName>` | any record on any topic | per record type, across topics |
| TopicRecordNameStrategy | `<topic>-<fqcn.RecordName>` | many record types | per (topic, record) |

Decision: one event type per topic -> TopicName. Several related types on one ordered log (event sourcing) -> TopicRecordName (canonical). Same schema reused across topics (fan-out) -> RecordName. Both producer and consumer must agree on the strategy.

```java
p.put("value.subject.name.strategy",
      "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy");
```

Gotchas:
- TopicNameStrategy locks you to one schema per topic; switching after release is a migration, not a config flip.
- RecordNameStrategy is hidden coupling: a breaking change to `Order` registered for one topic breaks ALL topics sharing it.
- Mismatched producer/consumer strategies silently look at the wrong subject for `/compatibility` and `use.latest.version` (decode by ID still works).
- TopicRecordNameStrategy creates many subjects per topic (operationally noisier).
- Connect / ksqlDB / SR CLI sometimes assume TopicNameStrategy.

## Specific vs Generic Records

What it is: SpecificRecord = generated typed class (ship the class). GenericRecord = runtime schema-driven container, `record.get("field")` -> Object (no codegen).

| Dimension | Specific | Generic |
|-----------|----------|---------|
| Type safety | compile-time | runtime |
| Refactor safety | IDE-aware | string keys break silently |
| Perf | ~10-20% faster | slightly slower |
| Classpath | generated classes | none |
| Framework/pipeline code | rigid (one class/schema) | flexible (any schema) |
| Cross-team coupling | shared module | registry only |

Use Specific for application code that owns the contract; Generic for bridges/sinks/transforms/tooling that process schemas they did not define. Both produce identical wire bytes; the consumer flag `specific.avro.reader` toggles globally (no per-topic switch).

For Specific, the reader schema is frozen at compile time; a newer writer (v3) projects onto your v1 (new fields dropped, missing fields use v1 defaults) - the BACKWARD safety net.

Gotchas:
- `GenericRecord.get("name")` returns `Utf8`, not `String`; `.equals("...")` against a Java String is false. Wrap with `toString()`.
- Generic does not apply logical-type conversions unless wired into `GenericData`.
- `GenericRecord.put("unknown", v)` throws, but `get("ID")` typo returns null silently.
- Never serialize SpecificRecord via Jackson/GSON reflection on the wire (misses Avro encoding).

## Schema References

What it is: split a reusable named type (`Address`, `Money`) into its own subject and depend on it from others. A reference is a triple: `name` (fqcn used in the parent), `subject` (where stored), `version` (PINNED, not "latest").

Registry resolves references, inlines them, and checks compatibility against the EXPANDED schema. Clients fetch parent by ID then traverse references (cached).

Use for: `Money` reused across `Order`/`Refund`/`Invoice`, a common `Header` envelope, domain primitives that must stay identical. Do not use for one-off nested types or types that may legitimately diverge per topic.

Maven plugin needs `<imports>` (imports first, then files that reference them, else `Undefined name`).

Gotchas:
- Reference pins to a VERSION: bumping `Money` v1->v2 does not auto-update parents; you register a new parent version pointing at v2 (which runs a compat check).
- Hard-deleting a referenced subject leaves dangling parents that cannot decode.
- Circular references are rejected at registration.
- Cross-team ownership: the referenced type's compatibility config now constrains everyone referencing it.
- Avro IDL `import schema "Money.avsc";` is a different mechanism than registry references.

## Avro vs Protobuf vs JSON Schema

What it is: three Confluent-supported formats, all using the same 5-byte framing; only the body differs.

| | Avro | Protobuf | JSON Schema |
|--|------|----------|-------------|
| Wire | compact binary | compact binary | textual JSON |
| Field identity | positional (name in schema) | numeric tag (`= 1`) | field name |
| Self-describing payload | no | partially (tags) | yes |
| Size vs JSON | 1/3 - 1/10 | 1/3 - 1/10 | 1/1 |
| Rename field | needs `aliases` | free on wire (tag unchanged) | breaks |
| Add required field | breaks | n/a (proto3 has none) | breaks if required |
| Schema at decode | required | not required | not required |
| Logical types | yes (decimal/uuid/timestamp) | wrappers (Timestamp) | weak `format` |
| RPC | rare | gRPC (de facto) | none |

Perf order: Protobuf ~ Avro >> JSON Schema. Pick Protobuf for polyglot/gRPC ecosystems; Avro for JVM + Kafka ecosystem (ksqlDB/Connect/Streams first-class); JSON Schema only when human-readable payloads are non-negotiable.

Gotchas:
- Avro: schema required at decode (no raw consume); evolution rules subtle.
- Protobuf: payload not schema-validated; defaults fixed by type (nullability traps); REUSING a tag number after removing a field is a permanent footgun.
- JSON Schema: brutal size at scale; `draft-04`/`draft-07`/`2020-12` tool disagreement; `additionalProperties:true` (default) weakens enforcement.

## Schema Registry REST API

What it is: a small HTTP API. Content-type `application/vnd.schemaregistry.v1+json`. The `schema` field is a JSON-ENCODED STRING (double-escaped), not an object.

Resource model:

```
/schemas/ids/{id}                              single schema by global ID
/subjects                                      list subjects
/subjects/{s}/versions                         list / POST register a version
/subjects/{s}/versions/{v|latest}              get a version
/subjects/{s}                                  POST: lookup ID for a schema body
/subjects/{s}                                  DELETE: soft delete
/subjects/{s}?permanent=true                   DELETE: hard delete
/compatibility/subjects/{s}/versions/{v|latest} dry-run a candidate
/config , /config/{s}                          global / per-subject compat mode
/mode , /mode/{s}                              READWRITE / READONLY / IMPORT
```

Soft delete (default): retains metadata + IDs; old records still decode by ID; re-registering identical bytes returns the SAME ID; hidden unless `?deleted=true`. Hard delete (`?permanent=true`): wipes metadata, IDs may be reassigned, records become undecodable - almost never in prod.

Modes: READWRITE (default), READONLY (freeze a contract), IMPORT (migrations preserving IDs - do not toggle casually).

Common errors:

| HTTP | Meaning |
|------|---------|
| 409 | schema incompatible with prior |
| 422 / 42201 | invalid Avro schema |
| 422 / 42202 | invalid compatibility level |
| 404 / 40401 | subject not found |
| 404 / 40403 | schema ID not found |

CI gate: `POST /compatibility/...` first, fail the build on `is_compatible:false`, then `POST /subjects/.../versions` from CD (idempotent).

Gotchas:
- Forgetting to escape quotes in the `schema` string is the #1 mistake.
- DELETE is SOFT by default; people assume hard and later blow up with `?permanent=true`.
- Content equality uses canonical form; differing field order may register as two versions (use `?normalize=true` to dedupe).
- No native pagination; big registries return huge JSON.

## Schema Registry Security & ACLs

What it is: the registry is a Jetty HTTP server with three independent planes: transport (HTTPS), authentication (BASIC / mTLS / bearer), authorization (OSS ACLs or commercial RBAC).

RBAC roles (commercial): `DeveloperRead`, `DeveloperWrite`, `ResourceOwner`, `SystemAdmin`. Typical: producers `DeveloperWrite` on `Subject:<topic>-value`, consumers `DeveloperRead`, platform `ResourceOwner` on `Subject:*`.

Client configs:

```java
props.put(SCHEMA_REGISTRY_URL_CONFIG, "https://sr.internal:8081");
props.put(BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
props.put(USER_INFO_CONFIG, "producer-svc:" + secret);
props.put("schema.registry.ssl.truststore.location", "/etc/ssl/truststore.jks");
```

Gotchas:
- TLS config prefix is `schema.registry.ssl.*`, distinct from broker `ssl.*`.
- `BASIC_AUTH_CREDENTIALS_SOURCE=URL` puts userinfo in the URL (leaks in logs); prefer `USER_INFO`.
- `auto.register.schemas=false` is a SECURITY control too (stops rogue registrations), not just stability.
- Anyone with Kafka write access to the `_schemas` topic owns the registry; lock it down.
- Subject ACLs do not restrict reading a schema BY ID once authenticated.
- mTLS principal mapping wrong -> every cert is "User:UNKNOWN".

## Testing with MockSchemaRegistryClient

What it is: `io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry`, an in-memory registry satisfying the same API. Two ways to use it.

1. `mock://` URL scope: any `schema.registry.url` starting with `mock://` binds to a per-scope client; same URL string = shared registry. Works with KafkaAvroSerializer/Deserializer and TopologyTestDriver, no code change.

```java
props.put("schema.registry.url", "mock://orders-test");
```

2. Constructed client for fine control:

```java
MockSchemaRegistryClient client = new MockSchemaRegistryClient();
client.register("orders-value", new AvroSchema(Order.SCHEMA$));
boolean ok = client.testCompatibility("orders-value", new AvroSchema(OrderV2.SCHEMA$));
```

Covers: registration, lookup, compatibility checks, `auto.register.schemas`, subject strategies, references. Does NOT cover: network failures/retries/timeouts, HTTP auth/TLS, backing-store latency, cross-process ID consistency. Use Testcontainers + real registry for those.

Gotchas:
- `mock://` scope is STATIC state; parallel tests on the same URL collide. Use unique scopes per test class.
- IDs start at 1 incrementally; do not assume they match a real registry.
- Forgetting `specific.avro.reader=true` yields GenericRecord and a ClassCastException (same trap as prod).
- TopologyTestDriver needs a configured serializer INSTANCE, not a class name.

## Migrating a Live Topic

What it is: zero-downtime schema evolution via expand-contract. Three change categories: trivially compatible (one rolling deploy), compatible-with-discipline (coordinated rollout), breaking (dual-publish or new topic).

Expand-contract: (1) Expand - add new field nullable with default; old and new producers coexist. (2) Migrate - roll producers to populate, roll consumers to read while tolerating absence. (3) Contract - once 100% of records within retention have it, drop the old field.

Rollout order:

| Mode | Deploy first | Deploy second |
|------|--------------|---------------|
| BACKWARD | Consumers v2 | Producers v2 |
| FORWARD | Producers v2 | Consumers v2 |
| FULL | Either | Either |

Truly breaking changes (semantics changed, unrelated type, splitting a record): new topic (`orders.v2`) with dual-write for a window; or same topic with TopicRecordNameStrategy (`OrderV2` alongside `Order`); or `NONE` + hard cutover (rarely defensible).

Playbook: PR with `.avsc` -> CI dry-run `/compatibility` -> merge -> CD `POST /subjects/.../versions` -> roll in correct order -> monitor console-consumer + DLQ -> hold a watch window (1-2 retention cycles or 24h) -> contract.

Gotchas:
- Deploying producers first on a BACKWARD topic surprises old consumers (projection saves them only if compatible BOTH ways, which BACKWARD is not).
- Compacted topics never age out old shapes: `BACKWARD_TRANSITIVE` is mandatory; the new consumer must tolerate the old shape indefinitely.
- A `default` does NOT backfill old records; reader-side defaults handle projection.
- Dual-publish without idempotency keys creates double-process risk.
- Do not forget Kafka Connect sinks and ksqlDB queries: they consume the topic with their own schema caches.
- Define a stop-the-bleeding metric (deserialization error rate) and an exit metric (% on V2) before starting.
