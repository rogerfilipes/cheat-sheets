# Redis Cheatsheet

## Contents
- [Data types](#data-types)
- [Commands and pipelining](#commands-and-pipelining)
- [Transactions (MULTI/EXEC/WATCH)](#transactions-multiexecwatch)
- [Lua scripting and EVAL](#lua-scripting-and-eval)
- [Pub/Sub vs Streams](#pubsub-vs-streams)
- [Persistence (RDB vs AOF)](#persistence-rdb-vs-aof)
- [Eviction and memory](#eviction-and-memory)
- [Keyspace notifications](#keyspace-notifications)
- [Replication and Sentinel](#replication-and-sentinel)
- [Cluster (sharding and hashtags)](#cluster-sharding-and-hashtags)
- [Client libraries (Lettuce vs Jedis)](#client-libraries-lettuce-vs-jedis)
- [Caching patterns](#caching-patterns)
- [Distributed locks (Redlock)](#distributed-locks-redlock)
- [Rate limiting recipes](#rate-limiting-recipes)
- [Leaderboard recipes](#leaderboard-recipes)

## Data types

One value, many shapes. Pick by: scalar or collection? if collection, need order/uniqueness/both?

| Type | What it is | Key commands | Use case |
|------|-----------|--------------|----------|
| String | Binary-safe bytes (<= 512 MB), int/float encodable | `SET` `GET` `INCR` `APPEND` `GETRANGE` | Counters, cached blobs, flags |
| List | Doubly-ended linked list | `LPUSH` `RPUSH` `LPOP` `BLPOP` `LRANGE` `LTRIM` | Queues, recent-N feeds |
| Hash | Field->value map in one key | `HSET` `HGET` `HINCRBY` `HGETALL` `HSCAN` | Objects (user:42 fields) |
| Set | Unordered unique strings | `SADD` `SISMEMBER` `SINTER` `SRANDMEMBER` | Tags, uniqueness, set algebra |
| Sorted set (ZSET) | Set + float score, ordered (skiplist+hash) | `ZADD` `ZRANGEBYSCORE` `ZINCRBY` `ZREVRANK` `ZRANGEBYLEX` | Leaderboards, priority, time-index |
| Stream | Append-only log + consumer groups | `XADD` `XREAD` `XREADGROUP` `XACK` `XPENDING` `XAUTOCLAIM` | Durable log, at-least-once queue |
| Bitmap (on String) | Bit-addressable string | `SETBIT` `GETBIT` `BITCOUNT` `BITPOS` `BITOP` | Daily-active flags, presence |
| HyperLogLog | Probabilistic cardinality (~0.81% err, 12 KB fixed) | `PFADD` `PFCOUNT` `PFMERGE` | Unique counts at scale |
| Geo (on ZSET) | Geohash coordinates | `GEOADD` `GEOSEARCH` | Proximity lookups |

Start with String (counters/flags), Hash (objects), ZSET (anything ranked/time-ordered): they cover most work.

Encoding: small aggregates pack flat as `listpack` (was `ziplist` pre-7.0), O(N) but N tiny. Crossing `hash-max-listpack-entries` etc. silently upgrades to hashtable/skiplist (10-20x memory). Strings holding small ints are `int`-encoded (why `INCR` is O(1)). Check `OBJECT ENCODING <key>`.

Gotchas:
- `HGETALL` / `SMEMBERS` / `LRANGE 0 -1` / `KEYS *` on a huge key blocks the server. Use `HSCAN`/`SSCAN`/`SCAN` with pagination.
- JSON-in-a-String forces full re-serialize on partial update and loses atomic field ops. Use Hash.
- Set has no order; "top N by score" needs a ZSET.
- HyperLogLog is approximate (~0.81%). Never for billing.
- `SETBIT key 1000000000 1` allocates the whole intermediate range (~half a GB for one bit).
- Streams retain forever unless trimmed (`XADD ... MAXLEN ~ 10000` or `XTRIM`).

## Commands and pipelining

A naive client is RTT-bound: one write, one read, repeat. On 1 ms RTT, 1 connection, ceiling ~1000 ops/sec regardless of server speed.

Pipelining: send N requests back-to-back, then read N replies. RESP has no request IDs; reply i matches request i only via in-order TCP delivery. Typical 5-50x throughput gain.

```bash
redis-benchmark -t set,get -n 100000 -q          # baseline ~80-85k req/s
redis-benchmark -t set,get -n 100000 -P 16 -q    # pipeline depth 16, ~600-700k
redis-cli --pipe < commands.txt                  # fastest bulk-load (mass-insert)
```

Variadic commands (`MGET` `MSET` `HMGET`) beat a pipeline of N: one command server-side, no per-command framing.
- `MGET k1 k2 k3` = 1 command, 1 reply, 1 RTT. Best when possible.
- Pipeline `GET k1; GET k2; GET k3` = 3 commands, 1 RTT. Use when commands differ.

Pipeline vs MULTI vs Lua: pipeline = throughput; MULTI = atomic visibility; Lua = atomic read-modify-write.

Gotchas:
- Pipelining is NOT atomic; other clients interleave. No rollback on a mid-pipeline error (other commands still run; you read N replies, one is an error).
- Huge pipelines (millions of commands) OOM both ends (server buffers replies, kernel buffers requests). Flush in chunks of 1k-10k.
- Pipelining cannot help a true dependency (cmd N reads what N-1 wrote). Use Lua.
- Blocking commands (`BLPOP`, `XREAD BLOCK`, `WAIT`) inside a pipeline stall the whole batch.
- `MONITOR` in prod is disastrous (duplicates every command to the monitor client). Dev only.

## Transactions (MULTI/EXEC/WATCH)

Queue commands, run the whole queue as one uninterrupted batch; replies return together. NOT a SQL transaction: no `ROLLBACK`. Serializable because Redis is single-threaded, not because of isolation levels.

```redis
MULTI
SET a 1
INCR a
EXEC            # commands replied QUEUED until EXEC, then run serially
DISCARD         # abort a queued transaction
```

Two error classes:
- Compile-time (unknown command, bad arity): whole transaction rejected at EXEC (since 2.6.5).
- Runtime (e.g. `INCR` on a string holding text): offending command errors, ALL OTHERS STILL EXECUTE. No rollback.

WATCH = optimistic CAS (compare-and-swap), no locking. `WATCH k1 k2` marks keys; if any is modified before EXEC, EXEC returns nil and nothing runs -> client retries.

```redis
WATCH balance:a
GET balance:a          # read, decide client-side
MULTI
DECRBY balance:a 50
INCRBY balance:b 50
EXEC                   # nil if balance:a changed since WATCH -> retry loop
```

Prefer Lua when the decision of WHAT to write depends on what you read (server-side decide, no retries, no extra RTT).

Gotchas:
- No rollback on runtime errors.
- Any modification to a watched key aborts EXEC, even set-to-same-value, even same client, even Lua.
- WATCH is cleared by EXEC/DISCARD/UNWATCH/connection-close; never survives reconnect.
- High-contention CAS livelocks; switch to Lua.
- Cluster: all keys must hash to one slot (use hashtag `{user:42}`) or CROSSSLOT.
- Blocking commands inside MULTI run non-blocking (return immediately).

## Lua scripting and EVAL

Ship a tiny program to the server; it runs in one atomic, uninterrupted step (same single thread, so nothing else runs meanwhile). A slow script stalls the entire server.

Two-call pattern:
```redis
SCRIPT LOAD "<source>"                  # -> sha1
EVALSHA <sha1> <numkeys> key1 ... arg1 ...
EVAL "<source>" <numkeys> key1 ... arg1 ...   # one-shot
SCRIPT EXISTS <sha1>
SCRIPT FLUSH                            # clears cache; EVALSHAs now NOSCRIPT
```

KEYS vs ARGV: anything naming a key goes in `KEYS[]` (needed for cluster routing and cross-slot detection); limits/TTLs/values go in `ARGV[]`.

NOSCRIPT: after restart/failover a replica has a cold cache; `EVALSHA` returns `NOSCRIPT`. Client must fall back to `EVAL` and re-warm. Jedis/Lettuce automate this.

Atomic INCR-with-cap (classic):
```lua
local cur = tonumber(redis.call("GET", KEYS[1]) or "0")
if cur + 1 > tonumber(ARGV[1]) then return {0, cur} end
local new = redis.call("INCR", KEYS[1])
if new == 1 then redis.call("EXPIRE", KEYS[1], ARGV[2]) end
return {1, new}
```

Killing a runaway script: `lua-time-limit` (default 5s) lets `SCRIPT KILL` work ONLY before the first write; after a write you must `SHUTDOWN NOSAVE` (lose RAM since last persist).

Functions (Redis 7): `FUNCTION LOAD` stores a Lua library on disk, replicated, survives restarts. Prefer over EVALSHA for long-lived prod scripts.

Gotchas:
- Blocks the whole server: a 200 ms script makes every client wait 200 ms. Never loop over millions of items or call `KEYS`/`SMEMBERS`-on-giant-set inside a script.
- Must be deterministic; avoid `math.random`/`os.time`. For time, call `redis.call("TIME")`. 7.x replicates effects (commands), not source.
- `tonumber("")` returns nil; guard string-to-int.
- Lua numbers are doubles; for > 2^53 counters use string-returning int ops.
- Forgetting a key in `KEYS[]` misroutes/rejects in Cluster.

## Pub/Sub vs Streams

Pub/Sub = live radio (no recording; tune in or miss it). Streams = durable logbook (every entry stays until trimmed; rewindable).

| | Pub/Sub | Streams |
|---|---------|---------|
| Storage | None | Append-only log, persisted (RDB/AOF) |
| Delivery | At-most-once | At-least-once (PEL + XACK + XCLAIM) |
| Replay | No | Yes (read from 0 or any ID) |
| Consumer groups | No | Yes (per-group cursor, one msg per consumer) |
| Slow/dead consumer | Dropped by server, msgs lost | Decoupled; only memory grows (bound via trim) |
| Failover | State lost | Data survives (normal key) |

```redis
SUBSCRIBE events / PSUBSCRIBE order.* / PUBLISH events "hi"
SSUBSCRIBE / SPUBLISH                    # sharded pub/sub (7.0+), keeps fanout local to slot
PUBSUB NUMSUB events / PUBSUB CHANNELS / PUBSUB SHARDNUMSUB

XADD orders * userId 42 amount 99.90     # auto ID ms-seq
XADD orders MAXLEN ~ 10000 * ...         # approx trim (~ is much cheaper)
XGROUP CREATE orders billing $ MKSTREAM  # $ = from now, 0 = from start
XREADGROUP GROUP billing c1 COUNT 10 BLOCK 5000 STREAMS orders >
XACK orders billing <id>
XPENDING orders billing                  # un-acked entries (PEL)
XAUTOCLAIM orders billing c2 30000 0     # reclaim entries idle > 30s to c2
XTRIM orders MAXLEN 100000
```

Pick: cache invalidation / real-time notify / loss-OK fan-out -> Pub/Sub. Work queue / event sourcing / replay / consumer scaling -> Streams. Cross-DC, exactly-once, partitioned ordering -> Kafka, not Redis.

Gotchas:
- Pub/Sub loses messages on disconnect, slow consumer, or failover. Never for must-not-lose.
- A subscribed connection cannot run normal commands. Dedicate one.
- Streams persist forever by default; forgetting `MAXLEN`/`XTRIM` = unbounded growth.
- Consumer crash without `XACK` leaves entries in PEL forever unless `XCLAIM`/`XAUTOCLAIM`.
- `PSUBSCRIBE` is O(patterns x channels) per publish; does not scale to thousands of patterns.
- `client-output-buffer-limit pubsub` defaults aggressive (32mb hard / 8mb soft over 60s); slow subscriber gets dropped silently.
- A Stream lives in one Cluster slot; shard at the app layer (`orders:{0}`, `orders:{1}`).

## Persistence (RDB vs AOF)

RDB = periodic full snapshot (Save As on a timer). AOF = journal of every write (Track Changes). Independent; usually run both.

| | RDB | AOF |
|---|-----|-----|
| What | Forked child serializes whole keyspace to `dump.rdb` | Logs every write command, replays on restart |
| Trigger | `save` config, `BGSAVE`, `SHUTDOWN` | continuous append + rewrite |
| Pro | Small file, fast restart, no steady-state IO | Better durability (down to ~1s loss) |
| Con | Lose everything since last snapshot; fork up to ~2x RAM (COW) | Bigger file, IO cost, slow single-threaded replay |
| Loss on crash | Since last snapshot | Per `appendfsync` policy |

`appendfsync` (the durability dial):

| Setting | Behavior | Loss on crash |
|---------|----------|---------------|
| `always` | fsync every write | ~0 (brutal: multi-ms stall per write) |
| `everysec` (default) | bg fsync once/sec | up to ~1 second |
| `no` | OS decides (~30s) | up to filesystem flush interval |

```redis
BGSAVE / LASTSAVE / BGREWRITEAOF
CONFIG SET appendfsync everysec
CONFIG SET save "3600 1 300 100"
CONFIG REWRITE                  # persist running config
```
```bash
redis-cli INFO persistence      # rdb_last_bgsave_status, aof_last_write_status, ...
redis-check-aof --fix <file>    # truncate at first corruption (destructive tail loss)
redis-check-rdb <file>          # reports; no fix, restore from backup
```

Standard prod config: `appendonly yes` + `appendfsync everysec` + low-frequency `save` as backup. On restart Redis prefers AOF. AOF rewrite (`BGREWRITEAOF`, auto via `auto-aof-rewrite-percentage`/`-min-size`) compacts; `aof-use-rdb-preamble yes` (default 5.0+) = RDB header + AOF tail, fast load.

Gotchas:
- Fork can OOM the host: COW pushes RSS toward ~2x dataset under heavy writes during BGSAVE. Set `vm.overcommit_memory=1` or saves fail silently.
- `appendfsync no` quietly throws away minutes on a panic.
- `SAVE` (uppercase command, synchronous) blocks the whole server. Never in prod.
- `aof-load-truncated yes` (default) drops the partial last command on power-loss truncation.
- AOF replay on multi-GB files is single-threaded and slow; restart windows matter.
- Replication is NOT a backup: `FLUSHALL` propagates instantly. Ship RDB snapshots off-host.

## Eviction and memory

`maxmemory <bytes>` caps the dataset; when a write would exceed it, `maxmemory-policy` decides what to throw out. Two axes: candidate set (`allkeys-*` / `volatile-*` / `noeviction`) and algorithm (LRU / LFU / random / TTL).

| Policy | Candidate set | Selection |
|--------|---------------|-----------|
| `noeviction` | none | Reject write with OOM error |
| `allkeys-lru` | all keys | Approx LRU |
| `allkeys-lfu` | all keys | Approx LFU |
| `allkeys-random` | all keys | Random |
| `volatile-lru` | keys with TTL | Approx LRU |
| `volatile-lfu` | keys with TTL | Approx LFU |
| `volatile-random` | keys with TTL | Random |
| `volatile-ttl` | keys with TTL | Shortest TTL first |

Rules of thumb: pure cache -> `allkeys-lru`/`allkeys-lfu` (LFU survives scans, better for slow backends). Cache + persistent store split by TTL -> `volatile-*`. Pure store -> `noeviction` (must shed writes upstream or get OOM errors).

LRU/LFU are approximate: Redis samples `maxmemory-samples` (default 5) random keys and evicts the worst; a hot key can be evicted if it misses the sample. Bump to 10 for sensitive caches.

```
used_memory          # logical dataset size
used_memory_rss      # what OS sees (matters for OOM-kill)
mem_fragmentation_ratio = rss/used_memory   # 1.0-1.5 healthy; >1.5 fragmentation; <1.0 = SWAP
maxmemory_policy
```
```redis
CONFIG SET maxmemory 256mb
CONFIG SET maxmemory-policy allkeys-lfu
MEMORY USAGE mykey / OBJECT ENCODING mykey / MEMORY STATS
INFO stats | grep evicted        # evicted_keys
CONFIG SET activedefrag yes      # jemalloc fragmentation
```
```bash
redis-cli --bigkeys / --memkeys   # find fat keys (run off-hours)
```

Gotchas:
- `noeviction` + write past cap = `OOM command not allowed` on every write. Reads still work.
- `volatile-*` evicts nothing if no key has a TTL -> refuses writes.
- Fragmentation > 1.5 -> active defrag; < 1.0 -> swapping (catastrophic latency; disable host swap).
- Encoding upgrades (listpack -> hashtable/skiplist) silently 10x a key. Bound element counts.
- Always set `maxmemory` (even at 80% host RAM); unset = unbounded -> OOM-kill.

## Keyspace notifications

Redis publishes a Pub/Sub message whenever a key is created/modified/expired/evicted/deleted. Off by default (CPU/bandwidth cost). Inherits all Pub/Sub weaknesses (at-most-once, no replay).

Two channels, same events:
- `__keyspace@<db>__:<key>` keyed by key name; body = event (`set`, `del`, `expired`, `hset`).
- `__keyevent@<db>__:<event>` keyed by event; body = key name.

`notify-keyspace-events <flags>`: need >= one channel (`K`/`E`) AND >= one category.

| Flag | Meaning | Flag | Meaning |
|------|---------|------|---------|
| `K` | Keyspace channel | `z` | Sorted set commands |
| `E` | Keyevent channel | `x` | Expired events |
| `g` | Generic (del/expire/rename) | `e` | Evicted events |
| `$` | String commands | `t` | Stream commands |
| `l` | List commands | `m` | Key-miss (7.0+) |
| `s` | Set commands | `n` | New key (7.2+) |
| `h` | Hash commands | `A` | Alias for `g$lshzxet` |

```bash
redis-cli CONFIG SET notify-keyspace-events Ex   # expired only (TTL workflows)
redis-cli PSUBSCRIBE '__keyevent@0__:expired'
# combos: KEA (everything, dev), Ex (TTL cleanup), Eg$ (cache-busting)
```

Gotchas:
- At-most-once: subscriber blip = lost events = silent inconsistency. Not for must-not-miss.
- Expired events fire when the key is actually removed (lazy on access, or background sampler at `hz` rate), NOT when it logically expired. Can lag seconds. Don't build precise schedulers on it.
- A `DEL` is not an `expired` event; manual deletes don't trigger an `expired`-only handler.
- Cluster: events are per-node; subscribe on every primary (or use sharded `SSUBSCRIBE`).
- For must-not-miss or fire-on-time, use a Stream or a ZSET-based scheduler instead.

## Replication and Sentinel

One primary takes writes; replicas keep async copies (always slightly behind). Replication is ASYNCHRONOUS: primary acks the client before replicas have the write. That one fact is the source of every gotcha.

Consequences: stale reads on replicas (miss your own just-written value); lost-write window on failover (primary acks, crashes before streaming).

`WAIT n ms`: block until n replicas ack the last write (or timeout). A synchronization point, not durability/consensus.

```redis
INFO replication            # role:master, connected_slaves, master_repl_offset, lag
REPLICAOF <ip> <port> / REPLICAOF NO ONE
SET critical 1
WAIT 1 200                  # -> count of replicas that acked (0 = timed out)
CONFIG SET min-replicas-to-write 1     # refuse writes if < N replicas
CONFIG SET min-replicas-max-lag 10     # ... within M seconds of lag (AP -> CP)
```

Sentinel = watchdog process (run 3 or 5, never 2). Failover: detect down (`down-after-milliseconds`) -> quorum agrees ODOWN -> elect leader (Raft-lite) -> pick best replica (priority, offset, run id) -> `REPLICAOF NO ONE` to promote -> reconfigure others -> notify clients via pub/sub.

```
sentinel monitor mymaster 10.0.0.11 6379 2     # quorum 2
sentinel down-after-milliseconds mymaster 5000
```
```redis
SENTINEL MASTERS / SENTINEL SLAVES mymaster / SENTINEL CKQUORUM mymaster
SENTINEL FAILOVER mymaster        # operator-triggered
```

Key tunables: `down-after-milliseconds` (tight = fast failover + more false positives), `quorum`, `min-replicas-to-write`/`min-replicas-max-lag` (durability vs availability), `repl-backlog-size` (circular buffer for partial resync; too small -> full RDB transfer).

Gotchas:
- Lost writes on failover (async). Mitigate with `WAIT`, `min-replicas-to-write`, or accept it.
- Split brain: old primary returns after partition, still accepts writes. `min-replicas-to-write 1` makes the orphan refuse writes.
- Sentinel needs odd N >= 3; 2 can't form quorum on a partition.
- `down-after-milliseconds` too low -> false failovers on every blip.
- Read-your-writes does not hold on replicas. For strong reads use `ReadFrom.MASTER` or `WAIT`.
- Full resync forks a child for an RDB transfer; minutes of CPU/network on a big dataset.
- Sentinel is NOT Cluster: Sentinel = HA for one primary; Cluster = sharding (its own gossip failover).

## Cluster (sharding and hashtags)

Split the keyspace across N primaries (3+), each owning a set of the 16384 fixed slots, with optional replicas. Coat-check with 16384 numbered hooks: a fixed hash tells you who owns a key.

```
slot = CRC16(key) mod 16384
slot("user:{42}:profile") = CRC16("42") mod 16384   # only the {tag} is hashed
slot("user:{42}:orders")  = CRC16("42") mod 16384   # same slot -> co-located
```

Hashtag `{...}` co-locates keys you must operate on together. Use it at the granularity of "I need atomic ops here," not "I want simple keys" (jamming a whole tenant under `{tenant}` puts it all on one shard).

Redirection (a node holds only its slots; it does not proxy):
- `MOVED <slot> <ip>:<port>` permanent: slot moved; client updates its map.
- `ASK <slot> <ip>:<port>` temporary (during migration): retry on target prefixed with `ASKING`.

Cluster-aware clients (Lettuce/Jedis Cluster) keep a slot->node map. Naive clients fail on MOVED.

Multi-key ops require all keys in ONE slot, or `CROSSSLOT`: `MGET`/`MSET`/`SUNIONSTORE`/`RENAME`, `MULTI`/`EXEC`, `EVAL` (`KEYS[]`), `WAIT`/`WATCH`.

```redis
CLUSTER INFO / CLUSTER NODES / CLUSTER SHARDS / CLUSTER SLOTS
CLUSTER KEYSLOT user:42
SET {user:42}:profile "Ada" / MGET {user:42}:profile {user:42}:orders   # ok, same slot
```
```bash
redis-cli -c ...                        # -c follows MOVED automatically
redis-cli --cluster reshard / --cluster rebalance node1:7001
```

Failover is built-in (Raft-lite gossip, `cluster-node-timeout`); no Sentinel needed. Resharding moves slots one at a time online (source returns ASK for handed-off keys).

Gotchas:
- No cross-slot multi-key ops; designs assuming "Redis is one big map" break.
- Hashtag-everything-into-one-key kills scaling (whole tenant on one shard).
- Migrating one large key blocks both source and target; keep keys small.
- MOVED storms during resharding for stale-map clients; use backoff on refresh.
- ASK is one-shot + needs `ASKING`; bad clients treat it like MOVED and corrupt their map.
- Cluster needs N >= 3 primaries to survive a partition (majority).
- `PUBLISH` fans out to all nodes (O(N)); use `SPUBLISH`.

## Client libraries (Lettuce vs Jedis)

A raw connection processes commands one at a time; two threads sharing one socket corrupt the RESP frame. The clients answer "how do many threads share connections" differently.

| | Jedis | Lettuce |
|---|-------|---------|
| Model | Blocking, one connection per thread | Netty, one thread-safe shared connection |
| Concurrency | `JedisPool` (borrow/use/return) | Internal command queue; no pool (usually) |
| Thread-safe instance | No (`Jedis`); yes (`JedisCluster`) | Yes |
| API tiers | Sync | Sync / async (`RedisFuture`) / reactive (`Mono`/`Flux`) |
| Pipelining | `.pipelined()` | Automatic in async mode |
| Cluster/Sentinel | Supported | First-class, adaptive topology refresh |
| Reactive | None | Yes (required by Spring WebFlux) |
| Deps | Small | Heavier (Netty + Reactor) |

Pick: simple blocking code -> Jedis. High concurrency / Cluster / reactive Spring / async pipelining -> Lettuce. Spring Boot 3.x default = Lettuce; reactive starter requires Lettuce. One Lettuce connection often replaces a whole Jedis pool.

Gotchas:
- Sharing a `Jedis` across threads corrupts the RESP stream. Always pool.
- Jedis pool exhaustion under load -> `JedisExhaustedPoolException`. Size to peak in-flight ops; long-running ops (blocking, transactions, pipelines) tie up a connection.
- Lettuce single connection + blocking command (`BLPOP`/`WAIT`) parks the connection; use a dedicated connection for blocking ops.
- `setAutoFlushCommands(false)` without `flushCommands()` -> commands hang forever.
- Lettuce default timeouts: 60s command, 10s connect.
- Forgetting `client.shutdown()` / `pool.close()` leaks threads/resources. Use try-with-resources.

## Caching patterns

A cache is a fast copy in front of a slow DB. The whole question is the write side: who writes to the cache, and when?

| Pattern | Write path | Consistency | Risk |
|---------|-----------|-------------|------|
| Cache-aside (look-aside) | App writes DB, then INVALIDATEs (DEL) cache | Cache may lag DB briefly | Stale window / dual-write race |
| Write-through | App writes through cache -> cache writes DB synchronously | Always consistent | Every write pays cache + DB latency; cache outage blocks writes |
| Write-behind (write-back) | App writes cache; worker drains to DB async | Cache leads DB | Data loss if Redis dies before drain |

Cache-aside (90% of real services): cache is dumb storage, app owns reads and writes.
```
read:  v=GET key; if miss { v=db.query(); if v: SETEX key ttl v }; return v
write: db.update(); DEL key      # invalidate, do NOT SET
```
Why DEL not SET: (1) avoids stale-overwrite race (paused writer A re-SETs old value after writer B); (2) DEL is free, SET cost scales with write rate.

Production add-ons:
- Negative caching: cache "not found" as a sentinel for a SHORT TTL to stop penetration (lookups for non-existent keys). Too long -> just-created record stays invisible.
- TTL jitter: `ttl = base +/- 10%` to avoid synchronized expiry -> thundering herd.
- Refresh-ahead: refresh very hot keys before expiry so callers never miss.
- Single-flight: `SET lock NX EX` so one of N concurrent missers does the DB call.

Three named failure modes: stampede / thundering herd (many readers miss same key, dogpile DB), cache penetration (lookups for never-existent keys), stale / dual-write inconsistency (DB and cache drift).

Gotchas:
- SET-on-write opens the stale-overwrite race. Use DELETE.
- No TTL on cache-aside keys -> fills until eviction. Always set TTL.
- Same TTL across keys -> synchronized expiry -> herd. Jitter.
- Caching results that depend on hidden state (locale, flags, user) -> wrong content. Encode all variation in the key.
- Write-behind data loss is inevitable on restart; never for systems of record.
- Forgetting to invalidate on indirect writes (update `role` should bust `user` cache) -> ships to prod most often.

## Distributed locks (Redlock)

Only one worker across many machines should do a thing at a time. Whoever creates the key first wins; everyone else backs off. A distributed lock is BEST-EFFORT, not a guarantee.

Correct single-node lock:
```redis
SET lock:resource <unique-token> NX PX 10000    # NX+PX atomic since 2.6.12
```
Release must verify ownership atomically (Lua):
```lua
if redis.call("GET", KEYS[1]) == ARGV[1]
then return redis.call("DEL", KEYS[1])
else return 0 end
```
Naive `SETNX` + `EXPIRE` is broken: non-atomic (crash between -> stuck lock with no TTL) and `DEL` without ownership check deletes someone else's lock.

Two failure modes survive the correct single-node lock:
1. GC pause / clock jump / stall: A holds lock (10s TTL), pauses 15s, TTL expires, B acquires, A wakes thinking it still owns it -> two in the critical section. A did nothing wrong; the world moved on.
2. Failover: A locks on master, master crashes before replication, replica promoted without the lock, B acquires.

Redlock (Sanfilippo) fixes #2 only: `SET NX PX` on N=5 independent masters (no replication between them); acquired if >= N/2+1 ack AND elapsed < TTL; effective TTL = ttl - elapsed; release Lua DEL-if-matches on all N.

Redlock controversy (interview favorite):
- Kleppmann: unsafe under clock drift/NTP jumps, GC pauses, and lacks fencing tokens. Use ZooKeeper/etcd or have the resource reject stale writes.
- Sanfilippo: those assumptions attack ANY lock; quorum reduces (not eliminates) failover loss; residual risk acceptable for the latency win.
- Verdict: best-effort mutual exclusion (job leader election, debouncing) -> single-node or Redlock fine. Financial/safety-critical -> not sufficient; the RESOURCE must enforce via fencing.

Fencing token = monotonic counter returned with the lock; resource accepts a write only if `token >= last_seen`. Redlock does not provide it; layer via `INCR fence:<resource>` and `UPDATE ... WHERE last_fence < :fence`.

Gotchas:
- No fencing by default; a paused client can corrupt the resource even when "correctly" holding the lock.
- TTL must exceed worst-case critical section incl. GC/IO stalls (p99 8s -> 10s TTL too tight).
- Watchdog renewal must extend ONLY if still owner (Lua), else you extend someone else's lock.
- Redlock needs N INDEPENDENT masters, NOT a Redis Cluster.
- Many waiters polling `SET NX` busy-loop the server; use client backoff (Redisson uses pub/sub on release).

## Rate limiting recipes

Counting requests per window and saying "no" past a threshold. Do read-compute-write as ONE atomic step (Lua); naive multi-round-trip is racy.

| | Fixed window | Sliding window | Token bucket |
|---|-------------|----------------|--------------|
| State | one counter/bucket | ZSET of timestamps | (tokens, last_ts) |
| Memory | O(1) | O(limit) per key | O(1) |
| Accuracy | bursts at boundaries | exact in window | smooth average |
| Best for | cheapest | "strictly N per period" | "avg rate, bursts OK" (default) |

Naive `INCR + EXPIRE` bug: not atomic; crash between them -> TTL-less zombie counter -> user locked out forever. Also fixed-window allows 2x burst at the boundary (100 in last sec of min 0 + 100 in first sec of min 1).

Fixed-window Lua (atomic, with TTL):
```lua
local n = redis.call("INCR", KEYS[1])
if n == 1 then redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2])) end
if n > tonumber(ARGV[1]) then return 0 else return 1 end
```

Sliding-window Lua (ZSET, exact):
```lua
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, tonumber(ARGV[1]) - tonumber(ARGV[2]))
local count = redis.call("ZCARD", KEYS[1])
if count >= tonumber(ARGV[3]) then return 0 end
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[4])
redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[2]))
return 1
-- ARGV: now_ms, window_ms, limit, unique_id
```

Token bucket Lua (default):
```lua
local state = redis.call("HMGET", KEYS[1], "tokens", "ts")
local tokens   = tonumber(state[1]) or tonumber(ARGV[1])
local last_ts  = tonumber(state[2]) or tonumber(ARGV[3])
local now, capacity, rate = tonumber(ARGV[3]), tonumber(ARGV[1]), tonumber(ARGV[2])
local refill = math.min(capacity - tokens, (now - last_ts) * rate)
tokens = tokens + refill
local allowed = 0
if tokens >= 1 then tokens = tokens - 1; allowed = 1 end
redis.call("HMSET", KEYS[1], "tokens", tokens, "ts", now)
redis.call("PEXPIRE", KEYS[1], 60000)
return allowed
-- ARGV: capacity, refill_per_ms, now_ms
```

Gotchas:
- Clock skew: if each app server passes its own `now_ms`, a fast clock "earns" extra tokens. Source time inside Lua via `redis.call("TIME")`.
- Keep Lua tiny (blocks the server). Use `EVALSHA` for high-traffic limiters.
- Time-bucketed keys (`rate:user:42:hr:2026052517`) get free TTL cleanup; no maintenance job.
- Sliding-window ZSETs grow to `limit` entries per key per window (1000 x 1M users = 1B entries).

## Leaderboard recipes

A ZSET indexed by score (double); always sorted, so every op is O(log N).

```redis
ZADD leaderboard 1500 "user:42"        # set/update score
ZINCRBY leaderboard 10 "user:42"       # delta
ZSCORE leaderboard "user:42"
ZREVRANK leaderboard "user:42"         # 0-based rank, high to low
ZREVRANGE leaderboard 0 9 WITHSCORES   # top 10
ZRANGEBYSCORE leaderboard 1000 2000
ZCARD leaderboard
```

"Around me" = `ZREVRANK` then slice `[rank-5, rank+5]`:
```java
Long r = redis.sync().zrevrank("lb", user);
var around = redis.sync().zrevrangeWithScores("lb", Math.max(0, r-5), r+5);
```

Lexicographic ranges (same score = byte ordering; useful for autocomplete):
```redis
ZADD autocomp 0 apple 0 appliance 0 approve
ZRANGEBYLEX autocomp "[app" "[app\xff"
```

Reset / decay:
- Rolling keys (`leaderboard:2026-W21`) + `EXPIREAT` so old data ages out via TTL; no maintenance job.
- Time-decay: do NOT `ZINCRBY` every member on a schedule. Bake decay into the score (e.g. `score + log10(epoch_seconds)`) or keep a per-period hot-window ZSET.

Gotchas:
- `ZINCRBY` on a non-existent member adds it (sometimes a bug for inactive users).
- `ZREVRANK` returns null for non-members; handle the brand-new user, do not NPE.
- A global "top 10" key takes every write -> single shard. Shard by region/segment if writes are heavy.
- ZSET memory grows linearly; reset weekly/monthly via rolling keys, do not let it grow forever.
