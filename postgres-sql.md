# PostgreSQL 16 / SQL Cheatsheet

## Contents

- [Connect & psql](#connect--psql)
- [Data types](#data-types)
- [DDL & constraints](#ddl--constraints)
- [SELECT & filtering](#select--filtering)
- [INSERT / UPDATE / DELETE](#insert--update--delete)
- [Joins](#joins)
- [Aggregation: GROUP BY, subqueries, set ops](#aggregation-group-by-subqueries-set-ops)
- [Normalization vs denormalization](#normalization-vs-denormalization)
- [Indexes](#indexes)
- [EXPLAIN (ANALYZE, BUFFERS)](#explain-analyze-buffers)
- [Transactions & isolation](#transactions--isolation)
- [MVCC & VACUUM](#mvcc--vacuum)
- [Locking & deadlocks](#locking--deadlocks)
- [CTEs, window functions, LATERAL](#ctes-window-functions-lateral)
- [Partitioning](#partitioning)
- [Replication & WAL](#replication--wal)
- [Backup / restore (pg_dump, PITR)](#backup--restore-pg_dump-pitr)

---

## Connect & psql

What it is: the auth chain and the client meta-commands you live in.

Auth must align: TCP reachability, a `pg_hba.conf` rule, valid role creds, `CONNECT` on the database. Default auth method is `scram-sha-256` (since 14).

psql meta-commands:

| Command | Does |
|---|---|
| `\l` | list databases |
| `\dt` / `\d <t>` | list tables / describe table |
| `\di` `\dn` `\du` `\df` | indexes / schemas / roles / functions |
| `\timing on` | show query timing |
| `\x` | expanded (vertical) output |
| `\watch 1` | re-run last query every 1s |
| `\e` | edit in `$EDITOR` |
| `\copy` | client-side COPY (file is on the client) |

```sql
SELECT version();
SELECT current_database(), current_user, inet_server_addr(), inet_server_port();

-- who is connected / what is running
SELECT pid, usename, state, wait_event_type, wait_event,
       now() - query_start AS runtime, left(query,80) AS query
FROM pg_stat_activity
WHERE backend_type = 'client backend'
ORDER BY runtime DESC NULLS LAST;
```

Key knobs: `listen_addresses`, `max_connections` (~10MB/backend; pool above ~200), `shared_buffers` (~25% RAM), `work_mem` (per sort/hash/node), `effective_cache_size` (~75% RAM), `default_statistics_target`.

Gotchas:
- `pg_hba.conf` is matched top-down, first match wins. `localhost` resolves v4 + v6; a missing `::1/128` line is the classic Mac/Linux failure.
- Reload (`SELECT pg_reload_conf()`) applies `pg_hba.conf` and sighup GUCs; `shared_buffers` needs a full restart. Check `pg_settings.context`.
- Backend-per-connection: 500 idle JDBC connections = 500 processes. Always pool (PgBouncer transaction mode).
- `idle_in_transaction_session_timeout = 0` (default) lets a forgotten BEGIN pin the xmin horizon and bloat every table.
- `\copy` paths are client-side; server-side `COPY` paths are on the server FS.

---

## Data types

What it is: column type sets size, legal operations, sort order, and which mistakes are rejected. Changing a type on a big table rewrites every row (AccessExclusiveLock) - treat as a one-way door.

Numerics:

| Type | Bytes | Use when |
|---|---|---|
| `smallint` | 2 | tiny enums, status codes |
| `integer` | 4 | default whole numbers, FKs |
| `bigint` | 8 | IDs > 2B, epoch millis |
| `numeric(p,s)` | var | money, exact decimal (slow) |
| `real` / `double precision` | 4/8 | scientific, never money |

Other families:
- Temporal: `timestamptz` (stores UTC instant, displays in session tz) almost always; `timestamp` drops the offset silently. Plus `date`, `time`, `interval`.
- Textual: `text` and `varchar(n)` are identical on disk; prefer `text` (+ CHECK if a cap is needed). `char(n)` space-pads, avoid.
- `jsonb` (binary, indexable, dedups keys) 99% of the time; `json` keeps raw text and re-parses on every read.
- Arrays (`int[]`, `text[]`): first-class, GIN-indexable (`@>`, `&&`), but a normalization smell once you join/aggregate elements.
- Ranges (`int4range`, `tstzrange`, `daterange`): operators `@>` contains, `&&` overlaps, `-|-` adjacent. Half-open `[a,b)` by default.
- TOAST: rows over ~2KB get large values compressed/moved to a side table; transparent, costs an extra fetch.

```sql
-- float is inexact; numeric is exact
SELECT 0.1::double precision + 0.2::double precision;  -- 0.30000000000000004
SELECT 0.1::numeric + 0.2::numeric;                    -- 0.3

-- jsonb
SELECT * FROM events WHERE payload @> '{"user":42}';        -- containment (GIN)
SELECT payload->>'type' AS type, (payload->'user')::int FROM events;
CREATE INDEX events_gin ON events USING gin (payload);              -- @> ? ?& ?|
CREATE INDEX events_gin2 ON events USING gin (payload jsonb_path_ops); -- smaller, only @>

-- prevent double-booking with a range + exclusion constraint
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE TABLE bookings (
  room int,
  during tstzrange,
  EXCLUDE USING gist (room WITH =, during WITH &&)
);
```

Gotchas:
- `timestamp` (no tz) drops the offset on insert; the same value means a different instant to every reader. Use `timestamptz`.
- Updating a `jsonb` column rewrites the whole value (MVCC new version) -> bloat on hot large blobs. Index only fields you query.
- GIN indexes can be 2-4x table size and slow writes.
- `varchar(n)` length is checked in characters, not bytes.
- `WHERE payload->>'type' = 'click'` does NOT use a `jsonb` GIN index (that index serves `@>`/key ops, not `->>` equality).

---

## DDL & constraints

What it is: a constraint is a rule handed to the database so it enforces correctness on every write, forever.

```sql
CREATE TABLE customers (
  id         bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  name       text        NOT NULL,
  email      text        UNIQUE,
  status     text        NOT NULL DEFAULT 'active',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
  id          bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  customer_id bigint NOT NULL REFERENCES customers(id),
  status      text   CHECK (status IN ('pending','paid','shipped')),
  total       numeric(12,2) CHECK (total >= 0)
);
```

Constraint family: `NOT NULL`, `DEFAULT`, `PRIMARY KEY` (implies UNIQUE + NOT NULL), `UNIQUE`, `CHECK` (per-row, no subqueries/other tables), `FOREIGN KEY` (`REFERENCES`, opt `ON DELETE CASCADE | SET NULL`).

Auto keys: `GENERATED BY DEFAULT AS IDENTITY` (modern; lets you supply your own) vs `GENERATED ALWAYS` (refuses supplied) vs older `bigserial` (sequence-backed). IDs are not reused on delete.

ALTER cost - instant vs full rewrite:

```sql
-- INSTANT (PG 11+): constant default is metadata-only
ALTER TABLE customers ADD COLUMN country_code text NOT NULL DEFAULT 'PT';

-- DANGER: volatile default (now(), random()) -> full table rewrite under AccessExclusiveLock
-- Safe pattern: add nullable -> backfill in batches -> SET NOT NULL
ALTER TABLE customers ADD COLUMN signup_token uuid;
ALTER TABLE customers ALTER COLUMN signup_token SET NOT NULL;

-- FKs are NOT auto-indexed on the referencing side - index them yourself
CREATE INDEX CONCURRENTLY ON orders (customer_id);  -- builds without locking writes

-- partial unique rule a plain constraint can't express
CREATE UNIQUE INDEX ON addresses (customer_id) WHERE is_primary;

-- find leftovers of a failed concurrent build
SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;
```

Gotchas:
- Volatile-default `ADD COLUMN` rewrites the whole table; constant defaults do not.
- Missing index on a FK referencing column means parent DELETE/UPDATE scans the whole child table.
- Failed `CREATE INDEX CONCURRENTLY` leaves an `indisvalid = false` index: ignored by planner, still maintained on writes. `DROP INDEX CONCURRENTLY` and retry.
- `CHECK` cannot reference another table or use a subquery.
- Circular FKs: make one `DEFERRABLE INITIALLY DEFERRED`, insert both in one transaction.
- Adding a FK validates every existing row; `ADD CONSTRAINT ... NOT VALID` then `VALIDATE CONSTRAINT` splits the slow scan off.
- UNIQUE constraint vs unique index: same on-disk artifact; only an index can be partial/expression; only a constraint can be a FK target.

---

## SELECT & filtering

What it is: declarative read. You describe what you want; the planner decides how.

Clause order - written vs evaluated:

```text
written:    SELECT cols  FROM t  WHERE filter  GROUP BY  HAVING  ORDER BY  LIMIT
evaluated:  FROM -> WHERE -> GROUP BY -> HAVING -> SELECT -> ORDER BY -> LIMIT
```

That order is why a SELECT alias is usable in ORDER BY but not in WHERE.

```sql
SELECT name, country FROM customers WHERE country = 'PT';
SELECT * FROM products WHERE price BETWEEN 10 AND 50;        -- inclusive
SELECT * FROM orders   WHERE status IN ('paid','shipped');
SELECT * FROM customers WHERE name LIKE 'A%';                -- case-sensitive
SELECT * FROM customers WHERE name ILIKE 'a%';               -- case-insensitive
SELECT name, total * 0.23 AS vat FROM orders ORDER BY total DESC LIMIT 10;
SELECT DISTINCT country FROM customers;
```

LIKE: `%` = any run of chars, `_` = exactly one char.

NULL = unknown. Comparison to NULL yields unknown, and WHERE keeps only true rows:

```sql
SELECT * FROM orders WHERE total = NULL;      -- WRONG: never matches
SELECT * FROM orders WHERE total IS NULL;     -- RIGHT
```

Gotchas:
- `WHERE x = NULL` returns nothing; use `IS NULL` / `IS NOT NULL`.
- `NOT (x = 5)` and `x <> 5` do not catch rows where x is NULL.
- A SELECT alias is not visible in WHERE.
- `LIMIT` without `ORDER BY` (+ a unique tiebreaker) returns nondeterministic rows.
- Leading-wildcard `LIKE '%foo'` cannot use a normal B-tree index.

---

## INSERT / UPDATE / DELETE

What it is: the three write verbs. An UPDATE/DELETE with no WHERE hits the entire table, no confirmation.

```sql
INSERT INTO customers (name, country) VALUES ('Ana','PT'),('Bo','ES');  -- multi-row, faster

INSERT INTO orders (customer_id, status, total) VALUES (42,'pending',0)
RETURNING id, ordered_at;            -- get generated values in one round trip

UPDATE products SET price = price * 1.10 WHERE category = 'electronics';
DELETE FROM orders WHERE status = 'cancelled' AND ordered_at < '2025-01-01';

-- upsert: atomic, race-free; EXCLUDED = the row you tried to insert
INSERT INTO products (id, name, price) VALUES (7,'Mouse',19.99)
ON CONFLICT (id) DO UPDATE SET price = EXCLUDED.price;
INSERT INTO products (id, name, price) VALUES (7,'Mouse',19.99)
ON CONFLICT (id) DO NOTHING;
```

Every statement runs in a transaction (autocommit by default). Wrap risky writes: `BEGIN; ... COMMIT;` (or `ROLLBACK;`).

Gotchas:
- No WHERE = every row affected.
- `ON CONFLICT` target must match a real unique constraint/index.
- `EXCLUDED` is the proposed (rejected) row, not the existing one.
- `TRUNCATE` ignores WHERE, bypasses DELETE triggers, can `RESTART IDENTITY`; not a faster filtered DELETE.
- `INSERT ... VALUES` with no column list breaks when columns change.
- An UPDATE matching 0 rows is not an error - check the row count.

---

## Joins

What it is: stitch tables back together. The whole game: which rows survive when there is no match.

| Join | Keeps |
|---|---|
| `INNER` (`JOIN`) | only matched pairs on both sides |
| `LEFT` | all left rows; right cols NULL when unmatched |
| `RIGHT` | mirror of LEFT |
| `FULL` | unmatched rows from both sides |
| `CROSS` | every combination (row explosion; usually accidental) |
| self | a table joined to itself via two aliases |

```sql
SELECT c.name, o.id, o.total
FROM customers c INNER JOIN orders o ON o.customer_id = c.id;

-- anti-join: rows in A with no match in B
SELECT c.name FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
WHERE o.id IS NULL;

-- LEFT JOIN trap: filter the right table in WHERE and it silently becomes INNER
-- BROKEN: drops customers with no paid order
SELECT c.name, o.status FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id WHERE o.status = 'paid';
-- FIXED: condition in ON keeps unmatched rows
SELECT c.name, o.status FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id AND o.status = 'paid';
```

Rule: conditions about whether two rows match go in ON; conditions that filter the final result go in WHERE.

Gotchas:
- WHERE on the right table of a LEFT JOIN demotes it to INNER (NULLs fail the filter).
- Comma-join with no condition = accidental CROSS JOIN = row explosion.
- Joining on a non-unique column multiplies rows (fan-out); SUM then inflates.
- `USING (col)` needs identical column names and collapses to one output column.

---

## Aggregation: GROUP BY, subqueries, set ops

What it is: collapse many rows into summaries.

```sql
SELECT count(*) FROM orders;                  -- counts rows
SELECT count(status) FROM orders;             -- counts NON-NULL values
SELECT count(DISTINCT customer_id) FROM orders;
SELECT country, count(*) FROM customers GROUP BY country;
```

Iron rule: every SELECT column is either in GROUP BY or inside an aggregate.

WHERE filters rows before grouping; HAVING filters groups after aggregation (aggregates are not allowed in WHERE):

```sql
SELECT country, count(*) AS active
FROM customers
WHERE status = 'active'        -- per-row, before grouping
GROUP BY country
HAVING count(*) > 100;         -- per-group, after counting
```

Conditional aggregation with FILTER (replaces sum(CASE WHEN ...)):

```sql
SELECT customer_id,
       count(*)                                  AS all_orders,
       count(*)   FILTER (WHERE status='paid')   AS paid_orders,
       sum(total) FILTER (WHERE status='paid')   AS paid_revenue
FROM orders GROUP BY customer_id;
```

Double-counting trap: never SUM a parent column across a one-to-many join. Pre-aggregate the child first:

```sql
SELECT o.customer_id, count(*) AS orders, sum(o.total) AS revenue, sum(i.items) AS total_items
FROM orders o
JOIN (SELECT order_id, sum(quantity) AS items FROM order_items GROUP BY order_id) i
  ON i.order_id = o.id
GROUP BY o.customer_id;
```

Subqueries:

```sql
SELECT name, total FROM orders WHERE total > (SELECT avg(total) FROM orders);  -- scalar
SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE status='paid');
SELECT name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id=c.id);
```

Set operations (compare whole rows by position; NULLs treated equal):

| Op | Returns |
|---|---|
| `UNION` | rows in either (dedup + sort) |
| `INTERSECT` | rows in both |
| `EXCEPT` | rows in first not in second (directional) |
| `... ALL` | same, keeps duplicates, skips the sort |

Gotchas:
- Non-aggregated, non-grouped column in SELECT is an error.
- `sum`/`avg`/`max`/`min` over zero rows return NULL, not 0 - guard with `COALESCE(sum(x),0)`.
- Aggregates skip NULLs (`avg` divides by non-NULL count).
- `WHERE count(*) > 5` is an error; use HAVING.
- `NOT IN (subquery)` returns zero rows if the subquery yields any NULL - use `NOT EXISTS`.
- `UNION` silently sorts + dedups; use `UNION ALL` when you do not need dedup.

---

## Normalization vs denormalization

What it is: normalization stores every fact in exactly one place; denormalize only for a known workload, knowing the consistency bill.

Normal forms (one-liner: every non-key column depends on the key, the whole key, and nothing but the key):
- 1NF: one atomic value per box, each row keyed.
- 2NF: no column depends on only part of a composite key.
- 3NF: no non-key column derived from another non-key column.

3NF is the OLTP default; the planner makes joins on indexed columns cheap. Measure before duplicating - most "joins are slow" is a missing index or bad join order.

When normalization stops paying:

| Workload | Tool | Bill |
|---|---|---|
| Expensive read-time aggregate | materialized view | staleness; refresh lock |
| Hot counter on a parent | counter column + trigger | row-lock contention, bloat, drift |
| Sparse / schemaless payload | `jsonb` column | no FK/CHECK, no stats, GIN write amp |
| Immutable history | wide append-only row | duplication is intentional (frozen truth) |
| Analytics shape | build it on a read replica | keep OLTP tables clean |

Generated columns are the one denormalization with no consistency risk (derived from the same row only, indexable):

```sql
ALTER TABLE customers
  ADD COLUMN email_lower text GENERATED ALWAYS AS (lower(email)) STORED;

CREATE MATERIALIZED VIEW customer_30d AS SELECT ...;
CREATE UNIQUE INDEX customer_30d_pk ON customer_30d (id);   -- required for CONCURRENTLY
REFRESH MATERIALIZED VIEW CONCURRENTLY customer_30d;
```

Gotchas:
- Counter-column triggers serialize all writes on the parent row (hotspot) and bloat it.
- Plain `REFRESH MATERIALIZED VIEW` takes AccessExclusiveLock (readers blocked); `CONCURRENTLY` needs a unique index and is slower (it diffs).
- Duplicated attributes drift when a path bypasses the sync trigger (bulk update).
- `jsonb` you end up filtering `payload->>'field'` everywhere reinvents columns with no validation.
- Generated column (same-row, no drift) vs trigger column (cross-row, can drift): prefer generated when possible.

---

## Indexes

What it is: a write-time tax for read-time speedup. Every index slows every write.

| Type | Best for | Operators | Not for |
|---|---|---|---|
| B-tree | equality, range, ORDER BY, IS NULL | `= < <= > >= BETWEEN IN` | substring, array contains |
| Hash | equality only | `=` | range, ORDER BY |
| GIN | multi-value (`jsonb`, `text[]`, `tsvector`) | `@> ? ?& ?| @@` | range |
| BRIN | huge tables physically correlated (time-series) | range on naturally-ordered data | random-access columns |
| GiST | geometry, ranges, full-text | `&& @> << >>` | strict equality (use B-tree) |
| SP-GiST | non-balanced (quad-trees, ip4r) | spatial / hierarchical | general use |

- B-tree: the workhorse. O(log n) lookup, range scans, sorted output, uniqueness.
- GIN: one entry per element; great for containment, expensive on write (tune `fastupdate`, `gin_pending_list_limit`).
- BRIN: stores min/max per block range; tiny (MB on a 100GB table); useless on randomly-inserted data.
- Multicolumn B-tree: leftmost-prefix rule. `(a,b)` serves `a=?`, `a=? AND b=?`, `a=? ORDER BY b`, but NOT `b=?` alone. Equality cols first, then range/sort col.
- Partial: `WHERE deleted_at IS NULL` indexes only live rows.
- Expression: `CREATE INDEX ON users (lower(email))` - query must use the same expression.
- Covering: `INCLUDE (col)` enables index-only scans (when visibility map is fresh).

```sql
CREATE INDEX be_cust_time ON big_events (customer_id, occurred_at DESC);
CREATE INDEX be_purchase ON big_events (customer_id, occurred_at) WHERE event_type='purchase';
CREATE INDEX be_payload ON big_events USING gin (payload jsonb_path_ops);
CREATE INDEX be_tags ON big_events USING gin (tags);
CREATE INDEX be_time_brin ON big_events USING brin (occurred_at) WITH (pages_per_range=32);
CREATE INDEX be_cover ON big_events (customer_id) INCLUDE (event_type, occurred_at);

-- which indexes are used / their size
SELECT relname, indexrelname, idx_scan, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes WHERE relname='big_events' ORDER BY idx_scan DESC;
```

Gotchas:
- Overindexed tables become write-bound.
- BRIN on randomly-inserted data is worse than no index.
- Expression index used only when the query uses the same expression (`lower(email)=?` hits; `email ILIKE ?` does not).
- Index-only scan silently degrades to heap fetches when the visibility map is stale (no recent VACUUM).
- Functional indexes need IMMUTABLE functions (`now()` is STABLE, not IMMUTABLE).
- Prefix `LIKE 'x%'` in i18n setups needs `text_pattern_ops` / `varchar_pattern_ops`.
- Seq scan despite an index usually means low selectivity (>~5-10% rows), stale stats, or non-indexable predicate.

---

## EXPLAIN (ANALYZE, BUFFERS)

What it is: the profiler. `EXPLAIN` = estimate; `ANALYZE` = run it + actual timings; `BUFFERS` = page hits/reads. First heuristic: estimate vs actual diverging by >10x anywhere is the bug.

Reading a node:

```text
Limit  (cost=0.43..8.45 rows=10 width=64) (actual time=0.05..0.12 rows=10 loops=1)
  Buffers: shared hit=4
  ->  Index Scan using orders_cust_idx on orders
      (cost=...) (actual time=... rows=10 loops=1)
      Index Cond: (customer_id = 42)
```

- `cost=startup..total` arbitrary units; planner minimizes total.
- `rows` estimate vs `actual rows`. `width` = avg bytes.
- `loops` = inner-side executions; multiply per-loop time by loops.
- `Buffers: shared hit=X read=Y` - hit = in shared_buffers, read = from OS/disk.

Scan types: Seq Scan (whole table; good >5-10% selectivity); Index Scan (random IO per row); Index Only Scan (from index alone, needs visibility map); Bitmap Index + Bitmap Heap Scan (collect TIDs, sort, sequential heap fetch; combines indexes; medium selectivity).

Join types: Nested Loop (small outer + indexed inner); Hash Join (build on smaller, probe with larger; needs work_mem or spills); Merge Join (both sorted).

Aggregate types: HashAggregate (hash of groups, fast if fits work_mem); GroupAggregate (needs sorted input).

Flags to spot: `Rows Removed by Filter` (predicate not indexable); `Heap Fetches` (visibility-map staleness, want ~0); `Sort Method: external merge Disk: ...` (work_mem too small); `Workers Launched: N`. `Index Cond` narrowed via index; `Filter` means rows read then discarded.

Planner GUCs: `random_page_cost` (default 4; SSD ~1.1); `effective_cache_size` (~75% RAM); `work_mem` (per-node); `default_statistics_target`; `enable_*` toggles (diagnostic only).

```sql
EXPLAIN (ANALYZE, BUFFERS, SETTINGS)
SELECT * FROM o WHERE customer_id = 42 ORDER BY occurred_at DESC LIMIT 10;
```

Gotchas:
- ANALYZE runs the statement - wrap DML in `BEGIN; ... ROLLBACK;`.
- Big divergence on correlated columns: add `CREATE STATISTICS`.
- `work_mem` is per-node per-process; a query with 3 sorts + parallel workers uses multiples.
- Prepared statements may flip to a generic plan at execution ~#6; `plan_cache_mode = force_custom_plan` to re-plan.
- Fast in psql, slow from app = generic prepared-statement plan.

---

## Transactions & isolation

What it is: a transaction is all-or-nothing (`BEGIN`...`COMMIT`/`ROLLBACK`). ACID = Atomicity, Consistency, Isolation, Durability. Isolation levels control which concurrency anomalies you tolerate.

Anomalies: dirty read (read uncommitted change); non-repeatable read (re-read same row, value changed); phantom read (re-run query, new matching row appears); serialization anomaly / write skew (two writes each fine alone, jointly illegal).

| Level | Dirty | Non-repeatable | Phantom | Serialization anomaly |
|---|---|---|---|---|
| Read Committed (default) | No | Allowed | Allowed | Allowed |
| Repeatable Read | No | No | No (PG stronger than standard) | Allowed |
| Serializable | No | No | No | No |

Postgres never allows dirty reads (asking for Read Uncommitted gives Read Committed). PG Repeatable Read = snapshot isolation; it prevents phantoms (the standard does not require that).

- Read Committed: a new snapshot per statement.
- Repeatable Read: one snapshot at first query; on a write conflict aborts with `ERROR: could not serialize access due to concurrent update` (SQLSTATE 40001). App must retry.
- Serializable: uses SSI (predicate / SIREAD locks) to detect dependency cycles; aborts one with 40001. Retry mandatory.

```sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; ... COMMIT;
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

Rule of thumb: RC for ~95% of features; RR for multi-step audits/reports; Serializable for ledgers / race-breakable invariants (always add retry logic). At Serializable, `SET TRANSACTION READ ONLY DEFERRABLE` gives a long report a never-aborted snapshot.

Explicit row locks (read-modify-write races):

| Clause | Meaning | Blocks writers | Blocks FOR SHARE |
|---|---|---|---|
| `FOR UPDATE` | I will change these rows | yes | yes |
| `FOR NO KEY UPDATE` | change, but not key columns | yes | yes |
| `FOR SHARE` | read, must not change | yes | no |
| `FOR KEY SHARE` | only the key must stay (FK check takes this) | no | no |

Modifiers: `NOWAIT` (fail instantly if locked); `SKIP LOCKED` (skip locked rows). `SKIP LOCKED` powers job queues:

```sql
BEGIN;
SELECT id FROM jobs WHERE status='pending' ORDER BY id LIMIT 1
FOR UPDATE SKIP LOCKED;     -- each worker grabs a different unclaimed job
UPDATE jobs SET status='done' WHERE id = :id;
COMMIT;
```

Gotchas:
- App must catch and retry 40001 (RR + SSI). ORMs hide it. Exponential backoff, capped, idempotent; retry the whole transaction.
- SSI predicate locks can escalate tuple -> page -> relation under pressure, causing false-positive aborts (a new broad index scan can trigger them).
- Long-running RR/SSI/read-only transactions pin xmin -> VACUUM cannot reclaim -> bloat.
- RC `SELECT FOR UPDATE` re-checks the WHERE on the new row version (EvalPlanQual); a row that stopped matching is silently skipped.

---

## MVCC & VACUUM

What it is: Postgres keeps multiple row versions so readers never block writers; VACUUM cleans the dead ones.

UPDATE/DELETE never edits in place: it writes a new version and marks the old dead (UPDATE = delete + insert). Hidden columns `xmin` (creating txid) / `xmax` (deleting txid). Visibility compares these to your snapshot.

Dead tuples cause bloat, cache pressure, and wraparound risk.

| Command | Does | Returns disk to OS | Lock |
|---|---|---|---|
| `VACUUM` (lazy/auto) | marks dead reusable, updates FSM + visibility map, holds off wraparound | No (reuses space in-file) | light |
| `VACUUM FULL` | rewrites whole table, no dead tuples | Yes (shrinks file) | AccessExclusiveLock |
| `VACUUM (ANALYZE)` | vacuum + refresh planner stats | No | light |
| `VACUUM FREEZE` | mark old rows visible-to-all (anti-wraparound) | No | light |

Classic trap: plain VACUUM does NOT return disk to the OS. To shrink a bloated file use `VACUUM FULL` (locks) or online `pg_repack` / `pg_squeeze`.

Autovacuum fires when: `dead_tuples > threshold(50) + scale_factor(0.20) * live_tuples`. Defaults are too lax for big tables (a 100M-row table waits for 20M dead). Tune per-table:

```sql
ALTER TABLE big_events SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_vacuum_cost_limit = 2000
);
```

HOT (Heap-Only Tuple): if no indexed column changed AND the new version fits the same page, indexes are not touched - huge win for hot counters. Track `n_tup_hot_upd` vs `n_tup_upd`; lower `fillfactor` to 80-90 to keep page slack.

```sql
-- vacuum health
SELECT relname, n_live_tup, n_dead_tup,
       round(n_dead_tup::numeric/nullif(n_live_tup,0),3) AS dead_ratio,
       last_autovacuum, n_tup_hot_upd, n_tup_upd
FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 20;

-- wraparound proximity
SELECT datname, age(datfrozenxid) FROM pg_database ORDER BY 2 DESC;
```

What pins the xmin horizon (stops reclamation cluster-wide): long-running / idle-in-transaction sessions; lagging logical replication slots (`pg_replication_slots.xmin`); hung prepared (two-phase) transactions (`pg_prepared_xacts`).

Wraparound: txids are 32-bit; FREEZE marks old rows permanently visible. Autovacuum launches an aggressive `(to prevent wraparound)` vacuum at `autovacuum_freeze_max_age` (default 200M).

Gotchas:
- Non-HOT updates on hot tables bloat fast.
- `VACUUM FULL` never on production tables (use pg_repack).
- `n_dead_tup` is refreshed by ANALYZE, not in real time.
- `idle_in_transaction_session_timeout = 0` + forgotten BEGIN = cluster-wide bloat overnight.
- `VACUUM` cannot run inside a transaction block.

---

## Locking & deadlocks

What it is: locks are mostly automatic; the skill is predicting which operation blocks which and reading the tools.

Row-level locks (cheap, in the tuple header): the four modes from the isolation section. Headline: `FOR UPDATE` blocks everyone; `FOR KEY SHARE` (FK enforcement) blocks almost no one.

Table-level locks (mostly DDL):

| Operation | Lock taken | Conflicts with |
|---|---|---|
| `SELECT` | AccessShare | only ALTER/DROP-class |
| `INSERT`/`UPDATE`/`DELETE` | RowExclusive | CREATE INDEX, DDL |
| VACUUM, ANALYZE, CREATE INDEX CONCURRENTLY, autovacuum | ShareUpdateExclusive | each other + DDL |
| `CREATE INDEX` (plain) | Share | writes |
| `ALTER TABLE`, `DROP`, `TRUNCATE`, `REINDEX` | AccessExclusive | everything, including plain SELECT |

Headline: SELECT blocks almost nothing and almost nothing blocks SELECT, except AccessExclusive (DDL), which blocks everything.

Deadlock = circular wait; Postgres detects it (`deadlock_timeout`, default 1s) and aborts one with `ERROR: deadlock detected` (SQLSTATE 40P01). Cause: locking the same rows in opposite order. Fix: consistent lock order (`ORDER BY id` when batch-updating).

Timeouts: `lock_timeout` (give up waiting for a lock), `statement_timeout`, `idle_in_transaction_session_timeout`. Always `SET lock_timeout` before a migration.

Why a queued ALTER takes down all reads: ALTER wants AccessExclusive, waits behind a long SELECT, and every new query queues behind the pending ALTER. `lock_timeout` makes it fail fast and release the queue.

Advisory locks (app-defined mutexes): `pg_advisory_lock(key)` (session), `pg_advisory_xact_lock(key)` (txn-scoped, safer), `pg_try_advisory_lock(key)` (non-blocking).

```sql
-- who blocks whom
SELECT a.pid AS waiting, pg_blocking_pids(a.pid) AS blockers,
       a.wait_event_type, left(a.query,80) AS query
FROM pg_stat_activity a
WHERE a.wait_event_type IS NOT NULL AND a.backend_type='client backend';

-- safe migration
SET lock_timeout = '3s'; SET statement_timeout = '5min';
BEGIN; ALTER TABLE big_table ADD COLUMN flag bool NOT NULL DEFAULT false; COMMIT;
```

Gotchas:
- Queued ALTER blocks every subsequent SELECT; always `SET lock_timeout` first.
- Adding a FK (without NOT VALID) takes ShareRowExclusive on both tables, blocking writes during validation.
- FK insert deadlocks: out-of-order child inserts referencing the same parents; sort by parent id, or use ON CONFLICT.
- Session-scoped advisory locks leak across pooled connections; prefer `pg_advisory_xact_lock` or unlock in finally.
- A long `pg_dump` holds AccessShare on every table; it blocks nothing until an ALTER arrives and then blocks everything new.

---

## CTEs, window functions, LATERAL

What it is: composable read tooling for analytics, hierarchies, and top-N-per-group.

CTEs: in PG 12+ a non-recursive CTE referenced once with no side effects is inlined by default (no fence). Force with `AS MATERIALIZED` / `AS NOT MATERIALIZED`.

```sql
WITH recent AS (SELECT * FROM orders WHERE created_at > now() - interval '7 days')
SELECT customer_id, count(*) FROM recent GROUP BY customer_id;

WITH RECURSIVE org AS (
  SELECT id, manager_id, name, 1 AS depth FROM employees WHERE manager_id IS NULL
  UNION ALL
  SELECT e.id, e.manager_id, e.name, o.depth+1
  FROM employees e JOIN org o ON e.manager_id = o.id
) SELECT * FROM org ORDER BY depth, name;
```

Window functions compute over a partitioned/ordered set without collapsing rows:

| Function | Does |
|---|---|
| `row_number()` | distinct number per row |
| `rank()` / `dense_rank()` | tied rows share rank (gap / no gap) |
| `lag(col,n)` / `lead(col,n)` | value from a relative offset row |
| `first_value` / `last_value` / `nth_value` | positional value in frame |
| any aggregate `OVER (...)` | sum/avg/count/... as a window |

`PARTITION BY` groups, `ORDER BY` orders within partition, the frame (`ROWS`/`RANGE BETWEEN`) picks participating rows.

```sql
SELECT customer_id, order_id, total_cents,
       rank() OVER (PARTITION BY customer_id ORDER BY total_cents DESC) AS rk,
       sum(total_cents) OVER (PARTITION BY customer_id ORDER BY created_at
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
FROM orders;

-- top-N per group (window): wrap to filter
SELECT * FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS rn
  FROM orders
) t WHERE rn <= 3;

-- top-N per group (LATERAL): often faster with index on (customer_id, created_at DESC)
SELECT c.id, o.*
FROM customers c
JOIN LATERAL (SELECT * FROM orders o WHERE o.customer_id = c.id
              ORDER BY created_at DESC LIMIT 3) o ON true;
```

LATERAL lets the right side reference left-side columns (plain FROM subqueries cannot).

Gotchas:
- Pre-12 "CTEs are slow because of the fence" is outdated; only MATERIALIZED / multi-referenced / DML CTEs are fenced in 16.
- Window functions cannot be used in WHERE - wrap in a subquery.
- Recursive CTE without cycle protection (`WHERE NOT EXISTS ...` or `CYCLE` syntax, 14+) runs forever.
- LATERAL with `LIMIT N` runs the inner once per outer row - fast only with a good index on the correlation column.
- `lag`/`lead` with no default return NULL at the edges.

---

## Partitioning

What it is: declarative partitioning (10+) splits a logical table into physical children by a partition key; the parent is empty and routes reads/writes.

| Strategy | By | Best for |
|---|---|---|
| RANGE | value range | time-series |
| LIST | enumerated value | tenant / region |
| HASH | hash mod N | even spread, no natural key |

Partition when: table > ~50-100GB with a clear key; maintenance too slow whole-table; you want fast drop-old-data (`DROP TABLE old_part`); queries filter on the key (pruning). Do NOT partition small tables, or when no query filters on the key, or with HASH on a skewed key.

Pruning is the killer feature: plan-time (list resolved at planning) and execution-time (`Subplans Removed: N` for parameters / prepared statements).

```sql
CREATE TABLE events (
  id bigserial, user_id bigint NOT NULL, occurred_at timestamptz NOT NULL, payload jsonb,
  PRIMARY KEY (id, occurred_at)              -- PK MUST include the partition key
) PARTITION BY RANGE (occurred_at);

CREATE TABLE events_2026_05 PARTITION OF events FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE events_default PARTITION OF events DEFAULT;
CREATE INDEX events_user_time ON events (user_id, occurred_at DESC);  -- propagates per partition

-- HASH
CREATE TABLE accounts (id bigint PRIMARY KEY, balance bigint) PARTITION BY HASH (id);
CREATE TABLE accounts_p0 PARTITION OF accounts FOR VALUES WITH (modulus 4, remainder 0);

ALTER TABLE events DETACH PARTITION events_2026_05 CONCURRENTLY;  -- 14+, no long lock
DROP TABLE events_2025_01;                                       -- instant vs DELETE
```

Gotchas:
- PK / UNIQUE / EXCLUDE must include the partition key (no globally unique `email` without it).
- Default partition + new range partition: ATTACH fails if default rows match the new range; default also disables some optimizations.
- Prepared statements may not prune at plan time - check `plan_cache_mode`, look for `Subplans Removed`.
- HASH skew makes one partition the bottleneck (distributes by value, not load).
- Cross-partition UPDATE (changing the key) does row movement (delete+insert on different tables).
- Predicate on an expression of the key (`date_trunc('month', occurred_at) = ...`) is opaque - no pruning.
- `CREATE INDEX CONCURRENTLY` not supported directly on the parent - do it per partition.

---

## Replication & WAL

What it is: WAL is the durable change log; every modification is logged before the data page is flushed. It powers crash recovery, replication, and PITR.

Key concepts: LSN (`X/YYYYYYYY` hex position); WAL segments (16MB files in `pg_wal/`); checkpoint (flush dirty buffers, write checkpoint record; by `checkpoint_timeout` 5min or `max_wal_size` 1GB). `wal_level`: `minimal` / `replica` (default, enables streaming + physical backup) / `logical` (adds row-level change data).

Physical (streaming) replication: byte-for-byte binary copy; WAL ships to replicas; replicas read-only (hot standby). Use for HA, read scaling, backup target.
- Async (default): commit then ship; RPO > 0.
- Sync (`synchronous_commit`, `synchronous_standby_names`): waits for replica ack; RPO = 0, higher latency; primary stalls if all sync replicas die.

Logical replication: decodes WAL into row events; publisher/subscriber (`CREATE PUBLICATION` / `CREATE SUBSCRIPTION`); per-table, cross-major-version. Use for zero-downtime upgrade, selective/multi-region, CDC (Debezium uses `pgoutput`). Needs `wal_level=logical` + PK (or `REPLICA IDENTITY FULL`).

Replication slots: primary retains WAL until the replica confirms. No slot = WAL may be recycled before a slow replica catches up. Orphan slots are the #1 cause of "WAL filled the disk".

```sql
SELECT pg_current_wal_lsn();
SELECT slot_name, active, restart_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots;

SELECT application_name, state, sync_state,
       pg_size_pretty(pg_wal_lsn_diff(flush_lsn, replay_lsn)) AS replay_lag
FROM pg_stat_replication;

SELECT now() - pg_last_xact_replay_timestamp() AS replica_lag;  -- on the replica
```

Gotchas:
- Orphan slots (`active=false`) fill the disk - monitor aggressively.
- Sync replication blocks the primary if all sync replicas are unreachable.
- Logical replication does not replicate DDL (schema drift) or sequence values.
- `REPLICA IDENTITY DEFAULT` uses the PK; no-PK tables need `FULL` (replicates whole old row, expensive).
- `hot_standby_feedback = on` stops recovery-conflict cancels but pins the primary xmin -> bloat.
- Failover does not auto-reverse; old primary diverges and needs `pg_rewind`.

---

## Backup / restore (pg_dump, PITR)

What it is: two approaches. Logical = portable SQL snapshot; physical = fast cluster copy + WAL for point-in-time recovery.

Logical (`pg_dump`): per-database, schema/table selectable, version-portable, consistent via a repeatable-read snapshot (does not block writes), no PITR, slow restore (rebuilds indexes).

Physical (`pg_basebackup` + WAL archive): whole data dir + WAL, cluster-wide, fast restore, required for PITR, same major version + architecture only.

`pg_dump` formats: `-Fp` plain SQL; `-Fc` custom (compressed, parallel restore `-j`); `-Fd` directory (parallel dump + restore `-j`); `-Ft` tar. Above a few GB use `-Fd -j N`.

```bash
pg_dump -U postgres -d app -Fc -f app.dump                    # custom dump
pg_dumpall -U postgres --globals-only > globals.sql           # roles/tablespaces (dump misses these)
pg_restore -U postgres -d app_restore -j 4 app.dump           # parallel restore
pg_restore -l app.dump                                        # list contents
pg_restore -U postgres -d db -t orders app.dump               # one table
```

```sql
SELECT * FROM pg_stat_archiver;             -- archive health
SELECT pg_current_wal_lsn();
SELECT pg_create_restore_point('before_migration');  -- mark BEFORE the disaster
```

PITR shape:
1. `wal_level=replica`, `archive_mode=on`, `archive_command='test ! -f /archive/%f && cp %p /archive/%f'`.
2. `pg_basebackup -D /backup/base -X stream -P -c fast`.
3. Continuously archive WAL.
4. Restore base, set `restore_command` + `recovery_target_time`/`_lsn`/`_xid`/`_name`, `touch recovery.signal`, start. It replays to the target then pauses; `pg_promote()` to make writable.

RPO (data you accept losing) vs RTO (time to recover): sync replication RPO=0; hot-standby promote RTO seconds; PITR of a 1TB base = hours. Match strategy to budget and test the restore quarterly.

Gotchas:
- A backup you have not restored is not a backup - rehearse restores.
- `pg_dump --clean` + wrong target db wipes that db.
- `pg_dump` misses roles/globals (`pg_dumpall --globals-only`), WAL, replication slots.
- A freshly restored cluster has zero stats - run `ANALYZE` (or `vacuumdb --analyze-only -j N`) before traffic.
- RPO bounded by `archive_timeout` (default 0 = only when a segment fills - can be hours on quiet systems).
- `recovery_target_action='promote'` is irreversible - use `pause` to verify first.
- Major-version mismatch on physical restore will not start - use a logical dump for cross-version migration.
- Schedule backups on a replica to avoid primary IO load.
