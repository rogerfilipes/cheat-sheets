# Elasticsearch 8.x Cheatsheet

## Contents
- [Hierarchy: cluster/node/index/shard/segment](#hierarchy)
- [Inverted index & analyzers](#inverted-index--analyzers)
- [Mappings: dynamic vs explicit & field types](#mappings--field-types)
- [text vs keyword](#text-vs-keyword)
- [Query DSL: leaf vs compound](#query-dsl-leaf-vs-compound)
- [Query vs filter context & caches](#query-vs-filter-context--caches)
- [BM25 scoring & relevance tuning](#bm25-scoring--relevance-tuning)
- [Aggregations: bucket/metric/pipeline](#aggregations)
- [Write path: refresh/flush/merge](#write-path-refresh-flush-merge)
- [ILM & rollover](#ilm--rollover)
- [Indexing: bulk API & backpressure](#bulk-api--backpressure)
- [Deep pagination](#deep-pagination)
- [Java client 8.x](#java-client-8x)
- [Production: sizing, heap, tiers](#production-sizing-heap-tiers)
- [Vector search & kNN](#vector-search--knn)

## Hierarchy

What it is: five nested layers; everything else is about one of them.

| Layer | What it is | Key fact |
|---|---|---|
| Cluster | Set of JVM processes sharing state via Raft-like protocol (7.x+) | One name; nodes join/leave |
| Node | One JVM process with roles | `master`, `data` (`data_hot/warm/cold/frozen`), `ingest`, `coordinating_only`, `ml`, `transform` |
| Index | Logical namespace you read/write | Split into shards at creation |
| Shard | A standalone Apache Lucene index | Primary count fixed at creation; replicas changeable |
| Segment | An immutable Lucene file on disk | Never modified; new data goes to a new segment |

Routing (picks the shard for a doc):
```text
shard = hash(_routing) % number_of_primary_shards
```
`_routing` defaults to `_id`. Primary count is in the formula -> immutable post-create (change via reindex / `_split` / `_shrink`).

Cluster health:

| Color | Meaning | Safe to write? |
|---|---|---|
| Green | All primaries + replicas allocated | Yes |
| Yellow | All primaries allocated, some replicas missing (normal single-node) | Yes, but no redundancy |
| Red | At least one primary unassigned | No, for that shard |

REST:
```text
GET  /_cluster/health?pretty
GET  /_cat/nodes?v&h=name,role,heap.percent,cpu,load_1m
GET  /_cat/shards/<index>?v
GET  /<index>/_segments?pretty
PUT  /<index>   { settings: { number_of_shards, number_of_replicas } }
```

Gotchas:
- Oversharding is the #1 sin: thousands of shards crush the master.
- `number_of_shards` immutable post-create.
- One unassigned primary turns cluster red; no auto-heal at flood-stage disk (95%) -> indices go read-only.
- Per-shard IDF: tiny per-shard doc counts make scores noisy. Use `?search_type=dfs_query_then_fetch` at latency cost.
- Run 3 dedicated master nodes (odd, for voting quorum) to avoid split-brain.

## Inverted index & analyzers

What it is: inverted index maps term -> postings list (docs containing it); analyzers decide what terms get stored.

- Lucene term dictionary stored as an FST (compressed trie), `O(log N)` lookup.
- Postings carry tf, positions, offsets (positions enable phrase queries).
- The contract: same analyzer must run on both index and query sides, or zero hits and no error.

Analyzer pipeline (3 stages, in order):

| Stage | Does | Examples |
|---|---|---|
| Char filters | Edit raw chars before split | `html_strip`, `mapping` |
| Tokenizer | Split into tokens (exactly one) | `standard`, `whitespace`, `ngram` |
| Token filters | Transform/add/drop tokens, ordered | `lowercase`, `asciifolding`, `stop`, `synonym`, `stemmer` |

```text
raw text -> [char filters] -> [tokenizer] -> [token filters] -> terms (stored)
'The Cafe-Latte!'                                              ['cafe','latte']
```

Built-in analyzers:

| Analyzer | Behavior | Use when |
|---|---|---|
| `standard` | Unicode split + lowercase. Default | General text |
| `simple` | Lowercase, split on non-letter | Quick text |
| `whitespace` | Split on whitespace, no lowercase | Pre-tokenized input |
| `stop` | `simple` + removes stopwords | Prose noise words |
| `keyword` | No-op, whole input is one token | IDs, tags, exact match |
| `pattern` | Split on regex | Custom delimiters |
| language (`english`, `portuguese`...) | Stopwords + stemming | Known-language text |

Lucene sibling structures (one `text` field can touch up to 4):
- Inverted index: search.
- doc_values: columnar; sort/agg/scripting. `text` has none (add a `keyword` sub-field).
- Stored fields (`_source`): raw values returned, not searched.
- norms: per-field length factor for scoring; set `norms: false` on filter-only fields.

REST:
```text
POST /_analyze   { analyzer | tokenizer+filter, text }
POST /<index>/_analyze   { field, text }
```

Gotchas:
- Mismatched index vs search analyzer -> zero results, no error (classic: synonym filter on one side only).
- `keyword` is not analyzed; `"Hello World"` is one token.
- `standard` splits `"O'Brien"` into `["o","brien"]`.
- Language stemmers are aggressive (collapse `organize`/`organization`): great recall, poor precision.
- Reindex is the only way to change an analyzer on existing data.
- N-gram/edge-n-gram explode index size.
- Prefer query-time synonyms (`synonym_graph` in `search_analyzer`) when the set churns (no reindex).

## Mappings & field types

What it is: a mapping is the index schema (fields + types + dynamic mode).

Field types:

| Type | Use for | Notes |
|---|---|---|
| `text` | Full-text | Analyzed; no doc_values -> can't sort/agg |
| `keyword` | Exact match, agg, sort | Whole value one token |
| `long`/`integer`/`short`/`byte` | Integers | Smallest that fits |
| `double`/`float`/`half_float`/`scaled_float` | Floats | `scaled_float` (factor 100) for money |
| `date` | Dates | Default `strict_date_optional_time||epoch_millis` |
| `boolean` | Bool | Stored 0/1 |
| `ip` | IPv4/IPv6 | CIDR range queries |
| `geo_point`/`geo_shape` | Geo | Distance sort, polygon search |
| `nested` | Array of objects | Each sub-doc is a hidden child doc |
| `object` | JSON object | Flattened by default |
| `flattened` | Unknown/many keys | Whole subtree one field; avoids mapping explosion |
| `dense_vector` | kNN/semantic | See kNN section |
| `join` | Parent/child | Shard-locked; usually avoid |

Three dynamic modes:

| Mode | Behavior |
|---|---|
| `dynamic: true` (default) | Auto-create field, guess type. Convenient and dangerous |
| `dynamic: strict` | Reject doc with `strict_dynamic_mapping_exception`. Production default |
| `dynamic: false` | Store in `_source` but do not index -> not searchable |

Shaping one value:
- Multi-fields: one source value indexed several ways.
```json
"name": { "type": "text", "fields": { "raw": { "type": "keyword" }, "ngram": { "type": "text", "analyzer": "edge_ngram_a" } } }
```
- `copy_to`: copy several fields into one catch-all `text` field (replaces removed `_all`).
- Runtime fields: computed at search time via Painless; not stored. Costs CPU per matching doc on every query that touches them.

REST:
```text
PUT  /<index>   { mappings: { dynamic, properties } }
PUT  /<index>/_mapping   { runtime: { ... } }
```

Gotchas:
- Mapping explosion: values-as-keys (`{ "user_123": ... }`) creates one field per key. `index.mapping.total_fields.limit` defaults to 1000; use `flattened`.
- Dynamic guessing wrong: `"01234"` -> long; first doc wins the type.
- Mappings mostly immutable: can add fields, cannot change a type -> reindex (runtime field is the no-downtime stopgap).
- `text` has no doc_values -> sort/agg fails or OOMs; use `.keyword`.
- `_source: true` doubles storage but is required for update/reindex/highlight.
- Date trap: `2024-05-25T10:00` parses, `2024-05-25 10:00` (space) does not.
- `nested` multiplies Lucene doc count; `index.mapping.nested_objects.limit` default 10000/parent.
- `geo_point` expects `{lat, lon}` or `[lon, lat]` (GeoJSON: longitude first).
- Use index + component templates (8.x) for `logs-*` patterns; version like migrations.

## text vs keyword

What it is: the binary string-field choice that causes the most outages.

| Aspect | `text` | `keyword` |
|---|---|---|
| Analyzed? | Yes, shredded to terms | No, stored whole (one token) |
| Default analyzer | `standard` | none (optional normalizer) |
| doc_values (sort/agg) | No (needs `fielddata: true`) | Yes, by default |
| Relevance scoring | Yes (BM25) | No (exact only) |
| Case | Lowercased by analyzer | Case-sensitive unless normalizer |
| Query to use | `match` (analyzed) | `term` (exact) |
| Good for | descriptions, titles, search boxes | IDs, SKUs, enums, tags, codes, paths |

When each:
- `text`: anything a human types into a search box; relevance matters.
- `keyword`: values you match exactly, sort, or aggregate; users will not typo them. Max 32766 bytes; cap with `ignore_above`.

Why `term` on `text` returns nothing: `text` was shredded at index time; `term` looks for the literal untouched string, which was never stored. Use `match` on `text`, `term` on `keyword`/`.keyword`.

Case-insensitive exact match: `keyword` + normalizer (char filters + subset of token filters like `lowercase`/`asciifolding`; no tokenizer, no stemmer; runs both index and query time).
```json
"email": { "type": "keyword", "normalizer": "lowercase_norm" }
```

Default dynamic string mapping = `text` + a `.keyword` sub-field (`ignore_above: 256`).

Gotchas:
- `term` on `text`: the #1 "why no hits" bug.
- Aggregating on `text` without doc_values fails; `fielddata: true` loads all terms on heap -> OOM. Add `.keyword` instead.
- `keyword` is case-sensitive without a normalizer (`"USA"` != `"usa"`).
- `ignore_above: 256` silently drops over-long values for indexing (still in `_source`).
- Sorting raw `text` -> `Fielddata is disabled on [field]`.

## Query DSL leaf vs compound

What it is: a search is a tree; leaf queries match one field, compound queries combine other queries.

Leaf queries:

| Family | Examples | On | Behavior |
|---|---|---|---|
| Term-level | `term`, `terms`, `range`, `exists`, `prefix`, `wildcard`, `regexp`, `fuzzy`, `ids` | `keyword`/numeric/date/bool | Exact, un-analyzed (byte-for-byte) |
| Full-text | `match`, `match_phrase`, `multi_match`, `query_string`, `simple_query_string` | `text` | Analyzes query with field analyzer, then matches |
| Geo/specialized | `geo_distance`, `geo_bounding_box`, `nested`, `has_child`/`has_parent`, `more_like_this` | special | Spatial/relational/similarity |

Full-text behavior:
- `match`: analyzes query, builds a `bool` of terms. Default `operator: or`.
- `match_phrase`: terms in order, `slop` words of wiggle (`slop: 0` = strict adjacency).
- `multi_match`: several fields; `type` = `best_fields` / `most_fields` / `cross_fields` / `phrase`.

Compound queries:

`bool` clauses:

| Clause | Must match? | Scores? | Cached? | Meaning |
|---|---|---|---|---|
| `must` | Yes | Yes | No | AND, relevance counts |
| `should` | See note | Yes | No | boosts score when matched |
| `filter` | Yes | No | Yes | required, keep-or-drop |
| `must_not` | Must NOT | No | Yes | exclude |

`should` note: with no `must`/`filter`, at least one `should` must match; with a `must`/`filter`, `should` is optional score boost. Set `minimum_should_match` explicitly.

Other compound:
- `dis_max`: matches any sub-query, takes the single best score; smooth with `tie_breaker`. Powers `multi_match: best_fields`.
- `function_score` / `script_score` (preferred, newer): custom score math; heavy.
- `constant_score`: wraps a filter, every match scores 1.0.
- `boosting`: `positive` + `negative` to demote (not exclude).

```json
{ "query": { "bool": {
  "must":   [ { "match": { "name": "whey protein chocolate" } } ],
  "filter": [
    { "term":  { "category": "supplements" } },
    { "range": { "price_cents": { "gte": 1000, "lte": 5000 } } },
    { "term":  { "in_stock": true } }
  ],
  "must_not": [ { "term": { "discontinued": true } } ],
  "should":  [ { "term": { "brand": "acme" } } ],
  "minimum_should_match": 0
} } }
```

Gotchas:
- `must` for predicates pollutes scoring and prevents filter-cache reuse.
- `query_string` exposes raw Lucene syntax to user input; use `simple_query_string`.
- Leading `*foo`/`.*foo` wildcards/regex are slow (no term-dict use); prefer `prefix`, edge-ngram, or completion suggester for autocomplete.
- `fuzzy` can explode; clamp `max_expansions` and `prefix_length`.
- `must_not` is filter context (no scoring); putting it under `must` misbehaves.
- Don't over-nest `bool` (10+ levels); use `profile: true`.

## Query vs filter context & caches

What it is: every clause scores (query context) or keeps/drops (filter context); the choice drives caching.

| Context | Question | Scores | Cached |
|---|---|---|---|
| Query (`must`/`should`) | "How relevant?" | Yes | No |
| Filter (`filter`/`must_not`) | "In or out?" | No | Yes (bitset) |

Rule: any true/false predicate (range, status, country, in-stock) goes in `filter` (cheaper, cacheable). `must_not`/`filter` are always filter context regardless of placement.

Three caches:

| Cache | Scope | Caches | Invalidation |
|---|---|---|---|
| Node query cache (filter cache) | Per-node LRU | Filter results as bitsets per segment | Per-segment; merge evicts |
| Shard request cache | Per-shard | Whole top-N response | On refresh; only `size: 0` by default |
| Field data cache | Per-node | `text` fielddata for sort/agg | Manual / memory pressure |

- Node query cache: skips segments < 10k docs and the single largest segment; kicks in for frequently-used filters.
- Shard request cache: only `size: 0` (aggs/counts) by default; enable for hits with `?request_cache=true` + `index.requests.cache.enable: true` (rare).

REST:
```text
GET  /_nodes/stats/indices/query_cache?pretty
GET  /_nodes/stats/indices/request_cache?pretty
GET  /<index>/_search?request_cache=true&size=0
```

Gotchas:
- Unrounded `now` (`gte: "now-1h"`) changes the cache key every second -> 0% hit rate. Round: `now/d`, `now/h`.
- Other cache busters: random `script_score`, per-request `preference`.
- Heavy ingestion: segment merges evict per-segment entries ("cache cliff").
- `terms` with `_terms_lookup` is not cached.
- `script` filter not cached unless it uses `params`.

## BM25 scoring & relevance tuning

What it is: default similarity since Lucene 6 / ES 6 (replaced TF-IDF).

```text
score(d,t) = IDF(t) * ( tf(t,d) * (k1 + 1) ) / ( tf(t,d) + k1 * (1 - b + b * |d|/avgdl) )
```

| Symbol | Plain meaning | Detail |
|---|---|---|
| IDF(t) | How special is the word | `log(1 + (N - n + 0.5)/(n + 0.5))`; rare terms score higher |
| tf(t,d) | How often in this doc | Raw count |
| `k1` (default 1.2) | TF saturation knob | Higher = TF keeps mattering; lower = saturates fast |
| `b` (default 0.75) | Length knob | `b=0` ignores length; `b=1` fully penalizes long docs |
| `\|d\|/avgdl` | Is this doc long | Field length over average |

- Total `_score` = sum of per-term contributions. `filter`/`must_not` contribute nothing (so `_score: 0.0` on a match usually = filter context).
- IDF (`N`, `n`) counted per shard -> uneven shards = noisy scores. Fix with `?search_type=dfs_query_then_fetch` (latency cost) or bigger/fewer shards.
- Score scales are not comparable across queries/signals; only relative order within one query is meaningful (why RRF fuses by rank).

Relevance tuning ladder (cheapest first):

| # | Lever | When |
|---|---|---|
| 1 | Field boosts (`name^3, body^1`) | First resort; free, ~80% of cases |
| 2 | `should` clauses | Soft additive signals |
| 3 | Rank feature fields (`rank_feature`/`rank_features`) | popularity/pagerank/CTR; log-saturating |
| 4 | `script_score` | Custom math; runs per matching doc, expensive |
| 5 | Rescore phase | Expensive query only on top-N |
| 6 | RRF (8.x) | Combine BM25 + kNN by rank |
| 7 | Learn-to-rank | external model |

REST:
```text
GET  /<index>/_search?explain=true   (per-doc, per-term score breakdown)
```
```json
"rescore": { "window_size": 200, "query": {
  "rescore_query": { "script_score": { "query": {"match_all":{}}, "script": {"source":"Math.log(2 + doc['likes'].value)"} } },
  "query_weight": 0.7, "rescore_query_weight": 0.3 } }
```

Gotchas:
- `b=0.75` penalizes long fields; long blogs can outrank short FAQs (raise `b`, or boost the title field).
- `script_score` runs per matching doc before sort -> dominates latency on big hit counts; use rescore.
- Over-boosting kills the original relevance signal.
- `constant_score` on free-text throws away relevance.
- Synonym expansion inflates TF (double-counting).
- Custom per-field `similarity` rarely worth it; tune `k1`/`b` only after measuring.

## Aggregations

What it is: bucket = group, metric = measure, pipeline = post-process. They nest.

| Family | What | Members |
|---|---|---|
| Bucket | Group docs (can nest) | `terms`, `date_histogram`, `histogram`, `range`, `filters`, `nested`, `composite`, `geo_grid` |
| Metric | Stat over docs in a bucket | `sum`, `avg`, `min`, `max`, `stats`, `extended_stats`, `cardinality`, `percentiles`, `top_hits` |
| Pipeline | Post-process other aggs' output | `bucket_script`, `bucket_selector`, `moving_avg`/`moving_fn`, `derivative`, `cumulative_sum` |

- Metric aggs read docs; pipeline aggs read other aggregations' results (so they never reduce scan cost).
- Aggs run on doc_values, not raw JSON. `text` has none -> agg fails; use `.keyword` (not `fielddata`).

Approximate aggs:
- `terms`: top-N per shard then merged -> counts can be inaccurate; ES reports `doc_count_error_upper_bound`. Raise `shard_size` (default `size * 1.5 + 10`); use `composite` for full enumeration.
- `cardinality`: HyperLogLog++, always approximate (~1% error at default `precision_threshold` 3000).
- `percentiles`: t-digest, approximate; tune via `compression`.

Time/pagination:
- `date_histogram`: prefer `fixed_interval: "1d"` (true duration) or `calendar_interval: "month"` (calendar-aware). Avoid `auto_date_histogram` (variable interval wrecks caching).
- `composite`: the only agg with deterministic pagination; returns `after_key`, feed back as `after`. Use for ETL out of ES.

```json
{ "size": 0,
  "aggs": { "by_country": {
    "terms": { "field": "country", "size": 20 },
    "aggs": { "monthly": {
      "date_histogram": { "field": "created_at", "calendar_interval": "month", "min_doc_count": 0 },
      "aggs": { "revenue": { "sum": { "field": "total_cents" } } } } } } } }
```

Gotchas:
- Bucket explosion: `terms` with `size: 100000` on high-cardinality -> OOM. Use `composite`.
- `cardinality` always approximate; finance needs different tech.
- `top_hits` inside a bucket loads source per bucket -> heap pressure.
- Nesting multiplies cost; 3+ levels time out.
- `min_doc_count: 0` fills empty time buckets (expensive on long ranges).
- Not transactionally consistent with concurrent writes.
- Set `size: 0` when you only want aggs.

## Write path: refresh flush merge

What it is: indexed, searchable, and durable are three separate moments.

| Stage | Promise | When |
|---|---|---|
| Indexed | In buffer + translog; GET-by-id works; not searchable | On acknowledged write |
| Refreshed | New segment exists -> searchable; not yet durable | Every `refresh_interval` (default 1s) |
| Flushed | Segments fsynced, translog cleared -> durable | Translog hits `flush_threshold_size` (512MB) or `_flush` |

```text
PUT /_doc/123
   |
   v
[ INDEXED ]   -- GET by id works -------- search? NO
   |  refresh (~1s)
   v
[ REFRESHED ] -- search works ----------- crash-safe? NOT YET
   |  flush (threshold / interval)
   v
[ FLUSHED ]   -- on disk, translog clear - fully durable
```

Mechanics:
- Refresh: makes docs searchable (new segment from buffer); why ES is "near real-time". Too-frequent refresh drowns the cluster in tiny segments.
- Translog: append-only crash-recovery log. Default `durability: request` (fsync every write, safe, slower); `async` fsyncs every `sync_interval` (default 5s, can lose seconds of acked writes).
- Flush: persists buffer + fsyncs segments + clears translog. flush ~= Lucene commit + translog truncation.
- Merge: background, `TieredMergePolicy` combines small segments. How deletes reclaim space (tombstone -> bytes gone on merge). Concurrency bounded by `indices.merge.scheduler.max_thread_count` (default `min(4, processors/2)`).

Per-request refresh:

| Param | Behavior | When |
|---|---|---|
| `?refresh=false` (default) | Visible after next scheduled refresh | Normal writes |
| `?refresh=wait_for` | Block until next refresh; no extra refresh | Read-your-write without hurting cluster |
| `?refresh=true` | Force refresh now | Tests only (segment per request) |

REST:
```text
POST /<index>/_forcemerge?max_num_segments=1   (read-only indices only)
POST /<index>/_flush
GET  /_nodes/stats/indices/merges?pretty
```

Bulk-load tuning: set `refresh_interval: -1` and `number_of_replicas: 0`, load, then restore both and `_forcemerge` (only on now read-only index).

Gotchas:
- `?refresh=true` in production write code explodes segment count.
- Read-your-write surprise: GET-by-id immediate, search waits for refresh.
- Force merge on a live index starves writes; a merge to 1 segment on a multi-GB shard runs tens of minutes with no clean cancel.
- `_delete_by_query` does not shrink disk until merge runs.
- Idle-shard refresh: `index.search.idle.after` (default 30s) stops refreshing; first query after idle waits up to `refresh_interval`.

## ILM & rollover

What it is: auto-rotate and age out high-volume append-mostly data (logs/metrics/events).

Phases:

| Phase | Plain | What happens | Cost |
|---|---|---|---|
| Hot | Active writes | All writes; rollover lives here; SSD, full replicas | Fast, expensive |
| Warm | Done writing, still read | `shrink`, `forcemerge`, warm nodes, fewer replicas | Slower, cheaper |
| Cold | Rare reads | `searchable_snapshot`, minimal compute | Slow, cheap |
| Frozen | Archive | Partial mount from object storage (S3) | Slowest, cheapest |
| Delete | Shredder | `DELETE`s the index | Gone |

Rollover (creates a new write index when a threshold trips, hot phase only): `max_age` (`7d`), `max_size`/`max_primary_shard_size` (`50gb`), `max_docs`. Only fires for writes via the write alias (data streams handle this).

Timing: ILM is a scheduler polling every 10 min (`indices.lifecycle.poll_interval`). `min_age` measured from rollover (not creation) for rolled indices; `max_age` is the hot rollover trigger.

Data streams (8.x): write to one name (`logs-app-prod`); ES manages backing indices (`.ds-...-000001` -> `-000002`) and swaps write target on rollover; reads span all; append-only by design.

Four pieces that wire it together:
1. ILM policy (phases + actions)
2. Component template(s) (reusable settings/mappings)
3. Index template (matches `logs-app-*`, `composed_of` components, declares `data_stream: {}`, links `index.lifecycle.name`)
4. Data stream (created on first write)

REST:
```text
PUT  /_ilm/policy/<name>   { policy: { phases: {...} } }
PUT  /_component_template/<name>
PUT  /_index_template/<name>   { index_patterns, data_stream:{}, composed_of, priority }
POST /<stream>/_rollover
GET  /<index>/_ilm/explain?pretty   (step_info)
POST /<index>/_ilm/retry
GET  /_ilm/status
```

Gotchas:
- `forcemerge` only safe on read-only indices; on a hot index it can corrupt segments.
- `shrink` needs source read-only, on one node, `number_of_shards` evenly divisible by target; fails silently otherwise.
- Stuck steps (no warm nodes, missing snapshot repo, watermark) hang the index; diagnose `_ilm/explain`, recover `_ilm/retry`.
- Rollover only sees write-alias writes.
- Deleting the policy does not stop it (indices run cached version).
- Don't dashboard on frozen data (object-storage range reads are slow).
- Avoid dynamic mapping in a data stream (one bad doc poisons the schema); use `dynamic: strict`.

## Bulk API & backpressure

What it is: batch writes (bulk), slow down when rejected (backpressure), check each item (partial failure).

Bulk body is NDJSON: action line + optional source line.
```text
{ "index":  { "_index": "products", "_id": "p-1" } }
{ "name": "Whey Protein", "priceCents": 2490 }
{ "delete": { "_index": "products", "_id": "p-2" } }
{ "update": { "_index": "products", "_id": "p-3" } }
{ "doc": { "priceCents": 2990 } }
```

Operations:

| Op | Does | Watch for |
|---|---|---|
| `index` | Create or replace by id; idempotent | Safe default for reprocessing |
| `create` | Insert only; fails if id exists | `version_conflict_engine_exception` on conflict |
| `update` | Partial read-modify-write in ES (optimistic concurrency) | Hot docs collide; set `retry_on_conflict: 3` |
| `upsert` | Update if exists, else insert | Update-or-create |
| `delete` | Remove by id | Action line only |

Sizing:
- Size by bytes, not doc count: 5-15 MB per request body.
- A few concurrent requests (start ~4-8); measure on real hardware.
- `wait_for_active_shards`: `"1"` (primary only, throughput) vs `"all"` (primary + replicas, durable, slower).

Partial failure: bulk returns HTTP 200 even if items failed. Check top-level `errors: true`, iterate `items`, retry only the failed ones (exponential backoff). Re-sending whole box double-indexes successes.

Backpressure: writes go through the `write` thread pool (size = processors) with a bounded queue of 10000. Full queue -> HTTP 429 ("Too Many Requests"). React with exponential backoff, not a tight retry loop. Java `BulkIngester` handles batching, concurrency, and 429 backoff for you.

REST:
```text
POST /_bulk    (Content-Type: application/x-ndjson)
```

Gotchas:
- Bulk by doc count instead of bytes.
- Treating `errors: false` as the only success check.
- Retrying the whole bulk on partial failure.
- `create` + retry on 429 -> `version_conflict_engine_exception`; use `index` unless you need create-only.
- `wait_for_active_shards: all` on a yellow cluster blocks forever; set `?timeout=`.
- Producers ignoring 429 -> queue overflow -> cluster instability.

## Deep pagination

What it is: pull large result sets without falling off the `from + size` cliff.

| Method | Limit / use |
|---|---|
| `from + size` | OK up to `index.max_result_window` (default 10000). `size: 10000` allowed; `from: 9000, size: 2000` not (crosses window) |
| `search_after` | Stateless cursor: sort by unique key (`field + _id`), pass last hit's sort values. No server state |
| Point-In-Time (PIT) | Frozen snapshot + `search_after`; consistent results. Recommended in 8.x. Sort by `_shard_doc` (cheapest stable cursor) |
| Scroll | Deprecated for user-facing paging; pins resources, leaked IDs OOM the node. Migrate to PIT |

REST:
```text
POST   /<index>/_pit?keep_alive=5m
GET    /_search   { pit: {id, keep_alive}, sort: [{"_shard_doc":"asc"}], search_after: [...] }
DELETE /_pit   { id }
```

Gotcha: close the PIT when done; `keep_alive` is only a safety net. `_doc` is the deprecated older sort vs `_shard_doc`.

## Java client 8.x

What it is: `co.elastic.clients:elasticsearch-java` (codegen'd from the API spec, type-safe). Replaces deprecated High-Level REST Client (`RestHighLevelClient`, removed in 8.x for Java 9+).

Layers:

| Layer | Job |
|---|---|
| `ElasticsearchClient` | Sync typed front door; every API hangs off it |
| `ElasticsearchAsyncClient` | Same API, returns `CompletableFuture` |
| `ElasticsearchTransport` | Carries request/response; default `RestClientTransport` |
| `JsonpMapper` (`JacksonJsonpMapper`) | Java <-> JSON; plug in your own `ObjectMapper` |
| low-level `RestClient` | HTTP plumbing (pool, timeouts) on Apache HttpClient |

Lambda DSL: each builder method takes a `Function<Builder, ObjectBuilder<T>>`; second type param is the deserialization target (record/DTO preferred, or `Map<String,Object>`, or `JsonData`).
```java
SearchResponse<Product> r = es.search(s -> s
    .index("products")
    .query(q -> q.bool(b -> b
        .must(m -> m.match(mm -> mm.field("name").query("whey")))
        .filter(f -> f.range(rg -> rg.field("priceCents").gte(JsonData.of(1000))))))
    .size(20), Product.class);
```

Bulk per-item error handling:
```java
BulkResponse br = es.bulk(b -> b.operations(ops));
if (br.errors()) {
    br.items().stream().filter(it -> it.error() != null)
        .forEach(it -> System.err.println(it.id() + " " + it.error().reason()));
}
```

Gotchas:
- One application-scoped client (thread-safe, owns the pool); never per request.
- Set explicit `connectTimeout`/`socketTimeout` (defaults unbounded -> hung threads).
- Plug your own `ObjectMapper` (register `JavaTimeModule`, disable `WRITE_DATES_AS_TIMESTAMPS`); else `Instant` serializes as `[2024,5,25,...]` and clashes with `date` mapping.
- Pooling/timeouts live on the low-level `RestClient`, not the typed client.
- `transport.close()` at shutdown or the pool leaks.
- JSON-P binding: Spring Boot 2 (`javax.json`) vs 3 (`jakarta.json`) classpath clashes.
- `BulkResponse.errors: true` does not fail the request; inspect per-item.
- Default `RestClient` retries on connection failure, not 5xx.
- `size()` default 10. Prefer `BulkIngester` for background loads.

## Production: sizing, heap, tiers

What it is: shard sizing, JVM heap split, tiering, and operations.

Shard sizing:
- Target 10-50 GB/shard (search), up to 200 GB for logs/append-only.
- Below 10 GB: paying fixed per-shard overhead. Above 50 GB: slow recovery on node death.
- Heap-to-shard: `total shards on a node <= 20 * heap_GB` (16 GB heap -> ~320 shards).
- One shard answers one query at a time.

JVM heap:
- Heap = max 50% of RAM, never above ~31 GB (leaves the rest for OS file cache).
- Below 31 GB: compressed oops (4-byte pointers); cross it and the JVM uses 8-byte pointers -> fewer effective objects (a 32 GB heap holds less than a 30 GB one).
- File cache matters: Lucene reads are mmap'd.
- Set min = max: `ES_JAVA_OPTS="-Xms16g -Xmx16g"`.

Node roles:

| Role | Job | Notes |
|---|---|---|
| Master-eligible | Cluster state, leader election | 3 dedicated, odd, voting quorum |
| Data | Stores shards | `data_hot/warm/cold/frozen` |
| Ingest | Pipeline processors | Co-locate or dedicate |
| Coordinating-only | Routes + reduce phase | Offload busy clusters |
| ML | ML jobs | Dedicate when in use |

Tiers:

| Tier | Storage | Replicas | Age | Compute |
|---|---|---|---|---|
| Hot | SSD, big heap | normal | today | writes + recent reads |
| Warm | HDD OK | fewer | weeks | read-only |
| Cold | searchable snapshot (S3) | - | older | minimal |
| Frozen | partial mount, object storage | - | months/years | slow, very cheap |

Route via ILM `allocate` + node attributes (`node.attr.data: warm` + `allocate { include: { data: warm } }`).

Capacity math (100 GB/day raw, 90d, 1 replica):
```text
  100 GB/day raw
x 1.2  (indexing overhead)   = ~120 GB/day
x 2    (1 replica)           = ~240 GB/day
x 90   (retention)           = ~21 TB
+ 25%  (watermark headroom)  = ~27 TB total
```

Disk watermarks:

| Stage | % | Effect |
|---|---|---|
| low | 85 | no new shards on this node |
| high | 90 | existing shards relocated off |
| flood_stage | 95 | indices go read-only; recover with `DELETE _all/_settings { "index.blocks.read_only_allow_delete": null }` |

Operations:
- Observe: `_cluster/stats`, `_cluster/health`, `_nodes/stats`, `_nodes/hot_threads`, slow logs (alert on them).
- Snapshots: incremental at the segment level; restore is not transactional (plan rename strategy); searchable snapshots keep cold/frozen queryable.
- Rolling restart: disable allocation (`cluster.routing.allocation.enable=primaries`), stop/upgrade/start one node, wait for green, re-enable allocation when done.

Gotchas:
- Oversharding crushes the master.
- Heap > 31 GB loses compressed oops; two smaller nodes beat one huge node.
- `bootstrap.memory_lock: true` without OS memlock -> ES won't start.
- `vm.max_map_count` too low (Linux) -> mmap failures; set 262144+.
- File descriptor limit too low -> "too many open files".
- Mixing JVM versions across nodes -> undefined behavior.
- CCR is enterprise-licensed; remote reindex is the OSS substitute.

## Vector search & kNN

What it is: semantic search by meaning. Embedding model maps text -> coordinates; kNN finds nearest neighbors.

`dense_vector` mapping:
```json
"embedding": {
  "type": "dense_vector", "dims": 384, "index": true, "similarity": "cosine",
  "index_options": { "type": "int8_hnsw", "m": 16, "ef_construction": 100 }
}
```
- `dims`: coordinate count; must match the model exactly (MiniLM 384, many OpenAI 1536).
- `similarity`: `cosine` (safe default), `dot_product` (faster, requires unit-normalized vectors), `l2_norm`, `max_inner_product`.
- `index: true` builds the HNSW graph (required for `knn`); `index: false` only allows brute-force `script_score`.

Index options (size vs accuracy):

| Type | Storage | Notes |
|---|---|---|
| `hnsw` | float32 full precision | Most accurate, largest |
| `int8_hnsw` | int8, ~4x smaller | Similar recall; 8.x default in newer minors |
| `int4_hnsw` | smaller (8.14+) | Smallest, more recall loss |

- `m` (default 16): connections per point; higher = better recall, bigger index.
- `ef_construction` (default 100): build effort; higher = better recall, slower indexing.

HNSW = approximate (recall < 100%); the trade for sub-linear search. Tune `m`/`ef_construction`/`num_candidates` for target recall.

kNN query:
```json
{ "knn": { "field": "embedding", "query_vector": [...], "k": 10, "num_candidates": 100,
           "filter": { "term": { "category": "supplements" } } } }
```
- `k`: results wanted. `num_candidates`: per-shard exploration budget (start 5-10x `k`). Runs per shard, then merged.

Filtered kNN:
- Pre-filter (8.x default): filter applied during graph traversal; still get K good results.
- Post-filter (trap): K nearest then discard -> recall collapses.

Hybrid with RRF (combines ranks, not incompatible scores):
```text
rrf(d) = sum over rankers r of  1 / (k + rank_r(d))   (default k = 60)
```
```json
{ "retriever": { "rrf": {
  "retrievers": [
    { "standard": { "query": { "match": { "name": "whey protein chocolate" } } } },
    { "knn": { "field": "embedding", "query_vector": [...], "k": 50, "num_candidates": 200 } }
  ],
  "rank_window_size": 50, "rank_constant": 60 } } }
```

Gotchas:
- `dims` mismatch at indexing -> `illegal_argument_exception`.
- `dot_product` on un-normalized vectors -> garbage rankings (L2-normalize).
- `dims`, similarity, and quantization are effectively immutable -> model/metric change = full reindex.
- HNSW costs heap and disk significantly; high-dim vectors (768/1024) drop indexing throughput.
- Mixing `knn` and `query` in the same body (non-retriever) scores by sum (scales do not match); use RRF retriever.
- Pre-filter matching very few docs can return fewer than `k` (no backtrack).
- Embedding `NaN` -> ES rejects the doc.
- Exclude vectors from `_source`: `"_source": { "excludes": ["embedding"] }`.
