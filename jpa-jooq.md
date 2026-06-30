# JPA / Hibernate + jOOQ Cheatsheet

## Contents

Part 1: JPA / Hibernate
- [Layers: JPA vs Hibernate vs Spring Data](#layers-jpa-vs-hibernate-vs-spring-data)
- [Entity mapping and identifiers](#entity-mapping-and-identifiers)
- [Relationships and fetch types](#relationships-and-fetch-types)
- [Cascading and orphan removal](#cascading-and-orphan-removal)
- [Inheritance strategies](#inheritance-strategies)
- [N+1 problem and fixes](#n1-problem-and-fixes)
- [JPQL](#jpql)
- [Criteria API and Specifications](#criteria-api-and-specifications)
- [JPQL vs Criteria (choosing)](#jpql-vs-criteria-choosing)
- [First and second-level cache](#first-and-second-level-cache)
- [Transactional semantics](#transactional-semantics)
- [Optimistic vs pessimistic locking](#optimistic-vs-pessimistic-locking)
- [Flushing, dirty checking, detached entities](#flushing-dirty-checking-detached-entities)
- [Spring Data repositories and projections](#spring-data-repositories-and-projections)
- [Testcontainers testing (JPA)](#testcontainers-testing-jpa)

Part 2: jOOQ
- [Why jOOQ](#why-jooq)
- [Setup and codegen](#setup-and-codegen)
- [Basic CRUD with the DSL](#basic-crud-with-the-dsl)
- [Joins, subqueries, CTEs](#joins-subqueries-ctes)
- [Window functions](#window-functions)
- [Dynamic query building](#dynamic-query-building)
- [Batch operations and bulk insert](#batch-operations-and-bulk-insert)
- [Transactions](#transactions)
- [Record mapping](#record-mapping)
- [Codegen strategies](#codegen-strategies)
- [Flyway + codegen wiring](#flyway--codegen-wiring)
- [Testcontainers testing (jOOQ)](#testcontainers-testing-jooq)

Comparison
- [Same query: JPQL vs Criteria vs jOOQ](#same-query-jpql-vs-criteria-vs-jooq)
- [jOOQ vs JPA: when to pick which](#jooq-vs-jpa-when-to-pick-which)

---

# Part 1: JPA / Hibernate

## Layers: JPA vs Hibernate vs Spring Data

What it is: three stacked layers - a rulebook, an engine, and a convenience shortcut.

| Layer | Role | Artifacts / package |
|---|---|---|
| JPA (Jakarta Persistence) | the spec (interfaces + annotations, no runtime code) | `@Entity`, `EntityManager`, JPQL; `jakarta.persistence.*` |
| Hibernate | the implementation (entity -> SQL, batching, caching) | `SessionFactory`, `Session`; `org.hibernate.*` |
| Spring Data JPA | repository abstraction over EntityManager | `JpaRepository`, derived queries |

Call flow: `repo.findBySku()` -> Spring Data proxy -> `EntityManager` (JPA) -> Hibernate `Session` -> JDBC + HikariCP -> PostgreSQL.

```java
@PersistenceContext EntityManager em;           // JPA spec API
Session session = em.unwrap(Session.class);     // Hibernate-native escape hatch
```

Gotchas:
- `LazyInitializationException` is a Hibernate class, not JPA: the bug lives in fetch/transaction boundary, not the repository.
- Mixing `javax.persistence.*` and `jakarta.persistence.*` after Boot 3 upgrade: Hibernate 6 ignores the wrong-package annotations, reports "Not a managed type".
- Spring `org.springframework...Transactional` vs `jakarta.transaction.Transactional`: different rollback defaults.
- Use `org.hibernate.SQL=DEBUG` + `org.hibernate.orm.jdbc.bind=TRACE`, not `spring.jpa.show-sql=true` (no bind values).
- `ddl-auto: validate` against a Flyway schema; never `update` in a shared env.

## Entity mapping and identifiers

What it is: map a Java class to a table with correct id strategy, column types, and equals/hashCode.

ID generation strategies (PostgreSQL behaviour):

| Strategy | How id is assigned | Can batch inserts? |
|---|---|---|
| `IDENTITY` | column `GENERATED AS IDENTITY`, id known after INSERT | No - inserts row-by-row |
| `SEQUENCE` | Hibernate calls `nextval()` before INSERT | Yes - ids known up front |
| `TABLE` | emulated in a table row | Yes but slow + lock-prone; avoid |
| `AUTO` | provider choice | Hibernate 6 on Postgres -> SEQUENCE per entity |

```java
@Id
@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "prod_seq")
@SequenceGenerator(name = "prod_seq", sequenceName = "prod_seq", allocationSize = 50)
private Long id;

@Enumerated(EnumType.STRING) @Column(length = 16)  // never ORDINAL
private ProductStatus status;
@Column(precision = 12, scale = 2)                  // money: BigDecimal + NUMERIC
private BigDecimal price;
@Column(updatable = false)                          // Instant -> timestamptz
private Instant createdAt;
@Version private long version;

@JdbcTypeCode(SqlTypes.JSON) @Column(columnDefinition = "jsonb")  // native jsonb, no extra lib
private Map<String, Object> attributes;
```

equals/hashCode (the classic trap): never base hashCode on the generated id - it changes on persist and breaks Set membership.

```java
@Override public boolean equals(Object o) {            // by business key, never id
    return o instanceof Sku other && code != null && code.equals(other.code);
}
@Override public int hashCode() { return code == null ? 0 : code.hashCode(); }
```

Column type rules: money -> `BigDecimal` + `NUMERIC(p,s)` (never float/double); instants -> `Instant`/`OffsetDateTime` + `timestamptz` (not `LocalDateTime`); enums -> `@Enumerated(STRING)`; long text -> `text` (no `@Lob`). UUID keys: 16-byte index vs 8 for BIGINT; prefer time-ordered UUIDv7 at high write volume.

Gotchas:
- `IDENTITY` + bulk insert: no JDBC batching, throughput collapses; switch to `SEQUENCE`.
- `allocationSize` (default 50) must match the SQL sequence `INCREMENT BY`, or ids jump in big gaps.
- Enum stored as ORDINAL: reorder enum -> silently corrupt all rows.
- Composite keys (`@EmbeddedId`/`@IdClass`) are usually a smell; prefer surrogate id + UNIQUE on natural key.

## Relationships and fetch types

What it is: a Java reference Hibernate keeps in sync with a foreign key. Two decisions: which side owns the FK, and when the other end loads.

Owning side rule: the "many" side owns. `@ManyToOne` is always the owner; `@OneToMany` is always inverse (`mappedBy`).

| Annotation | Sentence | FK lives | SQL shape |
|---|---|---|---|
| `@ManyToOne` | many lines -> one product | this (many) side | `order_line.product_id` |
| `@OneToMany` | one order -> many lines | other side (`mappedBy`) | reads `order_line.order_id` |
| `@OneToOne` | one user -> one profile | one side (FK) or shared PK | `user_profile.user_id` UNIQUE |
| `@ManyToMany` | many <-> many | a join table | `student_course(student_id, course_id)` |

Fetch defaults:

| Annotation | Default fetch | Action |
|---|---|---|
| `@ManyToOne`, `@OneToOne` | EAGER | override to `LAZY` |
| `@OneToMany`, `@ManyToMany` | LAZY | leave it |

```java
@ManyToOne(fetch = FetchType.LAZY, optional = false)   // always LAZY
@JoinColumn(name = "order_id", nullable = false)
private Order order;

@OneToMany(mappedBy = "order")                         // always mappedBy
private List<OrderLine> lines = new ArrayList<>();

public void addLine(OrderLine line) {                  // maintain BOTH sides
    lines.add(line);          // inverse (in-memory)
    line.setOrder(this);      // OWNING side - this fills order_id
}
```

Gotchas:
- Default-EAGER `@ManyToOne`: loading a list fires one query per parent (an N+1).
- `@OneToMany` without `mappedBy`: Hibernate invents a phantom join table + extra insert per row.
- Bidirectional sync forgotten (`getLines().add()` only): FK is null on insert -> not-null violation.
- Inverse-side `@OneToOne` LAZY is silently fetched eagerly; fix with `@MapsId` (shared PK) or `optional=false`.
- `Set` `@OneToMany` triggers a full collection load on `add()` to check uniqueness; `List` is cheaper.
- Plain `@ManyToMany` has no room for link columns and `CascadeType.REMOVE` deletes referenced entities; promote to a join entity with two `@ManyToOne` sides + `UNIQUE(a_id, b_id)`.
- `JOIN FETCH` on two `List` collections: `MultipleBagFetchException`.

## Cascading and orphan removal

What it is: control when child entities persist/delete with the parent.

```java
@OneToMany(mappedBy = "invoice",
           cascade = {CascadeType.PERSIST, CascadeType.MERGE},
           orphanRemoval = true)
private List<InvoiceLine> lines = new ArrayList<>();
// CascadeType values: PERSIST, MERGE, REMOVE, REFRESH, DETACH, ALL
```

| Cascade | Trigger (op on parent) | Effect on children |
|---|---|---|
| `PERSIST` | save/persist new parent | new children inserted |
| `MERGE` | merge detached parent | children merged |
| `REMOVE` | delete parent | children deleted (danger if shared) |

- `orphanRemoval = true` fires when a child is disassociated (removed from collection / back-ref nulled), even if parent lives. `CascadeType.REMOVE` fires only on parent delete.
- `em.merge(detached)` returns a managed copy; the input stays detached - use the return value.
- JPA cascade is app-level row-by-row; SQL `ON DELETE CASCADE` is DB-level set-based. For 10k+ rows use bulk `@Modifying delete` (no per-row callbacks).

Gotchas:
- `CascadeType.ALL` everywhere: someone adds a shared entity and a delete wipes the catalog.
- `orphanRemoval` + `setLines(newList)`: every original child is seen as orphan; use `clear()` + `addAll()`.
- Missing `CascadeType.PERSIST` on a new parent with new children: `TransientPropertyValueException`.
- `@PreRemove` callbacks do not fire on bulk `@Query("delete ...")`.

## Inheritance strategies

What it is: map a class hierarchy to flat tables.

| Strategy | Tables | Polymorphic `from Payment` | Best when |
|---|---|---|---|
| `SINGLE_TABLE` | one + discriminator column | single SELECT (fast) | reads dominate, few subclass-only fields |
| `JOINED` | root + one per subclass (FK-linked) | LEFT JOIN across N tables | need real per-subclass NOT NULL |
| `TABLE_PER_CLASS` | one per concrete class, no root | UNION ALL across tables | rarely - usually wrong |
| `@MappedSuperclass` | none (columns stamped per entity) | none | ~90% of cases - share audit columns |

```java
@Entity
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "payment_type", discriminatorType = DiscriminatorType.STRING, length = 16)
public abstract class Payment { @Id @GeneratedValue Long id; BigDecimal amount; }

@Entity @DiscriminatorValue("CARD")
public class CardPayment extends Payment { String cardLast4; }

@MappedSuperclass     // shared COLUMNS only, no hierarchy, no polymorphic query
public class Auditable { @Version Long version; Instant createdAt; }
```

Gotchas:
- `SINGLE_TABLE` with subclass-only `@Column(nullable=false)`: Hibernate makes it NOT NULL, blocking other subclasses. Enforce per-subclass with a partial CHECK instead.
- Forgetting `@DiscriminatorValue`: Hibernate uses the class name; renaming the class makes existing rows unloadable.
- `TABLE_PER_CLASS` + `IDENTITY`: ids collide across tables; needs a shared sequence.
- Switching strategy mid-life is a destructive migration.

## N+1 problem and fixes

What it is: 1 query to load N parents, then N more for a lazy association per row.

| Fix | What it does | Reach for when |
|---|---|---|
| `JOIN FETCH` (JPQL) | pulls association in the same SELECT | a specific query needs the graph |
| `@EntityGraph` | declarative fetch plan on a repo method | same plan reused across finders |
| `@BatchSize` | loads lazy collections in batches (`IN (...)`) | access is genuinely lazy but bulk |
| DTO projection | scalar columns into a DTO, no entities | read-only list/summary screens |

```java
@Query("select distinct o from Order o left join fetch o.lines l left join fetch l.product where o.status = :s")
List<Order> findWithLines(OrderStatus s);          // distinct avoids cartesian dups

@EntityGraph(attributePaths = {"lines", "lines.product"})
List<Order> findByStatus(OrderStatus status);

@OneToMany(mappedBy = "order") @BatchSize(size = 50)
private List<OrderLine> lines;
```

- `@EntityGraph(type = FETCH)`: load only the listed paths eagerly, everything else LAZY. `type = LOAD`: listed paths plus whatever was declared EAGER.
- Set `spring.jpa.open-in-view=false` (OSIV hides N+1 and pins the connection). A broad `hibernate.default_batch_fetch_size=32` is cheap insurance.
- Pagination + collection fetch -> "firstResult/maxResults applied in memory"; use the two-step: page parent ids, then fetch the graph for those ids.

Gotchas: `JOIN FETCH` two `List`s -> `MultipleBagFetchException`; EAGER-everywhere to "fix" N+1 explodes the graph; forgetting `distinct` returns one parent per child row; `@BatchSize` too high blows the bind limit.

### EntityGraph: Spring Data vs standard Jakarta Persistence

`@EntityGraph(attributePaths = {...})` on a repository method is a Spring Data
convenience. The portable Jakarta EE / JPA API has no such annotation - you either
declare a `@NamedEntityGraph` on the entity and pass it as a query hint, or build a
graph programmatically from the `EntityManager`. All three drive the same Hibernate
fetch plan.

| Style | API | Needs Spring |
|---|---|---|
| Spring Data shortcut | `@EntityGraph(attributePaths=...)` on repo method | yes |
| Named graph + hint | `@NamedEntityGraph` + `jakarta.persistence.fetchgraph` / `loadgraph` | no |
| Programmatic graph | `em.createEntityGraph(...)` + `addAttributeNodes/addSubgraph` | no |

Hint key meaning is the same as the Spring `type`: `jakarta.persistence.fetchgraph`
= listed paths EAGER, everything else LAZY (== `type=FETCH`);
`jakarta.persistence.loadgraph` = listed paths plus declared-EAGER (== `type=LOAD`).

```java
// 1) Declare a named graph on the entity (with a nested subgraph for lines.product)
@NamedEntityGraph(name = "Order.withLines", attributeNodes = {
    @NamedAttributeNode(value = "lines", subgraph = "lines.product")
}, subgraphs = {
    @NamedSubgraph(name = "lines.product", attributeNodes = @NamedAttributeNode("product"))
})
@Entity class Order { ... }

// 2a) Use it via EntityManager.find with a hint (pure Jakarta Persistence)
EntityGraph<?> g = em.getEntityGraph("Order.withLines");
Order o = em.find(Order.class, id,
    Map.of("jakarta.persistence.fetchgraph", g));

// 2b) Or on a JPQL query
List<Order> os = em.createQuery("select o from Order o where o.status = :s", Order.class)
    .setParameter("s", status)
    .setHint("jakarta.persistence.loadgraph", g)
    .getResultList();

// 3) Or build the graph programmatically, no @NamedEntityGraph needed
EntityGraph<Order> g2 = em.createEntityGraph(Order.class);
g2.addSubgraph("lines").addAttributeNodes("product");   // lines + lines.product
List<Order> os2 = em.createQuery("select o from Order o", Order.class)
    .setHint("jakarta.persistence.fetchgraph", g2)
    .getResultList();
```

In Spring Data you can also reference the named graph instead of inlining paths:
`@EntityGraph(value = "Order.withLines", type = EntityGraphType.FETCH)`.

## JPQL

What it is: SQL spoken in your domain vocabulary - you name entities and fields, Hibernate handles tables/columns/joins.

| Thing | What it is | Written against |
|---|---|---|
| JPQL | the Jakarta query language | entities |
| HQL | Hibernate superset of JPQL | entities |
| Native SQL | `@Query(nativeQuery=true)` | tables/columns |
| Derived query | Spring Data parses the method name | method name -> entities |

```java
@Query("select o from Order o where o.status = :status and o.createdAt > :cutoff order by o.createdAt desc")
List<Order> find(@Param("status") OrderStatus status, @Param("cutoff") Instant cutoff);

// constructor / DTO projection (senior default for read models; FQCN required)
@Query("select new com.example.OrderView(o.id, o.customer.name, o.status, o.total) from Order o")
List<OrderView> views();

// implicit vs explicit join vs join fetch
select o.customer.name from Order o                          // implicit join
select o from Order o join o.customer c where c.country = :x // explicit (aliased)
select distinct o from Order o left join fetch o.lines       // join fetch (N+1 fix)

// bulk update: one SQL statement, bypasses the persistence context
@Modifying(clearAutomatically = true)
@Query("update Order o set o.status = com.example.OrderStatus.CANCELLED where o.createdAt < :cutoff")
int cancelStale(@Param("cutoff") Instant cutoff);
```

- No `group by` -> select whole entities. With `group by` -> select keys + aggregates only (never the bare entity).
- Bind with `:named` (preferred) or `?1` positional; never concatenate; do not mix styles. Enum literals use FQCN or bind as a parameter.
- Native SQL only for Postgres-only features (`jsonb @>`, `tsvector`, window fns, `LATERAL`, `ON CONFLICT`).

Gotchas: bulk update without `clearAutomatically` leaves stale managed entities; `IN (:list)` over the ~65535 bind limit must be chunked; `join fetch` collection + `setMaxResults` pages in memory; constructor projections break at runtime on a constructor mismatch; Spring Data validates `@Query` JPQL at startup but not native SQL.

## First and second-level cache

What it is: the persistence context (L1), the SessionFactory cache (L2), and the query cache.

| Cache | Scope | On by default | Caches | Risk |
|---|---|---|---|---|
| L1 (persistence context) | one transaction / EntityManager | yes (always) | managed entity instances | none |
| L2 (SessionFactory) | whole JVM (or cluster) | no (needs provider) | entity state, re-hydrated per fetch | stale reads across nodes |
| Query cache | per query | no | the ids a query returned | aggressive invalidation |

```java
@Entity @Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_ONLY)  // or NONSTRICT_READ_WRITE, READ_WRITE, TRANSACTIONAL
public class Currency { ... }
```

L2 concurrency strategies (cheapest -> strictest): `READ_ONLY` (immutable data), `NONSTRICT_READ_WRITE` (brief staleness ok), `READ_WRITE` (soft-locks on write), `TRANSACTIONAL` (needs JTA).

- L1 is an identity map: two `em.find()` for the same id = one SELECT, same Java object. Dirty checking fires an UPDATE on flush without calling save().
- Inspect: `sessionFactory.getStatistics().getSecondLevelCacheHitCount()`. Evict: `getCache().evictEntity(Currency.class, key)`.

Gotchas: multi-node with local L2 (Caffeine/Ehcache) -> stale reads after a write on another node; use clustered (Hazelcast/Infinispan) or keep L2 off. Query cache thrashes under high write rate. Direct SQL (Flyway, admin UPDATE) never invalidates L2. `em.clear()` discards dirty unflushed entities - silent data loss.

## Transactional semantics

What it is: how Spring's declarative transactions work via an AOP proxy, plus propagation, isolation, and rollback.

```java
@Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.READ_COMMITTED,
               readOnly = false, rollbackFor = DomainException.class, timeout = 30)
public void doIt() { ... }
```

Propagation levels:

| Propagation | Behaviour | Use for |
|---|---|---|
| `REQUIRED` (default) | join existing, else create | almost everything |
| `REQUIRES_NEW` | suspend existing, always create new (independent commit) | audit log, outbox |
| `NESTED` | savepoint within existing tx (Postgres) | partial rollback |
| `MANDATORY` | join existing, else throw | must be called inside a tx |
| `NEVER` | run non-tx, throw if a tx exists | guard against accidental tx |
| `SUPPORTS` | join if exists, else non-transactional | reads that do not care |
| `NOT_SUPPORTED` | suspend existing, run non-transactional | long read outside a tx |

Isolation levels and anomalies:

| Level | Dirty read | Non-repeatable | Phantom | Write skew |
|---|---|---|---|---|
| `READ_UNCOMMITTED` | (Postgres = RC) | yes | yes | yes |
| `READ_COMMITTED` (Postgres default) | no | yes | yes | yes |
| `REPEATABLE_READ` | no | no | no (Postgres snapshot) | yes |
| `SERIALIZABLE` | no | no | no | no |

- Rollback default: rolls back on `RuntimeException` + `Error`; commits on checked exceptions unless `rollbackFor` is set.
- Postgres serialization failure is SQLSTATE `40001` (REPEATABLE READ / SERIALIZABLE) - the app must retry; deadlock is `40P01`.

Gotchas:
- Self-invocation (`this.txMethod()`) bypasses the proxy - the annotation is silently ignored. Only public methods are advised.
- Catching the exception inside the method swallows the rollback signal -> commits.
- `REQUIRES_NEW` uses a second connection - deadlock/pool-exhaustion risk under a long parent tx.
- `@Async` runs on another thread - the parent tx does not propagate.
- `readOnly = true` is a Hibernate hint (skips dirty checking), not enforced everywhere.

## Optimistic vs pessimistic locking

What it is: detect-conflict-at-commit (optimistic, `@Version`) vs lock-upfront (pessimistic, `FOR UPDATE`).

| | Optimistic (`@Version`) | Pessimistic (`FOR UPDATE`) |
|---|---|---|
| Locks rows? | no, conflict detected at commit | yes, held until commit |
| On conflict | throws `OptimisticLockException` -> retry | other tx waits |
| Best for | low contention, edit forms, web | counters, balances, inventory, queues |
| Risk | retries / lost work | deadlocks, blocked threads |

```java
@Version private long version;   // UPDATE ... WHERE id=? AND version=?; 0 rows -> OptimisticLockException

@Lock(LockModeType.PESSIMISTIC_WRITE)            // SELECT ... FOR UPDATE
Account lockById(Long id);
em.find(Product.class, id, LockModeType.PESSIMISTIC_WRITE);

// retry loop (Spring Retry); refetch on every attempt
@Retryable(retryFor = ObjectOptimisticLockingFailureException.class, maxAttempts = 3,
           backoff = @Backoff(delay = 50))
@Transactional
public void adjust(Long id, BigDecimal d) { var p = repo.findById(id).orElseThrow(); p.setPrice(p.getPrice().add(d)); }
```

| LockModeType | Meaning | SQL on Postgres |
|---|---|---|
| `OPTIMISTIC` | version check at commit | version check |
| `OPTIMISTIC_FORCE_INCREMENT` | bump version even if unchanged | version++ |
| `PESSIMISTIC_READ` | shared lock | `FOR SHARE` |
| `PESSIMISTIC_WRITE` | exclusive lock | `FOR UPDATE` |
| `PESSIMISTIC_FORCE_INCREMENT` | exclusive lock + version bump | `FOR UPDATE` + version++ |
| `NONE` | no lock | - |

Lock timeout hint: `query.setHint("jakarta.persistence.lock.timeout", ms)` (0 = NOWAIT, -2 = SKIP_LOCKED). `SELECT ... FOR UPDATE SKIP LOCKED` is the concurrent-queue primitive.

Gotchas: missing `@Version` -> silent lost updates; `@Version` on `int` overflows (~2B) - use `long`; bulk `@Modifying` does not bump `@Version`; a re-attached detached entity carries its old version (spurious conflict); always lock rows in a consistent order to avoid deadlocks.

## Flushing, dirty checking, detached entities

What it is: when Hibernate sends SQL (flush), how it detects changes (dirty checking), and the entity lifecycle.

| State | Meaning | Mutations tracked? |
|---|---|---|
| Transient | `new`, no id, never persisted | no |
| Managed | tracked by the context | yes - auto-flushed |
| Detached | was managed, context closed/evicted | no - mutations lost |
| Removed | marked for DELETE | (going away) |

Transitions: `persist` (transient->managed), commit/`close`/`detach` (managed->detached), `remove` (managed->removed), `merge` (detached->managed, returns a managed copy).

```java
@Transactional
public void rename(Long id, String name) {
    Product p = repo.findById(id).orElseThrow();  // MANAGED
    p.setName(name);                              // dirty checking -> UPDATE on flush; no save() needed
}

em.flush();   // force SQL now (surface a constraint violation early; get a generated id pre-commit)
em.clear();   // detach everything (drops snapshot) - discards unflushed changes
em.refresh(e);// reload from DB, discard in-memory changes
```

Flush timing: `FlushModeType.AUTO` (default) flushes before an affected query + at commit; `COMMIT` flushes only at commit. Batch hygiene: `flush()` + `clear()` every ~50 rows to keep the context small.

Gotchas: mutating a detached entity does nothing (a frequent "why did it not save"); keep the `merge` return value, not the original; `flush()` does not commit; `LazyInitializationException` = touching a lazy field on a detached entity; large contexts (10k+) scan all entities on every flush.

## Spring Data repositories and projections

What it is: repository abstraction (derived queries, paging) and projections to fetch only the columns needed.

Hierarchy: `Repository` -> `CrudRepository` -> `PagingAndSortingRepository` -> `JpaRepository` (+ `JpaSpecificationExecutor` for Criteria).

```java
findByStatusAndCountry(...)  findByTotalGreaterThan(...)  findByCreatedAtBetween(a,b)
findByNameContaining(...)    findByStatusOrderByCreatedAtDesc(...)  findTop10By...
countByStatus(...)  existsBySku(...)  deleteByStatus(...)

Page<Order> findByStatus(OrderStatus s, Pageable p);   // Page = content + count (extra count query)
Slice<Order> findByStatus(OrderStatus s, Pageable p);  // Slice = no count, just "has next"
```

Projections:

| Type | Example | Columns selected |
|---|---|---|
| Entity | `List<Order>` | all |
| Interface (closed) | `interface V { Long getId(); }` | only mapped |
| Class/record | `record V(Long id, Status s) {}` | only mapped |
| Dynamic | `<T> List<T> findBy...(..., Class<T> type)` | only mapped |
| Open (`@Value` SpEL) | computed getter | all (avoid - loses optimization) |

`findById` does an eager SELECT (returns `Optional`); `getReferenceById` returns a lazy proxy with no SELECT - ideal for FK assignment (throws `EntityNotFoundException` lazily if the id is absent).

Gotchas: `Page` runs an extra COUNT (use `Slice` if you only need next-page); derived names past ~3 conditions become unreadable, switch to `@Query`; returning entities from a read endpoint drags lazy associations into serialization (project instead); `deleteByX` loads then deletes one-by-one (callbacks fire) unlike `@Modifying delete`; nested interface projections cause N+1.

## Testcontainers testing (JPA)

What it is: integration-test against a real PostgreSQL container, never H2.

Why not H2: "Postgres mode" is not Postgres - no `jsonb`/arrays/`LATERAL`/`ON CONFLICT`/`RETURNING`, different sequence/locking/isolation, different SQLSTATE. Use the same major version as production.

```java
@SpringBootTest @Testcontainers
abstract class AbstractPostgresIT {
    @Container @ServiceConnection                  // Boot 3.1+: no @DynamicPropertySource needed
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");
}

// N+1 guard via statistics
var stats = em.getEntityManagerFactory().unwrap(SessionFactory.class).getStatistics();
stats.clear();
repo.findAllBy();
assertThat(stats.getPrepareStatementCount()).isLessThanOrEqualTo(2);
```

| Decorator | Loads | Rollback | Use for |
|---|---|---|---|
| `@DataJpaTest` | JPA/repositories only | auto-rollback | repo tests (fast) |
| `@SpringBootTest` | full context | no auto-rollback | service beans, multi-bean |

Gotchas: `@DataJpaTest` silently swaps in H2 unless `@AutoConfigureTestDatabase(replace = NONE)`; `static` container shared per class is far faster than per-method; `@Transactional` test rollback hides flush/constraint issues - flush explicitly; pin the exact image (`postgres:16`), not `latest`; do not assert on auto-generated id values (sequence allocation jumps).

---

# Part 2: Criteria

## Criteria API and Specifications

What it is: build queries as composable Java objects - the right tool for dynamic/optional filters.

| Object | Role |
|---|---|
| `CriteriaBuilder` (cb) | factory for predicates, queries, functions |
| `CriteriaQuery<T>` (cq) | the query being assembled; T = result type |
| `Root<Order>` | the FROM clause + handle to fields |
| `Path<X>` | a field reference (`o.get("status")`) |
| `Predicate` | a boolean WHERE condition |
| `Join` | a join to an association |

```java
CriteriaBuilder cb = em.getCriteriaBuilder();           // 1. factory
CriteriaQuery<Order> cq = cb.createQuery(Order.class);  // 2. start query
Root<Order> o = cq.from(Order.class);                   // 3. FROM Order o
cq.select(o).where(cb.equal(o.get("status"), PAID));    // 4. SELECT + WHERE
List<Order> r = em.createQuery(cq).getResultList();     // 5. run

// predicates
cb.greaterThan(o.<Instant>get("createdAt"), cutoff)     // typed path needed for compares
cb.like(o.get("name"), "An%")   cb.and(p1, p2)   cb.or(p1, p2)
cb.conjunction()   // always-TRUE = the "skip this filter" idiom
Join<Order, Customer> c = o.join("customer");           // join by field name

// Spring Data Specification - the practical wrapper
public static Specification<Order> hasStatus(OrderStatus s) {
    return (root, query, cb) -> s == null ? cb.conjunction() : cb.equal(root.get("status"), s);
}
var spec = hasStatus(status).and(createdAfter(cutoff)).and(minTotal(min));
orders.findAll(spec);   // repo extends JpaSpecificationExecutor<Order>

// type-safe metamodel (hibernate-jpamodelgen)
root.get(Order_.status);   // compile error on rename
root.get("status");        // runtime error on rename
```

Gotchas:
- The middle `query` arg: use `query.getResultType()` to skip `fetch`/`orderBy` during the count pass of a `Page`, else the count query joins and over-counts.
- Composing specs that each call `root.join("customer")` produces duplicate joins.
- Spec lambdas are evaluated more than once (count + content) - keep them pure (no I/O, no mutation).
- Quarkus has the raw Criteria API but no `Specification` equivalent.

## JPQL vs Criteria (choosing)

What it is: pick by query shape, not habit. Both compile to the same SQM -> SQL, so it is not a performance choice.

Rule: JPQL for static queries, Criteria/Specifications for dynamic ones, native SQL for Postgres-only features.

| If the query is... | Use |
|---|---|
| Static, known at compile time | JPQL (`@Query`) or derived method |
| Dynamic - optional filters at runtime | Criteria / `Specification` |
| Postgres-specific (`jsonb @>`, window fns, LATERAL) | native SQL |
| Simple lookup by 1-3 fixed fields | derived query |

| Dimension | JPQL | Criteria / Specification |
|---|---|---|
| Readability (static) | excellent | verbose |
| Dynamic filters | string glue (brittle) | composable |
| Refactor safety | strings break at runtime | safe with metamodel |
| Startup validation | yes (Spring `@Query`) | no (built at runtime) |
| Postgres-only features | no -> native | no -> native |

Switch to Specifications the moment a query has 2+ optional filters. Criteria turning into unreadable `cb.` soup is the signal it wanted to be JPQL/native.

---

# Part 3: jOOQ

## Why jOOQ

What it is: a typesafe SQL DSL where SQL is the model and Java is generated from the database schema. Not an ORM - no persistence context, dirty checking, lazy loading, or N+1 surprises.

| Dimension | JPA / Hibernate | jOOQ |
|---|---|---|
| Source of truth | Java entities | DB schema |
| SQL control | indirect (generated) | explicit (you write it) |
| Type safety | strings (JPQL) | compile-time (generated) |
| Codegen direction | schema from code (optional) | code from schema (required) |
| Caching / dirty-check | yes (L1/L2) | none |
| Best at | graph persistence, CRUD | complex reads, reports, analytics |

```java
DSLContext ctx = DSL.using(dataSource, SQLDialect.POSTGRES);
String sql = ctx.renderInlined(select(BOOK.ID).from(BOOK));  // render without executing
```

Gotchas: build-time codegen required, schema drift breaks the build (not runtime); no lazy navigation or cascade; commercial license for Oracle/SQL Server/DB2 (Postgres/MySQL are free); `fetchInto(Pojo.class)` matches by column name, aliased columns fail silently.

## Setup and codegen

What it is: configure jooq-codegen-maven to read a live (migrated) schema and emit Tables, Records, POJOs.

```xml
<plugin>
  <groupId>org.jooq</groupId>
  <artifactId>jooq-codegen-maven</artifactId>
  <executions>
    <execution>
      <id>generate-jooq</id>
      <phase>generate-sources</phase>
      <goals><goal>generate</goal></goals>
    </execution>
  </executions>
  <configuration>
    <jdbc>
      <driver>org.testcontainers.jdbc.ContainerDatabaseDriver</driver>
      <url>jdbc:tc:postgresql:16-alpine:///app?TC_REUSABLE=true</url>
      <user>test</user><password>test</password>
    </jdbc>
    <generator>
      <database>
        <name>org.jooq.meta.postgres.PostgresDatabase</name>
        <inputSchema>public</inputSchema>
        <includes>.*</includes>
        <excludes>flyway_schema_history</excludes>
      </database>
      <target>
        <packageName>com.example.jooq.generated</packageName>
        <directory>target/generated-sources/jooq</directory>
      </target>
      <generate>
        <records>true</records>
        <pojos>false</pojos>
        <javaTimeTypes>true</javaTimeTypes>
        <fluentSetters>true</fluentSetters>
      </generate>
    </generator>
  </configuration>
</plugin>
```

Generated artifacts: `Tables` (static handles, e.g. `BOOK`), `XxxRecord` (mutable active-record per row), `Keys`/`Indexes`/`Sequences`, optional POJOs.

Gotchas: codegen needs a reachable migrated DB at build time (Flyway-before-codegen or Testcontainers); `TC_REUSABLE=true` needs `testcontainers.reuse.enable=true` in `~/.testcontainers.properties`; `inputSchema` mismatch (use `public`) yields empty generation; committing generated sources risks drift + big diffs; exclude `flyway_schema_history`.

## Basic CRUD with the DSL

What it is: INSERT/SELECT/UPDATE/DELETE as typesafe DSL calls; prefer explicit DSL over active-record.

```java
// INSERT returning generated key
Long id = ctx.insertInto(BOOK)
             .columns(BOOK.AUTHOR_ID, BOOK.TITLE, BOOK.STATUS)
             .values(42L, "Effective jOOQ", BookStatus.PUBLISHED)
             .returning(BOOK.ID).fetchOne().getId();

// SELECT into generated records / into a Java record
Result<BookRecord> rows = ctx.selectFrom(BOOK).where(BOOK.PUBLISHED_YEAR.gt(2020))
                             .orderBy(BOOK.TITLE.asc()).fetch();
record BookView(Long id, String title, Integer year) {}
List<BookView> v = ctx.select(BOOK.ID, BOOK.TITLE, BOOK.PUBLISHED_YEAR).from(BOOK)
                      .fetch(Records.mapping(BookView::new));

// UPDATE / DELETE (with optional RETURNING)
int n = ctx.update(BOOK).set(BOOK.STATUS, BookStatus.ARCHIVED)
           .where(BOOK.PUBLISHED_YEAR.lt(2000)).execute();
int d = ctx.deleteFrom(BOOK).where(BOOK.ID.eq(id)).execute();

// active-record (UpdatableRecord)
BookRecord r = ctx.newRecord(BOOK); r.setTitle("X"); r.store();  // INSERT or UPDATE by PK
r.setTitle("Y"); r.store();  r.delete();
```

fetch variants: `.fetch()` (Result), `.fetchOne()` (one or null, throws if >1), `.fetchOptional()`, `.fetchSingle()` (exactly one), `.fetchInto(Pojo.class)`, `.execute()` (writes, returns affected count).

Gotchas: forgetting `.execute()` means the write never runs (lazy until terminal call); `fetchOne()` throws `TooManyRowsException` if >1 - use `fetchAny()`/`fetchOptional()`; `record.store()` decides INSERT vs UPDATE by PK presence; Postgres `DEFAULT now()` is overwritten unless `record.changed(field, false)`; `recordVersionFields` (optimistic locking) only applies to `UpdatableRecord`, not `dslContext.update(...)`.

## Joins, subqueries, CTEs

What it is: multi-table reads - inner/outer/LATERAL joins, correlated/EXISTS subqueries, typed and recursive CTEs. Use `import static org.jooq.impl.DSL.*;`.

```java
// joins
ctx.select(AUTHOR.NAME, BOOK.TITLE).from(AUTHOR)
   .leftJoin(BOOK).on(BOOK.AUTHOR_ID.eq(AUTHOR.ID)).fetch();

// correlated subquery in SELECT
ctx.select(AUTHOR.NAME,
       field(selectCount().from(BOOK).where(BOOK.AUTHOR_ID.eq(AUTHOR.ID))).as("book_count"))
   .from(AUTHOR).fetch();

// EXISTS
ctx.select(AUTHOR.ID).from(AUTHOR)
   .where(exists(selectOne().from(BOOK).where(BOOK.AUTHOR_ID.eq(AUTHOR.ID)))).fetch();

// typed CTE
CommonTableExpression<Record2<Long, Integer>> top = name("top_authors").fields("author_id", "book_count")
    .as(select(BOOK.AUTHOR_ID, count().cast(Integer.class)).from(BOOK)
        .groupBy(BOOK.AUTHOR_ID).orderBy(count().desc()).limit(10));
ctx.with(top).select(AUTHOR.NAME, top.field("book_count", Integer.class)).from(top)
   .join(AUTHOR).on(AUTHOR.ID.eq(top.field("author_id", Long.class))).fetch();

// recursive CTE
ctx.withRecursive(tree).selectFrom(tree).fetch();
```

Gotchas: a missing `.on(...)` is a cartesian product (compiles fine); recursive CTEs without a termination clause loop forever (add `WHERE depth < N`); self-joins need aliases (`BOOK.as("b2")`); CTE column refs via `field(name("cte","col"), Type.class)` lose generated type safety; scalar subqueries must return exactly one row.

## Window functions

What it is: analytic functions `OVER (PARTITION BY ... ORDER BY ...)` for ranking, running totals, lag/lead without collapsing rows.

```java
import static org.jooq.impl.DSL.*;

rowNumber().over(partitionBy(BOOK.AUTHOR_ID).orderBy(BOOK.PUBLISHED_YEAR.desc())).as("rn")
rank().over(orderBy(BOOK.TOTAL.desc()))
denseRank().over(partitionBy(BOOK.AUTHOR_ID).orderBy(BOOK.TOTAL.desc()))
count().over(partitionBy(BOOK.AUTHOR_ID).orderBy(BOOK.PUBLISHED_YEAR))          // running count
lag(BOOK.PUBLISHED_YEAR).over(partitionBy(BOOK.AUTHOR_ID).orderBy(BOOK.PUBLISHED_YEAR))
avg(field("page_count", Integer.class))
    .over(orderBy(BOOK.PUBLISHED_YEAR).rowsBetweenPreceding(2).andCurrentRow())  // moving avg
```

Functions: `rowNumber`, `rank`, `denseRank`, `lag`, `lead`, `firstValue`, `lastValue`, `ntile(n)`, plus aggregates `.over(...)`.

Top-N per group: wrap the rowNumber query in a subquery/CTE and filter `rn <= N` outside.

Gotchas: a window-function alias cannot be referenced in `WHERE` - wrap in a subquery; the default frame on an ORDER BY window is `RANGE UNBOUNDED PRECEDING ... CURRENT ROW` (RANGE groups peers, ROWS counts physical rows); `lastValue()` needs an explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`; ranking on non-deterministic tiebreakers gives unstable pagination.

## Dynamic query building

What it is: compose queries from optional filters via `Condition` objects, never string concatenation.

```java
Condition cond = noCondition();                       // identity for AND (emits nothing)
if (title != null)   cond = cond.and(BOOK.TITLE.likeIgnoreCase("%" + title + "%"));
if (yearFrom != null) cond = cond.and(BOOK.PUBLISHED_YEAR.ge(yearFrom));
if (statuses != null && !statuses.isEmpty()) cond = cond.and(BOOK.STATUS.in(statuses));
ctx.selectFrom(BOOK).where(cond).orderBy(BOOK.PUBLISHED_YEAR.desc()).limit(100).fetch();

// list of conditions (AND-combined)
List<Condition> cs = new ArrayList<>();
if (status != null) cs.add(BOOK.STATUS.eq(status));
ctx.selectFrom(BOOK).where(cs).fetch();

// OR accumulator seeds with falseCondition()
Condition any = falseCondition();
for (String s : skus) any = any.or(BOOK.SKU.eq(s));

// dynamic partial UPDATE
Map<Field<?>, Object> upd = new LinkedHashMap<>();
if (newTitle != null) upd.put(BOOK.TITLE, newTitle);
if (!upd.isEmpty()) ctx.update(BOOK).set(upd).where(BOOK.ID.eq(id)).execute();

// stable plan cache: = ANY(array) instead of IN(varying list)
ctx.selectFrom(BOOK).where(BOOK.ID.eq(any(ids.toArray(Long[]::new)))).fetch();
```

`noCondition()` (no-op, preferred seed) vs `trueCondition()` (`1=1`) vs `falseCondition()` (`1=0`). `.and()` does not mutate in place - reassign. An empty `IN` list renders a safe `false`, no SQL error.

Gotchas: forgetting to cap LIMIT returns the whole table; `IN(varyingList)` churns the Postgres plan cache (prefer `= ANY(array)`); `likeIgnoreCase("%x%")` defeats indexes (use pg_trgm/full-text); dynamic ORDER BY must whitelist known `Field`s, never accept raw column names; `DSL.sql("WHERE " + filter)` reopens injection; deep OFFSET pagination degrades (use keyset).

## Batch operations and bulk insert

What it is: load many rows efficiently - JDBC batch, multi-row VALUES, INSERT ... SELECT, COPY FROM STDIN.

| API | What it does | Use when |
|---|---|---|
| `.values().values()...` | one multi-row INSERT statement | small/medium known set |
| `batchInsert(records)` | one JDBC batch, INSERT only | many records |
| `batchStore(records)` | batched INSERT/UPDATE | mixed new/changed |
| `batch(query).bind(...)` | reuse one SQL string, many bind sets | max throughput |
| `loadInto(...).loadCSV/...` | bulk loader with commit/batch control | huge imports |

```java
int[] counts = ctx.batchInsert(records).execute();

ctx.insertInto(BOOK, BOOK.SKU, BOOK.NAME)
   .values("A","Apple").values("B","Banana").execute();      // multi-row VALUES

// upsert (idempotent) - needs a unique constraint on the conflict column
ctx.insertInto(BOOK, BOOK.ID, BOOK.TITLE).values(id, title)
   .onConflict(BOOK.ID).doUpdate().set(BOOK.TITLE, BOOK.excluded(BOOK.TITLE)).execute();

// INSERT ... SELECT
ctx.insertInto(BOOK_ARCHIVE, BOOK_ARCHIVE.ID, BOOK_ARCHIVE.TITLE)
   .select(select(BOOK.ID, BOOK.TITLE).from(BOOK).where(BOOK.STATUS.eq(BookStatus.ARCHIVED)))
   .execute();

// COPY FROM STDIN (fastest bulk path)
ctx.connection(conn -> {
    var copy = conn.unwrap(org.postgresql.PGConnection.class).getCopyAPI();
    copy.copyIn("COPY book(author_id, title) FROM STDIN WITH (FORMAT csv)", new StringReader(csv));
});
```

Driver URL: `jdbc:postgresql://host/db?reWriteBatchedInserts=true`.

Gotchas: without `reWriteBatchedInserts=true`, `batchInsert` is no faster than a loop (silent perf cliff); Postgres caps 65535 bind params per statement (a 4-column multi-row INSERT caps ~16k rows - chunk at 1k-10k); `RETURNING` is unreliable with batching; one tx for 1M rows bloats WAL (chunk-commit); `ON CONFLICT(col)` requires a unique constraint; COPY still fires triggers and FK checks.

## Transactions

What it is: atomic multi-statement work via `DSLContext.transaction` (lambda-scoped commit/rollback).

```java
ctx.transaction(cfg -> {                 // commit on success, rollback on exception
    DSLContext tx = cfg.dsl();           // MUST use cfg.dsl(), not the outer ctx
    tx.insertInto(AUTHOR).columns(AUTHOR.NAME).values("Knuth").execute();
});

Long id = ctx.transactionResult(cfg -> cfg.dsl().insertInto(BOOK)...returning(BOOK.ID).fetchOne().getId());

// nested transaction = savepoint (partial rollback of inner only)
ctx.transaction(outer -> {
    outer.dsl().insertInto(AUTHOR)...execute();
    try { outer.dsl().transaction(inner -> { ...; throw new RuntimeException(); }); }
    catch (RuntimeException ignored) {}   // outer continues, inner rolled back to savepoint
});

// pessimistic lock + serialization-failure retry
ctx.selectFrom(BOOK).where(BOOK.ID.eq(id)).forUpdate().fetchOne();
// catch DataAccessException; if "40001".equals(e.sqlState()) retry with backoff
```

| API | Returns |
|---|---|
| `transaction(TransactionalRunnable)` | void |
| `transactionResult(TransactionalCallable<T>)` | T |
| nested `transaction(...)` | via savepoint |

Spring integration: wire a `TransactionAwareDataSourceProxy` + `SpringTransactionProvider` so jOOQ participates in `@Transactional`. Without it jOOQ opens a fresh connection and `@Transactional` does not scope the work.

Gotchas: using the outer `ctx` instead of `cfg.dsl()` runs outside the tx (no rollback, silent split-brain); jOOQ's own `transaction()` and Spring `@Transactional` can double-manage - pick one boundary; REPEATABLE READ / SERIALIZABLE need app-level retry on `40001`; `lastID()` is session-scoped - use `RETURNING`; nested transactions are savepoints, not independent connections (use Spring `REQUIRES_NEW` for that).

## Record mapping

What it is: convert between jOOQ `Record`s and your types - generated records, Java records via `Records.mapping`, reflection POJOs, custom converters.

```java
// Java record (positional, compile-checked)
record BookView(Long id, String title, Integer year) {}
List<BookView> v = ctx.select(BOOK.ID, BOOK.TITLE, BOOK.PUBLISHED_YEAR).from(BOOK)
                      .fetch(Records.mapping(BookView::new));

// POJO via reflection (by column name, tolerant)
List<BookPojo> p = ctx.selectFrom(BOOK).fetchInto(BookPojo.class);

// nested record mapping
record Author(Long id, String name) {}
record BookWithAuthor(Long id, String title, Author author) {}
ctx.select(BOOK.ID, BOOK.TITLE, row(AUTHOR.ID, AUTHOR.NAME).mapping(Author::new))
   .from(BOOK).join(AUTHOR).on(BOOK.AUTHOR_ID.eq(AUTHOR.ID))
   .fetch(Records.mapping(BookWithAuthor::new));

// grouping helpers
Map<Long, BookRecord> byId = ctx.selectFrom(BOOK).fetchMap(BOOK.ID);
Map<Long, List<BookRecord>> grp = ctx.selectFrom(BOOK).fetchGroups(BOOK.AUTHOR_ID);

// custom Converter (DB type <-> Java type), wired in codegen forcedTypes
class MetadataConverter implements Converter<JSONB, BookMetadata> {
    public BookMetadata from(JSONB db) { ... } public JSONB to(BookMetadata u) { ... }
    public Class<JSONB> fromType() { return JSONB.class; }
    public Class<BookMetadata> toType() { return BookMetadata.class; }
}
```

| Method | Returns |
|---|---|
| `fetchInto(T.class)` | `List<T>` |
| `fetchOneInto(T.class)` | one `T` (null if none) |
| `fetch(Records.mapping(Ctor::new))` | custom-mapped list |
| `fetchMap(key)` | `Map<K, Record>` |
| `fetchGroups(key)` | `Map<K, List<Record>>` |

Gotchas: `fetchInto(Pojo.class)` matches by column name - `BOOK.TITLE.as("t")` silently nulls `getTitle()`; `Records.mapping` is positional - reorder gives a compile error (type change) or silent corruption (compatible types); a heavy converter (Jackson) runs per row - cache the ObjectMapper; Postgres arrays come back as `Object[]` unless typed/converted.

## Codegen strategies

What it is: customize what/how codegen emits - multi-schema, forced types, generator strategy, generated surface.

```xml
<forcedTypes>
  <!-- bind a column/type to a Java type or converter, across all matches -->
  <forcedType>
    <userType>com.example.BookMetadata</userType>
    <converter>com.example.MetadataConverter</converter>
    <includeExpression>book\.metadata</includeExpression>
    <includeTypes>jsonb</includeTypes>
  </forcedType>
  <forcedType><name>INSTANT</name><includeTypes>TIMESTAMPTZ</includeTypes></forcedType>
</forcedTypes>

<generate>
  <records>true</records><pojos>false</pojos><daos>false</daos>
  <javaTimeTypes>true</javaTimeTypes><fluentSetters>true</fluentSetters>
</generate>

<strategy><name>com.example.codegen.ExampleGeneratorStrategy</name></strategy>
```

```java
public class ExampleGeneratorStrategy extends DefaultGeneratorStrategy {
    @Override public String getJavaClassName(Definition d, Mode mode) {
        return mode == Mode.RECORD ? super.getJavaClassName(d, mode) + "Row" : super.getJavaClassName(d, mode);
    }
}
```

`forcedTypes` is the big lever (map TIMESTAMPTZ -> Instant, a status text -> enum, numeric -> money). `<includeExpression>` is column regex, `<includeTypes>` is SQL-type regex; both are fully qualified (`schema.table.COLUMN`).

Gotchas: a custom `GeneratorStrategy` must be on the codegen plugin classpath before it runs (separate module - chicken-and-egg); forced-type regex over-matches if not anchored; default naming is snake_case table -> PascalCase class, columns -> UPPER_CASE fields; `<javaTimeTypes>false</javaTimeTypes>` (old default) yields `java.sql.Timestamp` - set `true`; generated DAOs encourage anti-patterns, prefer `false`.

## Flyway + codegen wiring

What it is: run Flyway migrations before jOOQ codegen so generated code always matches the migrated schema.

```xml
<!-- Flyway FIRST (declared before jOOQ in the same generate-sources phase) -->
<plugin>
  <groupId>org.flywaydb</groupId><artifactId>flyway-maven-plugin</artifactId>
  <executions><execution><id>migrate</id><phase>generate-sources</phase>
    <goals><goal>migrate</goal></goals></execution></executions>
  <configuration>
    <url>jdbc:tc:postgresql:16-alpine:///app?TC_REUSABLE=true</url>
    <user>test</user><password>test</password>
    <locations><location>filesystem:src/main/resources/db/migration</location></locations>
  </configuration>
</plugin>
<!-- jOOQ codegen SECOND, reading the migrated schema (same URL) -->
```

Plugin execution order within one Maven phase = declaration order in the POM.

| Approach | How | Trade-off |
|---|---|---|
| Live dev DB | codegen points at a running migrated DB | simple, drift risk |
| Flyway-then-codegen | Flyway migrates a build DB, then codegen | reproducible, needs a build DB |
| Testcontainers codegen | Flyway + codegen on a throwaway container | hermetic, CI-friendly, slower |
| `DDLDatabase` | codegen parses `.sql` files directly | no DB, limited Postgres DDL support |

Postgres DDL transactionality (migration safety): most DDL is transactional, but `CREATE INDEX CONCURRENTLY`, `VACUUM`, `REINDEX CONCURRENTLY` are NOT (need `executeInTransaction=false`). `ALTER TYPE ADD VALUE` (enum) cannot use the new value in the same tx. Backwards-compatible deploy: add nullable/new -> deploy app reading both shapes -> backfill + make non-null/drop old -> deploy final app.

Gotchas: wrong plugin order -> codegen on an empty schema -> empty/incorrect classes; renaming an applied migration changes its checksum and aborts the next `migrate` (treat applied migrations as immutable); `DDLDatabase` chokes on Postgres-specific DDL; long `ALTER TABLE` holds ACCESS EXCLUSIVE - set `lock_timeout`; repeatable (`R__`) migrations re-run on any checksum change.

## Testcontainers testing (jOOQ)

What it is: integration-test jOOQ against a real Postgres container; reuse for speed.

```java
@Testcontainers
abstract class PostgresIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine")
        .withReuse(true)
        .withCommand("postgres", "-c", "fsync=off", "-c", "synchronous_commit=off");  // tests only
    static DSLContext ctx;

    @BeforeAll static void setup() {
        var ds = new HikariDataSource(/* PG url/user/pass */);
        Flyway.configure().dataSource(ds).locations("classpath:db/migration").load().migrate();
        ctx = DSL.using(ds, SQLDialect.POSTGRES);
    }
    @BeforeEach void reset() { ctx.execute("TRUNCATE author, book RESTART IDENTITY CASCADE"); }
}

// Spring Boot 3.1+: @ServiceConnection auto-wires the autoconfigured DSLContext
@SpringBootTest @Testcontainers
class JooqIT {
    @Container @ServiceConnection
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16-alpine").withReuse(true);
    @Autowired DSLContext ctx;
}
```

Speed: reusable container (`.withReuse(true)` + properties) turns 3s startup into <100ms reconnect; one container per Maven module; reset state via TRUNCATE / `@Transactional` rollback / template DB; `fsync=off` for 2-3x write speed (never prod).

Gotchas: `.withReuse(true)` does nothing without `testcontainers.reuse.enable=true` in `~/.testcontainers.properties` (silent); reused container preserves state - TRUNCATE/rollback between tests; `@Transactional` test rollback hides read-your-writes from another connection; forgetting to migrate before building the DSLContext -> "relation does not exist"; pin the image version; do not assert on generated sequence ids.

---

# Comparison

## Same query: JPQL vs Criteria vs jOOQ

Query: PAID orders for customers in country PT, newest first.

```java
// JPQL (static, most readable)
@Query("select o from Order o join o.customer c "
     + "where o.status = :s and c.country = :country order by o.createdAt desc")
List<Order> find(@Param("s") OrderStatus s, @Param("country") String country);
```

```java
// Criteria / Specification (shines when status/country are OPTIONAL filters)
Specification<Order> spec = hasStatus(status).and(inCountry(country));  // null args drop out
orders.findAll(spec, Sort.by("createdAt").descending());
// hasStatus: (root, q, cb) -> status == null ? cb.conjunction() : cb.equal(root.get("status"), status);
```

```java
// jOOQ (explicit SQL, typed)
ctx.select(ORDER.fields())
   .from(ORDER).join(CUSTOMER).on(ORDER.CUSTOMER_ID.eq(CUSTOMER.ID))
   .where(ORDER.STATUS.eq(OrderStatus.PAID).and(CUSTOMER.COUNTRY.eq("PT")))
   .orderBy(ORDER.CREATED_AT.desc())
   .fetch();
```

JPQL and Criteria both compile to Hibernate SQM -> SQL (not a perf choice between them); jOOQ emits SQL directly and returns records/DTOs with no persistence context.

## jOOQ vs JPA: when to pick which

| Workload | Pick | Why |
|---|---|---|
| Single-aggregate write with cascade (Order + lines + payment) | JPA | identity map, cascade, dirty checking, optimistic lock - free |
| Standard CRUD on entities | JPA | least ceremony |
| Caching / dirty tracking / lifecycle | JPA | built in |
| Database portability across vendors | JPA | dialect abstraction |
| Dynamic search with many optional filters | jOOQ | composable `Condition`, SQL is the API |
| Reporting / analytics, window functions, CTEs | jOOQ | first-class; JPA forces native queries |
| Bulk insert/upsert of 100k+ rows | jOOQ | batchInsert, ON CONFLICT, multi-row VALUES, COPY |
| Postgres-specific (jsonb ops, LATERAL, full-text) | jOOQ | typed access; JPA loses safety on nativeQuery |
| Read model for an API endpoint | jOOQ | map straight to DTO records, skip the entity graph |
| Audit / append-only event log | jOOQ | no graph, no identity needed |
| Mixed app | both | JPA for writes/CRUD, jOOQ for complex reads/reports |

Using both in one app:
- Wire `TransactionAwareDataSourceProxy` + `SpringTransactionProvider` so jOOQ shares Spring's transaction.
- Flush ordering trap: JPA mutates an entity (dirty, not flushed), then jOOQ reads the same row and sees the OLD value. Fix: `em.flush()` before the jOOQ read.
- JPA L2 cache goes stale when jOOQ writes to cached tables - disable L2 for those tables or evict explicitly.
- `@Version` (JPA) and `recordVersionFields` (jOOQ) are independent - coordinate manually if both touch the same row.
- `hibernate.hbm2ddl.auto` should be `validate`/`none` when Flyway owns the schema; renaming a column requires updating both the `@Entity` and regenerating jOOQ.
