# Java 21 Cheatsheet

## Table of contents

Core:
- [Primitives and boxing](#primitives-and-boxing)
- [Strings and text blocks](#strings-and-text-blocks)
- [Collections (impl + Big-O)](#collections-impl--big-o)
- [Generics and wildcards (PECS)](#generics-and-wildcards-pecs)
- [Exceptions](#exceptions)
- [Lambdas and functional interfaces](#lambdas-and-functional-interfaces)
- [Streams](#streams)
- [Optional](#optional)
- [equals / hashCode / Comparable](#equals--hashcode--comparable)
- [Enums](#enums)
- [IO and NIO.2](#io-and-nio2)
- [Records, sealed, pattern matching](#records-sealed-pattern-matching)
- [Dates and times (java.time)](#dates-and-times-javatime)
- [Modules (JPMS)](#modules-jpms)

Advanced:
- [JVM memory model](#jvm-memory-model)
- [Garbage collectors (G1 / ZGC / Parallel)](#garbage-collectors-g1--zgc--parallel)
- [JIT and escape analysis](#jit-and-escape-analysis)
- [java.util.concurrent](#javautilconcurrent)
- [CompletableFuture](#completablefuture)
- [Virtual threads and structured concurrency](#virtual-threads-and-structured-concurrency)
- [Atomic and VarHandle](#atomic-and-varhandle)
- [Happens-before and memory visibility](#happens-before-and-memory-visibility)
- [Reflection and method handles](#reflection-and-method-handles)
- [Annotation processing](#annotation-processing)
- [Class loaders](#class-loaders)
- [JMH benchmarking](#jmh-benchmarking)
- [JFR and async-profiler](#jfr-and-async-profiler)
- [Native interop (JNI vs Panama FFM)](#native-interop-jni-vs-panama-ffm)
- [Sandboxing (post SecurityManager)](#sandboxing-post-securitymanager)

---

## Primitives and boxing

What it is: 8 value-semantic types and their heap wrapper classes.

| Primitive | Width | Range / notes | Default |
|---|---|---|---|
| `boolean` | JVM-defined | `true`/`false` | `false` |
| `byte` | 8-bit signed | -128..127 | 0 |
| `short` | 16-bit signed | -32768..32767 | 0 |
| `char` | 16-bit unsigned | UTF-16 code unit | `' '` |
| `int` | 32-bit signed | +-2.1B | 0 |
| `long` | 64-bit signed | +-9.2E18 | 0L |
| `float` | 32-bit IEEE-754 | ~7 digits | 0.0f |
| `double` | 64-bit IEEE-754 | ~15 digits | 0.0 |

```java
Integer i = 42;   // autobox -> Integer.valueOf(42)
int x = i;        // unbox   -> i.intValue()
```

Gotchas:
- `==` on boxed `Integer`/`Long`/etc. is identity. `Integer.valueOf` caches -128..127 (tunable `-XX:AutoBoxCacheMax`): `==` true at 127, false at 128. Always use `.equals`.
- Unboxing a `null` wrapper throws NPE inside the compiler-inserted `.intValue()`. Classic: `int n = map.get("missing");`.
- `List<Integer>.remove(int)` calls `remove(int index)`, not by value. Use `list.remove(Integer.valueOf(x))`.
- Integer arithmetic overflows silently (`Integer.MAX_VALUE + 1 == Integer.MIN_VALUE`); use `Math.addExact`/`multiplyExact`/`toIntExact` to throw.
- IEEE-754: `0.1 + 0.2 != 0.3`, `NaN != NaN` (use `Double.isNaN`), `1.0/0.0 == Double.POSITIVE_INFINITY`. Use `BigDecimal` for money.
- Boxed accumulator in a loop (`Long total = 0L; total += x;`) allocates per iteration. Use `long`.
- Narrowing cast wraps silently: `(byte) 200 == -56`.
- Prefer `IntStream`/`int[]` over `Stream<Integer>`/`List<Integer>` (~4x memory); `LongAdder` over `AtomicLong` under contention.

## Strings and text blocks

What it is: immutable UTF-16/Latin-1 char sequence; `byte[] value` + `byte coder` since JDK 9 (compact strings).

| Pattern | Use when |
|---|---|
| `"a" + "b"` literals | Compile-time folded. Free. |
| `a + b + c` single expr | `invokedynamic` + `StringConcatFactory`, single allocation. |
| `+=` in a loop | New String each iteration. O(n^2). Forbidden. |
| `StringBuilder` | Manual/conditional loop; presize with `new StringBuilder(n)`. |
| `String.join(",", parts)` | Join known collection, single allocation. |
| `String.format` / `s.formatted(...)` | Localizable, low-volume; per-call parse cost. |
| Text block `"""` | Multi-line JSON/SQL/HTML. |

```java
String json = """
        { "user": "%s", "active": %b }
        """.formatted("ana", true);
```

- `length()` = UTF-16 code units (emoji counts as 2). Use `codePointCount` for real chars.
- Text block strips incidental whitespace per the least-indented non-blank line (closing `"""` counts). Newlines are `\n` even on Windows. `\` at line end suppresses newline; `\s` keeps a trailing space.
- Useful: `repeat(n)`, `lines()`, `isBlank()`, `strip()` (Unicode-aware vs `trim`).

Gotchas:
- `s += t` in a loop is O(n^2); compiler does NOT hoist a StringBuilder across iterations.
- `String.matches/split/replaceAll` recompile the regex every call. Cache `static final Pattern`.
- `intern()` inflates the pool, slows lookup, pins entries. Avoid in prod.
- `toLowerCase()`/`toUpperCase()` without `Locale.ROOT`: Turkish-i bug (`"TITLE".toLowerCase()` in tr_TR -> `title` with dotless i).
- `"a:b:".split(":")` drops trailing empties (2 tokens); `split(":", -1)` keeps them (3).
- `String.format("%s", null)` yields literal `"null"`, not NPE.
- `==` compares references; use `.equals`.

## Collections (impl + Big-O)

What it is: List/Set/Map implementations with distinct order, null, and concurrency semantics.

| Impl | Order | Nulls | add/get/remove | Notes |
|---|---|---|---|---|
| `ArrayList` | insertion | values yes | O(1) amortized tail add, O(1) get, O(n) mid-remove | array, 1.5x growth |
| `LinkedList` | insertion | values yes | O(1) head/tail, O(n) get | big constants; avoid |
| `ArrayDeque` | insertion | no nulls | O(1) head/tail | replaces Stack and LinkedList-as-queue |
| `HashMap` | unspecified | 1 null key, null vals | O(1) avg | treeifies bucket at >=8 if key Comparable; LF 0.75, cap 16, doubles |
| `LinkedHashMap` | insertion or access | 1 null key | O(1) | LRU via `accessOrder=true` + `removeEldestEntry` |
| `TreeMap` | sorted | no null key | O(log n) | red-black; `firstKey`, `subMap`, range scans |
| `HashSet`/`LinkedHashSet`/`TreeSet` | mirror map | mirror map | mirror map | backed by matching map |
| `ConcurrentHashMap` | unspecified | no nulls anywhere | O(1) | lock-striped; `compute`/`merge` atomic, `get`+`put` not |
| `CopyOnWriteArrayList` | insertion | values yes | O(n) write, O(1) read | read-mostly listener lists |

```java
// sized to avoid resize
new HashMap<>((int)(expected / 0.75f) + 1);
counts.merge(key, 1, Integer::sum);  // atomic on ConcurrentHashMap
```

Gotchas:
- `Arrays.asList(arr)` is fixed-size; `add`/`remove` throw UnsupportedOperationException. Wrap in `new ArrayList<>(...)` for mutable.
- `List.of/Map.of/Set.of` reject nulls and duplicates; immutable.
- `ConcurrentModificationException` is fail-fast on single-thread iterators, NOT a concurrency guarantee. Use `Iterator.remove` or `removeIf` for in-iteration removal.
- View aliasing: `subList`, `keySet`, `values`, `entrySet` share state with the backing collection. `subList(1,4).clear()` mutates the backer.
- `ConcurrentHashMap.get` then `put` is not atomic; use `compute`/`merge`/`putIfAbsent`.
- `TreeMap`/`TreeSet` with a comparator inconsistent with `equals` silently merge entries.
- Mutable keys: mutating fields used in `equals`/`hashCode` after insertion strands the entry.

## Generics and wildcards (PECS)

What it is: compile-time type safety, erased at runtime (`List<String>` and `List<Integer>` are both `List`).

| Site | Wildcard | Rule |
|---|---|---|
| Read only (produces) | `? extends T` | can read as T, cannot add (except null) |
| Write only (consumes) | `? super T` | can add T, read only as Object |
| Read and write | invariant `T` | no wildcard |

Mnemonic: Producer Extends, Consumer Super. Canonical: `Collections.copy(List<? super T> dest, List<? extends T> src)`.

```java
static <T extends Comparable<? super T>> T max(List<? extends T> xs) { ... }
@SafeVarargs static <T> List<T> listOf(T... items) { ... }
```

Gotchas:
- Cannot do `new T[10]`, `T.class`, `instanceof List<String>` (use `List<?>`), `catch (MyEx<T> e)`.
- Arrays are covariant (`Object[] a = new Integer[1]; a[0]="x"` -> ArrayStoreException); generics are invariant (`List<Object> = new ArrayList<String>()` does not compile).
- Raw types disable generic checking for the whole expression -> heap pollution, CCE far from the bug.
- Methods differing only by type parameter collide after erasure.
- `@SafeVarargs` asserts no heap pollution via varargs; the compiler trusts, does not verify (final/static/private only).
- `var list = new ArrayList<>();` infers `ArrayList<Object>`. Supply a target type.
- Array of generic: take `IntFunction<T[]>` (`String[]::new`) or `Class<T>` + `Array.newInstance`.

## Exceptions

What it is: `Throwable` -> `Error` (JVM-level, do not catch) and `Exception` -> `RuntimeException` (unchecked) + checked.

```text
Throwable
+- Error            (OutOfMemoryError, StackOverflowError) - do not catch
+- Exception
   +- RuntimeException  (NPE, IllegalArgumentException) - unchecked
   +- other (IOException, SQLException)               - checked: catch-or-declare
```

Checked vs unchecked: use checked only if the caller can plausibly recover (retry/fallback). Modern service style is mostly unchecked (also: checked exceptions do not compose with lambdas/streams).

```java
try (var r = open("a"); var w = open("b")) { ... }  // close(w) then close(r)
throw new IllegalStateException("cannot read " + p, e);  // preserve cause
```

- try-with-resources closes `AutoCloseable` in reverse declaration order. If body and `close()` both throw, body is primary, close is attached via `addSuppressed` (read `getSuppressed()`).
- Cause = "same failure caused by"; suppressed = "secondary failure during handling".

Gotchas:
- `return`/`throw` inside `finally` silently swallows the body's exception.
- `catch (Exception e) { throw new RuntimeException(e.getMessage()); }` drops cause and trace. Pass `e`.
- `e.printStackTrace()` bypasses logger and races. Use `log.error("context", e)`.
- Catching `InterruptedException` without re-interrupting (`Thread.currentThread().interrupt()`) breaks cancellation.
- Catching `Throwable` includes `Error`. Almost always wrong.
- Sealed exception hierarchy + pattern-match switch gives compile-time exhaustiveness over domain failures.

## Lambdas and functional interfaces

What it is: SAM-interface instances; compiled to a synthetic method + `invokedynamic` via `LambdaMetafactory`.

| Shape | Interface | Method |
|---|---|---|
| `T -> R` | `Function<T,R>` | `apply` |
| `T -> boolean` | `Predicate<T>` | `test` |
| `T -> ()` | `Consumer<T>` | `accept` |
| `() -> T` | `Supplier<T>` | `get` |
| `(T,U) -> R` | `BiFunction<T,U,R>` | `apply` |
| `(T,T) -> T` | `BinaryOperator<T>` | `apply` |
| `T -> T` | `UnaryOperator<T>` | `apply` |

Primitive specializations avoid boxing: `IntFunction`, `ToIntFunction`, `IntPredicate`, `IntUnaryOperator`, `IntBinaryOperator`, etc.
Composition: `f.andThen(g)`, `f.compose(g)`, `Predicate.and/or/negate`, `Predicate.not(p)`, `Function.identity()` (singleton), `Comparator.comparing(...).thenComparing(...).reversed()`.

- Non-capturing lambda: metafactory-cached, effectively a singleton (free reuse).
- Capturing lambda: new instance per construction.
- Captures effectively-final locals; for mutable state use a 1-element array or `AtomicReference`.

Gotchas:
- Inside a lambda `this` is the enclosing instance (not an inner class) -> can leak the outer object via long-lived listeners.
- Reassigning a captured local is a compile error (effectively-final).
- Standard functional interfaces do not declare checked exceptions: define a `ThrowingFunction` or wrap-as-unchecked inside the lambda.
- `parallelStream().forEach(list::add)` on a non-thread-safe list is a data race; use `collect`.
- Megamorphic call sites (10+ lambda impls through one `apply`) degrade JIT inlining.
- Serializing a lambda needs an intersection target type `(Function<T,R> & Serializable)`.

## Streams

What it is: lazy pipeline over a Spliterator; nothing runs until a terminal op.

| Axis | Examples |
|---|---|
| Intermediate | `map`, `filter`, `flatMap`, `peek`, `distinct`, `sorted`, `limit`, `skip` |
| Terminal | `collect`, `reduce`, `forEach`, `toList`, `count`, `findFirst`, `anyMatch` |
| Stateful | `sorted`, `distinct`, `limit`, `skip` (buffer/coordinate) |
| Short-circuit | `limit`, `findFirst`, `anyMatch`, `noneMatch`, `allMatch` |

Common ops:
- `map(f)` one-to-one transform; `flatMap(f)` one-to-many flattened.
- `reduce(identity, op)` must be associative for parallel correctness.
- `mapToInt/Long/Double` -> primitive stream; unlocks `sum`, `average`, `summaryStatistics`.
- `findFirst` deterministic; `findAny` faster in parallel, non-deterministic.
- `toList()` (since 16) returns an unmodifiable list; preferred over `collect(toList())`.

Collectors:

| Collector | Use |
|---|---|
| `toMap(k, v, merge)` | always supply merge; duplicate keys without it throw |
| `groupingBy(fn)` | `Map<K, List<V>>` |
| `groupingBy(fn, downstream)` | e.g. `groupingBy(User::dept, counting())` |
| `partitioningBy(pred)` | `Map<Boolean, List<T>>`, both keys always present |
| `mapping(f, downstream)` | transform before downstream |
| `teeing(c1, c2, merge)` | two collectors in one pass (since 12) |
| `joining(", ")` | string concat |

parallel() uses `ForkJoinPool.commonPool` (size = cores - 1). Helps when source is sized/splittable (ArrayList, int[], IntStream.range), CPU-bound, associative reducer, work > ~100us. Hurts on LinkedList/unsized, I/O, ordered terminals, stateful collectors.

```java
var pool = new ForkJoinPool(8);
var r = pool.submit(() -> data.parallelStream().map(...).toList()).get();
```

Gotchas:
- `toMap(k, v)` without merge throws on duplicate keys.
- A stream is consumed once; reuse throws IllegalStateException.
- `peek` is for debugging; may be skipped when an element is not needed.
- `sorted()` on an infinite stream hangs (stateful).
- `Stream.iterate(seed, next)` (2-arg) is infinite; 3-arg form with predicate stops.
- Default common pool is shared JVM-wide; use a custom ForkJoinPool for non-trivial parallel work.

## Optional

What it is: `@ValueBased` container for present/absent; intended as a method return type only.

| Method | Returns | Evaluation |
|---|---|---|
| `get()` | T or NoSuchElementException | - |
| `orElse(other)` | T or other | other ALWAYS evaluated |
| `orElseGet(supplier)` | T or supplier.get() | supplier only when empty |
| `orElseThrow(supplier)` | T or thrown | supplier only when empty |
| `map(f)` | `Optional<U>` | f only when present |
| `flatMap(f)` | `Optional<U>` | flattens nested Optional |
| `ifPresentOrElse(c, r)` | void | runnable only when empty (since 9) |
| `or(supplier)` | `Optional<T>` | supplier only when empty (since 9) |
| `stream()` | `Stream<T>` of 0/1 | for flat-mapping |

```java
ids.stream().map(repo::find).flatMap(Optional::stream).toList();
```

Gotchas:
- `orElse(expensive())` always runs `expensive()`; use `orElseGet`.
- `isPresent` + `get` is the universal smell; use `ifPresent`/`map`/`orElse`/`orElseThrow`.
- Do not use as a field or parameter type (not Serializable, extra indirection).
- Do not return `Optional<List<T>>`; return an empty collection.
- `Optional.of(null)` NPEs; use `ofNullable`.
- Never synchronize on or `==`-compare Optionals (value-based).
- Primitive `OptionalInt`/`OptionalLong`/`OptionalDouble` avoid boxing; not subtypes of `Optional<Integer>`.

## equals / hashCode / Comparable

What it is: value-identity and ordering contracts that every hash/sorted collection depends on.

equals contract: reflexive, symmetric, transitive, consistent, `x.equals(null)==false`.
hashCode contract: equal objects -> equal hash codes (converse not required).

```java
@Override public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Point p)) return false;   // pattern match
    return x == p.x && y == p.y;
}
@Override public int hashCode() { return Objects.hash(x, y); }
```

- `instanceof`: lenient, lets subclasses participate (used with composition); `getClass()`: strict, but breaks proxies/CGLIB (Spring, Hibernate).
- Records auto-generate equals/hashCode/toString (same-class, deep equality). Use for value types.
- `Comparable` = single natural order (TreeMap/TreeSet, `Arrays.sort`); `Comparator` = many external orders.
- `compareTo == 0` should imply `equals`. Counter-example: `BigDecimal("2.0").compareTo("2.00")==0` but `.equals` is false -> TreeSet dedups, HashSet does not.

```java
Comparator.comparingInt(User::age).thenComparing(User::name).reversed().nullsFirst(...);
```

Gotchas:
- Override one of equals/hashCode without the other -> broken hash collections.
- Mutable fields in equals/hashCode strand entries after mutation. Prefer stable id (DB key/UUID).
- `instanceof` + subclass adding fields breaks symmetry.
- `Comparator.comparing(Foo::age)` autoboxes; use `comparingInt`/`comparingLong`/`comparingDouble`.
- `Double.compare` handles NaN consistently; raw subtraction does not.
- `compareTo` via `a.size() - b.size()` overflows; use `Integer.compare(...)`.

## Enums

What it is: a fixed finite set of singleton instances; each has `ordinal()` and `name()`.

Strategy via function-valued field (preferred, testable):
```java
enum Op {
    PLUS(Integer::sum), MINUS((a, b) -> a - b);
    private final IntBinaryOperator op;
    Op(IntBinaryOperator op) { this.op = op; }
    public int apply(int a, int b) { return op.applyAsInt(a, b); }
}
```

- `EnumSet` = internal bitset (one/two longs); `contains` is bitwise AND. `EnumMap` = `Object[]` by ordinal, no hashing/boxing. Default to both over hash-based for enum keys.
- Exhaustive switch expression without `default` over an enum -> adding a constant is a compile error.
- Best singleton: `enum X { INSTANCE; }` (reflection-safe, serialization-safe).

Gotchas:
- Never persist `ordinal()` (reorder shifts all stored refs). Persist `name()` or a stable code field.
- `Enum.valueOf("UNKNOWN")` throws IllegalArgumentException; wrap untrusted input in Optional.
- A `default` clause defeats exhaustiveness checking.
- Cross-classloader enum `==` fails (distinct singletons per loader).
- `switch` on a null enum throws NPE (JDK 21: `case null`).
- Mutable enum state is global -> design smell.

## IO and NIO.2

What it is: three coexisting file APIs; use NIO.2 (`Path`/`Files`) for 95% of work.

| Generation | Entry points | Style |
|---|---|---|
| java.io (1.0) | `File`, `FileInputStream`, `Reader`/`Writer` | blocking, IOException |
| java.nio (1.4) | Channels, `ByteBuffer`, Selector | buffer-oriented, non-blocking |
| NIO.2 (1.7) | `Path`, `Files`, `WatchService` | modern facade |

| Need | Call |
|---|---|
| Read whole text | `Files.readString(p, UTF_8)` |
| Stream lines | `try (var s = Files.lines(p, UTF_8))` (must close) |
| Read all bytes | `Files.readAllBytes(p)` |
| Write text atomically | `Files.writeString(p, s, UTF_8, CREATE, TRUNCATE_EXISTING)` |
| Walk tree | `try (var s = Files.walk(p))` |
| Copy/move | `Files.copy(src, dst, REPLACE_EXISTING)`; `ATOMIC_MOVE` |
| Metadata | `Files.readAttributes(p, BasicFileAttributes.class)` |

- ByteBuffer state: `flip()` after write to read (limit=pos, pos=0); `clear()` to write again; `compact()` to keep unread.
- Zero-copy: `FileChannel.transferTo(0, size, target)` uses kernel sendfile/splice (disk to socket without user-space copy).
- Direct buffers (`allocateDirect`) live off-heap, transfer to native without copy, but are expensive to allocate; pool them.
- Modern recipe: virtual thread per request + blocking `Files.*` (no async machinery needed).

Gotchas:
- Always specify charset (UTF-8 default since JDK 18, but `new FileReader(file)` still a smell).
- Forgetting to close `Files.lines`/`Files.walk`/`newDirectoryStream` leaks file handles.
- `Files.readAllBytes` on a huge file -> OutOfMemoryError; stream instead.
- Forgetting `flip()` between write and read -> reads zero bytes.
- `Files.copy` without `REPLACE_EXISTING` throws on existing destination.
- Durability needs explicit `FileChannel.force(true)` / `getFD().sync()`.

## Records, sealed, pattern matching

What it is: algebraic-data-type toolkit (immutable carriers + closed hierarchies + destructuring).

```java
public record Point(int x, int y) {}                 // fields, ctor, accessors, equals/hashCode/toString
public record Money(BigDecimal amount, String ccy) {
    public Money { if (amount == null) throw new IllegalArgumentException(); }  // compact ctor: validate, do not assign
}
public sealed interface Shape permits Circle, Square {}  // permits final/sealed/non-sealed subtypes

String describe(Shape s) {
    return switch (s) {                              // exhaustive, no default
        case Circle(double r) -> "circle " + r;       // record pattern
        case Square sq when sq.side() > 0 -> "square";  // when guard
        case Square sq -> "degenerate";
    };
}
if (obj instanceof String str && !str.isBlank()) { ... }  // instanceof pattern, str scoped in positive branch
```

- Records: implicitly final, extend `Record`, accessors are `x()` not `getX()`, Serializable via canonical ctor, `getRecordComponents()` reflection API.
- Sealed + exhaustive switch: adding a permitted subtype breaks every switch at compile time.

Gotchas:
- Records cannot extend a class or add instance fields; use a sealed interface for hierarchy.
- Compact constructor validates/normalizes; do NOT write `this.x = x` (already done).
- Mutable components leak: `record Holder(List<String> items)` shares the list. Defensive-copy in ctor and accessor.
- Not for JPA entities (need no-arg ctor, mutability, lifecycle).
- `default` over a sealed type loses exhaustiveness.
- `case null` must be explicit or NPE before any pattern matches.
- Record patterns require the exact declared component types.
- Pattern dominance: a broader case before a narrower one is a compile error.

## Dates and times (java.time)

What it is: immutable, thread-safe JSR-310 types replacing Date/Calendar/SimpleDateFormat.

| Type | Represents | Use for |
|---|---|---|
| `Instant` | point on timeline, UTC nanos | logs, audit, storage |
| `LocalDate` | date, no time/zone | birthdays, fiscal periods |
| `LocalTime` | time of day, no zone | schedules |
| `LocalDateTime` | date+time, no zone | often the wrong choice |
| `OffsetDateTime` | date+time+offset | wire protocols |
| `ZonedDateTime` | date+time+zone | display, DST-aware scheduling |
| `Duration` | time amount (s/nanos) | timeouts, elapsed |
| `Period` | date amount (y/m/d) | "add 3 months" |

Rule: store `Instant` (UTC), convert to `ZonedDateTime` only for display (zones change; IANA tz db ships via JRE).

```java
nowUtc.atZone(ZoneId.of("Europe/Lisbon"));
Clock fixed = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneId.of("UTC"));
Instant now = Instant.now(fixed);   // inject Clock for deterministic tests
```

- `Duration.between(instant1, instant2)` exact; `Period.between(date1, date2)` calendar-aware (y/m/d).
- DST gap: `ZonedDateTime.of(...)` shifts forward; use `withEarlierOffsetAtOverlap`/`withLaterOffsetAtOverlap` for overlaps.

Gotchas:
- `LocalDateTime` for storage is ambiguous (no zone).
- `SimpleDateFormat` is not thread-safe; `DateTimeFormatter` is (cache `static final`).
- Pattern case matters: `mm` minute vs `MM` month vs `MMM` month-text.
- `Duration.ofDays(1)` on Instant adds exactly 86400s (not calendar-aware); use `ZonedDateTime.plusDays(1)`.
- `Period` arithmetic clamps and is non-commutative: `Jan31 + 1 month = Feb28`.
- `LocalDate.now()` uses JVM default zone; pass explicit `ZoneId`.
- Jackson default writes arrays for `LocalDateTime`; register `JavaTimeModule`, disable `WRITE_DATES_AS_TIMESTAMPS`.
- `Instant` does not count leap seconds.

## Modules (JPMS)

What it is: encapsulation unit above the package; `module-info.java` declares dependencies, exports, services.

| Directive | Meaning |
|---|---|
| `requires X` | read X's exported packages |
| `requires transitive X` | plus re-export X to my readers |
| `requires static X` | compile-time only, optional at runtime |
| `exports p` | public types of p accessible to all readers |
| `exports p to M1, M2` | qualified export |
| `opens p` | deep reflection at runtime (Spring/Hibernate/Jackson) |
| `opens p to M` | restricted reflective access |
| `uses S` | I `ServiceLoader.load(S)` |
| `provides S with Impl` | I supply impl of service S |

Module flavours: named (has `module-info`, strong encapsulation), automatic (plain JAR on modulepath, `Automatic-Module-Name` manifest, reads/exports all), unnamed (classpath, reads all, unreadable by named `requires`).

```text
--add-opens java.base/java.lang=ALL-UNNAMED   # runtime reflection escape hatch
--add-exports                                 # compile-time package exposure
```

Gotchas:
- "does not read" = you required it but the source did not export the package (or only `exports ... to` others).
- "module not found" = JAR on classpath not modulepath.
- Reflection on a non-`open` package throws InaccessibleObjectException (since 17); add `--add-opens` or `open`.
- Split packages (same package across two modules) are illegal.
- `requires` cycles forbidden; refactor to a shared module.
- `exports` (compile + runtime public API) is not `opens` (runtime reflective incl. non-public).
- Spring Boot needs `--add-opens` for reflection on records/entities; the maven plugin adds them for `spring-boot:run` but not bare `java -jar`.

---

## JVM memory model

What it is: separate memory regions (heap, stacks, Metaspace, code cache, direct/native) each with its own OOM mode and tuning flags.

| Region | Flag | OOM message | Default | Notes |
|---|---|---|---|---|
| Heap | -Xms/-Xmx | "Java heap space" | -Xmx ~= 1/4 RAM | set -Xms == -Xmx in prod |
| Stack (per thread) | -Xss | StackOverflowError / "unable to create new native thread" | ~1MB | each platform thread costs ~1MB |
| Metaspace | -XX:MaxMetaspaceSize | "Metaspace" | unbounded (danger) | native; class metadata, bytecode, const pool |
| Code cache | -XX:ReservedCodeCacheSize | silent slowdown | 120-240MB | full -> JIT stops, interpreter fallback |
| Direct/mapped buffers | -XX:MaxDirectMemorySize | "Direct buffer memory" | = -Xmx | NIO/Netty; reclaimed via Cleaner |
| Native (JNI, GC) | none | invisible to heap dumps | unbounded | visible only via NMT |

```bash
java -Xms4g -Xmx4g -Xss512k -XX:MaxMetaspaceSize=512m \
     -XX:MaxDirectMemorySize=512m -XX:ReservedCodeCacheSize=256m \
     -XX:NativeMemoryTracking=summary -XX:+HeapDumpOnOutOfMemoryError \
     -XX:+ExitOnOutOfMemoryError -jar app.jar
jcmd <pid> VM.native_memory summary
jcmd <pid> GC.heap_dump /tmp/heap.hprof
```

- Object header ~12 bytes (compressed oops under ~32GB); references 4 bytes compressed, 8 above 32GB.
- Compressed oops disabled above ~32GB heap (utilization can worsen there).
- `MappedByteBuffer` is not bounded by MaxDirectMemorySize (lives in OS page cache).

Gotchas:
- RSS > heap-dump size does not mean a heap leak; check Metaspace, direct, native (NMT).
- `-XX:+DisableExplicitGC` breaks direct-buffer reclaim under pressure.
- Classloader leaks show up as Metaspace OOM (caching `Class<?>` in statics, JDBC drivers not deregistered, thread-context-loader retention).
- Container limit must exceed -Xmx + Metaspace + direct + code cache + ~1MB*threads + overhead (rule of thumb: -Xmx * 1.5-2).

## Garbage collectors (G1 / ZGC / Parallel)

What it is: GC strategies trading pause vs throughput; G1 default since JDK 9; ZGC generational since JDK 21.

| GC | Pause goal | Throughput | Heap range | When |
|---|---|---|---|---|
| Serial | n/a (single-threaded) | low | < 100MB | tiny containers, CLIs |
| Parallel | scales with live set | highest | any | batch, max-throughput |
| G1 (default) | ~200ms target, predictable | high | 1GB to dozens GB | default for services; region-based |
| ZGC (generational) | < 1ms typical | slightly below G1 | 100MB to 16TB | latency-critical, large heaps |

- G1: regions 1-32MB (`-XX:G1HeapRegionSize`); humongous objects (>50% region) go to old gen and fragment; Full GC is the failure mode (tune `-XX:InitiatingHeapOccupancyPercent` lower).
- ZGC: colored pointers + load barriers, constant-time pauses; `-XX:SoftMaxHeapSize` = normal working set, grows to -Xmx only under pressure.

```bash
# G1
java -XX:+UseG1GC -Xms4g -Xmx4g -XX:MaxGCPauseMillis=200 \
     -XX:InitiatingHeapOccupancyPercent=35 -Xlog:gc* -jar app.jar
# ZGC
java -XX:+UseZGC -Xms8g -Xmx8g -XX:SoftMaxHeapSize=6g -Xlog:gc* -jar app.jar
```

Gotchas:
- `-XX:MaxGCPauseMillis=10` shrinks young gen, raises GC frequency and CPU; there is a floor.
- Humongous objects (e.g. `new byte[8MB]` on 16MB region) fragment old gen; raise region size or chunk the buffer.
- `System.gc()` without `-XX:+ExplicitGCInvokesConcurrent` triggers a Full GC.
- Promotion failure (to-space exhausted) -> Full GC; increase heap/reserve.
- Soft-reference behavior differs by GC; fragile as a cache-eviction mechanism.

## JIT and escape analysis

What it is: tiered compilation (interpreter -> C1 -> C2); escape analysis proves bounded lifetime so allocations can be elided; speculation + deopt.

```text
Interpreter --~1500 calls--> C1 (profiling) --~10000 calls--> C2 (aggressive)
                                                                    |
                                              deopt on failed speculation -> interpreter
```

C2 optimizations:
1. Inlining: callees < `-XX:MaxInlineSize=35` bytes (hot: `-XX:FreqInlineSize=325`); enabler of everything else.
2. Escape analysis: NoEscape (scalar-replaced, no allocation), ArgEscape (lock elision eligible), GlobalEscape (heap).
3. Scalar replacement: `new Point(x,y)` becomes two locals; no GC pressure.
4. Lock elision: `synchronized` on a NoEscape object removed.
5. Speculation + deopt: assumes monomorphic type/branch; on miss, deopt to interpreter.

```bash
java -XX:+UnlockDiagnosticVMOptions -XX:+PrintCompilation -XX:+PrintInlining -jar app.jar
java -XX:-DoEscapeAnalysis -jar app.jar   # measure EA effect only, do NOT ship
```

Gotchas:
- Megamorphic call sites (3+ impls) lose inlining -> kill EA -> GC pressure.
- Long warmup: p99 in first minutes can be 10x steady-state; mitigate with CDS/AOT/synthetic warmup.
- OSR (on-stack replacement) compiles hot loops mid-run but is less optimized; avoid heavy work in one giant loop.
- Autoboxing/logging-with-args in a hot loop boxes -> the boxed object escapes -> defeats scalar replacement.
- Code-cache exhaustion silently disables JIT.
- A single rare exception can cause repeated deopt cycles (check JFR `jdk.Deoptimization`).

## java.util.concurrent

What it is: executors (thread pools), locks, and concurrent collections for safe multithreading.

Pool fill order (the key trap): start core thread if below corePoolSize; else enqueue if queue accepts; else start non-core up to maximumPoolSize; else RejectedExecutionHandler. An unbounded queue means the queue never fills, so maximumPoolSize is dead and the queue grows until OOM.

| Factory | Queue | Trap |
|---|---|---|
| `newFixedThreadPool(n)` | unbounded LinkedBlockingQueue | OOM under burst; pool never grows |
| `newCachedThreadPool()` | SynchronousQueue | unbounded threads -> resource exhaustion |
| `newScheduledThreadPool(n)` | delay queue | periodic/delayed tasks |
| `newWorkStealingPool()` | per-worker deques | divide-and-conquer |
| `newVirtualThreadPerTaskExecutor()` | none | I/O-bound, high concurrency (JDK 21) |

```java
var exec = new ThreadPoolExecutor(8, 16, 60, TimeUnit.SECONDS,
    new ArrayBlockingQueue<>(1000),
    Thread.ofPlatform().name("worker-", 0).factory(),
    new ThreadPoolExecutor.CallerRunsPolicy());   // backpressure
// shutdown(): then awaitTermination(...), then shutdownNow() on timeout
```

| Lock | Features | Pins VT? | When |
|---|---|---|---|
| `synchronized` | JVM-intrinsic | yes | simple, no timeout/fairness needed |
| `ReentrantLock` | `tryLock(timeout)`, fairness, `Condition`, interruptible | no | timeout/fairness/VT-compat |
| `ReentrantReadWriteLock` | many readers XOR one writer | no | read-heavy (writer-starvation risk) |
| `StampedLock` | optimistic lock-free reads via stamp | no | read-heavy, non-reentrant |
| `Semaphore` | N permits | no | bulkhead / rate-limit |
| `CountDownLatch` | one-shot, wait count -> 0 | no | wait for N events |
| `CyclicBarrier` / `Phaser` | reusable / dynamic barrier | no | wait for all participants |

Concurrent collections: `ConcurrentHashMap` (lock-striped), `CopyOnWriteArrayList` (read-mostly, O(n) write), `ConcurrentLinkedQueue` (lock-free, unbounded), `ArrayBlockingQueue` (bounded backpressure), `LinkedBlockingQueue` (usually unbounded).

Gotchas:
- `newFixedThreadPool`/`newCachedThreadPool` -> OOM or thread explosion; use a custom `ThreadPoolExecutor` with a bounded queue.
- `ConcurrentHashMap.computeIfAbsent` reentrant on the same key/thread can deadlock.
- `synchronized` pins virtual threads; migrate hot blocks to `ReentrantLock`.
- `StampedLock` is not reentrant -> re-acquire on same thread deadlocks.
- `CountDownLatch` cannot reset; use `Phaser`.
- `Future.get()` without timeout in business code hangs the service.
- `Collections.synchronizedMap` still needs external sync while iterating.
- `volatile` is not a substitute for a lock on read-modify-write.

## CompletableFuture

What it is: composable async pipeline where each stage's executor decides which thread runs it.

| Suffix | Runs on | Risk |
|---|---|---|
| `xxx(...)` | whoever completed the previous stage | may hijack an I/O thread |
| `xxxAsync(...)` | `ForkJoinPool.commonPool` | shared (cores-1); one blocking call starves all |
| `xxxAsync(..., executor)` | your executor | standard; use almost always |

Methods:
- Source: `supplyAsync(supplier, exec)`, `runAsync`, `completedFuture(v)`, `failedFuture(t)`.
- Map: `thenApply` (transform), `thenAccept` (consume), `thenRun` (side-effect).
- Compose (flatMap): `thenCompose(fn -> CompletionStage)` flattens `CF<CF<U>>`.
- Combine: `thenCombine(other, BiFunction)`, `allOf(...)`, `anyOf(...)`.
- Error: `exceptionally(fn)` (fallback), `handle((v,e)->...)` (recover/transform either way), `whenComplete((v,e)->...)` (peek, pass through).
- Timeout: `orTimeout(d,u)`, `completeOnTimeout(v,d,u)`.

```java
Throwable cause = (t instanceof CompletionException ce) ? ce.getCause() : t;  // always unwrap
```

Gotchas:
- Non-Async stages run on whoever completed the source (could be an HTTP event loop); blocking there can deadlock.
- commonPool defaults to cores-1; on a 2-vCPU box that is 1 worker -> any blocking freezes all CFs and parallel streams.
- `cancel(true)` does NOT interrupt the running task; it only marks downstream cancelled.
- `allOf(...).join()` rethrows the first exception; sibling results are lost unless captured per-future.
- `exceptionally`/`handle` see a `CompletionException` wrapper; unwrap before `instanceof`.
- `join()` throws unchecked `CompletionException`; `get()` throws checked `ExecutionException`.
- Returning `null` from a stage silently propagates -> downstream NPE.
- MDC/ThreadLocal does not follow stages across thread switches; copy and re-set explicitly.

## Virtual threads and structured concurrency

What it is: lightweight JVM-scheduled continuations on a small carrier pool, enabling millions of concurrent blocking tasks (JDK 21).

| | Platform thread | Virtual thread |
|---|---|---|
| Mapping | 1:1 OS thread | user-mode continuation on carrier |
| Stack | ~1MB pre-allocated | few hundred bytes + dynamic heap stack |
| Scheduling | kernel | JVM onto carrier pool (= availableProcessors) |
| Count | thousands | millions |
| Unmounts on | n/a | NIO, `LockSupport.park`, `Thread.sleep`, `ReentrantLock` |

```java
Thread.ofVirtual().start(runnable);
var exec = Executors.newVirtualThreadPerTaskExecutor();
try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {  // or ShutdownOnSuccess
    var f = scope.fork(callable);
    scope.join().throwIfFailed();
}
```

Gotchas:
- Not faster per task; throughput scales under I/O concurrency, latency is unchanged.
- Do NOT pool virtual threads (re-introduces contention).
- Pinning: `synchronized` block or JNI frame blocks the carrier -> carrier starvation. Migrate hot `synchronized` to `ReentrantLock`. Detect with `-Djdk.tracePinnedThreads=full` or JFR `jdk.VirtualThreadPinned`.
- `ThreadLocal` scales poorly at million-VT scale; prefer `ScopedValue`.
- A bounded DB pool still bottlenecks; 100k VTs on 20 connections gains nothing.
- Carrier count = availableProcessors, often wrong in containers; override `-Djdk.virtualThreadScheduler.parallelism=N`.
- `setName` is a no-op on a VT; use `threadId()` for diagnostics.

## Atomic and VarHandle

What it is: CAS-based atomic types and tunable memory-access modes that trade contention for throughput.

- CAS: `compareAndSet(expected, new)` is a single atomic linearization point (maps to `lock cmpxchg` / LL-SC).
- `AtomicInteger`/`AtomicLong`: fine read-heavy; retry storms under contention. `compareAndExchange` (9+) returns the witness, no re-read.
- `LongAdder`/`DoubleAdder`/`LongAccumulator`: striped cells, `sum()` reads all; for high-write metrics, NOT for invariant checks (sum is not a linearization point).

VarHandle access modes (weakest to strongest): Plain (no ordering) -> Opaque (atomic, no ordering) -> Acquire/Release (one-way fence, cheaper than volatile, fine for producer/consumer handoff) -> Volatile (full sequential consistency, slowest). CAS modes default to volatile semantics.

```java
private volatile Object value;
private static final VarHandle VALUE =
    MethodHandles.lookup().findVarHandle(Holder.class, "value", Object.class);
VALUE.setRelease(this, v);
Object o = VALUE.getAcquire(this);
```

- ABA: `compareAndSet(A, C)` succeeds even if value went A->B->A. Use `AtomicStampedReference` (value + version) for lock-free structures.

Gotchas:
- "increment is atomic" but "read then act" is not.
- `LongAdder.sum()` is not a linearization point; do not use for invariants.
- Store the VarHandle in `static final`; recreating per call defeats inlining.
- `volatile` on a reference array makes the reference volatile, not the elements; use `AtomicReferenceArray`.
- Non-public field VarHandle needs `MethodHandles.privateLookupIn` and possibly `--add-opens`.
- High-contention `incrementAndGet` on one `AtomicLong` does not scale -> switch to `LongAdder` (measured roughly 1.2s vs 0.15s for 16 threads x 10M).

## Happens-before and memory visibility

What it is: the partial order guaranteeing one action's effects are visible to another; violations allow stale or partially-constructed reads (especially on ARM).

Happens-before edges:
1. Program order within one thread.
2. Monitor: unlock on M before subsequent lock on M.
3. volatile: write before subsequent read of same field.
4. `Thread.start()` before the started thread's actions.
5. `Thread.join()`: thread's actions before join returns.
6. `interrupt()` before detection.
7. final-field writes in constructor before the reference is published (if `this` does not escape).
8. Transitivity.

j.u.c. mirrors these: `Lock.unlock/lock`, `Semaphore.release/acquire`, `CountDownLatch.countDown/await`, queue `offer/poll`.

```java
private volatile Holder instance;   // correct double-checked locking
Holder get() {
    Holder h = instance;
    if (h == null) synchronized (this) {
        h = instance;
        if (h == null) instance = h = new Holder();
    }
    return h;
}
```

Safe publication: write to a volatile field, store/read under the same lock, store in a final field (this not escaped), or pass through a j.u.c. collection.

Gotchas:
- x86 is TSO so bugs hide; ARM exposes them.
- `volatile` on a reference does not make the referent's fields volatile.
- `this` escaping the constructor destroys the final-field guarantee.
- Double-checked locking without `volatile` can publish a partially constructed object.
- A non-volatile spin-loop flag can be hoisted by the JIT -> infinite busy wait; make it volatile or use `LockSupport.park`.
- `synchronized(this)` exposes the lock; prefer a private final lock object.
- Chained atomics across multiple fields are not atomic together.
- Test JMM races with jcstress (reports ACCEPTABLE / FORBIDDEN outcomes).

## Reflection and method handles

What it is: three dispatch mechanisms trading type safety and speed.

| Mechanism | Type safety | Performance | Use |
|---|---|---|---|
| `java.lang.reflect` | erased to Object | slower (JEP 416 reimplemented on MH) | frameworks, plugins, legacy |
| `MethodHandle` | typed via `MethodType` | near-direct after JIT (`invokeExact`) | modern dynamic invocation, indy |
| `LambdaMetafactory` | statically typed lambda | identical to direct call | hot-path stable functional ifaces |

```java
var lookup = MethodHandles.lookup();
MethodHandle mh = lookup.findVirtual(String.class, "substring",
    MethodType.methodType(String.class, int.class, int.class));
String r = (String) mh.invokeExact("hello", 1, 4);   // exact signature required
```

Module access (enforced 17+): reflection on non-public members needs the package `open` to the caller, or `--add-opens java.base/java.lang=ALL-UNNAMED`. Use `MethodHandles.privateLookupIn(target, lookup)` for private members same module.

Gotchas:
- `setAccessible(true)` no longer bypasses module encapsulation (17+); without `--add-opens` -> InaccessibleObjectException.
- `Method.invoke` boxes primitives (allocation in hot loops); `invokeExact` does not.
- `invokeExact` is strict: signature/return type mismatch -> WrongMethodTypeException.
- `getMethod` finds only public; `getDeclaredMethod` only the declaring class (walk the hierarchy yourself).
- `Class.forName(name, true, loader)` runs static initializers (side effects).
- Cache `Method`/`MethodHandle` in `static final`; first reflective calls are ~10x slower.
- Lambda-generated classes are hidden (JEP 371), not normally reflectable.

## Annotation processing

What it is: compile-time javac plugins that generate sources/resources, removing runtime reflection (and GraalVM-friendly).

Runs in rounds until no new annotations appear; can emit sources/resources/diagnostics but cannot modify existing source.

```java
@SupportedAnnotationTypes("com.example.Builder")
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class BuilderProcessor extends AbstractProcessor {
    @Override public boolean process(Set<? extends TypeElement> annos, RoundEnvironment env) {
        for (Element e : env.getElementsAnnotatedWith(Builder.class)) {
            if (e.getKind() != ElementKind.CLASS) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "classes only", e);
                continue;
            }
            // processingEnv.getFiler().createSourceFile(fqn).openWriter()
        }
        return true;   // claim these annotations
    }
}
```

- Register via `META-INF/services/javax.annotation.processing.Processor`, `@AutoService`, or `-processor`. Maven: `maven-compiler-plugin` `annotationProcessorPaths`.
- Navigate the mirror API (`TypeElement`/`ExecutableElement`/`VariableElement`/`TypeMirror`), never `Class<?>` (source not yet compiled).
- Used by MapStruct, Dagger, Immutables, AutoValue, Hibernate model gen, Micronaut. Generate code with JavaPoet.

Gotchas:
- Returning `true` claims annotations so other processors will not see them; return `false` if only observing.
- `createSourceFile` twice with the same name throws FilerException; track emissions per round.
- Do not emit new source after `processingOver()` (final round).
- `Class.forName` in a processor cannot see the class being compiled.
- Lombok-style bytecode mutation is not annotation processing; it hooks compiler internals and breaks across JDKs.
- Generic type info IS available at compile time (DeclaredType with full args); use it rather than assuming erasure.

## Class loaders

What it is: parent-first delegation chain loading bytecode (bootstrap -> platform -> system -> custom).

```text
Bootstrap (null in API; java.base, java.*)
  -> Platform (java.sql, java.xml, ...)
    -> System/Application (classpath / modulepath)
      -> Custom (plugins, hot reload)
```

- Parent-first: ask parent before loading locally; only on miss does the child `findClass`.
- A class's identity is name + loader. Same FQN under two loaders are different types (`instanceof` false, CCE on assignment).

| Symptom | Cause | Type |
|---|---|---|
| `ClassNotFoundException` | reflection/forName could not find it | checked |
| `NoClassDefFoundError` | missing at runtime OR prior static-init failure | unchecked |
| `LinkageError` family | compiled vs runtime version mismatch | unchecked |

```text
jcmd <pid> VM.classloaders
jcmd <pid> VM.class_hierarchy com.example.Foo
```

Gotchas:
- A loader is GC'd only when no live instances, no live `Class<?>`, and no ThreadLocal/static refs in parent loaders.
- ThreadLocal on a pooled thread holding a child-loaded class pins the child loader forever (classic Metaspace leak).
- Hot reload without restart leaks loaders; same JAR on two loaders doubles Metaspace and JIT work.
- `Class.forName("X")` uses the caller's loader; use the 3-arg form to choose.
- `ServiceLoader` uses the thread-context loader by default -> empty results if it is wrong.
- A custom loader must override `getClassLoadingLock` to avoid concurrent-load deadlock.
- A class whose static initializer threw is permanently unusable.

## JMH benchmarking

What it is: microbenchmark harness that defeats JIT dead-code/constant-folding so you measure real work.

| Mode | Reports |
|---|---|
| Throughput | ops per time unit (default) |
| AverageTime | time per op |
| SampleTime | sampled distribution (p99/p999) |
| SingleShotTime | one invocation (cold start) |

```java
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Param({"16", "1024", "65536"})
@Benchmark public int bench(Blackhole bh) { ... }   // return or bh.consume()
```

Profilers: `-prof gc`, `-prof perfasm`, `-prof stack`, `-prof async`.

Gotchas:
- A `void` method with no `Blackhole.consume` is eliminated as dead code -> measures an empty loop.
- Missing `@State` allows constant folding and recreates state per invocation (allocation noise).
- `@Fork(0)` leaks JIT state between trials; use `@Fork >= 1` (ideally 2+).
- An internal loop inside `@Benchmark` unrolls differently than JMH iteration; let JMH iterate.
- `System.nanoTime()` inside the body adds ~30-100ns overhead.
- Comparing different output time units is meaningless; noisy machines (laptop/container/CI) swing 30%+.
- `@Setup(Level.Invocation)` serializes setup with measurement and inflates time.

## JFR and async-profiler

What it is: JFR is a built-in low-overhead (1-2%) structured event recorder; async-profiler is a sampling profiler with flame graphs.

| Investigation | Tool | Why |
|---|---|---|
| Production p99 anomaly, historical | JFR | always-on, low overhead, structured |
| CPU 100% | async-profiler -e cpu | best flame graphs, native frames |
| Allocation hot spots | async-profiler -e alloc | per-site, sampled |
| Lock contention | async -e lock / JFR jdk.JavaMonitorEnter | identifies contended monitors |
| GC behavior | JFR | best GC event set |
| Virtual-thread pinning | JFR jdk.VirtualThreadPinned | only source |
| Off-CPU / blocked | async-profiler -e wall | includes sleeping threads |

```text
java -XX:StartFlightRecording=filename=app.jfr,maxsize=1g,maxage=24h,dumponexit=true -jar app.jar
jcmd <pid> JFR.start name=incident settings=profile duration=2m filename=/tmp/i.jfr
jfr print --events GarbageCollection /tmp/i.jfr
./profiler.sh -d 30 -e cpu -f cpu.html <pid>
```

```java
@Label("Order Processed") @Category("App")
class OrderProcessedEvent extends jdk.jfr.Event { @Label("Order ID") long orderId; }
// e.begin(); ...; e.orderId = id; e.commit();
```

Gotchas:
- JFR default does not enable allocation profiling; add `jdk.ObjectAllocationSample`.
- `settings=profile` has higher overhead; not for permanent recording. `OldObjectSample` is costly and off by default.
- async-profiler in Docker without SYS_ADMIN falls back to itimer and misses kernel frames.
- Analyze JFR on a dev machine, not the loaded JVM (JMC is itself Java).
- Forgetting `dumponexit=true` loses the recording on crash; `.jfc` settings files override programmatic settings.

## Native interop (JNI vs Panama FFM)

What it is: JNI is the old hand-written C glue; FFM (Foreign Function and Memory, JEP 442) does typed native calls without C (preview in 21, final 22).

```java
Linker linker = Linker.nativeLinker();
MethodHandle strlen = linker.downcallHandle(
    linker.defaultLookup().find("strlen").orElseThrow(),
    FunctionDescriptor.of(JAVA_LONG, ADDRESS));
try (Arena a = Arena.ofConfined()) {
    MemorySegment s = a.allocateUtf8String("hello");
    long len = (long) strlen.invoke(s);
}
```

| Arena | Lifetime | Thread |
|---|---|---|
| `ofConfined()` | until close() | creating thread only |
| `ofShared()` | until close() | any |
| `ofAuto()` | GC-managed | any |
| `global()` | JVM lifetime | any |

- `jextract` generates Java bindings from C headers.
- Decision: new code on 21+ or system libs (libc/OpenSSL) -> FFM + jextract; existing JNI or heavy JVM callbacks -> JNI.

```text
javac --enable-preview --release 21 Foo.java
java  --enable-preview --enable-native-access=ALL-UNNAMED Foo
```

Gotchas:
- JNI `GetByteArrayElements` may copy or pin (GC-stopping); never assume zero-copy. Critical sections disable GC; keep < 100us.
- A native crash (segfault) kills the whole JVM; native malloc is invisible to NMT.
- FFM `Arena.ofConfined` accessed from another thread -> WrongThreadException.
- FFM in 21 is preview; APIs changed across 20/21/22 (pin the JDK in CI).
- Use `--enable-native-access=<module>` (not ALL-UNNAMED) in production.
- Calling FFM from a virtual thread pins the carrier.
- `MemorySegment` defaults to platform endianness; set `ValueLayout.JAVA_INT.withOrder(...)` explicitly.

## Sandboxing (post SecurityManager)

What it is: SecurityManager is deprecated (JEP 411, JDK 17); modern isolation is a portfolio of OS/module/process boundaries.

| Concern | Mechanism |
|---|---|
| File/network access | OS sandbox: seccomp, AppArmor, SELinux, container mounts |
| Untrusted execution | separate process / container |
| Native code | `--enable-native-access=<module>` |
| Reflection on JDK internals | modules + `--add-opens` (audit each) |
| Dynamic agent attach | `-XX:+EnableDynamicAgentLoading=false` (JEP 451) |
| Deserialization gadgets | JEP 290 serial filters (`-Djdk.serialFilter`) |
| Plugin trust | signed JARs + classloader + well-defined API |

```text
java --enable-native-access=com.example.app \
     -XX:+EnableDynamicAgentLoading=false \
     -Djdk.serialFilter='maxdepth=20;maxrefs=1000;java.base/**;com.example.**;!*' \
     -Djava.security.manager=disallow -jar app.jar
```

```java
var filter = ObjectInputFilter.Config.createFilter(
    "maxdepth=20;maxrefs=10000;java.base/**;com.example.Allowed;!*");  // deny-by-default
ois.setObjectInputFilter(filter);
```

Gotchas:
- Do not rely on SecurityManager in 2026 (deprecated + CVEs); `java.policy` files are inert without it.
- Java serialization is a CVE goldmine; prefer JSON/Protobuf and remove it entirely where possible.
- `--add-opens java.base/java.lang=ALL-UNNAMED` is a legacy-code sign; audit and constrain.
- Signing proves origin, not safety; combine with allow-listing.
- A custom classloader without process isolation is not a sandbox (same address space; the plugin can spawn threads, allocate unbounded, call `System.exit`).
- `-javaagent` from a user env var is a privilege-escalation hole.
- Disabling deserialization while RMI/JMX/LDAP are exposed defeats the filter.
- Recommended untrusted-plugin model: out-of-process (IPC over stdin/stdout) in a container with seccomp/AppArmor and prlimit CPU/memory caps.
