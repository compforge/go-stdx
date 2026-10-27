# go-stdx

Go stdlib extensions — **admitted only when the standard library doesn't offer it.**

The long-term ambition is the slot Guava fills in Java: the utility layer a project reaches for before hand-writing a helper. Go's ecosystem splits that slot today — [samber/lo](https://github.com/samber/lo) owns generic collection transforms, [gods](https://github.com/emirpasic/gods) owns data structures, [lancet](https://github.com/duke-git/lancet) goes kitchen-sink — and go-stdx competes on discipline, not breadth: stdlib-mirror naming, and only the small, common data operations every service would otherwise re-wrap.

The name is the admission rule: a hand-rolled `max`, a `slices.Clone` re-implementation, or a `strconv` wrapper does not belong here — use stdlib. What earns a slot is the three-to-five-line wrapper real projects keep re-writing because the stdlib deliberately omits it. Generating the underlying data (a UUID, a hash) is *not* our job — we lean on a mature library for that and wrap only the shape callers pass around.

Subpackages mirror stdlib naming so call sites read like the standard library they extend:

| package | what | why not stdlib / a lib |
|---|---|---|
| `iterx` | `NewLoadingIterator`, `Batch`, `LoadingSource`, `UniqueKeys` — lazy pages, stream batching, keyed lookup and identity deduplication | stdlib defines the iteration protocol; it does not load or look up external data |
| `slicesx` | `Uniq`, `UniqBy` — first-occurrence, order-preserving dedup | the "seen map" loop everyone re-writes; stdlib's `slices` has no transform family |
| `stringsx` | `Truncate` / `TruncateEllipsis` — byte-budget cuts; `FirstNonBlank`; `SplitAndTrim` | the log-truncation and human-written-list helpers every service re-writes (`cmp.Or` covers non-empty, not non-blank) |
| `osx` | `EnvStr` / `EnvBool` / `EnvInt` / `EnvInt64` / `EnvDuration` — typed env lookups with defaults; `WriteFileAtomic` — temp+rename write | config-from-env boilerplate in every service; `os.WriteFile` can leave readers a torn file |
| `ptrx` | `To` / `Value` / `FormatOr` — optional-field pointer helpers | no stdlib answer to `&literal` (k8s.io/utils/ptr exists because of the gap) |
| `filepathx` | `DirBytes` — recursive regular-file byte count | the quota/GC accounting walk everyone re-writes |
| `tarx` | `PackDir` / `UnpackDir` — directory ⇄ tar.gz with zip-slip defense | `archive/tar` leaves both loops to the caller, and the extraction loop is famously easy to get wrong |
| `shellx` | `Quote` — POSIX single-quoting | Go has no `shlex`; unquoted interpolation into a shell line is an injection |
| `netx` | `IsDNSHostname` — ASCII DNS hostname syntax without IP literals | `net` parses IP addresses but does not export hostname validation |
| `randx` | `Hex(n)` — n random bytes as lowercase hex | the "short random id" helper every daemon re-writes |
| `uuid` | `New`, `NewWithPrefix`, `V4`, `V7`, `V7Hex` — resource / random / time-ordered ids | thin wrappers over `google/uuid` for the resource ID, string, and dashless-hex shapes services keep re-wrapping |
| `timeline` | operation/stage interfaces and detached, structured snapshots | joins stages from independent processes by business ID; snapshots retain intervals, hierarchy, actors and results |

Rules of the house:

- **Don't reinvent the data, wrap the shape**: no chasing zero dependencies — a mature foundational library (`google/uuid`, …) is a fine dependency. What we add is the small wrapper around it, not a re-implementation of it.
- **stdlib-first**: when Go's standard library grows an equivalent, the entry here is deprecated and removed.
- **Positioning is the bar**: anything genuinely generic that projects would otherwise hand-write belongs here — no waiting for N copies to accumulate first.

```go
import (
	"github.com/compforge/go-stdx/osx"
	"github.com/compforge/go-stdx/randx"
	"github.com/compforge/go-stdx/slicesx"
	"github.com/compforge/go-stdx/uuid"
)

port := osx.EnvInt("APP_PORT", 8080)
id := "job-" + randx.Hex(6)
resourceID := uuid.NewWithPrefix("job")
ids := slicesx.Uniq(rawIDs)
```

Used by [case-code-review](https://github.com/qiankunli/case-code-review), [hostel](https://github.com/qiankunli/hostel), and other Go projects under this account.

## Lazy loading iterators

Supply a page loader and consume its results one item at a time:

```go
items := iterx.NewLoadingIterator(ctx, loadNextPage)
for item, err := range items {
    if err != nil {
        return err
    }
    process(item)
}
```

Import `github.com/compforge/go-stdx/iterx`. The loader has signature
`func(context.Context) ([]T, bool, error)`; the boolean says whether another page
may exist. It owns filters, page size and cursor state. Give each iterator its own
loader state and close query resources before returning a page.

Construction performs no I/O. Iteration loads synchronously, holds one batch at a
time and loads the next only after the current batch is consumed. The final
batch is yielded even when the boolean is false. Empty pages with more results
are allowed if the loader advances its cursor. Errors discard the failed page,
yield `(zero T, err)` once and stop; context cancellation stops loading and item
delivery. There is no prefetch, background worker or automatic retry.

`LoadingIterator[T]` aliases `iter.Seq2[T, error]`. Each constructed iterator is
single-use and must not be traversed concurrently. An early `break` ends that
instance; subsequent traversals yield nothing. Restart with a fresh loader and
iterator. A fetched page cursor is not a processed-item checkpoint: after an early
break it may be ahead of the items actually consumed. Cross-round checkpoints and
business retries belong to the caller. A nil loader returns nil, which must not
be ranged.

See the [executable example](iterx/example_test.go).

## Loading sources and unique keys

A `LoadingSource[K, V]` combines candidate enumeration with independent key
lookups. Choose the constructor that matches the data you already have:

| Constructor | Input | Enumeration |
|---|---|---|
| `NewLoadingSource(ctx, page, mget, key)` | Value pages, batch lookup, identity function | Extract keys lazily with `key(V) K` |
| `NewLoadingSourceFromKeys(ctx, page, mget)` | Key pages and batch lookup | Yield keys without loading values |
| `NewLookupSource(ctx, mget)` | Batch lookup only | Empty, non-nil iterator |
| `NewMapSource(ctx, values, less)` | An already loaded `map[K]V`, optional key comparator | Keys from a shallow snapshot |

Page loaders have signature `func(context.Context) ([]T, bool, error)`; `T` is
`V` for value pages and `K` for key pages. Batch lookup has signature
`func(context.Context, []K) (map[K]V, error)`. Required nil callbacks return nil;
`NewMapSource` returns nil for a nil map, but a valid empty source for an empty
non-nil map. Nil sources must not be ranged directly through `Keys()`.
All constructors require a non-nil context and perform no external I/O.

Sources can have different value types while sharing key identity semantics:

```go
db := iterx.NewLoadingSourceFromKeys(ctx, dbKeyPage, dbMGet)
actual := iterx.NewMapSource(ctx, runtimeSnapshot, func(a, b string) bool { return a < b })
for keys, err := range iterx.Batch(iterx.UniqueKeys[string](db, actual), 100) {
    if err != nil {
        return err
    }
    dbValues, err := db.MGet(keys)
    if err != nil {
        return err
    }
    actualValues, err := actual.MGet(keys)
    if err != nil {
        return err
    }
    reconcile(keys, dbValues, actualValues)
}
```

Use `db.Keys()` or `actual.Keys()` directly when only one side supplies candidates.
`source.Keys()` retains duplicates and order. All calls share one single-use
traversal; after completion or early exit, subsequent traversals yield nothing.
Create a new source and fresh page state to scan again. Sources must not be used
concurrently. Get/MGet remain usable before, during and after enumeration while
the construction context is active. Even a lookup-only source's empty traversal
reports context cancellation.

`source.MGet(keys)` deduplicates the request in first-occurrence order and calls
the batch loader independently of enumeration. Empty input performs no I/O.
Candidate filters belong to `page`; `mget` must resolve identities independently
of those filters. In particular, an object outside the candidate phase can still
exist. The loader must return only requested keys with the same identity semantics
as enumeration and `key`, when supplied. A missing key means confirmed absence
only on a successful lookup. Errors, including cancellation, discard partial
results. `source.Get(key)` returns `(value, found, error)`; a present zero value
is distinguishable from absence.

LoadingSource itself adds no cache or request accumulation. Repeated `Get` calls
invoke the loader individually. Use `Batch` and `MGet` to avoid one lookup per key;
[the executable examples](iterx/example_test.go) show each source constructor and
batch lookups across different value types.

`NewMapSource` copies the input map at construction. Subsequent changes to the
original map or returned result maps cannot alter source bindings; referenced
objects, slices and pointers remain shared. Do not mutate the input concurrently
with construction. Map keys are authoritative. A non-nil `less` must define a
strict weak ordering; use a total order for deterministic traversal. With nil
`less`, key order is unspecified. This adapter costs O(n) copying plus O(n log n)
when sorting; it does not make an already loaded map lazy or refresh its values.

`Batch(items, size)` accepts any `iter.Seq2[T, error]`, preserves order and duplicates,
and loads synchronously without prefetch. Every yielded batch has independent
slice storage; normal completion yields the final nonempty partial batch. An
input error discards the pending incomplete batch, yields `(nil, err)` once and
stops; earlier batches are not rolled back. Its iterator is single-use, including
after early exit, and must not be traversed concurrently. Cancellation comes from
the input iterator. A nonpositive size panics; otherwise nil input returns nil,
which must not be ranged directly. For an already loaded slice, use standard
library `slices.Chunk` instead.

`UniqueKeys` consumes any `KeySource[K]` (`Keys() iter.Seq2[K, error]`) in argument
order and emits each identity only on its first occurrence. It is a single-use,
non-concurrent iterator. It never calls `MGet`, sorts or interleaves sources.
Early exit leaves later sources unstarted. A source error is yielded once and
ends the traversal; earlier outputs and caller actions are not rolled back.
Cancellation is supplied by each source. Nil sources and nil iterators are
skipped; zero arguments return nil, which must not be ranged directly.

Loading retains one page per source; deduplication additionally retains
`O(unique keys)` identities. These APIs do not promise a cross-source snapshot,
perform reconciliation, or maintain processing checkpoints. Source identity
mapping, batch sizing, action safety and retry policy remain with the caller.

## Operation timelines

Collect stage facts from concurrent components and processes under one timeline ID.
The `timeline/manager` package supplies six business entry points:
`Begin`, `End`, `Record`, `Read`, and optional `Start` / `Finish`.
You can record stages without declaring an operation beginning or result.

```go
import (
    "github.com/compforge/go-stdx/timeline"
    "github.com/compforge/go-stdx/timeline/store/sqlstore"
)

// Configure once per process using the application's existing *sql.DB.
m, err := timeline.NewManager(sqlstore.New(db), timeline.Config{
    Actor: timeline.Actor{Name: podName},
    MaxTimelines: 1024,
    BatchLimit: 64,
})
if err != nil {
    return err
}
timeline.SetDefault(m)

// Neither Start nor Finish is required.
if _, err := timeline.Begin(operationID, "acquire_carrier"); err != nil {
    return err
}
workErr := acquireCarrier(ctx)
if err := timeline.End(operationID, "acquire_carrier", workErr); err != nil {
    return err
}
snapshot, recordingErr := timeline.Read(ctx, operationID, false)
// json.Marshal(snapshot) serializes the detached view.
```

`Record(id, Stage)` imports a completed interval with its source timestamps and actor.
`Start(id, operation, ...Attribute)` optionally records the operation beginning;
`Finish(id, result)` optionally records its outcome, even without Start. Undeclared
outcomes remain `unknown`; Finish accepts late stages. For parallel stages sharing
a name, retain their returned handles to end them precisely.

For cache-only recording, pass nil or `store.NewNoopStore()` to `NewManager`.
NoopStore retains no documents; facts are lost when their cached copy is evicted.
Applications own exporting snapshots to files or other formats.

Manager serves reads and writes from memory, loading Store on a cache miss and
creating missing timelines for writes. Its two workers save pending batches and load peer updates through the shared Store.
One `BatchLimit` bounds each save batch, MGet and Latest query. Both workers accept
ID notifications for earlier processing. It saves changes in the background, so
long-running applications normally do not need to call `Flush`.
At capacity, LRU eviction releases cached copies; persisted stages can be restored and
continued through their existing handles. Eviction does not finish a timeline.

This is a best-effort cache: warm reads may lag other processes, and unsaved facts
can be lost if final saving fails. `Read` returns the current view without waiting
for persistence. Use `m.Flush(ctx, id, true)` for an explicit persistence checkpoint,
or `m.Flush(ctx, id, false)` to wake the background save worker and return immediately;
use Store directly when each operation must observe or update durable state.
Use `Read(ctx, id, true)` to refresh from Store while retaining local pending changes.
Stage identity is `(StageID, Actor)`: Actor is optional for single-process use;
an empty Actor is the default executor. When supplied, ID takes precedence over Name.
Different actors retain separate records; competing
writes for the same actor use last accepted state, without a consistency guarantee.
Snapshot methods include `Summary()`, `RunningStages()` and `LatestFailedStage()`.

At shutdown, stop producers, call `m.Shutdown` with an independent bounded context, then
close the database. Use an explicit Manager's methods for isolation. `timeline.New`
provides the optional object-style API with explicit checkpoints using the same cache/Store protocol; `timeline/gospan` provides a
sealed process-local backend. See [the design](docs/timeline.md) and the
[executable example](timeline/global_example_test.go).

## Development

```sh
make fix                 # Format Go packages.
make lint                # Check formatting and run go vet without changing source.
make test                # Run all tests, including executable examples.
make fix LINT_FILES="iterx/loading.go iterx/loading_test.go"
make lint LINT_FILES="iterx/loading.go iterx/loading_test.go"
make test TEST_FILES="iterx/loading_test.go"
make test TEST_PACKAGES=./iterx
```

File selections use space-separated paths relative to the module:

- `LINT_FILES` limits formatting to the selected existing Go files; `lint` runs
  `go vet` on their complete packages.
- `TEST_FILES` runs tests for the complete containing packages and takes precedence
  over `TEST_PACKAGES`.
- Unset or empty file selections keep the defaults above. Non-Go or missing files
  cause `vet` and tests to fall back to all packages; `fix` still only formats
  selected existing Go files.

`LINT_WORKERS` and `TEST_WORKERS` default to 4 concurrent packages;
`GO_TEST_PARALLEL` defaults to 1 parallel test per package. Override them when
needed. Validation uses only the Go toolchain.

## License

MIT
