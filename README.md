# go-stdx

Go stdlib extensions — **admitted only when the standard library doesn't offer it.**

The long-term ambition is the slot Guava fills in Java: the utility layer a project reaches for before hand-writing a helper. Go's ecosystem splits that slot today — [samber/lo](https://github.com/samber/lo) owns generic collection transforms, [gods](https://github.com/emirpasic/gods) owns data structures, [lancet](https://github.com/duke-git/lancet) goes kitchen-sink — and go-stdx competes on discipline, not breadth: stdlib-mirror naming, and only the small, common data operations every service would otherwise re-wrap.

The name is the admission rule: a hand-rolled `max`, a `slices.Clone` re-implementation, or a `strconv` wrapper does not belong here — use stdlib. What earns a slot is the three-to-five-line wrapper real projects keep re-writing because the stdlib deliberately omits it. Generating the underlying data (a UUID, a hash) is *not* our job — we lean on a mature library for that and wrap only the shape callers pass around.

Subpackages mirror stdlib naming so call sites read like the standard library they extend:

| package | what | why not stdlib / a lib |
|---|---|---|
| `iterx` | `NewLoadingIterator` — lazy, batched loading through standard `iter.Seq2` | stdlib defines the iteration protocol; it does not load pages |
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
