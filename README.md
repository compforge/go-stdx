# go-stdx

Go stdlib extensions — **admitted only when the standard library doesn't offer it.**

The long-term ambition is the slot Guava fills in Java: the utility layer a project reaches for before hand-writing a helper. Go's ecosystem splits that slot today — [samber/lo](https://github.com/samber/lo) owns generic collection transforms, [gods](https://github.com/emirpasic/gods) owns data structures, [lancet](https://github.com/duke-git/lancet) goes kitchen-sink — and go-stdx competes on discipline, not breadth: stdlib-mirror naming, and only the small, common data operations every service would otherwise re-wrap.

The name is the admission rule: a hand-rolled `max`, a `slices.Clone` re-implementation, or a `strconv` wrapper does not belong here — use stdlib. What earns a slot is the three-to-five-line wrapper real projects keep re-writing because the stdlib deliberately omits it. Generating the underlying data (a UUID, a hash) is *not* our job — we lean on a mature library for that and wrap only the shape callers pass around.

Subpackages mirror stdlib naming so call sites read like the standard library they extend:

| package | what | why not stdlib / a lib |
|---|---|---|
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
| `timeline` | operation/stage interfaces and detached, structured snapshots | joins stages from independent processes by business ID; snapshots retain intervals, hierarchy, optional actors and results |

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

## Operation timelines

Record one operation across concurrent components and processes using its business ID.
Each process installs a default Manager against the same Store; business components obtain
coordinator handles with `timeline.Start(ctx, id, operation)` and participant handles
with `timeline.For(id)`. Stages retain their own intervals, parent IDs, results,
and optional executor identity; parallel work
stays parallel in the snapshot.

```go
// Configure once per process with the application's existing *sql.DB.
store := sqlstore.New(db)
manager, err := timeline.NewManager(store, timeline.ManagerConfig{})
if err != nil {
    return err
}
timeline.SetDefaultManager(manager)
// At service shutdown, after producers stop:
defer func() {
    cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    if err := manager.Shutdown(cleanupCtx); err != nil {
        log.Printf("timeline shutdown: %v", err)
    }
}()

// Coordinator: create a handle and record the business start boundary.
tl, err := timeline.Start(ctx, operationID, "sandbox_start")
if err != nil {
    return err // recording error; the application chooses its policy
}

// Another component needs only the ID; the global entry uses this process's Manager.
worker, err := timeline.For(operationID,
    timeline.WithActor(timeline.Actor{Name: podName}), // optional
)
if err != nil {
    return err
}
stage := worker.Begin("acquire_carrier")
operationErr := acquireCarrier(ctx)
stage.End(operationErr)
// Begin, attribute updates and End are persisted automatically in the background.

// Or report an interval whose actual boundaries are already known.
if err := worker.Record(timeline.Stage{
    ID: "pod-uid:image-pull:attempt-1", Name: "image_pull",
    StartedAt: pullingAt, FinishedAt: pulledAt, Status: timeline.Succeeded,
}); err != nil {
    return err
}

// Coordinator records the business outcome; late stages can still be collected.
snapshot, captureErr := tl.Finish(ctx, operationErr)
// json.Marshal(snapshot) persists data directly; no separate application DTO.
```

If `Start` returns a non-nil handle with a persistence error, retry with that handle's
`Flush(ctx)` to preserve the original start boundary.

Background persistence is eventually visible. For a strict cross-process handoff,
the producer can explicitly `Flush(ctx)` before publishing completion. Manager
retains pending records even after business code discards a handle; it bounds
buffers, retries failed writes, and drains on shutdown. Monitor `Manager.Stats()`
for dropped updates and configure `OnError` for background failures.

Import `github.com/compforge/go-stdx/timeline` and
`github.com/compforge/go-stdx/timeline/sqlstore`. Create the SQL schema through the
application's migrations before use; see [storage and lifecycle](docs/timeline.md).
The application owns connection pools, IO budgets and retention. SQLite is used
only by the storage integration tests; applications choose their database driver.

`timeline.Read(ctx, id)` reads persisted records without flushing writers. `Start`, `For` and `Read`
return `ErrNoDefaultManager` before setup. For isolated integrations, call `manager.New(id)`
directly; `timeline.New` always constructs a standalone handle, independent of the default.
Without `WithStore`, `New` uses a private in-memory store. A shared
`NewMemoryStore()` joins handles in one process. The optional
`timeline/gospan` backend records into a local projection and seals its writer at
Finish; it does not aggregate across replicas. Its Registry remains process-local.

Pass `timeline.Timeline` directly to business functions. `NewContext` / `FromContext`
and `BeginContext` are optional helpers. `StageFromContext` / `NewStageContext` let shared recorders
carry a serializable parent reference across process boundaries.

`Snapshot.Collection.LocalFlushed` and `StoreRead` describe this handle's flush and
backend read. Neither claims all producers have reported. Actor is optional;
omitting it does not change recording. Document and Snapshot JSON deduplicate actors
into a payload-local `actors` table with 1-based stage `actor_ref` values; decoding
restores full Actor values. Standalone Stage JSON remains self-contained.

See the [global-entry example](timeline/global_example_test.go),
[explicit shared-store example](timeline/shared_example_test.go), and
[lifecycle, storage and collection contract](docs/timeline.md).

## License

MIT
