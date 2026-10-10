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

Record one operation across concurrent components and processes using its operation ID.
Each process installs a Manager against the same Store. Business code uses the ID and
stage names; the Manager finds live recorders and persists their updates in the background.
Stages retain their own intervals, parents, results and executor identities, including
parallel work and late observations.

```go
// Configure once per process with the application's existing *sql.DB.
manager, err := timeline.NewManager(sqlstore.New(db), timeline.ManagerConfig{})
if err != nil {
    return err
}
timeline.SetDefaultManager(manager)

// Business code passes only the operation ID between components.
if _, err := timeline.Start(ctx, operationID, "sandbox_start"); err != nil {
    return err
}
if _, err := timeline.Begin(operationID, "acquire_carrier",
    timeline.WithStageActor(timeline.Actor{Name: podName}),
); err != nil {
    return err
}
workErr := acquireCarrier(ctx)
if err := timeline.End(operationID, "acquire_carrier", workErr); err != nil {
    return err
}
snapshot, recordingErr := timeline.Finish(ctx, operationID, workErr)
// json.Marshal(snapshot) serializes the complete view; workErr and recordingErr
// describe separate business and recording outcomes.
```

`SetAttributes(id, ...)` updates operation attributes; `SetStageAttributes(id, name, ...)`
updates a running stage. `BeginContext(ctx, id, name, ...)` carries parentage in context.
`Record(id, Stage)` imports a completed interval with its original timestamps and actor.
Stages with the same name may run concurrently; name-based updates then return
`ErrAmbiguousStage`. Keep their returned StageHandles to identify them precisely.

`Capture(ctx, id)` flushes this ID's local writers and returns a Snapshot.
`Read(ctx, id)` only reads persisted data. Background persistence is eventually visible;
for a strict cross-process handoff, call `Flush(ctx, id)` before publishing completion.
The coordinator process calls `Start` and `Finish`; other processes contribute stages.
Snapshot methods provide `Summary()`, `RunningStages()` and `LatestFailedStage()`.

Failed start persistence retains the coordinator for `Flush` retry. Failed Finish retains
it for another Finish attempt with the original result. Successful Finish releases the
coordinator; End releases the stage's name. `Release(id)` abandons local lookup state
without changing business results, discarding pending writes or deleting stored history.
Active operations, stages and pending writes have separate capacity limits in ManagerConfig.

At shutdown, stop and join producers, call `manager.Shutdown` with an independent bounded
context, then close the database. The application owns migrations, connection pools and
retention. See [storage and lifecycle](docs/timeline.md) and the
[executable example](timeline/global_example_test.go).

For isolated instances, use the same operations on an explicit Manager (`FlushID` selects
one ID; `Flush` drains all local IDs). `For(id)` / `manager.New(id)` return independent writer
handles; `timeline.New` creates a standalone writer with explicit flushing. The typed
`BeginWithContext(ctx, tl, ...)` adapter also supports standalone and gospan handles.
Global entry points return `ErrNoDefaultManager` until one is installed.

## License

MIT
