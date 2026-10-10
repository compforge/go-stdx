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

Collect stage facts from concurrent components and processes under one timeline ID.
The `timeline/manager` package supplies six business entry points:
`Begin`, `End`, `Record`, `Read`, and optional `Start` / `Finish`.
You can record stages without declaring an operation beginning or result.

```go
import (
    "time"

    "github.com/compforge/go-stdx/timeline"
    "github.com/compforge/go-stdx/timeline/manager"
    "github.com/compforge/go-stdx/timeline/store/sqlstore"
)

// Configure once per process using the application's existing *sql.DB.
m, err := manager.New(sqlstore.New(db), manager.Config{
    TTL: time.Hour,
    MaxTimelines: 1024,
})
if err != nil {
    return err
}
manager.SetDefault(m)

// Neither Start nor Finish is required.
if _, err := manager.Begin(operationID, "acquire_carrier",
    timeline.WithStageActor(timeline.Actor{Name: podName}),
); err != nil {
    return err
}
workErr := acquireCarrier(ctx)
if err := manager.End(operationID, "acquire_carrier", workErr); err != nil {
    return err
}
snapshot, recordingErr := manager.Read(ctx, operationID)
// json.Marshal(snapshot) serializes the detached view.
```

`Record(id, Stage)` imports a completed interval with its source timestamps and actor.
`Start(id, operation, ...Attribute)` optionally records the operation beginning;
`Finish(id, result)` optionally records its outcome, even without Start. Both buffer
facts without remote IO. Undeclared outcomes remain `unknown`; Finish accepts late stages.
For parallel stages sharing a name, retain their returned handles to end them precisely.

Manager automatically persists accepted records. Local timelines expire at a fixed TTL
from creation; access updates LRU but never extends TTL. At capacity, the least recently
used timeline is evicted. Eviction invalidates local handles while preserving accepted
pending writes and stored history. ID-based writes can create a fresh local entry.

`Read` checkpoints this ID's local writers and returns a Snapshot, including collection
status. It never claims remote writers are complete. For a strict cross-process handoff,
use `m.FlushID(ctx, id)` before publishing completion. Snapshot methods include
`Summary()`, `RunningStages()` and `LatestFailedStage()`.

At shutdown, stop producers, call `m.Shutdown` with an independent bounded context, then
close the database. Use an explicit Manager's methods for isolation. `timeline.New`
provides a standalone writer with explicit checkpoints; `timeline/gospan` provides a
sealed process-local backend. See [the design](docs/timeline.md) and the
[executable example](timeline/manager/global_example_test.go).

## License

MIT
