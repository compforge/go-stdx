//go:build cgo

package sqlstore_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
	"github.com/compforge/go-stdx/timeline/store/sqlstore"
	_ "github.com/mattn/go-sqlite3"
)

func open(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=10000&_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(2)
	t.Cleanup(func() { db.Close() })
	return db
}

func schema(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE timelines (id TEXT PRIMARY KEY, payload TEXT NOT NULL, version INTEGER NOT NULL, created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL)`)
	if err != nil {
		t.Fatal(err)
	}
}

// Each child has its own address space, handle and SQL pool. Sharing a Go map
// cannot accidentally satisfy this test. The parent reads after children exit.
func TestIndependentProcessesContributeStages(t *testing.T) {
	if path := os.Getenv("TIMELINE_TEST_DB"); path != "" {
		db := open(t, path)
		id := os.Getenv("TIMELINE_TEST_ACTOR")
		tl, err := timeline.New("sandbox-1", timeline.WithStore(sqlstore.New(db)),
			timeline.WithActor(timeline.Actor{ID: id, Name: "pod-" + id}))
		if err != nil {
			t.Fatal(err)
		}
		var ref timeline.StageRef
		if err := json.Unmarshal([]byte(os.Getenv("TIMELINE_TEST_PARENT")), &ref); err != nil {
			t.Fatal(err)
		}
		ctx := timeline.NewStageContext(context.Background(), ref)
		_, stage := timeline.BeginWithContext(ctx, tl, "ensure_runtime", timeline.WithAttributes(timeline.Attribute{Key: "worker", Value: id}))
		// Persist a running boundary as a live observer would see it.
		if err := tl.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if id == "worker-0" {
			stage.End(errors.New("quota exceeded"), timeline.WithCode("ResourceQuotaExceeded"))
		} else {
			stage.End(nil, timeline.WithCode("Reused"))
		}
		if err := tl.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "timeline.db")
	db := open(t, path)
	schema(t, db)
	store := sqlstore.New(db)
	owner, _ := timeline.New("sandbox-1", timeline.WithStore(store), timeline.WithActor(timeline.Actor{ID: "api"}))
	if err := owner.Start(ctx, "sandbox_start"); err != nil {
		t.Fatal(err)
	}
	parentCtx, parent := timeline.BeginWithContext(ctx, owner, "startup")
	ref, _ := timeline.StageFromContext(parentCtx)
	encoded, _ := json.Marshal(ref)
	if err := owner.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	const workers = 3
	var wg sync.WaitGroup
	failures := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestIndependentProcessesContributeStages$")
			cmd.Env = append(os.Environ(), "TIMELINE_TEST_DB="+path, fmt.Sprintf("TIMELINE_TEST_ACTOR=worker-%d", i), "TIMELINE_TEST_PARENT="+string(encoded))
			if output, err := cmd.CombinedOutput(); err != nil {
				failures <- fmt.Errorf("child %d: %w: %s", i, err, output)
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	parent.End(nil)
	snapshot, err := owner.Finish(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != timeline.Succeeded || len(snapshot.Stages) != workers+1 {
		t.Fatalf("snapshot: %+v", snapshot)
	}
	actors := make(map[string]bool)
	ids := make(map[timeline.StageID]bool)
	for _, stage := range snapshot.Stages {
		if ids[stage.ID] {
			t.Fatal("colliding stage IDs")
		}
		ids[stage.ID] = true
		if stage.Actor.ID == "api" {
			continue
		}
		actors[stage.Actor.ID] = true
		wantStatus, wantCode, wantError := timeline.Succeeded, "Reused", ""
		if stage.Actor.ID == "worker-0" {
			wantStatus, wantCode, wantError = timeline.Failed, "ResourceQuotaExceeded", "quota exceeded"
		}
		if stage.Code != wantCode || stage.Error != wantError {
			t.Fatalf("cross-process stage result lost: %+v", stage)
		}
		if stage.ParentID != ref.StageID || stage.Status != wantStatus || stage.Elapsed <= 0 || stage.Actor.Name != "pod-"+stage.Actor.ID {
			t.Fatalf("stage: %+v", stage)
		}
	}
	if len(actors) != workers {
		t.Fatalf("actors: %v", actors)
	}
	// Reopen through a new pool: no local registry or recorder state survives.
	reader, _ := timeline.New("sandbox-1", timeline.WithStore(sqlstore.New(open(t, path))))
	restored, err := reader.Snapshot(ctx)
	if err != nil || restored.FinishedAt != snapshot.FinishedAt || len(restored.Stages) != len(snapshot.Stages) {
		t.Fatalf("reopen: %+v %v", restored, err)
	}
	failed, ok := restored.LatestFailedStage()
	if !ok || failed.Code != "ResourceQuotaExceeded" {
		t.Fatalf("reopened error code lost: %+v", failed)
	}
	// A contributor may legitimately arrive after the business terminal record.
	_, late := timeline.BeginWithContext(ctx, reader, "late_report")
	late.End(nil)
	after, err := reader.Snapshot(ctx)
	if err != nil || len(after.Stages) != workers+2 || after.Status != timeline.Succeeded {
		t.Fatalf("late: %+v %v", after, err)
	}
	var rows int
	if err := db.QueryRow("SELECT COUNT(*) FROM timelines").Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("one document expected: %d %v", rows, err)
	}
}

func TestSQLIdempotencyConflictAndIsolation(t *testing.T) {
	ctx := context.Background()
	db := open(t, filepath.Join(t.TempDir(), "documents.db"))
	schema(t, db)
	store := sqlstore.New(db)
	operation := timelinestore.OperationRecord{Revision: 1, Operation: "start", StartedAt: time.Now().UTC(), Status: timeline.Running,
		Attributes: map[string]json.RawMessage{"data": json.RawMessage(`{"z":18446744073709551615,"a":1}`)}}
	update := timelinestore.Update{Operation: &operation}
	if err := store.Merge(ctx, "a", update); err != nil {
		t.Fatal(err)
	}
	// Emulate normalization by a native JSON column while preserving large numbers.
	operation.Attributes["data"] = json.RawMessage(`{ "a": 1, "z": 18446744073709551615 }`)
	if err := store.Merge(ctx, "a", update); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRow("SELECT version FROM timelines WHERE id = ?", "a").Scan(&version); err != nil || version != 1 {
		t.Fatalf("duplicate advanced version: %d %v", version, err)
	}
	operation.Operation = "other"
	if err := store.Merge(ctx, "a", update); !errors.Is(err, timelinestore.ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if err := store.Merge(ctx, "b", update); err != nil {
		t.Fatal(err)
	}
	got, err := store.Read(ctx, "a")
	if err != nil || got.Operation != "start" {
		t.Fatalf("overwrite: %+v %v", got, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Read(canceled, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := store.Read(ctx, "missing"); !errors.Is(err, timelinestore.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

// Subprocesses share only the SQL database. Each independently replays one
// completed external interval and adds its own stage after the root is finished.
func TestProcessesRecordCompletedStages(t *testing.T) {
	at := time.Date(2026, 9, 28, 1, 0, 0, 0, time.UTC)
	if path := os.Getenv("TIMELINE_RECORD_DB"); path != "" {
		tl, err := timeline.New("record-operation", timeline.WithStore(sqlstore.New(open(t, path))))
		if err != nil {
			t.Fatal(err)
		}
		data := timeline.Stage{ID: "external:shared", Name: "image_pull", Actor: timeline.Actor{Name: "kubelet"}, StartedAt: at, FinishedAt: at.Add(time.Second), Status: timeline.Succeeded}
		for range 2 {
			if err := tl.Record(data); err != nil {
				t.Fatal(err)
			}
		}
		data.ID = timeline.StageID(os.Getenv("TIMELINE_RECORD_WORKER"))
		data.Actor = timeline.Actor{ID: string(data.ID), Name: "pod-" + string(data.ID)}
		if err := tl.Record(data); err != nil {
			t.Fatal(err)
		}
		data.ID += ":followup"
		if err := tl.Record(data); err != nil {
			t.Fatal(err)
		}
		if err := tl.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "record.db")
	db := open(t, path)
	schema(t, db)
	owner, _ := timeline.New("record-operation", timeline.WithStore(sqlstore.New(db)))
	if err := owner.Start(ctx, "start"); err != nil {
		t.Fatal(err)
	}
	first, err := owner.Finish(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessesRecordCompletedStages$")
			command.Env = append(os.Environ(), "TIMELINE_RECORD_DB="+path, fmt.Sprintf("TIMELINE_RECORD_WORKER=worker-%d", i))
			if out, err := command.CombinedOutput(); err != nil {
				t.Errorf("child %d: %v\n%s", i, err, out)
			}
		}()
	}
	wg.Wait()
	got, err := owner.Snapshot(ctx)
	if err != nil || len(got.Stages) != 7 || got.Status != timeline.Succeeded || !got.FinishedAt.Equal(first.FinishedAt) {
		t.Fatalf("process replay: %+v %v", got, err)
	}
	for _, stage := range got.Stages {
		if stage.ID == "external:shared" {
			if stage.Actor != (timeline.Actor{Name: "kubelet"}) {
				t.Fatal(stage)
			}
		} else if stage.Actor.ID == "" || (string(stage.ID) != stage.Actor.ID && string(stage.ID) != stage.Actor.ID+":followup") || stage.Actor.Name != "pod-"+stage.Actor.ID {
			t.Fatal(stage)
		}
	}
	var payload []byte
	if err := db.QueryRow("SELECT payload FROM timelines WHERE id = ?", "record-operation").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Actors []timeline.Actor `json:"actors"`
	}
	if err := json.Unmarshal(payload, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.Actors) != 4 {
		t.Fatalf("expected shared kubelet and 3 workers: %s", payload)
	}
}

func TestSQLActorReferencesArePayloadLocal(t *testing.T) {
	ctx := context.Background()
	db := open(t, filepath.Join(t.TempDir(), "actors.db"))
	schema(t, db)
	store := sqlstore.New(db)
	tl, err := timeline.New("actors", timeline.WithStore(store))
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 28, 1, 0, 0, 0, time.UTC)
	actorA := timeline.Actor{ID: "a", Name: "pod-a"}
	actorB := timeline.Actor{ID: "b", Name: "pod-b"}
	later := timeline.Stage{ID: "later", Name: "step", StartedAt: at.Add(time.Second), FinishedAt: at.Add(2 * time.Second), Status: timeline.Succeeded, Actor: actorA}
	if err := tl.Record(later); err != nil {
		t.Fatal(err)
	}
	if err := tl.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	earlier := later
	earlier.ID, earlier.Actor, earlier.StartedAt = "earlier", actorB, at
	if err := tl.Record(earlier); err != nil {
		t.Fatal(err)
	}
	if err := tl.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Sorting puts B first, changing A's payload-local ref from 1 to 2. A retry
	// still compares full Actors after decoding and must remain idempotent.
	if err := tl.Record(later); err != nil {
		t.Fatal(err)
	}
	if err := tl.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var payload []byte
	var version int64
	if err := db.QueryRow("SELECT payload, version FROM timelines WHERE id = ?", "actors").Scan(&payload, &version); err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Actors []timeline.Actor `json:"actors"`
		Stages []struct {
			ID     string          `json:"id"`
			Ref    uint64          `json:"actor_ref"`
			Inline json.RawMessage `json:"actor"`
		} `json:"stages"`
	}
	if err := json.Unmarshal(payload, &wire); err != nil {
		t.Fatal(err)
	}
	if version != 2 || len(wire.Actors) != 2 || wire.Actors[0] != actorB || wire.Actors[1] != actorA || len(wire.Stages) != 2 || wire.Stages[0].Ref != 1 || wire.Stages[1].Ref != 2 || len(wire.Stages[1].Inline) != 0 {
		t.Fatalf("payload=%s version=%d", payload, version)
	}
	got, err := store.Read(ctx, "actors")
	if err != nil || got.Stages[0].Actor != actorB || got.Stages[1].Actor != actorA {
		t.Fatalf("read=%+v err=%v", got, err)
	}
	conflicting := later
	conflicting.Actor = actorB
	if err := tl.Record(conflicting); !errors.Is(err, timelinestore.ErrConflict) {
		t.Fatalf("lost actor conflict: %v", err)
	}
	if err := tl.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}
