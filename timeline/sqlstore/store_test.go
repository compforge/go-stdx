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
	"github.com/compforge/go-stdx/timeline/sqlstore"
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
		_, stage := timeline.BeginContext(ctx, tl, "ensure_runtime", timeline.WithFields(timeline.Field{Key: "worker", Value: id}))
		// Persist a running boundary as a live observer would see it.
		if err := tl.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		stage.End(nil)
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
	parentCtx, parent := timeline.BeginContext(ctx, owner, "startup")
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
		if stage.ParentID != ref.StageID || stage.Status != timeline.Succeeded || stage.Elapsed <= 0 || stage.Actor.Name != "pod-"+stage.Actor.ID {
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
	// A contributor may legitimately arrive after the business terminal record.
	_, late := timeline.BeginContext(ctx, reader, "late_report")
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
	operation := timeline.OperationRecord{Revision: 1, Operation: "start", StartedAt: time.Now().UTC(), Status: timeline.Running,
		Fields: map[string]json.RawMessage{"data": json.RawMessage(`{"z":18446744073709551615,"a":1}`)}}
	update := timeline.Update{Operation: &operation}
	if err := store.Merge(ctx, "a", update); err != nil {
		t.Fatal(err)
	}
	// Emulate normalization by a native JSON column while preserving large numbers.
	operation.Fields["data"] = json.RawMessage(`{ "a": 1, "z": 18446744073709551615 }`)
	if err := store.Merge(ctx, "a", update); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRow("SELECT version FROM timelines WHERE id = ?", "a").Scan(&version); err != nil || version != 1 {
		t.Fatalf("duplicate advanced version: %d %v", version, err)
	}
	operation.Operation = "other"
	if err := store.Merge(ctx, "a", update); !errors.Is(err, timeline.ErrConflict) {
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
	if _, err := store.Read(ctx, "missing"); !errors.Is(err, timeline.ErrNotFound) {
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
		data := timeline.Stage{ID: "external:shared", Name: "image_pull", StartedAt: at, FinishedAt: at.Add(time.Second), Status: timeline.Succeeded}
		for range 2 {
			if err := tl.Record(data); err != nil {
				t.Fatal(err)
			}
		}
		data.ID = timeline.StageID(os.Getenv("TIMELINE_RECORD_WORKER"))
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
	if err != nil || len(got.Stages) != 4 || got.Status != timeline.Succeeded || !got.FinishedAt.Equal(first.FinishedAt) {
		t.Fatalf("process replay: %+v %v", got, err)
	}
}
