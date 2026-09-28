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
	_, err := db.Exec(`CREATE TABLE timeline_records (
 record_seq INTEGER PRIMARY KEY AUTOINCREMENT,
 timeline_id TEXT NOT NULL, record_id TEXT NOT NULL, payload TEXT NOT NULL,
 UNIQUE(timeline_id, record_id))`)
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
		_, stage := tl.Begin(ctx, "ensure_runtime", timeline.Field{Key: "worker", Value: id})
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
	parentCtx, parent := owner.Begin(ctx, "startup")
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
	_, late := reader.Begin(ctx, "late_report")
	late.End(nil)
	after, err := reader.Snapshot(ctx)
	if err != nil || len(after.Stages) != workers+2 || after.Status != timeline.Succeeded {
		t.Fatalf("late: %+v %v", after, err)
	}
}

func TestSQLIdempotencyConflictAndIsolation(t *testing.T) {
	ctx := context.Background()
	db := open(t, filepath.Join(t.TempDir(), "records.db"))
	schema(t, db)
	store := sqlstore.New(db)
	record := timeline.Record{ID: "r1", Kind: timeline.OperationStarted, Operation: "start", At: time.Now().UTC()}
	for i := 0; i < 2; i++ {
		if err := store.Append(ctx, "a", []timeline.Record{record}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.Read(ctx, "a")
	if err != nil || len(got) != 1 {
		t.Fatalf("duplicates: %v %v", got, err)
	}
	record.Operation = "other"
	if err := store.Append(ctx, "a", []timeline.Record{record}); !errors.Is(err, timeline.ErrRecordConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if err := store.Append(ctx, "b", []timeline.Record{record}); err != nil {
		t.Fatal(err)
	}
	got, err = store.Read(ctx, "a")
	if err != nil || got[0].Operation != "start" {
		t.Fatalf("overwrite: %v %v", got, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Read(canceled, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
}
