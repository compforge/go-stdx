package manager_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	managed "github.com/compforge/go-stdx/timeline/manager"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func TestRejectedTerminalAdmissionCanBeRetried(t *testing.T) {
	for _, kind := range []string{"operation", "stage"} {
		t.Run(kind, func(t *testing.T) {
			store := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
			entered, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			store.merge = func(ctx context.Context, id string, u timelinestore.Update) error {
				if calls.Add(1) == 1 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return store.MemoryStore.Merge(ctx, id, u)
			}
			m := newManager(t, store, managed.Config{MaxPendingUpdates: 1, ExportTimeout: time.Second})
			var end func(error) error
			if kind == "operation" {
				if err := m.Start("task", "work"); err != nil {
					t.Fatal(err)
				}
				end = func(err error) error { return m.Finish("task", err) }
			} else {
				if _, err := m.Begin("task", "work"); err != nil {
					t.Fatal(err)
				}
				end = func(err error) error { return m.End("task", "work", err) }
			}
			waitSignal(t, entered)
			businessErr := errors.New("first result")
			if err := end(businessErr); !errors.Is(err, managed.ErrBufferFull) {
				t.Fatalf("expected backpressure: %v", err)
			}
			if kind == "stage" && m.Stats().ActiveStages != 1 {
				t.Fatal("rejected End released the stage")
			}
			close(release)
			eventually(t, func() bool { return m.Stats().PendingHandles == 0 })
			// The first valid result remains fixed even if the retry supplies nil.
			if err := end(nil); err != nil {
				t.Fatal(err)
			}
			snapshot, err := m.Read(context.Background(), "task")
			if err != nil {
				t.Fatal(err)
			}
			status, message := snapshot.Status, snapshot.Error
			if kind == "stage" {
				status, message = snapshot.Stages[0].Status, snapshot.Stages[0].Error
			}
			if status != timeline.Failed || message != businessErr.Error() {
				t.Fatalf("terminal lost on retry: %+v", snapshot)
			}
		})
	}
}

func TestReadRetainsActiveParticipantRecordingError(t *testing.T) {
	m := newManager(t, timelinestore.NewMemoryStore(), managed.Config{})
	stage, err := m.Begin("task", "work")
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.SetAttributes(timeline.Attribute{Key: "invalid", Value: make(chan int)}); !errors.Is(err, timeline.ErrInvalidAttribute) {
		t.Fatal(err)
	}
	eventually(t, func() bool { return m.Stats().PendingHandles == 0 })
	snapshot, err := m.Read(context.Background(), "task")
	if !errors.Is(err, timeline.ErrInvalidAttribute) || snapshot.Collection.LocalFlushed || !snapshot.Collection.StoreRead || m.Stats().ActiveStages != 1 {
		t.Fatalf("active error forgotten: %+v %v", snapshot, err)
	}
	stage.End(nil)
	eventually(t, func() bool { return m.Stats().PendingHandles == 0 })
	if _, err := m.Read(context.Background(), "task"); !errors.Is(err, timeline.ErrInvalidAttribute) {
		t.Fatalf("ended participant error forgotten: %v", err)
	}
}

func TestPermanentConflictRetiresWithoutPoisoningQueue(t *testing.T) {
	ctx := context.Background()
	m := newManager(t, timelinestore.NewMemoryStore(), managed.Config{MaxPendingHandles: 1})
	now := time.Now()
	stage := timeline.Stage{ID: "stable", Name: "work", StartedAt: now, FinishedAt: now, Status: timeline.Succeeded}
	if err := m.Record("task", stage); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Read(ctx, "task"); err != nil {
		t.Fatal(err)
	}
	stage.Name = "conflict"
	if err := m.Record("task", stage); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Read(ctx, "task"); !errors.Is(err, timelinestore.ErrConflict) {
		t.Fatal(err)
	}
	eventually(t, func() bool { return m.Stats().PendingHandles == 0 })
	if m.Stats().RejectedUpdates != 1 {
		t.Fatalf("missing rejection accounting: %+v", m.Stats())
	}
	if _, err := m.Begin("healthy", "work"); err != nil {
		t.Fatal(err)
	}
	if err := m.End("healthy", "work", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Read(ctx, "healthy"); err != nil {
		t.Fatal(err)
	}
	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("permanent rejection blocked shutdown: %v", err)
	}
}

func TestEvictionKeepsAcceptedOutbox(t *testing.T) {
	ctx := context.Background()
	store := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
	var available atomic.Bool
	store.merge = func(ctx context.Context, id string, u timelinestore.Update) error {
		if !available.Load() {
			return errors.New("offline")
		}
		return store.MemoryStore.Merge(ctx, id, u)
	}
	m := newManager(t, store, managed.Config{MaxTimelines: 1})
	if _, err := m.Begin("evicted", "work"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Begin("current", "work"); err != nil {
		t.Fatal(err)
	}
	if m.Stats().PendingHandles != 2 {
		t.Fatal("eviction discarded accepted writes")
	}
	available.Store(true)
	if err := m.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"evicted", "current"} {
		doc, err := store.Read(ctx, id)
		if err != nil || len(doc.Stages) != 1 {
			t.Fatalf("outbox lost %s: %+v %v", id, doc, err)
		}
	}
}
