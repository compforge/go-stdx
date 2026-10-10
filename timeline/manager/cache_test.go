package manager_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	managed "github.com/compforge/go-stdx/timeline/manager"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func TestPersistedStageHandleSurvivesTTL(t *testing.T) {
	ctx := context.Background()
	backend := timelinestore.NewMemoryStore()
	m := newManager(t, backend, managed.Config{TTL: 30 * time.Millisecond})
	stage, err := m.Begin("task", "work", timeline.WithAttributes(timeline.Attribute{Key: "attempt", Value: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := backend.Read(ctx, "task")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return m.Stats().CachedTimelines == 0 })
	if err = stage.SetAttributes(timeline.Attribute{Key: "restored", Value: true}); err != nil {
		t.Fatal(err)
	}
	if err = stage.End(nil); err != nil {
		t.Fatal(err)
	}
	if err = m.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := backend.Read(ctx, "task")
	if err != nil || len(after.Stages) != 1 || after.Stages[0].ID != stage.ID() || after.Stages[0].Status != timeline.Succeeded || !after.Stages[0].StartedAt.Equal(before.Stages[0].StartedAt) || after.Stages[0].Revision != 3 || string(after.Stages[0].Attributes["attempt"]) != "1" || string(after.Stages[0].Attributes["restored"]) != "true" {
		t.Fatalf("restore: %+v %v", after, err)
	}
	// Repeated End through the old handle remains idempotent after another eviction.
	eventually(t, func() bool { return m.Stats().CachedTimelines == 0 })
	if err = stage.End(errors.New("ignored repeat")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := m.Read(ctx, "task")
	if err != nil || snapshot.Stages[0].Status != timeline.Succeeded {
		t.Fatalf("repeat: %+v %v", snapshot, err)
	}
}

func TestBoundariesAndNamedStageRestoreAfterCapacityEviction(t *testing.T) {
	ctx := context.Background()
	backend := timelinestore.NewMemoryStore()
	m := newManager(t, backend, managed.Config{MaxTimelines: 1})
	if err := m.Start("task", "operation"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Begin("task", "work"); err != nil {
		t.Fatal(err)
	}
	if err := m.Finish("task", nil); err != nil {
		t.Fatal(err)
	}
	if err := m.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	before, _ := backend.Read(ctx, "task")
	if _, err := m.Begin("other", "work"); err != nil {
		t.Fatal(err)
	}
	if err := m.Start("task", "operation"); err != nil {
		t.Fatal(err)
	}
	if err := m.Finish("task", nil); err != nil {
		t.Fatal(err)
	}
	if err := m.End("task", "work", nil); err != nil {
		t.Fatal(err)
	}
	after, err := m.Read(ctx, "task")
	if err != nil || !after.StartedAt.Equal(before.StartedAt) || !after.FinishedAt.Equal(before.FinishedAt) || len(after.Stages) != 1 || after.Stages[0].ID != before.Stages[0].ID || after.Stages[0].Status != timeline.Succeeded {
		t.Fatalf("restore: %+v %v", after, err)
	}
	if m.Stats().CachedTimelines != 1 {
		t.Fatal("capacity exceeded")
	}
}

func TestFinishDoesNotEvictOrEndStages(t *testing.T) {
	m := newManager(t, timelinestore.NewMemoryStore(), managed.Config{MaxTimelines: 1})
	if _, err := m.Begin("task", "running"); err != nil {
		t.Fatal(err)
	}
	if err := m.Finish("task", nil); err != nil {
		t.Fatal(err)
	}
	if got := m.Stats(); got.CachedTimelines != 1 {
		t.Fatalf("finish changed lifetime: %+v", got)
	}
	if err := m.End("task", "running", nil); err != nil {
		t.Fatal(err)
	}
	if m.Stats().CachedTimelines != 1 {
		t.Fatal("End evicted timeline")
	}
}

func TestEvictionAttemptsFinalSaveAndRestores(t *testing.T) {
	backend := timelinestore.NewMemoryStore()
	m := newManager(t, backend, managed.Config{FlushInterval: time.Hour})
	stage, err := m.Begin("task", "work")
	if err != nil {
		t.Fatal(err)
	}
	m.Evict("task")
	eventually(t, func() bool {
		doc, err := backend.Read(context.Background(), "task")
		return err == nil && len(doc.Stages) == 1
	})
	if err = stage.End(nil); err != nil {
		t.Fatal(err)
	}
	if err = m.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	doc, err := backend.Read(context.Background(), "task")
	if err != nil || len(doc.Stages) != 1 || doc.Stages[0].Status != timeline.Succeeded {
		t.Fatalf("restored: %+v %v", doc, err)
	}
}

func TestFailedEvictionReleasesPendingMemory(t *testing.T) {
	backend := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
	failure := errors.New("offline")
	backend.merge = func(context.Context, string, timelinestore.Update) error { return failure }
	var reports atomic.Int32
	m := newManager(t, backend, managed.Config{FlushInterval: time.Hour, OnError: func(string, error) { reports.Add(1) }})
	stage, err := m.Begin("task", "work")
	if err != nil {
		t.Fatal(err)
	}
	m.Evict("task")
	eventually(t, func() bool { return m.Stats().PendingTimelines == 0 && reports.Load() == 1 })
	if got := m.Stats(); got.CachedTimelines != 0 || got.PendingUpdates != 0 || got.DroppedUpdates != 1 {
		t.Fatalf("not released: %+v", got)
	}
	if err = stage.End(nil); !errors.Is(err, managed.ErrStageNotFound) {
		t.Fatalf("invented lost stage: %v", err)
	}
}

func TestPermanentSaveConflictRetiresAndIsReported(t *testing.T) {
	backend := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
	backend.merge = func(ctx context.Context, id string, u timelinestore.Update) error {
		if id == "conflict" {
			return timelinestore.ErrConflict
		}
		return backend.MemoryStore.Merge(ctx, id, u)
	}
	m := newManager(t, backend, managed.Config{MaxPendingTimelines: 1, FlushInterval: time.Hour})
	if _, err := m.Begin("conflict", "work"); err != nil {
		t.Fatal(err)
	}
	if err := m.FlushID(context.Background(), "conflict"); !errors.Is(err, timelinestore.ErrConflict) {
		t.Fatal(err)
	}
	if got := m.Stats(); got.PendingTimelines != 0 || got.RejectedUpdates != 1 {
		t.Fatalf("conflict retained: %+v", got)
	}
	s, err := m.Read(context.Background(), "conflict")
	if err != nil || s.Collection.LocalFlushed {
		t.Fatalf("lost save reported healthy: %+v %v", s, err)
	}
	if _, err = m.Begin("healthy", "work"); err != nil {
		t.Fatal(err)
	}
	if err = m.FlushID(context.Background(), "healthy"); err != nil {
		t.Fatal(err)
	}
}

func TestTTLCleanupDoesNotWaitForStore(t *testing.T) {
	backend := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	backend.merge = func(ctx context.Context, id string, u timelinestore.Update) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return backend.MemoryStore.Merge(ctx, id, u)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m := newManager(t, backend, managed.Config{TTL: 30 * time.Millisecond, ExportTimeout: time.Second})
	defer close(release)
	if _, err := m.Begin("task", "work"); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, entered)
	eventually(t, func() bool { return m.Stats().CachedTimelines == 0 })
	if m.Stats().PendingTimelines != 1 {
		t.Fatal("in-flight final save disappeared")
	}
}
