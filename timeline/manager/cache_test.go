package manager_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	managed "github.com/compforge/go-stdx/timeline/manager"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func TestFixedTTLExpiresUnfinishedTimelineDespiteReads(t *testing.T) {
	ctx := context.Background()
	store := timelinestore.NewMemoryStore()
	m := newManager(t, store, managed.Config{TTL: 100 * time.Millisecond})
	stage, err := m.Begin("task", "work")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Read(ctx, "task"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for m.Stats().CachedTimelines != 0 && time.Now().Before(deadline) {
		if _, err := m.Read(ctx, "task"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := m.Stats(); got.CachedTimelines != 0 || got.ActiveStages != 0 {
		t.Fatalf("reads refreshed TTL: %+v", got)
	}
	if err := stage.End(nil); !errors.Is(err, managed.ErrExpired) {
		t.Fatalf("expired handle wrote: %v", err)
	}
	snapshot, err := m.Read(ctx, "task")
	if err != nil || snapshot.Status != timeline.Unknown || snapshot.Stages[0].Status != timeline.Running {
		t.Fatalf("expiry invented completion: %+v %v", snapshot, err)
	}
	if m.Stats().CachedTimelines != 0 {
		t.Fatal("Read recreated expired cache")
	}
	if _, err := m.Begin("task", "late"); err != nil {
		t.Fatal(err)
	}
	if err := m.End("task", "late", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err = m.Read(ctx, "task")
	if err != nil || len(snapshot.Stages) != 2 {
		t.Fatalf("recreation lost history: %+v %v", snapshot, err)
	}
}

func TestCapacityEvictsLRUAndPreservesPendingFacts(t *testing.T) {
	ctx := context.Background()
	store := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
	store.merge = func(ctx context.Context, id string, u timelinestore.Update) error {
		return store.MemoryStore.Merge(ctx, id, u)
	}
	m := newManager(t, store, managed.Config{MaxTimelines: 2})
	a, err := m.Begin("a", "work")
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.Begin("b", "work")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Read(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Begin("c", "work"); err != nil {
		t.Fatalf("capacity rejected creation: %v", err)
	}
	if err := m.End("b", "work", nil); !errors.Is(err, managed.ErrStageNotFound) {
		t.Fatalf("wrong LRU victim: %v", err)
	}
	if err := b.End(nil); !errors.Is(err, managed.ErrExpired) {
		t.Fatalf("evicted handle alive: %v", err)
	}
	if err := a.End(nil); err != nil {
		t.Fatalf("recent entry evicted: %v", err)
	}
	if err := m.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := m.Read(ctx, "b")
	if err != nil || len(snapshot.Stages) != 1 || snapshot.Stages[0].Status != timeline.Running {
		t.Fatalf("eviction lost accepted fact: %+v %v", snapshot, err)
	}
	if got := m.Stats(); got.CachedTimelines != 2 {
		t.Fatalf("capacity: %+v", got)
	}
	data := snapshot.Stages[0]
	data.FinishedAt = time.Now()
	data.Status = timeline.Succeeded
	if err := m.Record("b", data); err != nil {
		t.Fatal(err)
	}
	snapshot, err = m.Read(ctx, "b")
	if err != nil || snapshot.Stages[0].Status != timeline.Succeeded {
		t.Fatalf("late completion after eviction: %+v %v", snapshot, err)
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
	if got := m.Stats(); got.CachedTimelines != 1 || got.ActiveStages != 1 {
		t.Fatalf("finish changed lifetime: %+v", got)
	}
	if err := m.End("task", "running", nil); err != nil {
		t.Fatal(err)
	}
	if m.Stats().CachedTimelines != 1 {
		t.Fatal("End evicted timeline")
	}
}
