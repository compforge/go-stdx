package manager_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	managed "github.com/compforge/go-stdx/timeline/manager"
	"github.com/compforge/go-stdx/timeline/store"
)

func TestPeerActorsShareStageAndFreshReadKeepsPending(t *testing.T) {
	ctx := context.Background()
	backend := store.NewMemoryStore()
	a := newManager(t, backend, managed.Config{Actor: timeline.Actor{ID: "a"}, FlushInterval: time.Hour})
	b := newManager(t, backend, managed.Config{Actor: timeline.Actor{Name: "b"}, FlushInterval: time.Hour})
	sa, err := a.Begin("op", "work", timeline.WithStageID("same"))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(ctx, "op", true); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Read(ctx, "op", true); err != nil {
		t.Fatal(err)
	}
	sb, err := b.Begin("op", "work", timeline.WithStageID("same"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sa.End(errors.New("a failed")); err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(ctx, "op", true); err != nil {
		t.Fatal(err)
	}
	s, err := b.Read(ctx, "op", true)
	if err != nil || len(s.Stages) != 2 || s.Collection.LocalFlushed || !s.Collection.StoreRead {
		t.Fatalf("fresh merged view: %+v %v", s, err)
	}
	// A read must not publish B's pending start.
	persisted, _ := backend.Read(ctx, "op")
	if len(persisted.Stages) != 1 {
		t.Fatal("fresh read flushed B")
	}
	if err := b.End("op", "work", nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(ctx, "op", true); err != nil {
		t.Fatal(err)
	}
	s, err = a.Read(ctx, "op", true)
	if err != nil || len(s.Stages) != 2 {
		t.Fatalf("peer read: %+v %v", s, err)
	}
	for _, stage := range s.Stages {
		if stage.Actor.ID == "a" && stage.Status != timeline.Failed {
			t.Fatal("B ended A's stage")
		}
		if stage.Actor.Name == "b" && stage.Status != timeline.Succeeded {
			t.Fatal("B did not end its stage")
		}
	}
	if sb.ID() != sa.ID() {
		t.Fatal("test must share a StageID")
	}
}

func TestLatestDiscoversPeerAndLoadQueueRefreshesKnownID(t *testing.T) {
	ctx := context.Background()
	backend := store.NewMemoryStore()
	a := newManager(t, backend, managed.Config{Actor: timeline.Actor{Name: "a"}, FlushInterval: time.Hour})
	b := newManager(t, backend, managed.Config{Actor: timeline.Actor{Name: "b"}, LoadInterval: 10 * time.Millisecond, BatchLimit: 1})
	stage, err := a.Begin("discovered", "work")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(ctx, "discovered", true); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return b.Stats().CachedTimelines == 1 })
	// Cache is already populated by Latest, so Read does not need a miss loader.
	snapshot, err := b.Read(ctx, "discovered", false)
	if err != nil || snapshot.Collection.StoreRead || len(snapshot.Stages) != 1 {
		t.Fatalf("discovery: %+v %v", snapshot, err)
	}
	if err := stage.End(nil); err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(ctx, "discovered", true); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		s, err := b.Read(ctx, "discovered", false)
		return err == nil && len(s.Stages) == 1 && s.Stages[0].Status == timeline.Succeeded
	})
}

func TestSaveBatchLimitAndPeriodicFairness(t *testing.T) {
	backend := &managedStore{MemoryStore: store.NewMemoryStore()}
	calls := make(chan string, 32)
	backend.merge = func(ctx context.Context, id string, u store.Update) error {
		err := backend.MemoryStore.Merge(ctx, id, u)
		calls <- id
		return err
	}
	m := newManager(t, backend, managed.Config{FlushInterval: 200 * time.Millisecond, BatchLimit: 2})
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		if _, err := m.Begin(id, "work"); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for range 2 {
		select {
		case id := <-calls:
			seen[id] = true
		case <-time.After(time.Second):
			t.Fatal("periodic save did not run")
		}
	}
	select {
	case id := <-calls:
		t.Fatalf("first periodic batch exceeded limit: %s", id)
	case <-time.After(30 * time.Millisecond):
	}
	for len(seen) < 5 {
		select {
		case id := <-calls:
			seen[id] = true
		case <-time.After(2 * time.Second):
			t.Fatal("periodic batch starved remaining IDs")
		}
	}
}

func TestFlushCheckpointDoesNotWaitForLaterBatch(t *testing.T) {
	backend := &managedStore{MemoryStore: store.NewMemoryStore()}
	entered, release := make(chan struct{}), make(chan struct{})
	later := make(chan struct{})
	var once sync.Once
	calls := 0
	backend.merge = func(ctx context.Context, id string, u store.Update) error {
		calls++
		if calls == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			once.Do(func() { close(later) })
			<-ctx.Done()
			return ctx.Err()
		}
		return backend.MemoryStore.Merge(ctx, id, u)
	}
	m := newManager(t, backend, managed.Config{FlushInterval: time.Hour, ExportTimeout: time.Second})
	stage, err := m.Begin("op", "work")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- m.Flush(context.Background(), "op", true) }()
	waitSignal(t, entered)
	if err := stage.End(nil); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("checkpoint waited for post-call End")
	}
	waitSignal(t, later)
}
