package manager_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	managed "github.com/compforge/go-stdx/timeline/manager"
	"github.com/compforge/go-stdx/timeline/store"
)

type loadingStore struct {
	*store.MemoryStore
	read func(context.Context, string) (store.Document, error)
}

func (s *loadingStore) Read(ctx context.Context, id string) (store.Document, error) {
	return s.read(ctx, id)
}
func seed(t *testing.T, s *store.MemoryStore, id string) {
	t.Helper()
	now := time.Now()
	if err := s.Merge(context.Background(), id, store.Update{Completed: []timeline.Stage{{Actor: timeline.Actor{Name: "test"}, ID: timeline.StageID(id + "-stage"), Name: "seed", StartedAt: now, FinishedAt: now, Status: timeline.Succeeded}}}); err != nil {
		t.Fatal(err)
	}
}

func TestLoadingCacheSharesMissAndIsolatesCanceledWaiter(t *testing.T) {
	backend := &loadingStore{MemoryStore: store.NewMemoryStore()}
	seed(t, backend.MemoryStore, "task")
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	backend.read = func(ctx context.Context, id string) (store.Document, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
			return backend.MemoryStore.Read(ctx, id)
		case <-ctx.Done():
			return store.Document{}, ctx.Err()
		}
	}
	m := newManager(t, backend, managed.Config{Actor: managed.Actor{Name: "test"}, ExportTimeout: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := m.Read(ctx, "task", false); first <- err }()
	waitSignal(t, entered)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := m.Read(context.Background(), "task", false)
			if err != nil || len(s.Stages) != 1 {
				t.Errorf("load: %+v %v", s, err)
			}
		}()
	}
	close(release)
	wg.Wait()
	s, err := m.Read(context.Background(), "task", false)
	if err != nil || s.Collection.StoreRead || calls.Load() != 1 {
		t.Fatalf("cache hit invoked loader: %+v calls=%d %v", s, calls.Load(), err)
	}
}

func TestLoadingFailuresAreBoundedAndNotCached(t *testing.T) {
	backend := &loadingStore{MemoryStore: store.NewMemoryStore()}
	var healthy atomic.Bool
	backend.read = func(ctx context.Context, id string) (store.Document, error) {
		if !healthy.Load() {
			<-ctx.Done()
			return store.Document{}, ctx.Err()
		}
		return backend.MemoryStore.Read(ctx, id)
	}
	m := newManager(t, backend, managed.Config{Actor: managed.Actor{Name: "test"}, ExportTimeout: 10 * time.Millisecond})
	if _, err := m.Read(context.Background(), "task", false); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	healthy.Store(true)
	if _, err := m.Read(context.Background(), "task", false); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	if m.Stats().CachedTimelines != 0 {
		t.Fatal("failed load cached")
	}
	if _, err := m.Begin("task", "new"); err != nil {
		t.Fatal(err)
	}
	s, err := m.Read(context.Background(), "task", false)
	if err != nil || len(s.Stages) != 1 {
		t.Fatalf("write loader did not create: %+v %v", s, err)
	}
}

func TestLRUUsesStoreOnMiss(t *testing.T) {
	backend := &loadingStore{MemoryStore: store.NewMemoryStore()}
	for _, id := range []string{"a", "b", "c"} {
		seed(t, backend.MemoryStore, id)
	}
	var calls atomic.Int32
	backend.read = func(ctx context.Context, id string) (store.Document, error) {
		calls.Add(1)
		return backend.MemoryStore.Read(ctx, id)
	}
	m := newManager(t, backend, managed.Config{Actor: managed.Actor{Name: "test"}, MaxTimelines: 2})
	read := func(id string) timeline.Snapshot {
		t.Helper()
		s, err := m.Read(context.Background(), id, false)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	read("a")
	read("b")
	read("a")
	read("c")
	if read("a").Collection.StoreRead {
		t.Fatal("LRU evicted recently used a")
	}
	if !read("b").Collection.StoreRead || calls.Load() != 4 {
		t.Fatalf("b did not reload: calls=%d", calls.Load())
	}

}

func TestReadOwnsDataAndDoesNotFlush(t *testing.T) {
	backend := store.NewMemoryStore()
	m := newManager(t, backend, managed.Config{Actor: managed.Actor{Name: "test"}, FlushInterval: time.Hour})
	now := time.Now()
	data := timeline.Stage{Actor: timeline.Actor{Name: "test"}, ID: "external", Name: "source", StartedAt: now, FinishedAt: now, Status: timeline.Succeeded, Attributes: map[string]json.RawMessage{"value": json.RawMessage(`{"n":1}`)}}
	if err := m.Record("task", data); err != nil {
		t.Fatal(err)
	}
	data.Attributes["value"][5] = '9'
	snapshot, err := m.Read(context.Background(), "task", false)
	if err != nil || string(snapshot.Stages[0].Attributes["value"]) != `{"n":1}` || snapshot.Collection.LocalFlushed || snapshot.Collection.StoreRead {
		t.Fatalf("read: %+v %v", snapshot, err)
	}
	snapshot.Stages[0].Attributes["value"][5] = '8'
	again, err := m.Read(context.Background(), "task", false)
	if err != nil || string(again.Stages[0].Attributes["value"]) != `{"n":1}` {
		t.Fatal("snapshot aliases cache")
	}
	if _, err = backend.Read(context.Background(), "task"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("Read forced persistence")
	}
	if err = m.Flush(context.Background(), "task", true); err != nil {
		t.Fatal(err)
	}
	if doc, err := backend.Read(context.Background(), "task"); err != nil || string(doc.Stages[0].Attributes["value"]) != `{"n":1}` {
		t.Fatalf("save aliases caller: %+v %v", doc, err)
	}
}
