package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
)

func queueStage(id string) store.Update {
	return store.Update{Stages: []store.StageUpdate{{Stage: model.Stage{ID: model.StageID(id), Name: "work", Actor: model.Actor{Name: "worker"}, StartedAt: time.Now(), Status: model.Running}, Revision: 1}}}
}
func TestFullSaveQueueRetainsPeriodicPendingWork(t *testing.T) {
	backend := store.NewMemoryStore()
	c, err := newCache(backend, Config{BatchLimit: 1, MaxPendingTimelines: 2, FlushInterval: 5 * time.Millisecond}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())
	for _, id := range []string{"a", "b"} {
		if err := c.Write(context.Background(), id, queueStage(id)); err != nil {
			t.Fatal(err)
		}
	}
	c.requestSave("unrelated")
	c.requestSave("unrelated")
	c.requestSave("b")
	if len(c.saveCh) != cap(c.saveCh) {
		t.Fatal("queue fixture was not full")
	}
	// Start the real worker only after saturation; no producer gets a privileged
	// direct Store path to hide a lost queue notification.
	c.done = make(chan struct{})
	go c.runSave()
	deadline := time.After(time.Second)
	for {
		a, ea := backend.Read(context.Background(), "a")
		b, eb := backend.Read(context.Background(), "b")
		if ea == nil && eb == nil && len(a.Stages) == 1 && len(b.Stages) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("full queue lost periodic pending work")
		case <-time.After(time.Millisecond):
		}
	}
}

type loadProbe struct {
	*store.MemoryStore
	mu      sync.Mutex
	batches [][]string
	limits  []int
	afters  []time.Time
}

func (s *loadProbe) MGet(ctx context.Context, ids []string) ([]store.Document, error) {
	s.mu.Lock()
	s.batches = append(s.batches, append([]string(nil), ids...))
	s.mu.Unlock()
	return nil, errors.New("mget unavailable")
}
func (s *loadProbe) Latest(ctx context.Context, after time.Time, limit int) ([]store.Document, error) {
	s.mu.Lock()
	s.limits = append(s.limits, limit)
	s.afters = append(s.afters, after)
	s.mu.Unlock()
	return s.MemoryStore.Latest(ctx, after, limit)
}
func TestLoadBatchLimitAndLatestAfterMGetFailure(t *testing.T) {
	backend := &loadProbe{MemoryStore: store.NewMemoryStore()}
	c, err := newCache(backend, Config{BatchLimit: 2, LoadInterval: 10 * time.Millisecond, OnError: func(string, error) {}}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())
	for _, id := range []string{"a", "b", "c", "d"} {
		c.loadCh <- id
	}
	c.loadDone = make(chan struct{})
	go c.runLoad()
	deadline := time.After(time.Second)
	for {
		backend.mu.Lock()
		ready := len(backend.batches) >= 2 && len(backend.limits) >= 2
		backend.mu.Unlock()
		if ready {
			break
		}
		select {
		case <-deadline:
			t.Fatal("load worker stalled")
		case <-time.After(time.Millisecond):
		}
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	for _, ids := range backend.batches {
		if len(ids) > 2 {
			t.Fatalf("oversized MGet: %v", ids)
		}
	}
	for _, limit := range backend.limits {
		if limit != 2 {
			t.Fatalf("Latest has separate limit: %d", limit)
		}
	}
	if backend.afters[0] != (time.Time{}) || backend.afters[1].IsZero() {
		t.Fatal("Latest time window did not advance")
	}
}

type staleReadStore struct {
	*store.MemoryStore
	entered chan struct{}
	release chan struct{}
}

func (s *staleReadStore) Read(ctx context.Context, id string) (store.Document, error) {
	d, err := s.MemoryStore.Read(ctx, id)
	close(s.entered)
	select {
	case <-s.release:
		return d, err
	case <-ctx.Done():
		return store.Document{}, ctx.Err()
	}
}
func TestFreshReadCannotRegressConcurrentSavedState(t *testing.T) {
	ctx := context.Background()
	backend := &staleReadStore{MemoryStore: store.NewMemoryStore(), entered: make(chan struct{}), release: make(chan struct{})}
	c, err := newCache(backend, Config{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(ctx)
	update := queueStage("stage")
	if err := backend.MemoryStore.Merge(ctx, "op", update); err != nil {
		t.Fatal(err)
	}
	doc, _ := backend.MemoryStore.Read(ctx, "op")
	c.mergeLoaded(doc, nil, false)
	result := make(chan model.Snapshot, 1)
	go func() {
		snapshot, err := c.Read(ctx, "op", true)
		if err != nil {
			t.Error(err)
		}
		result <- snapshot
	}()
	<-backend.entered
	update.Stages[0].Status = model.Succeeded
	update.Stages[0].FinishedAt = time.Now()
	if err := c.Write(ctx, "op", update); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(ctx, "op", true); err != nil {
		t.Fatal(err)
	}
	close(backend.release)
	snapshot := <-result
	if len(snapshot.Stages) != 1 || snapshot.Stages[0].Status != model.Succeeded {
		t.Fatalf("stale read regressed just-saved state: %+v", snapshot)
	}
	if c.Stats().PendingUpdates != 0 {
		t.Fatal("remote refresh re-enqueued saved records")
	}
}
