package timeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
)

func handle(t *testing.T, id string, store timeline.Store, actor string) *timeline.Recorder {
	t.Helper()
	tl, err := timeline.New(id, timeline.WithStore(store), timeline.WithActor(timeline.Actor{ID: actor, Name: "pod-" + actor}))
	if err != nil {
		t.Fatal(err)
	}
	return tl
}

func TestSharedHandlesActorHierarchyAndLateStages(t *testing.T) {
	ctx := context.Background()
	store := timeline.NewMemoryStore()
	owner := handle(t, "sandbox", store, "api")
	if err := owner.Start(ctx, "sandbox_start"); err != nil {
		t.Fatal(err)
	}
	parentCtx, parent := owner.Begin(ctx, "start")
	ref, ok := timeline.StageFromContext(parentCtx)
	if !ok {
		t.Fatal("missing stage reference")
	}
	// Only pure identity crosses this boundary, as it would over a queue/DB.
	raw, _ := json.Marshal(ref)
	var remote timeline.StageRef
	if err := json.Unmarshal(raw, &remote); err != nil {
		t.Fatal(err)
	}
	worker := handle(t, "sandbox", store, "scheduler")
	remoteCtx := timeline.NewStageContext(ctx, remote)
	_, stage := worker.Begin(remoteCtx, "acquire_carrier")
	if err := worker.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	parent.End(nil)
	done, err := owner.Finish(ctx, nil)
	if err != nil || done.Status != timeline.Succeeded || len(done.RunningStages()) != 1 {
		t.Fatalf("early business end: %+v %v", done, err)
	}
	stage.End(nil)
	stage.End(errors.New("second end"))
	if err := worker.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	_, late := worker.Begin(ctx, "late_observation")
	late.End(nil)
	got, err := worker.Snapshot(ctx)
	if err != nil || len(got.Stages) != 3 || len(got.RunningStages()) != 0 {
		t.Fatalf("late snapshot: %+v %v", got, err)
	}
	for _, s := range got.Stages {
		if s.Name == "acquire_carrier" && (s.ParentID != ref.StageID || s.Actor.ID != "scheduler" || s.Actor.Name != "pod-scheduler" || s.Status != timeline.Succeeded || s.Elapsed <= 0) {
			t.Fatalf("remote stage: %+v", s)
		}
	}
	if !got.FinishedAt.Equal(done.FinishedAt) || got.StartedAt != done.StartedAt {
		t.Fatal("new handles reset operation boundaries")
	}
	// A reference belonging to another timeline cannot create a foreign edge.
	unrelated := handle(t, "other", store, "other")
	_, s := unrelated.Begin(remoteCtx, "isolated")
	s.End(nil)
	isolated, _ := unrelated.Snapshot(ctx)
	if isolated.Stages[0].ParentID != isolated.RootStageID {
		t.Fatal("foreign timeline parent leaked")
	}
}

func TestConcurrentHandlesAndSnapshots(t *testing.T) {
	ctx := context.Background()
	store := timeline.NewMemoryStore()
	owner := handle(t, "same", store, "owner")
	if err := owner.Start(ctx, "parallel"); err != nil {
		t.Fatal(err)
	}
	const count = 24
	var wg sync.WaitGroup
	failures := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tl := handle(t, "same", store, fmt.Sprint(i))
			for j := 0; j < 8; j++ {
				_, stage := tl.Begin(ctx, "parallel")
				stage.SetFields(timeline.Field{Key: "index", Value: j})
				if j%2 == 0 {
					if _, err := tl.Snapshot(ctx); err != nil {
						failures <- err
						return
					}
				}
				stage.End(nil)
			}
			failures <- tl.Flush(ctx)
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := owner.Finish(ctx, nil)
	if err != nil || len(got.Stages) != count*8 {
		t.Fatalf("count=%d err=%v", len(got.Stages), err)
	}
	ids := make(map[timeline.StageID]bool)
	for _, stage := range got.Stages {
		if ids[stage.ID] || stage.Status != timeline.Succeeded {
			t.Fatalf("collision/incomplete: %+v", stage)
		}
		ids[stage.ID] = true
	}
}

type lostAcknowledgement struct {
	timeline.Store
	once sync.Once
}

func (s *lostAcknowledgement) Merge(ctx context.Context, id string, update timeline.Update) error {
	if err := s.Store.Merge(ctx, id, update); err != nil {
		return err
	}
	var err error
	s.once.Do(func() { err = errors.New("commit acknowledgement lost") })
	return err
}

func TestFlushRetriesAcceptedRecordsWithoutDuplicating(t *testing.T) {
	ctx := context.Background()
	store := &lostAcknowledgement{Store: timeline.NewMemoryStore()}
	tl := handle(t, "retry", store, "writer")
	if err := tl.Start(ctx, "start"); err == nil {
		t.Fatal("lost acknowledgement should be visible")
	}
	if err := tl.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	_, stage := tl.Begin(ctx, "work")
	stage.End(nil)
	snapshot, err := tl.Finish(ctx, nil)
	if err != nil || len(snapshot.Stages) != 1 {
		t.Fatalf("retry: %+v %v", snapshot, err)
	}
	doc, _ := store.Read(ctx, "retry")
	if len(doc.Stages) != 1 || doc.Revision != 2 {
		t.Fatalf("retry: %+v", doc)
	}
	// Another coordinator cannot rewrite the accepted business outcome.
	other := handle(t, "retry", store, "other")
	got, err := other.Finish(ctx, errors.New("conflicting outcome"))
	if !errors.Is(err, timeline.ErrNotStarted) || got.Status != timeline.Succeeded {
		t.Fatalf("terminal conflict: %+v %v", got, err)
	}
}

func TestMergeIgnoresDelayedStageUpdates(t *testing.T) {
	at := time.Now().UTC()
	started := timeline.StageRecord{ID: "s", Revision: 1, StartedAt: at, Status: timeline.Running}
	ended := started
	ended.Revision, ended.FinishedAt, ended.Status = 2, at.Add(time.Second), timeline.Succeeded
	store := timeline.NewMemoryStore()
	for _, stage := range []timeline.StageRecord{ended, started, ended} {
		if err := store.Merge(context.Background(), "operation", timeline.Update{Stages: []timeline.StageRecord{stage}}); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := store.Read(context.Background(), "operation")
	if len(got.Stages) != 1 || got.Stages[0].Status != timeline.Succeeded {
		t.Fatalf("out-of-order: %+v", got)
	}
}

func TestSharedSnapshotOwnershipAndEncodingFailure(t *testing.T) {
	ctx := context.Background()
	store := timeline.NewMemoryStore()
	tl := handle(t, "json", store, "worker")
	if err := tl.Start(ctx, "start"); err != nil {
		t.Fatal(err)
	}
	input := map[string]any{"large": uint64(math.MaxUint64), "name": "before"}
	_, stage := tl.Begin(ctx, "work", timeline.Field{Key: "input", Value: input})
	input["name"] = "after"
	stage.End(nil)
	snapshot, err := tl.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var restored timeline.Snapshot
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot, restored) {
		t.Fatal("snapshot JSON did not preserve data")
	}
	fields := restored.Stages[0].Fields
	var value struct {
		Large uint64 `json:"large"`
		Name  string `json:"name"`
	}
	if err := json.Unmarshal(fields["input"], &value); err != nil {
		t.Fatal(err)
	}
	if value.Large != math.MaxUint64 || value.Name != "before" {
		t.Fatalf("input changed: %+v", value)
	}
	snapshot.Stages[0].Fields["input"][0] = '!'
	snapshot.Stages[0].Actor.Name = "changed"
	again, _ := tl.Snapshot(ctx)
	if again.Stages[0].Fields["input"][0] == '!' || again.Stages[0].Actor.Name == "changed" {
		t.Fatal("snapshot aliases store")
	}
	tl.SetFields(timeline.Field{Key: "bad", Value: make(chan int)})
	bad, err := tl.Finish(ctx, nil)
	if !errors.Is(err, timeline.ErrInvalidField) || bad.Collection.LocalFlushed || !bad.Collection.StoreRead || bad.Status != timeline.Succeeded {
		t.Fatalf("collection vs business: %+v %v", bad, err)
	}
}

type blockedStore struct {
	timeline.Store
	entered chan struct{}
	unblock chan struct{}
}

func (s *blockedStore) Merge(ctx context.Context, id string, update timeline.Update) error {
	close(s.entered)
	select {
	case <-s.unblock:
		return s.Store.Merge(ctx, id, update)
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestFlushDeadlineWhileAnotherFlushIsBlocked(t *testing.T) {
	store := &blockedStore{Store: timeline.NewMemoryStore(), entered: make(chan struct{}), unblock: make(chan struct{})}
	tl := handle(t, "blocked", store, "writer")
	_, stage := tl.Begin(context.Background(), "work")
	stage.End(nil)
	done := make(chan error, 1)
	go func() { done <- tl.Flush(context.Background()) }()
	<-store.entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := tl.Flush(ctx)
	close(store.unblock)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("flush did not respect deadline: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestActorIsOptionalAndMayContainOnlyName(t *testing.T) {
	for _, actor := range []timeline.Actor{{}, {Name: "pod-a"}, {ID: "pod-uid"}} {
		tl, err := timeline.New("actor", timeline.WithActor(actor))
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := tl.Start(ctx, "start"); err != nil {
			t.Fatal(err)
		}
		_, stage := tl.Begin(ctx, "work")
		stage.End(nil)
		snapshot, err := tl.Finish(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), `"actor":`) != (actor != (timeline.Actor{})) {
			t.Fatalf("optional actor: %s", raw)
		}
		var restored timeline.Snapshot
		if err := json.Unmarshal(raw, &restored); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(snapshot, restored) || restored.Stages[0].Actor != actor {
			t.Fatalf("roundtrip: %+v", restored)
		}
	}
}
