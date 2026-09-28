package timeline_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	gospantimeline "github.com/compforge/go-stdx/timeline/gospan"
)

func finishTimeline(t *testing.T, tl timeline.Timeline) timeline.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := tl.Finish(ctx, nil)
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	return s
}

func TestRegistryConcurrentCreateAndParallelRecording(t *testing.T) {
	var creates atomic.Int32
	r := timeline.NewRegistry(func(ctx context.Context, id, operation string, fields ...timeline.Field) (timeline.Timeline, error) {
		creates.Add(1)
		return gospantimeline.New(ctx, id, operation, fields...)
	})
	const count = 32
	winners := make(chan timeline.Timeline, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tl, err := r.Create(context.Background(), "start-42", "sandbox_start")
			if err == nil {
				winners <- tl
			} else if !errors.Is(err, timeline.ErrAlreadyExists) {
				t.Errorf("create: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(winners)
	if len(winners) != 1 || creates.Load() != 1 {
		t.Fatalf("winners=%d backend creates=%d", len(winners), creates.Load())
	}
	owner := <-winners
	t.Cleanup(func() { finishTimeline(t, owner) })
	if owner.ID() != "start-42" {
		t.Fatalf("ID=%q", owner.ID())
	}
	ctx := timeline.NewContext(context.Background(), owner)
	if got, ok := timeline.FromContext(ctx); !ok || got != owner {
		t.Fatal("optional context binding lost registry handle")
	}
	_, parent := timeline.BeginContext(ctx, owner, "owner_work")
	if _, err := owner.Finish(ctx, nil); !errors.Is(err, timeline.ErrActiveStages) {
		t.Fatalf("early finish: %v", err)
	}
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tl, ok := r.Lookup(owner.ID())
			if !ok || tl != owner {
				t.Error("lookup returned a different or missing instance")
				return
			}
			childCtx, stage := timeline.BeginContext(ctx, tl, fmt.Sprintf("component-%d", i))
			if got, ok := timeline.FromContext(childCtx); !ok || got != owner {
				t.Error("Begin replaced registry context handle")
			}
			_, child := timeline.BeginContext(childCtx, tl, "child")
			child.End(nil)
			stage.End(nil)
		}()
	}
	wg.Wait()
	parent.End(nil)
	s := finishTimeline(t, owner)
	if s.ID != owner.ID() || s.RootStageID == "" || len(s.Stages) != 1+2*count {
		t.Fatalf("lost identity or work: %+v", s)
	}
	for _, stage := range s.Stages {
		if stage.Status != timeline.Succeeded {
			t.Fatalf("unfinished stage: %+v", stage)
		}
	}
	if _, ok := r.Lookup(owner.ID()); ok {
		t.Fatal("completed timeline still indexed")
	}
}

func TestRegistryInitializationDoesNotBlockOtherIDs(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	failed := errors.New("backend unavailable")
	r := timeline.NewRegistry(func(ctx context.Context, id, operation string, fields ...timeline.Field) (timeline.Timeline, error) {
		if id == "slow" {
			close(entered)
			<-release
			return nil, failed
		}
		return gospantimeline.New(ctx, id, operation, fields...)
	})
	first := make(chan error, 1)
	go func() { _, err := r.Create(context.Background(), "slow", "work"); first <- err }()
	<-entered
	checked := make(chan error, 1)
	go func() {
		if _, ok := r.Lookup("slow"); ok {
			checked <- errors.New("published before factory returned")
			return
		}
		if _, err := r.Create(context.Background(), "slow", "work"); !errors.Is(err, timeline.ErrAlreadyExists) {
			checked <- fmt.Errorf("duplicate reservation: %v", err)
			return
		}
		other, err := r.Create(context.Background(), "fast", "work")
		if err == nil {
			_, err = other.Finish(context.Background(), nil)
		}
		checked <- err
	}()
	select {
	case err := <-checked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("backend initialization blocked unrelated operations")
	}
	unblock()
	if err := <-first; !errors.Is(err, failed) {
		t.Fatalf("factory error: %v", err)
	}
	if _, ok := r.Lookup("slow"); ok {
		t.Fatal("failed initialization retained")
	}
}

func TestRegistryFailedCreateCanRetry(t *testing.T) {
	var calls int
	failed := errors.New("backend unavailable")
	r := timeline.NewRegistry(func(ctx context.Context, id, operation string, fields ...timeline.Field) (timeline.Timeline, error) {
		calls++
		if calls == 1 {
			return nil, failed
		}
		return gospantimeline.New(ctx, id, operation, fields...)
	})
	if _, err := r.Create(context.Background(), "retry", "work"); !errors.Is(err, failed) {
		t.Fatalf("first error: %v", err)
	}
	tl, err := r.Create(context.Background(), "retry", "work")
	if err != nil {
		t.Fatal(err)
	}
	finishTimeline(t, tl)
}

func TestRegistryOldFinishCannotRemoveReplacement(t *testing.T) {
	r := timeline.NewRegistry(gospantimeline.New)
	old, err := r.Create(context.Background(), "same", "first")
	if err != nil {
		t.Fatal(err)
	}
	first := finishTimeline(t, old)
	current, err := r.Create(context.Background(), "same", "second")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { finishTimeline(t, current) })
	if again := finishTimeline(t, old); again.Operation != first.Operation || again.ID != first.ID {
		t.Fatal("old handle lost its snapshot")
	}
	if got, ok := r.Lookup("same"); !ok || got != current || got == old {
		t.Fatal("old Finish removed replacement")
	}
}

func TestRegistryLookupBeginAndFinishRace(t *testing.T) {
	r := timeline.NewRegistry(gospantimeline.New)
	for i := range 32 {
		id := fmt.Sprintf("start-%d", i)
		tl, err := r.Create(context.Background(), id, "work")
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if found, ok := r.Lookup(id); ok {
					_, stage := timeline.BeginContext(context.Background(), found, "worker")
					stage.End(nil)
				}
			}()
		}
		_, err = tl.Finish(context.Background(), nil)
		if err != nil && !errors.Is(err, timeline.ErrActiveStages) {
			t.Errorf("finish: %v", err)
		}
		wg.Wait()
		s := finishTimeline(t, tl)
		if s.ID != id || s.Status != timeline.Succeeded || len(s.RunningStages()) != 0 {
			t.Fatalf("bad final snapshot: %+v", s)
		}
		if _, ok := r.Lookup(id); ok {
			t.Fatal("completed timeline remains indexed")
		}
	}
}

func TestTimelineRejectsEmptyID(t *testing.T) {
	r := timeline.NewRegistry(gospantimeline.New)
	if _, err := r.Create(context.Background(), "", "work"); !errors.Is(err, timeline.ErrEmptyID) {
		t.Fatalf("registry error: %v", err)
	}
	if _, err := gospantimeline.New(context.Background(), "", "work"); !errors.Is(err, timeline.ErrEmptyID) {
		t.Fatalf("backend error: %v", err)
	}
}
