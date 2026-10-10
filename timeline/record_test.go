package timeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	gospantimeline "github.com/compforge/go-stdx/timeline/gospan"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func completedStage() timeline.Stage {
	at := time.Date(2026, 9, 28, 1, 0, 0, 0, time.UTC)
	return timeline.Stage{Actor: timeline.Actor{Name: "test"}, ID: "external:pull:1", Name: "image_pull", StartedAt: at, FinishedAt: at.Add(3 * time.Second), Status: timeline.Succeeded, Attributes: map[string]json.RawMessage{"container": json.RawMessage(`"sandbox"`)}}
}

func recordingBackends(t *testing.T, test func(*testing.T, timeline.Timeline)) {
	t.Helper()
	for _, backend := range []string{"shared", "gospan"} {
		t.Run(backend, func(t *testing.T) {
			var tl timeline.Timeline
			if backend == "shared" {
				shared, err := timeline.New(t.Name(), timeline.WithActor(timeline.Actor{Name: "test"}))
				if err != nil {
					t.Fatal(err)
				}
				if err := shared.Start(context.Background(), "start"); err != nil {
					t.Fatal(err)
				}
				tl = shared
			} else {
				var err error
				tl, err = gospantimeline.New(context.Background(), t.Name(), "start")
				if err != nil {
					t.Fatal(err)
				}
			}
			if backend == "gospan" {
				tl = actorTimeline{tl}
			}
			t.Cleanup(func() { _, _ = tl.Finish(context.Background(), nil) })
			test(t, tl)
		})
	}
}

func TestRecordCompletedDataAndLateReplay(t *testing.T) {
	recordingBackends(t, func(t *testing.T, tl timeline.Timeline) {
		ctx := context.Background()
		live := tl.Begin("prepare")
		if live.ID() == "" {
			t.Fatal("missing handle identity")
		}
		live.End(nil)
		first, err := tl.Finish(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		original := completedStage()
		input := completedStage()
		if err := tl.Record(input); err != nil {
			t.Fatal(err)
		}
		input.Attributes["container"][1] = 'X'
		input.Name = "mutated"
		if err := tl.Record(original); err != nil {
			t.Fatal(err)
		}
		got, err := tl.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Stages) != 2 || got.Status != timeline.Succeeded || !got.FinishedAt.Equal(first.FinishedAt) {
			t.Fatalf("late record changed operation: %+v", got)
		}
		stage := got.Stages[0]
		if stage.Name != original.Name || string(stage.Attributes["container"]) != `"sandbox"` || stage.Duration(got.CapturedAt) != 3*time.Second || stage.ParentID != "" || stage.Actor != original.Actor {
			t.Fatalf("bad imported stage: %+v", stage)
		}
		raw, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), `"revision"`) {
			t.Fatal("storage revision leaked into snapshot")
		}
		var restored timeline.Snapshot
		if err := json.Unmarshal(raw, &restored); err != nil {
			t.Fatal(err)
		}
		if len(restored.Stages) != len(got.Stages) || restored.Stages[0].Duration(restored.CapturedAt) != 3*time.Second {
			t.Fatal("JSON round trip lost interval")
		}
		got.Stages[0].Attributes["container"][1] = 'Y'
		again, err := tl.Snapshot(ctx)
		if err != nil || string(again.Stages[0].Attributes["container"]) != `"sandbox"` {
			t.Fatal("snapshot aliases retained data", err)
		}
	})
}

func TestRecordRejectsIncompleteOrInvalidData(t *testing.T) {
	recordingBackends(t, func(t *testing.T, tl timeline.Timeline) {
		mutations := []func(*timeline.Stage){
			func(s *timeline.Stage) { s.ID = "" },
			func(s *timeline.Stage) { s.Name = "" },
			func(s *timeline.Stage) { s.StartedAt = time.Time{} },
			func(s *timeline.Stage) { s.FinishedAt = time.Time{} },
			func(s *timeline.Stage) { s.FinishedAt = s.StartedAt.Add(-time.Second) },
			func(s *timeline.Stage) { s.Status = timeline.Running },
			func(s *timeline.Stage) { s.Elapsed = -1 },
			func(s *timeline.Stage) { s.ParentID = s.ID },
		}
		for i, mutate := range mutations {
			data := completedStage()
			mutate(&data)
			if err := tl.Record(data); !errors.Is(err, timeline.ErrInvalidStage) {
				t.Fatalf("case %d: %v", i, err)
			}
		}
		data := completedStage()
		data.Attributes["broken"] = json.RawMessage(`{`)
		if err := tl.Record(data); !errors.Is(err, timeline.ErrInvalidAttribute) {
			t.Fatal(err)
		}
		got, err := tl.Snapshot(context.Background())
		if err != nil || len(got.Stages) != 0 {
			t.Fatalf("rejected input was buffered: %+v %v", got, err)
		}
	})
}

func TestRecordReplacesSameActorStateBeforeAndAfterFlush(t *testing.T) {
	for _, flushFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(flushFirst), func(t *testing.T) {
			recordingBackends(t, func(t *testing.T, tl timeline.Timeline) {
				ctx := context.Background()
				a, b := completedStage(), completedStage()
				b.FinishedAt = b.FinishedAt.Add(time.Second)
				if err := tl.Record(a); err != nil {
					t.Fatal(err)
				}
				if flushFirst {
					if err := tl.Flush(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if err := tl.Record(b); err != nil {
					t.Fatalf("conflict silently accepted: %v", err)
				}
				snapshot, err := tl.Snapshot(ctx)
				if err != nil || len(snapshot.Stages) != 1 || !snapshot.Stages[0].FinishedAt.Equal(b.FinishedAt) {
					t.Fatalf("rejected import changed facts: %+v %v", snapshot, err)
				}
			})
		})
	}
}

func TestExplicitTimesAndParentWithoutContext(t *testing.T) {
	recordingBackends(t, func(t *testing.T, tl timeline.Timeline) {
		data := completedStage()
		parent := tl.Begin("parent")
		actor := timeline.Actor{Name: "kubelet"}
		child := tl.Begin("schedule_pod", timeline.WithStageID("pod:schedule"), timeline.WithParent(parent.ID()), timeline.WithStartTime(data.StartedAt), timeline.WithStageActor(actor), timeline.WithAttributes(timeline.Attribute{Key: "reason", Value: "pending"}))
		child.SetAttributes(timeline.Attribute{Key: "reason", Value: "scheduled"})
		child.End(nil, timeline.WithEndTime(data.FinishedAt), timeline.WithEndAttributes(timeline.Attribute{Key: "node", Value: "node-a"}))
		parent.End(nil)
		got, err := tl.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		c := got.Stages[0]
		if c.ID != child.ID() || c.ParentID != parent.ID() || c.Actor != actor || !c.StartedAt.Equal(data.StartedAt) || !c.FinishedAt.Equal(data.FinishedAt) || string(c.Attributes["reason"]) != `"scheduled"` || string(c.Attributes["node"]) != `"node-a"` {
			t.Fatalf("lost source facts: %+v", c)
		}
	})
}

func TestConcurrentRecordHandlesDeduplicateAndRetainEveryStage(t *testing.T) {
	ctx := context.Background()
	store := timelinestore.NewMemoryStore()
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tl, _ := timeline.New("operation", timeline.WithActor(timeline.Actor{Name: "test"}), timeline.WithStore(store))
			for j := 0; j < 2; j++ {
				if err := tl.Record(completedStage()); err != nil {
					t.Error(err)
				}
			}
			own := completedStage()
			own.ID = timeline.StageID(fmt.Sprintf("worker:%d", i))
			if err := tl.Record(own); err != nil {
				t.Error(err)
			}
			if err := tl.Flush(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	doc, err := store.Read(ctx, "operation")
	if err != nil || len(doc.Stages) != 21 {
		t.Fatalf("lost or duplicated stages: %d %v", len(doc.Stages), err)
	}
}

func TestRecordCompletesKnownStartWithoutCallerRevision(t *testing.T) {
	ctx := context.Background()
	store := timelinestore.NewMemoryStore()
	a, _ := timeline.New("id", timeline.WithActor(timeline.Actor{Name: "test"}), timeline.WithStore(store))
	data := completedStage()
	a.Begin(data.Name, timeline.WithStageID(data.ID), timeline.WithStartTime(data.StartedAt))
	if err := a.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	pending, _ := store.Read(ctx, "id")
	b, _ := timeline.New("id", timeline.WithActor(timeline.Actor{Name: "test"}), timeline.WithStore(store))
	if err := b.Record(data); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Same actor writers are best effort: a later accepted start may reopen it.
	if err := store.Merge(ctx, "id", timelinestore.Update{Stages: pending.Stages}); err != nil {
		t.Fatal(err)
	}
	before, _ := store.Read(ctx, "id")
	if before.Stages[0].Status != timeline.Running {
		t.Fatal(before)
	}
	if _, err := b.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Record(data); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ := store.Read(ctx, "id")
	if after.Stages[0].Status != timeline.Succeeded {
		t.Fatal("replay modified terminal interval")
	}
}

// Missing parent data must not block a component's own recording. Preserving
// the reference allows an independently reported parent to arrive later.
func TestStagesAcceptAbsentParentAndRetainLateAssociation(t *testing.T) {
	recordingBackends(t, func(t *testing.T, tl timeline.Timeline) {
		ctx := context.Background()
		parentID := timeline.StageID("late-parent")
		imported := completedStage()
		imported.ParentID = parentID
		if err := tl.Record(imported); err != nil {
			t.Fatal(err)
		}
		live := tl.Begin("live-child", timeline.WithParent(parentID))
		live.End(nil)
		if err := tl.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		before, err := tl.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(before.Stages) != 2 {
			t.Fatalf("lost children before parent arrived: %+v", before.Stages)
		}
		for _, stage := range before.Stages {
			if stage.ParentID != parentID {
				t.Fatalf("missing parent reference rewritten: %+v", stage)
			}
		}
		if !strings.Contains(before.Summary(), imported.Name) || !strings.Contains(before.Summary(), "live-child") {
			t.Fatalf("unresolved children missing from summary: %s", before.Summary())
		}
		parent := completedStage()
		parent.ID, parent.Name = parentID, "parent"
		if err := tl.Record(parent); err != nil {
			t.Fatal(err)
		}
		after, err := tl.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(after.Stages) != 3 {
			t.Fatalf("lost late parent or children: %+v", after.Stages)
		}
		parentFound := false
		for _, stage := range after.Stages {
			if stage.ID == parentID {
				parentFound = true
				if stage.ParentID != "" {
					t.Fatalf("omitted parent must stay empty: %+v", stage)
				}
			} else if stage.ParentID != parentID {
				t.Fatalf("late association lost: %+v", stage)
			}
		}
		if !parentFound {
			t.Fatal("late parent was not recorded")
		}
	})
}

// Context is a carrier, never a source of implicit parentage.
func TestContextDoesNotAssignParent(t *testing.T) {
	recordingBackends(t, func(t *testing.T, tl timeline.Timeline) {
		ctx, parent := timeline.BeginWithContext(context.Background(), tl, "parent")
		_, independent := timeline.BeginWithContext(ctx, tl, "independent")
		_, child := timeline.BeginWithContext(ctx, tl, "child", timeline.WithParent(parent.ID()))
		independent.End(nil)
		child.End(nil)
		parent.End(nil)
		snapshot, err := tl.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, stage := range snapshot.Stages {
			want := timeline.StageID("")
			if stage.ID == child.ID() {
				want = parent.ID()
			}
			if stage.ParentID != want {
				t.Fatalf("stage %s parent = %q, want %q", stage.Name, stage.ParentID, want)
			}
		}
	})
}

// The native adapter accepts actors at each Begin; keep the common backend
// fixtures attributed without changing explicit actor options.
type actorTimeline struct{ timeline.Timeline }

func (t actorTimeline) Begin(name string, opts ...timeline.StageOption) timeline.StageHandle {
	return t.Timeline.Begin(name, append([]timeline.StageOption{timeline.WithStageActor(timeline.Actor{Name: "test"})}, opts...)...)
}
