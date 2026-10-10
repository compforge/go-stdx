package timeline_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	gospantimeline "github.com/compforge/go-stdx/timeline/gospan"
)

func TestDefaultActorAndNamedActorRemainIndependent(t *testing.T) {
	ctx := context.Background()
	m, err := timeline.NewManager(nil, timeline.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(ctx)
	previous := timeline.SetDefault(m)
	defer timeline.SetDefault(previous)
	id := t.Name()
	if err := timeline.Start(id, "work"); err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.Begin(id, "phase", timeline.WithStageID("shared")); err != nil {
		t.Fatal(err)
	}
	peer := timeline.Actor{ID: "peer"}
	if _, err := timeline.Begin(id, "phase", timeline.WithStageID("shared"), timeline.WithStageActor(peer)); err != nil {
		t.Fatal(err)
	}
	if err := timeline.End(id, "phase", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := timeline.Read(ctx, id, false)
	if err != nil || len(snapshot.Stages) != 2 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	for _, stage := range snapshot.Stages {
		want := timeline.Succeeded
		if stage.Actor == peer {
			want = timeline.Running
		}
		if stage.Status != want {
			t.Fatalf("named End selected another actor: %+v", stage)
		}
	}
	if err := timeline.End(id, "phase", nil, timeline.WithEndActor(peer)); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-time.Second)
	if err := timeline.Record(id, timeline.Stage{ID: "import", Name: "import", StartedAt: at, FinishedAt: at.Add(time.Millisecond), Status: timeline.Succeeded}); err != nil {
		t.Fatal(err)
	}
	if err := timeline.Finish(id, nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err = timeline.Read(ctx, id, false)
	if err != nil || snapshot.Status != timeline.Succeeded || len(snapshot.Stages) != 3 {
		t.Fatalf("final snapshot=%+v err=%v", snapshot, err)
	}
}

func TestObjectBackendsRecordWithoutActor(t *testing.T) {
	factories := map[string]func(context.Context, string) (timeline.Timeline, error){
		"handle": func(_ context.Context, id string) (timeline.Timeline, error) { return timeline.New(id) },
		"gospan": func(ctx context.Context, id string) (timeline.Timeline, error) {
			return gospantimeline.New(ctx, id, "work")
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tl, err := factory(ctx, t.Name())
			if err != nil {
				t.Fatal(err)
			}
			if err := tl.Begin("local").End(nil); err != nil {
				t.Fatal(err)
			}
			at := time.Now().Add(-time.Second)
			if err := tl.Record(timeline.Stage{ID: "import", Name: "import", StartedAt: at, FinishedAt: at.Add(time.Millisecond), Status: timeline.Succeeded}); err != nil {
				t.Fatal(err)
			}
			snapshot, err := tl.Finish(ctx, nil)
			if err != nil || len(snapshot.Stages) != 2 {
				t.Fatalf("snapshot=%+v err=%v", snapshot, err)
			}
			raw, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(raw, []byte(`"actors"`)) || bytes.Contains(raw, []byte(`"actor_ref"`)) {
				t.Fatalf("default actor added wire identity: %s", raw)
			}
			var decoded timeline.Snapshot
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			for _, stage := range decoded.Stages {
				if stage.Actor != (timeline.Actor{}) || stage.Status != timeline.Succeeded {
					t.Fatalf("lost default actor facts: %+v", stage)
				}
			}
		})
	}
}
