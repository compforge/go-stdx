package gospantimeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	gospantimeline "github.com/compforge/go-stdx/timeline/gospan"
)

func TestSameStageIDDifferentActors(t *testing.T) {
	ctx := context.Background()
	tl, err := gospantimeline.New(ctx, "op", "work")
	if err != nil {
		t.Fatal(err)
	}
	a := tl.Begin("work", timeline.WithStageID("shared"), timeline.WithStageActor(timeline.Actor{ID: "a"}))
	b := tl.Begin("work", timeline.WithStageID("shared"), timeline.WithStageActor(timeline.Actor{Name: "b"}))
	if err := a.End(nil); err != nil {
		t.Fatal(err)
	}
	if err := b.End(nil); err != nil {
		t.Fatal(err)
	}
	s, err := tl.Finish(ctx, nil)
	if err != nil || len(s.Stages) != 2 {
		t.Fatalf("collapsed actors: %+v %v", s, err)
	}
	for _, stage := range s.Stages {
		if stage.ID != "shared" || stage.Status != timeline.Succeeded {
			t.Fatal(stage)
		}
	}
	replacement := s.Stages[0]
	replacement.FinishedAt = replacement.FinishedAt.Add(time.Second)
	replacement.Code = "updated"
	if err := tl.Record(replacement); err != nil {
		t.Fatal(err)
	}
	s, err = tl.Snapshot(ctx)
	if err != nil || len(s.Stages) != 2 {
		t.Fatalf("same actor duplicated: %+v %v", s, err)
	}
	found := false
	for _, stage := range s.Stages {
		if stage.Key() == replacement.Key() {
			found = stage.Code == "updated"
		}
	}
	if !found {
		t.Fatal("imported replacement lost")
	}
}
