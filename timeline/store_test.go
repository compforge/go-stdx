package timeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
)

func TestDocumentMergeConflictIsAtomicAndTerminalImmutable(t *testing.T) {
	at := time.Now().UTC()
	stage := timeline.StageUpdate{Revision: 1, Stage: timeline.Stage{ID: "s", Name: "work", StartedAt: at, Status: timeline.Running}}
	doc, _, err := timeline.MergeDocument("id", timeline.Document{}, timeline.Update{Stages: []timeline.StageUpdate{stage}})
	if err != nil {
		t.Fatal(err)
	}
	changed := stage
	changed.Name = "wrong"
	another := stage
	another.ID = "another"
	if _, _, err := timeline.MergeDocument("id", doc, timeline.Update{Stages: []timeline.StageUpdate{another, changed}}); !errors.Is(err, timeline.ErrConflict) {
		t.Fatal(err)
	}
	if len(doc.Stages) != 1 || doc.Stages[0].Name != "work" {
		t.Fatalf("input mutated: %+v", doc)
	}
	stage.Revision, stage.Status, stage.FinishedAt = 2, timeline.Succeeded, at.Add(time.Second)
	doc, _, err = timeline.MergeDocument("id", doc, timeline.Update{Stages: []timeline.StageUpdate{stage}})
	if err != nil {
		t.Fatal(err)
	}
	stage.Revision, stage.Status, stage.FinishedAt = 3, timeline.Running, time.Time{}
	if _, _, err := timeline.MergeDocument("id", doc, timeline.Update{Stages: []timeline.StageUpdate{stage}}); !errors.Is(err, timeline.ErrConflict) {
		t.Fatalf("reopened terminal: %v", err)
	}
}

func TestRootRevisionsAndDocumentRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := timeline.NewMemoryStore()
	owner := handle(t, "id", store, "api")
	if err := owner.Start(ctx, "start", timeline.Attribute{Key: "phase", Value: "queued"}); err != nil {
		t.Fatal(err)
	}
	first, _ := store.Read(ctx, "id")
	owner.SetAttributes(timeline.Attribute{Key: "phase", Value: "running"})
	if _, err := owner.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Merge(ctx, "id", timeline.Update{Operation: &first.OperationRecord}); err != nil {
		t.Fatal(err)
	}
	last, _ := store.Read(ctx, "id")
	if last.Status != timeline.Succeeded || string(last.Attributes["phase"]) != `"running"` {
		t.Fatalf("stale root: %+v", last)
	}
	raw, err := json.Marshal(last)
	if err != nil {
		t.Fatal(err)
	}
	var restored timeline.Document
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	_, changed, err := timeline.MergeDocument("id", restored, timeline.Update{Operation: &last.OperationRecord})
	if err != nil || changed {
		t.Fatalf("roundtrip changes document: %t %v", changed, err)
	}
	other := handle(t, "id", store, "other")
	if err := other.Start(ctx, "start"); !errors.Is(err, timeline.ErrConflict) {
		t.Fatalf("second coordinator: %v", err)
	}
}
