package store_test

import (
	"errors"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func TestDocumentMergeConflictIsAtomicAndTerminalImmutable(t *testing.T) {
	at := time.Now().UTC()
	stage := timelinestore.StageUpdate{Revision: 1, Stage: timeline.Stage{ID: "s", Name: "work", StartedAt: at, Status: timeline.Running}}
	doc, _, err := timelinestore.MergeDocument("id", timelinestore.Document{}, timelinestore.Update{Stages: []timelinestore.StageUpdate{stage}})
	if err != nil {
		t.Fatal(err)
	}
	changed := stage
	changed.Name = "wrong"
	another := stage
	another.ID = "another"
	if _, _, err := timelinestore.MergeDocument("id", doc, timelinestore.Update{Stages: []timelinestore.StageUpdate{another, changed}}); !errors.Is(err, timelinestore.ErrConflict) {
		t.Fatal(err)
	}
	if len(doc.Stages) != 1 || doc.Stages[0].Name != "work" {
		t.Fatalf("input mutated: %+v", doc)
	}
	stage.Revision, stage.Status, stage.FinishedAt = 2, timeline.Succeeded, at.Add(time.Second)
	doc, _, err = timelinestore.MergeDocument("id", doc, timelinestore.Update{Stages: []timelinestore.StageUpdate{stage}})
	if err != nil {
		t.Fatal(err)
	}
	stage.Revision, stage.Status, stage.FinishedAt = 3, timeline.Running, time.Time{}
	if _, _, err := timelinestore.MergeDocument("id", doc, timelinestore.Update{Stages: []timelinestore.StageUpdate{stage}}); !errors.Is(err, timelinestore.ErrConflict) {
		t.Fatalf("reopened terminal: %v", err)
	}
}
