package store_test

import (
	"errors"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func TestOptionalOperationBoundariesMergeInEitherOrder(t *testing.T) {
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	start := timelinestore.OperationRecord{Revision: 1, Operation: "task", StartedAt: at, Status: timeline.Unknown}
	finish := timelinestore.OperationRecord{Revision: 1, FinishedAt: at.Add(time.Second), Status: timeline.Succeeded}
	for _, order := range [][]timelinestore.OperationRecord{{start, finish}, {finish, start}} {
		var doc timelinestore.Document
		for _, fact := range order {
			var err error
			doc, _, err = timelinestore.MergeDocument("id", doc, timelinestore.Update{Operation: &fact})
			if err != nil {
				t.Fatal(err)
			}
		}
		if doc.Operation != "task" || !doc.StartedAt.Equal(at) || !doc.FinishedAt.Equal(finish.FinishedAt) || doc.Status != timeline.Succeeded {
			t.Fatalf("boundary overwritten: %+v", doc)
		}
		for _, fact := range order {
			_, changed, err := timelinestore.MergeDocument("id", doc, timelinestore.Update{Operation: &fact})
			if err != nil || changed {
				t.Fatalf("retry changed facts: %v %v", changed, err)
			}
		}
		changed := finish
		changed.Status = timeline.Failed
		if _, _, err := timelinestore.MergeDocument("id", doc, timelinestore.Update{Operation: &changed}); !errors.Is(err, timelinestore.ErrConflict) {
			t.Fatal(err)
		}
	}
}
