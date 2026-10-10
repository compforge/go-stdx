package timeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/compforge/go-stdx/timeline"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func TestRootRevisionsAndDocumentRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := timelinestore.NewMemoryStore()
	owner := handle(t, "id", store, "api")
	if err := owner.Start(ctx, "start", timeline.Attribute{Key: "phase", Value: "queued"}); err != nil {
		t.Fatal(err)
	}
	first, _ := store.Read(ctx, "id")
	owner.SetAttributes(timeline.Attribute{Key: "phase", Value: "running"})
	if _, err := owner.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Merge(ctx, "id", timelinestore.Update{Operation: &first.OperationRecord}); err != nil {
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
	var restored timelinestore.Document
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	_, changed, err := timelinestore.MergeDocument("id", restored, timelinestore.Update{Operation: &last.OperationRecord})
	if err != nil || changed {
		t.Fatalf("roundtrip changes document: %t %v", changed, err)
	}
	other := handle(t, "id", store, "other")
	if err := other.Start(ctx, "start"); !errors.Is(err, timelinestore.ErrConflict) {
		t.Fatalf("second coordinator: %v", err)
	}
}
