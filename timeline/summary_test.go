package timeline_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
)

func TestSummaryPreservesOperationAndCollectionFacts(t *testing.T) {
	at := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	s := timeline.Snapshot{
		ID: "private-operation-id", Operation: "sandbox_start", Status: timeline.Succeeded,
		StartedAt: at, FinishedAt: at.Add(2 * time.Second), CapturedAt: at.Add(3 * time.Second),
		Collection: timeline.Collection{LocalFlushed: true},
		Attributes: map[string]json.RawMessage{"internal": json.RawMessage(`"hidden"`)},
		Stages: []timeline.Stage{
			{ID: "private-stage-id", Name: "attempt", Status: timeline.Failed, Error: "try again",
				StartedAt: at, FinishedAt: at.Add(time.Second), Elapsed: 500 * time.Millisecond},
			{Name: "retry", Status: timeline.Succeeded, StartedAt: at, FinishedAt: at.Add(2 * time.Second)},
			{Name: "late_report", Status: timeline.Running, StartedAt: at.Add(time.Second)},
		},
	}
	want := "sandbox_start: succeeded (2s)\n" +
		"  attempt: failed (500ms): try again\n" +
		"  retry: succeeded (2s)\n" +
		"  late_report: running (2s)\n" +
		"collection: local_flushed=true, store_read=false"
	if got := s.Summary(); got != want {
		t.Fatalf("Summary() = %q, want %q", got, want)
	}
	if got := s.Summary(); got != want {
		t.Fatalf("repeated Summary() = %q, want %q", got, want)
	}
}

func TestSummaryOperationError(t *testing.T) {
	at := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	s := timeline.Snapshot{Operation: "start", Status: timeline.Canceled, Error: "context canceled",
		StartedAt: at, FinishedAt: at.Add(time.Second), CapturedAt: at.Add(5 * time.Second),
		Collection: timeline.Collection{LocalFlushed: true, StoreRead: true}}
	want := "start: canceled (1s): context canceled\ncollection: local_flushed=true, store_read=true"
	if got := s.Summary(); got != want {
		t.Fatalf("Summary() = %q, want %q", got, want)
	}
}
