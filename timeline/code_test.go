package timeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/compforge/go-stdx/timeline"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func TestStageCodeIndependentOfResult(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		code     string
		status   timeline.Status
		wantCode string
	}{
		{"failure", errors.New("quota exceeded"), "ResourceQuotaExceeded", timeline.Failed, "ResourceQuotaExceeded"},
		{"canceled", fmt.Errorf("interrupted: %w", context.Canceled), "UserCanceled", timeline.Canceled, "UserCanceled"},
		{"deadline", context.DeadlineExceeded, "WaitExpired", timeline.Canceled, "WaitExpired"},
		{"unclassified", errors.New("ResourceQuotaExceeded"), "", timeline.Failed, ""},
		{"success", nil, "Cached", timeline.Succeeded, "Cached"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recordingBackends(t, func(t *testing.T, tl timeline.Timeline) {
				ctx := context.Background()
				stage := tl.Begin("create_pod")
				running, err := tl.Snapshot(ctx)
				if err != nil || len(running.Stages) != 1 || running.Stages[0].Code != "" {
					t.Fatalf("running: %+v, %v", running, err)
				}
				stage.End(tc.err, timeline.WithCode(tc.code))
				// A repeated End cannot replace any part of the winning result.
				stage.End(errors.New("late"), timeline.WithCode("LateFailure"))
				snapshot, err := tl.Finish(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				got := snapshot.Stages[0]
				wantError := ""
				if tc.err != nil {
					wantError = tc.err.Error()
				}
				if got.Status != tc.status || got.Code != tc.wantCode || got.Error != wantError {
					t.Fatalf("stage result: %+v", got)
				}
				if snapshot.Status != timeline.Succeeded {
					t.Fatal("stage failure changed caller-owned operation result")
				}
				raw, err := json.Marshal(snapshot)
				if err != nil {
					t.Fatal(err)
				}
				var restored timeline.Snapshot
				if err := json.Unmarshal(raw, &restored); err != nil {
					t.Fatal(err)
				}
				if restored.Stages[0].Code != tc.wantCode {
					t.Fatalf("JSON lost code: %s", raw)
				}
				if tc.wantCode == "" && strings.Contains(string(raw), `"code":`) {
					t.Fatalf("empty code was not omitted: %s", raw)
				}
			})
		})
	}
}

func TestRecordedCodeIsImmutable(t *testing.T) {
	for _, status := range []timeline.Status{timeline.Succeeded, timeline.Failed, timeline.Canceled} {
		t.Run(string(status), func(t *testing.T) {
			recordingBackends(t, func(t *testing.T, tl timeline.Timeline) {
				input := completedStage()
				input.Status, input.Code = status, "Observed"
				if err := tl.Record(input); err != nil {
					t.Fatal(err)
				}
				if err := tl.Record(input); err != nil {
					t.Fatal(err)
				}
				snapshot, err := tl.Snapshot(context.Background())
				if err != nil || len(snapshot.Stages) != 1 || snapshot.Stages[0].Code != input.Code {
					t.Fatalf("recorded code: %+v, %v", snapshot, err)
				}
				input.Code = "DifferentCode"
				if err := tl.Record(input); !errors.Is(err, timelinestore.ErrConflict) {
					t.Fatalf("changed terminal code accepted: %v", err)
				}
				snapshot, err = tl.Snapshot(context.Background())
				if err != nil || len(snapshot.Stages) != 1 || snapshot.Stages[0].Code != "Observed" {
					t.Fatalf("rejected code changed facts: %+v %v", snapshot, err)
				}
			})
		})
	}
}

func TestLegacyStageCodeIsOptional(t *testing.T) {
	// Both public snapshots and durable documents accept old payloads without code.
	const legacy = `{"id":"operation","stages":[{"id":"stage","status":"failed","error":"quota exceeded"}]}`
	var snapshot timeline.Snapshot
	var document timelinestore.Document
	if err := json.Unmarshal([]byte(legacy), &snapshot); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(legacy), &document); err != nil {
		t.Fatal(err)
	}
	if snapshot.Stages[0].Code != "" || document.Stages[0].Code != "" {
		t.Fatal("legacy payload inferred a code")
	}
	for _, value := range []any{snapshot, document, snapshot.Stages[0]} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), `"code":`) {
			t.Fatalf("empty code was not omitted: %s", raw)
		}
	}
}
