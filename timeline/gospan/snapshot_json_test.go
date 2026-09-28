package gospantimeline

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/compforge/go-stdx/timeline"
)

func TestSnapshotJSONRoundTripAndOwnership(t *testing.T) {
	ctx := context.Background()
	input := map[string]any{"names": []string{"before"}, "id": uint64(math.MaxUint64)}
	tl, err := New(ctx, t.Name(), "start", timeline.Field{Key: "input", Value: input})
	if err != nil {
		t.Fatal(err)
	}
	input["names"].([]string)[0] = "after"
	parentCtx, parent := tl.Begin(ctx, "parent")
	_, child := tl.Begin(parentCtx, "child", timeline.Field{Key: "count", Value: 9007199254740993})
	progress, err := tl.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	roundTripSnapshot(t, progress)
	if !progress.Complete || progress.Status != timeline.Running || len(progress.RunningStages()) != 2 {
		t.Fatalf("progress=%+v", progress)
	}
	child.End(context.Canceled)
	parent.End(nil)
	final, err := tl.Finish(ctx, errors.New("business failure"))
	if err != nil {
		t.Fatal(err)
	}
	restored := roundTripSnapshot(t, final)
	if restored.Status != timeline.Failed || restored.Stages[1].ParentID != restored.Stages[0].ID || restored.Stages[1].Status != timeline.Canceled {
		t.Fatalf("lost lifecycle facts: %+v", restored)
	}
	var want struct {
		Names []string `json:"names"`
		ID    uint64   `json:"id"`
	}
	want.Names, want.ID = []string{"before"}, math.MaxUint64
	got, ok := timeline.FieldValue[struct {
		Names []string `json:"names"`
		ID    uint64   `json:"id"`
	}](restored.Fields, "input")
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("input=%+v ok=%v", got, ok)
	}
	count, ok := timeline.FieldValue[int64](restored.Stages[1].Fields, "count")
	if !ok || count != 9007199254740993 {
		t.Fatalf("integer lost precision: %d %v", count, ok)
	}
	// An exported byte slice cannot mutate either a later snapshot or its peer.
	final.Fields["input"][0] = '!'
	again, err := tl.Snapshot(ctx)
	if err != nil || !reflect.DeepEqual(again, restored) {
		t.Fatalf("snapshot aliases recorder: %+v %v", again, err)
	}
}

func roundTripSnapshot(t *testing.T, original timeline.Snapshot) timeline.Snapshot {
	t.Helper()
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored timeline.Snapshot
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, restored) || original.Duration() != restored.Duration() {
		t.Fatalf("round trip changed snapshot: %s", raw)
	}
	return restored
}

func TestInvalidFieldsReportCollectionFailureAndStillFinish(t *testing.T) {
	ctx := context.Background()
	if _, err := New(ctx, t.Name(), "invalid", timeline.Field{Key: "channel", Value: make(chan int)}); !errors.Is(err, timeline.ErrInvalidField) {
		t.Fatalf("constructor err=%v", err)
	}
	for _, call := range []string{"begin", "set", "end"} {
		t.Run(call, func(t *testing.T) {
			tl, err := New(ctx, t.Name(), "work")
			if err != nil {
				t.Fatal(err)
			}
			invalid := timeline.Field{Key: "nan", Value: math.NaN()}
			var stage timeline.Stage
			if call == "begin" {
				_, stage = tl.Begin(ctx, "child", invalid)
			} else {
				_, stage = tl.Begin(ctx, "child")
			}
			if call == "set" {
				tl.SetFields(invalid)
			}
			if call == "end" {
				stage.End(nil, invalid)
			} else {
				stage.End(nil)
			}
			final, err := tl.Finish(ctx, nil)
			if !errors.Is(err, timeline.ErrInvalidField) || final.Complete || final.Status != timeline.Succeeded {
				t.Fatalf("result=%+v err=%v", final, err)
			}
			if _, err := json.Marshal(final); err != nil {
				t.Fatal(err)
			}
			if stats := tl.(*recorder).tracer.Stats(); stats.SpansInFlight != 0 || stats.TracesInFlight != 0 {
				t.Fatalf("resources leaked: %+v", stats)
			}
		})
	}
}
