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
	tl, err := New(ctx, t.Name(), "start", timeline.Attribute{Key: "input", Value: input})
	if err != nil {
		t.Fatal(err)
	}
	input["names"].([]string)[0] = "after"
	parentCtx, parent := timeline.BeginWithContext(ctx, tl, "parent")
	_, child := timeline.BeginWithContext(parentCtx, tl, "child", timeline.WithAttributes(timeline.Attribute{Key: "count", Value: 9007199254740993}))
	progress, err := tl.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	roundTripSnapshot(t, progress)
	if !(progress.Collection.LocalFlushed && progress.Collection.StoreRead) || progress.Status != timeline.Running || len(progress.RunningStages()) != 2 {
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
	got, ok := timeline.AttributeValue[struct {
		Names []string `json:"names"`
		ID    uint64   `json:"id"`
	}](restored.Attributes, "input")
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("input=%+v ok=%v", got, ok)
	}
	count, ok := timeline.AttributeValue[int64](restored.Stages[1].Attributes, "count")
	if !ok || count != 9007199254740993 {
		t.Fatalf("integer lost precision: %d %v", count, ok)
	}
	// An exported byte slice cannot mutate either a later snapshot or its peer.
	final.Attributes["input"][0] = '!'
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

func TestInvalidAttributesReportCollectionFailureAndStillFinish(t *testing.T) {
	ctx := context.Background()
	if _, err := New(ctx, t.Name(), "invalid", timeline.Attribute{Key: "channel", Value: make(chan int)}); !errors.Is(err, timeline.ErrInvalidAttribute) {
		t.Fatalf("constructor err=%v", err)
	}
	for _, call := range []string{"begin", "set", "end"} {
		t.Run(call, func(t *testing.T) {
			tl, err := New(ctx, t.Name(), "work")
			if err != nil {
				t.Fatal(err)
			}
			invalid := timeline.Attribute{Key: "nan", Value: math.NaN()}
			var stage timeline.StageHandle
			if call == "begin" {
				_, stage = timeline.BeginWithContext(ctx, tl, "child", timeline.WithAttributes(invalid))
			} else {
				_, stage = timeline.BeginWithContext(ctx, tl, "child")
			}
			if call == "set" {
				tl.SetAttributes(invalid)
			}
			if call == "end" {
				stage.End(nil, timeline.WithEndAttributes(invalid))
			} else {
				stage.End(nil)
			}
			final, err := tl.Finish(ctx, nil)
			if !errors.Is(err, timeline.ErrInvalidAttribute) || (final.Collection.LocalFlushed && final.Collection.StoreRead) || final.Status != timeline.Succeeded {
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
