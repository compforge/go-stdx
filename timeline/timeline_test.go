package timeline

import (
	"context"
	"testing"
	"time"
)

func TestIntervalDuration(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snapshot := Snapshot{StartedAt: start, CapturedAt: start.Add(10 * time.Second)}
	stage := StageRecord{StartedAt: start.Add(time.Second)}
	if snapshot.Duration() != 10*time.Second || stage.Duration(snapshot.CapturedAt) != 9*time.Second {
		t.Fatal("running intervals must use the snapshot capture time")
	}
	snapshot.FinishedAt = start.Add(5 * time.Second)
	stage.FinishedAt = start.Add(3 * time.Second)
	if snapshot.Duration() != 5*time.Second || stage.Duration(snapshot.CapturedAt) != 2*time.Second {
		t.Fatal("finished intervals must keep their own end time")
	}
	if (Snapshot{}).Duration() != 0 || (StageRecord{}).Duration(start) != 0 {
		t.Fatal("uncollected intervals must not report a fabricated duration")
	}
}

type contextTimeline struct{ Timeline }

func TestContext(t *testing.T) {
	tl := &contextTimeline{}
	ctx, cancel := context.WithCancel(context.Background())
	wrapped := NewContext(ctx, tl)
	cancel()
	if got, ok := FromContext(wrapped); !ok || got != tl || wrapped.Err() != context.Canceled {
		t.Fatal("timeline propagation changed the context lifetime or identity")
	}
	for _, empty := range []context.Context{nil, context.Background(), NewContext(context.Background(), nil)} {
		if got, ok := FromContext(empty); got != nil || ok {
			t.Fatal("empty context unexpectedly carries a timeline")
		}
	}
}
