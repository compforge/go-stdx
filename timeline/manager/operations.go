package manager

import (
	"context"
	"github.com/compforge/go-stdx/timeline"
	"github.com/compforge/go-stdx/timeline/store"
)

// Start optionally records the operation beginning. Identical repeats preserve
// its stored timestamp, including after a cache miss. Stages never require Start.
func (m *Manager) Start(id, operation string, attributes ...timeline.Attribute) error {
	return m.apply(id, func(r *timeline.Recorder, _ store.Document) error { return r.RecordStart(operation, attributes...) })
}

// Finish optionally records a result. It neither ends stages nor expires cache.
func (m *Manager) Finish(id string, operationErr error) error {
	return m.apply(id, func(r *timeline.Recorder, _ store.Document) error { return r.RecordFinish(operationErr) })
}

// Record accepts a completed source interval, preserving times and Actor.
func (m *Manager) Record(id string, data timeline.Stage) error {
	return m.saveUpdate(context.Background(), id, store.Update{Completed: []timeline.Stage{data}})
}

// BeginContext preserves cancellation/deadlines and inherits parents only from
// the same timeline. Explicit parent options override the inherited reference.
func (m *Manager) BeginContext(ctx context.Context, id, name string, options ...timeline.StageOption) (context.Context, timeline.StageHandle, error) {
	if ref, ok := timeline.StageFromContext(ctx); ok && ref.TimelineID == id && ref.StageID != "" {
		options = append([]timeline.StageOption{timeline.WithParent(ref.StageID)}, options...)
	}
	stage, err := m.Begin(id, name, options...)
	if err != nil {
		return ctx, nil, err
	}
	return timeline.NewStageContext(ctx, timeline.StageRef{TimelineID: id, StageID: stage.ID()}), stage, nil
}
