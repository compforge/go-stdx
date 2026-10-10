package manager

import (
	"context"
	"github.com/compforge/go-stdx/timeline/store"
)

// Start optionally records the operation beginning. Identical repeats preserve
// its stored timestamp, including after a cache miss. Stages never require Start.
func (m *Manager) Start(id, operation string, attributes ...Attribute) error {
	return m.apply(id, func(r *recorder, _ store.Document) error { return r.RecordStart(operation, attributes...) })
}

// Finish optionally records a result. It neither ends stages nor evicts cached data.
func (m *Manager) Finish(id string, operationErr error) error {
	return m.apply(id, func(r *recorder, _ store.Document) error { return r.RecordFinish(operationErr) })
}

// Record accepts a completed source interval, preserving times and Actor.
func (m *Manager) Record(id string, data Stage) error {
	return m.cache.Write(context.Background(), id, store.Update{Completed: []Stage{data}})
}

// BeginContext binds the new stage identity without changing cancellation.
// Parentage comes only from an explicit WithParent option.
func (m *Manager) BeginContext(ctx context.Context, id, name string, options ...StageOption) (context.Context, StageHandle, error) {
	stage, err := m.Begin(id, name, options...)
	if err != nil {
		return ctx, nil, err
	}
	return NewStageContext(ctx, StageRef{TimelineID: id, StageID: stage.ID()}), stage, nil
}
