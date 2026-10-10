package manager

import (
	"context"
	"errors"
	"time"

	"github.com/compforge/go-stdx/timeline"
)

func (m *Manager) operationWriter(id string) (*timeline.Recorder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	local, err := m.localLocked(id)
	if err != nil {
		return nil, err
	}
	if local.root == nil {
		local.root = m.recorderLocked(local, timeline.Actor{})
	}
	return local.root, nil
}

// Start optionally records an operation beginning without remote IO. Identical
// repeats preserve the first timestamp. Stage recording never requires Start.
func (m *Manager) Start(id, operation string, attributes ...timeline.Attribute) error {
	r, err := m.operationWriter(id)
	if err != nil {
		return err
	}
	return r.RecordStart(operation, attributes...)
}

// Finish optionally records a result, even without Start. It does not read
// storage, expire the timeline, end stages or reject later observations.
func (m *Manager) Finish(id string, operationErr error) error {
	r, err := m.operationWriter(id)
	if err != nil {
		return err
	}
	return r.RecordFinish(operationErr)
}

// Record buffers a completed source interval, preserving times and Actor.
func (m *Manager) Record(id string, data timeline.Stage) error {
	r, err := m.NewWriter(id, timeline.Actor{})
	if err != nil {
		return err
	}
	return r.Record(data)
}

// BeginContext is a context adapter for Begin. It preserves cancellation and
// deadlines and only inherits parent references belonging to the same timeline.
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

// FlushID is an infrastructure checkpoint for this ID's current local writers.
// Recording errors remain visible for the cache lifetime, even after an affected
// participant's pending queue has drained. It never flushes remote writers.
func (m *Manager) FlushID(ctx context.Context, id string) error {
	if id == "" {
		return timeline.ErrEmptyID
	}
	err := m.flush(ctx, id)
	m.mu.Lock()
	defer m.mu.Unlock()
	if item := m.cache.Get(id); item != nil {
		err = errors.Join(err, item.Value().err)
	}
	return err
}

// Read checkpoints this ID's current local records and reads a detached snapshot.
// Collection reports submission/read success, never distributed completeness.
// It touches LRU without extending TTL or creating a cache entry. Read remains
// available after Shutdown while the application keeps the Store open.
func (m *Manager) Read(ctx context.Context, id string) (timeline.Snapshot, error) {
	if id == "" {
		return timeline.Snapshot{}, timeline.ErrEmptyID
	}
	flushErr := m.FlushID(ctx, id)
	doc, readErr := m.store.Read(ctx, id)
	if doc.ID == "" {
		doc.ID = id
	}
	snapshot := doc.Snapshot(time.Now().UTC())
	snapshot.Collection = timeline.Collection{LocalFlushed: flushErr == nil, StoreRead: readErr == nil}
	return snapshot, errors.Join(flushErr, readErr)
}
