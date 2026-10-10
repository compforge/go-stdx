package timeline

import (
	"context"
	"errors"
	"time"
)

// managedTimeline indexes live local ownership, not persisted history. Each
// independent Recorder still owns its revisions and actor. Root completion does
// not revoke stage ownership held by concurrent participants.
type managedTimeline struct {
	coordinator *Recorder
	stages      map[string]map[*recordedStage]struct{}
}

// localLocked requires m.mu. It never locks a Recorder or performs IO.
func (m *Manager) localLocked(id string) (*managedTimeline, error) {
	if m.closed {
		return nil, ErrManagerClosed
	}
	local := m.active[id]
	if local == nil {
		if len(m.active) >= m.config.MaxActiveOperations {
			return nil, ErrActiveOperationLimit
		}
		local = &managedTimeline{stages: make(map[string]map[*recordedStage]struct{})}
		m.active[id] = local
	}
	return local, nil
}

// register runs under r.mu, before Start publishes its root state. Lookup never
// holds m.mu while calling a Recorder, preserving the recording lock order.
func (m *Manager) register(r *Recorder) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	local, err := m.localLocked(r.id)
	if err != nil {
		return err
	}
	if local.coordinator != nil {
		return ErrAlreadyStarted
	}
	local.coordinator = r
	return nil
}

func (m *Manager) coordinator(id string) (*Recorder, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrManagerClosed
	}
	local := m.active[id]
	if local == nil || local.coordinator == nil {
		return nil, ErrNotStarted
	}
	return local.coordinator, nil
}

func (m *Manager) removeIdleLocked(id string, local *managedTimeline) {
	if local.coordinator == nil && len(local.stages) == 0 {
		delete(m.active, id)
	}
}

func (m *Manager) release(r *Recorder) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if local := m.active[r.id]; local != nil && local.coordinator == r {
		local.coordinator = nil
		m.removeIdleLocked(r.id, local)
	}
}

// Start creates and registers this process's coordinator. Only one local
// coordinator may own an ID. Recording/flush errors retain that coordinator;
// retry FlushID instead of creating a new start boundary. Admission errors
// return a nil handle. Options follow New.
func (m *Manager) Start(ctx context.Context, id, operation string, options ...Option) (*Recorder, error) {
	r, err := m.New(id, options...)
	if err != nil {
		return nil, err
	}
	err = r.Start(ctx, operation)
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started {
		return nil, err
	}
	return r, err
}

// Begin records a participant stage, without starting or taking over the root.
// WithStageActor attributes this stage independently of the coordinator.
func (m *Manager) Begin(id, name string, options ...StageOption) (StageHandle, error) {
	r, err := m.New(id)
	if err != nil {
		return nil, err
	}
	stage := r.Begin(name, options...)
	if err := r.recordingError(); err != nil {
		return nil, err
	}
	return stage, nil
}

// BeginContext inherits a parent reference for this ID and returns a context
// carrying the new stage reference. Cancellation/deadlines remain unchanged.
func (m *Manager) BeginContext(ctx context.Context, id, name string, options ...StageOption) (context.Context, StageHandle, error) {
	r, err := m.New(id)
	if err != nil {
		return ctx, nil, err
	}
	stageCtx, stage := BeginWithContext(ctx, r, name, options...)
	if err := r.recordingError(); err != nil {
		return ctx, nil, err
	}
	return stageCtx, stage, nil
}

// Record imports a completed interval, preserving its identity and Actor.
func (m *Manager) Record(id string, stage Stage) error {
	r, err := m.New(id)
	if err != nil {
		return err
	}
	return r.Record(stage)
}

// SetAttributes delegates to the coordinator started by this Manager. It does
// not load a persisted root or acquire completion authority from another process.
func (m *Manager) SetAttributes(id string, attributes ...Attribute) error {
	r, err := m.coordinator(id)
	if err != nil {
		return err
	}
	r.SetAttributes(attributes...)
	return r.recordingError()
}

func (r *Recorder) recordingError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attributeErr
}

// FlushID submits this ID's currently pending local writers, including writers
// made with New. It never flushes other IDs or remote processes.
func (m *Manager) FlushID(ctx context.Context, id string) error {
	if id == "" {
		return ErrEmptyID
	}
	err := m.flush(ctx, id)
	m.mu.Lock()
	var r *Recorder
	if local := m.active[id]; local != nil {
		r = local.coordinator
	}
	m.mu.Unlock()
	if r != nil {
		err = errors.Join(err, r.recordingError())
	}
	return err
}

// Capture flushes this ID's local writers and reads a current snapshot. It
// reports partial collection alongside errors, without claiming remote completion.
func (m *Manager) Capture(ctx context.Context, id string) (Snapshot, error) {
	flushErr := m.FlushID(ctx, id)
	return m.capture(ctx, id, flushErr)
}

func (m *Manager) capture(ctx context.Context, id string, flushErr error) (Snapshot, error) {
	doc, readErr := m.Read(ctx, id)
	if doc.ID == "" {
		doc.ID, doc.RootStageID = id, rootID(id)
	}
	snapshot := doc.Snapshot(time.Now().UTC())
	snapshot.Collection = Collection{LocalFlushed: flushErr == nil, StoreRead: readErr == nil}
	return snapshot, errors.Join(flushErr, readErr)
}

// Finish records the local coordinator's result, collects all currently pending
// local writers for this ID, and releases the coordinator on success. Failures
// retain it for retry; the first result remains authoritative. Late stages are
// still accepted. A process that did not Start the root receives ErrNotStarted.
func (m *Manager) Finish(ctx context.Context, id string, operationErr error) (Snapshot, error) {
	r, err := m.coordinator(id)
	if err != nil {
		return Snapshot{}, err
	}
	finishErr := r.finishOperation(operationErr)
	flushErr := m.FlushID(ctx, id)
	snapshot, err := m.capture(ctx, id, errors.Join(finishErr, flushErr))
	if err == nil {
		m.release(r)
	}
	return snapshot, err
}

// Release relinquishes this ID's local coordinator and stage-name lookup without
// finishing anything or deleting durable data. Use it when abandoning local work; already
// queued writes remain managed and retained handles stay usable. Successful
// Finish releases automatically. Release is idempotent.
func (m *Manager) Release(id string) error {
	if id == "" {
		return ErrEmptyID
	}
	m.mu.Lock()
	if local := m.active[id]; local != nil {
		for _, stages := range local.stages {
			m.activeStages -= len(stages)
		}
	}
	delete(m.active, id)
	m.mu.Unlock()
	return nil
}
