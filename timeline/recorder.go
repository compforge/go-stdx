package timeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Option configures a local handle. It does not perform remote IO.
type Option func(*Recorder)

func WithStore(store Store) Option { return func(t *Recorder) { t.store = store } }
func WithActor(actor Actor) Option { return func(t *Recorder) { t.actor = actor } }

// Recorder is a local handle bound to an operation ID. Independent handles use
// the same ID and Store to contribute to one timeline; no Registry is needed.
// Recording methods encode and buffer facts. Flush, Snapshot and Finish perform
// bounded IO using their supplied context. Flush at business handoff boundaries
// and before discarding a handle; unflushed records are lost on process exit.
type Recorder struct {
	id           string
	store        Store
	actor        Actor
	mu           sync.Mutex
	flushGate    chan struct{}
	pending      []Update
	operation    OperationRecord
	attributeErr error
	started      bool
	finished     bool
}

// New constructs a handle without starting/restarting an operation or looking
// it up remotely. WithStore selects shared storage; the default is private
// in-memory storage. Only the business coordinator calls Start and Finish.
func New(id string, options ...Option) (*Recorder, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	t := &Recorder{id: id, store: NewMemoryStore(), flushGate: make(chan struct{}, 1)}
	for _, option := range options {
		option(t)
	}
	if t.store == nil {
		return nil, errors.New("timeline: nil Store")
	}
	return t, nil
}

func (t *Recorder) ID() string { return t.id }

// Start records the operation boundary and flushes it. Calling Start on another
// handle never resets the document: a different start boundary conflicts. Retry a
// failed flush with Flush; do not invent a second business start timestamp.
func (t *Recorder) Start(ctx context.Context, operation string, attributes ...Attribute) error {
	t.mu.Lock()
	if t.started || t.finished {
		t.mu.Unlock()
		return ErrAlreadyStarted
	}
	t.started = true
	t.operation = OperationRecord{Revision: 1, StartedAt: time.Now().UTC(), Operation: operation, Status: Running, Attributes: t.encode(attributes)}
	t.enqueueOperation()
	t.mu.Unlock()
	return t.Flush(ctx)
}

// StageRef can cross a process boundary as plain data. Only identity is carried;
// cancellation, deadlines and a mutable recorder never cross with it.
type StageRef struct {
	TimelineID string  `json:"timeline_id"`
	StageID    StageID `json:"stage_id"`
}
type stageContextKey struct{}

func NewStageContext(ctx context.Context, ref StageRef) context.Context {
	return context.WithValue(ctx, stageContextKey{}, ref)
}
func StageFromContext(ctx context.Context) (StageRef, bool) {
	if ctx == nil {
		return StageRef{}, false
	}
	ref, ok := ctx.Value(stageContextKey{}).(StageRef)
	return ref, ok
}

func rootID(id string) StageID { return StageID("operation:" + id) }

func (t *Recorder) Begin(name string, opts ...StageOption) StageHandle {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	data := Stage{ID: StageID(uuid.NewString()), ParentID: rootID(t.id), Name: name,
		StartedAt: now.UTC(), Status: Running, Actor: t.actor}
	for _, opt := range opts {
		if err := opt(&data); err != nil {
			t.attributeErr = errors.Join(t.attributeErr, err)
			return noopStage{}
		}
	}
	if data.ID == "" || data.ID == data.ParentID || data.ID == rootID(t.id) || data.StartedAt.IsZero() {
		t.attributeErr = errors.Join(t.attributeErr, ErrInvalidStage)
		return noopStage{}
	}
	data.Attributes = cloneJSONAttributes(data.Attributes)
	s := &recordedStage{owner: t, started: now, record: StageUpdate{Stage: data}}
	if !data.StartedAt.Equal(now) {
		s.started = time.Time{}
	}
	s.enqueue()
	return s
}

func (t *Recorder) SetAttributes(attributes ...Attribute) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.started {
		t.attributeErr = errors.Join(t.attributeErr, ErrNotStarted)
		return
	}
	if t.finished {
		return
	}
	t.operation.Attributes = mergeAttributes(t.operation.Attributes, t.encode(attributes))
	t.operation.Revision++
	t.enqueueOperation()
}

type recordedStage struct {
	owner   *Recorder
	record  StageUpdate
	started time.Time // retains the monotonic clock for the measured duration
	ended   bool
}

func (s *recordedStage) ID() StageID { return s.record.ID }

func (s *recordedStage) enqueue() {
	s.record.Revision++
	record := s.record
	record.Attributes = cloneJSONAttributes(record.Attributes)
	s.owner.pending = append(s.owner.pending, Update{Stages: []StageUpdate{record}})
}

func (s *recordedStage) SetAttributes(attributes ...Attribute) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.ended {
		return
	}
	s.record.Attributes = mergeAttributes(s.record.Attributes, s.owner.encode(attributes))
	s.enqueue()
}

func (s *recordedStage) End(err error, opts ...EndOption) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.ended {
		return
	}
	s.ended = true
	now := time.Now()
	s.record.FinishedAt = now.UTC()
	for _, opt := range opts {
		if optionErr := opt(&s.record.Stage); optionErr != nil {
			s.owner.attributeErr = errors.Join(s.owner.attributeErr, optionErr)
		}
	}
	if s.record.FinishedAt.IsZero() || s.record.FinishedAt.Before(s.record.StartedAt) {
		s.owner.attributeErr = errors.Join(s.owner.attributeErr, ErrInvalidStage)
		return
	}
	if !s.started.IsZero() && s.record.FinishedAt.Equal(now) {
		s.record.Elapsed = now.Sub(s.started)
	}
	s.record.Status, s.record.Error = result(err)
	s.enqueue()
}

func result(err error) (Status, string) {
	if err == nil {
		return Succeeded, ""
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Canceled, err.Error()
	}
	return Failed, err.Error()
}

func (t *Recorder) encode(attributes []Attribute) map[string]json.RawMessage {
	if len(attributes) == 0 {
		return nil
	}
	values := make(map[string]json.RawMessage, len(attributes))
	for _, attribute := range attributes {
		value, err := json.Marshal(attribute.Value)
		if err != nil {
			if t.attributeErr == nil {
				t.attributeErr = fmt.Errorf("%w: %s: %v", ErrInvalidAttribute, attribute.Key, err)
			}
			continue
		}
		values[attribute.Key] = value
	}
	return values
}

func mergeAttributes(dst, src map[string]json.RawMessage) map[string]json.RawMessage {
	if len(dst) == 0 && len(src) == 0 {
		return nil
	}
	if dst == nil {
		dst = make(map[string]json.RawMessage, len(src))
	}
	for key, value := range src {
		dst[key] = append(json.RawMessage(nil), value...)
	}
	return dst
}

// Flush acknowledges records buffered before this call acquired the flush lock.
// A failed/ambiguous merge retains identical revisions for a safe retry.
// It cannot flush buffers owned by another handle or process.
func (t *Recorder) Flush(ctx context.Context) error {
	select {
	case t.flushGate <- struct{}{}:
		defer func() { <-t.flushGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	updates := append([]Update(nil), t.pending...)
	attributeErr := t.attributeErr
	t.mu.Unlock()
	if len(updates) == 0 {
		return attributeErr
	}
	// Coalesce intermediate states before IO; only latest revisions are durable.
	update := Update{}
	stages := make(map[StageID]StageUpdate)
	for _, pending := range updates {
		update.Completed = append(update.Completed, pending.Completed...)
		if pending.Operation != nil {
			update.Operation = pending.Operation
		}
		for _, stage := range pending.Stages {
			if old, ok := stages[stage.ID]; ok && old.Revision == stage.Revision && !sameJSON(old, stage) {
				return errors.Join(ErrConflict, attributeErr)
			}
			stages[stage.ID] = stage
		}
	}
	for _, stage := range stages {
		update.Stages = append(update.Stages, stage)
	}
	if err := t.store.Merge(ctx, t.id, update); err != nil {
		return errors.Join(err, attributeErr)
	}
	t.mu.Lock()
	t.pending = append([]Update(nil), t.pending[len(updates):]...)
	t.mu.Unlock()
	return attributeErr
}

func (t *Recorder) Snapshot(ctx context.Context) (Snapshot, error) {
	flushErr := t.Flush(ctx)
	doc, readErr := t.store.Read(ctx, t.id)
	if doc.ID == "" {
		doc.ID, doc.RootStageID = t.id, rootID(t.id)
	}
	snapshot := doc.Snapshot(time.Now().UTC())
	snapshot.Collection = Collection{LocalFlushed: flushErr == nil, StoreRead: readErr == nil}
	return snapshot, errors.Join(flushErr, readErr)
}

// Finish records the business result once on this handle and returns the current
// shared snapshot. Only the handle that called Start may finish; accepted boundaries are immutable.
// It does not terminate stages, reject late observations, or close shared IO.
func (t *Recorder) Finish(ctx context.Context, operationErr error) (Snapshot, error) {
	t.mu.Lock()
	if !t.started {
		t.mu.Unlock()
		snapshot, err := t.Snapshot(ctx)
		return snapshot, errors.Join(ErrNotStarted, err)
	}
	if !t.finished {
		t.finished = true
		t.operation.Revision++
		t.operation.FinishedAt = time.Now().UTC()
		t.operation.Status, t.operation.Error = result(operationErr)
		t.enqueueOperation()
	}
	t.mu.Unlock()
	return t.Snapshot(ctx)
}

func (t *Recorder) enqueueOperation() {
	operation := t.operation
	operation.Attributes = cloneJSONAttributes(operation.Attributes)
	t.pending = append(t.pending, Update{Operation: &operation})
}

var _ Timeline = (*Recorder)(nil)
