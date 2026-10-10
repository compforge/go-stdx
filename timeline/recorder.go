package timeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
	"github.com/google/uuid"
)

var ErrNotStarted = errors.New("timeline: operation attributes require a locally recorded start")

// Option configures a writer without performing remote IO.
type Option func(*Recorder)

func WithStore(backend store.Store) Option { return func(t *Recorder) { t.store = backend } }
func WithActor(actor Actor) Option         { return func(t *Recorder) { t.actor = actor } }

// Writer accepts immutable recording facts and provides a persistence checkpoint.
// A rejected write must not be retained. Acceptance and retention depend on the
// implementation: Manager uses a best-effort loading cache; standalone Recorder
// buffers until an explicit Flush. Implementations support concurrent callers.
type Writer interface {
	Write(store.Update) error
	// RecordError retains collection loss independently of pending queue membership.
	RecordError(error)
	Flush(context.Context) error
}

// WithWriter selects the local submission policy. store.Store remains the read backend.
// The writer must submit to the same store and operation ID as the Recorder.
func WithWriter(writer Writer) Option { return func(t *Recorder) { t.writer = writer } }

// Recorder owns one writer's revisions, operation facts and stage timing. It has
// no process cache, worker or expiry policy. Multiple writers contribute to one ID.
type Recorder struct {
	id           string
	store        store.Store
	actor        Actor
	writer       Writer
	mu           sync.Mutex
	operation    store.OperationRecord
	attributeErr error
	started      bool
	finished     bool
	terminal     *store.OperationRecord
}

// New binds a writer to an ID without recording a start or querying storage.
func New(id string, options ...Option) (*Recorder, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	t := &Recorder{id: id, store: store.NewMemoryStore()}
	for _, option := range options {
		option(t)
	}
	if t.store == nil {
		return nil, errors.New("timeline: nil Store")
	}
	if t.writer == nil {
		t.writer = &localWriter{id: id, store: t.store, gate: make(chan struct{}, 1)}
	}
	return t, nil
}
func (t *Recorder) ID() string { return t.id }

// RecordStart accepts an optional beginning without forcing a flush. Repeating the
// same facts is idempotent and retains the original time. Finish may arrive first.
func (t *Recorder) RecordStart(operation string, attributes ...Attribute) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	values, err := encodeAttributes(attributes)
	if err != nil {
		return t.recordError(err)
	}
	if t.started {
		if t.operation.Operation != operation || !model.SameJSON(t.operation.Attributes, values) {
			return store.ErrConflict
		}
		return nil
	}
	next := t.operation
	next.Revision++
	next.Operation, next.StartedAt, next.Attributes = operation, time.Now().UTC(), values
	if !t.finished {
		next.Status = Unknown
	}
	if err := t.enqueueOperation(next); err != nil {
		return err
	}
	t.operation, t.started = next, true
	return nil
}

// Start records the optional beginning and checkpoints this standalone writer.
// Manager's ID-based Start uses RecordStart and submits in the background.
func (t *Recorder) Start(ctx context.Context, operation string, attributes ...Attribute) error {
	if err := t.RecordStart(operation, attributes...); err != nil {
		return err
	}
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

// Begin records a running stage. Use Err to inspect admission/encoding errors
// when a noop handle is returned. Manager.Begin returns the error directly.
func (t *Recorder) Begin(name string, opts ...StageOption) StageHandle {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	data := Stage{ID: StageID(uuid.NewString()), ParentID: model.RootID(t.id), Name: name, StartedAt: now.UTC(), Status: Running, Actor: t.actor}
	for _, opt := range opts {
		if err := opt(&data); err != nil {
			t.recordError(err)
			return noopStage{}
		}
	}
	if data.Name == "" || data.ID == "" || data.ID == data.ParentID || data.ID == model.RootID(t.id) || data.StartedAt.IsZero() {
		t.recordError(ErrInvalidStage)
		return noopStage{}
	}
	data.Attributes = model.CloneJSONAttributes(data.Attributes)
	record := store.StageUpdate{Stage: data, Revision: 1}
	if err := t.writer.Write(store.Update{Stages: []store.StageUpdate{record}}); err != nil {
		t.recordError(err)
		return noopStage{}
	}
	s := &recordedStage{owner: t, id: data.ID, started: now, record: record}
	if !data.StartedAt.Equal(now) {
		s.started = time.Time{}
	}
	return s
}

func (t *Recorder) SetAttributes(attributes ...Attribute) {
	_ = t.UpdateAttributes(attributes...)
}

// UpdateAttributes updates a locally recorded start's attributes, returning
// admission errors immediately. It does not require an operation to remain open.
func (t *Recorder) UpdateAttributes(attributes ...Attribute) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.started {
		return t.recordError(ErrNotStarted)
	}
	values, err := encodeAttributes(attributes)
	if err != nil {
		return t.recordError(err)
	}
	next := t.operation
	next.Attributes = mergeAttributes(model.CloneJSONAttributes(next.Attributes), values)
	next.Revision++
	if err := t.enqueueOperation(next); err != nil {
		return err
	}
	t.operation = next
	return nil
}

type recordedStage struct {
	id       StageID // immutable even while record is replaced under the owner lock
	owner    *Recorder
	record   store.StageUpdate
	started  time.Time
	ended    bool
	terminal *store.StageUpdate
}

func (s *recordedStage) ID() StageID { return s.id }

func (s *recordedStage) SetAttributes(attributes ...Attribute) error {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.ended || s.terminal != nil {
		return nil
	}
	values, err := encodeAttributes(attributes)
	if err != nil {
		return s.owner.recordError(err)
	}
	next := s.record
	next.Attributes = mergeAttributes(model.CloneJSONAttributes(next.Attributes), values)
	next.Revision++
	if err := s.owner.writer.Write(store.Update{Stages: []store.StageUpdate{next}}); err != nil {
		return err
	}
	s.record = next
	return nil
}

func (s *recordedStage) End(stageErr error, opts ...EndOption) error {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.ended {
		return nil
	}
	if s.terminal == nil {
		now := time.Now()
		next := s.record
		next.Attributes = model.CloneJSONAttributes(next.Attributes)
		next.FinishedAt = now.UTC()
		for _, opt := range opts {
			if err := opt(&next.Stage); err != nil {
				return s.owner.recordError(err)
			}
		}
		if next.FinishedAt.IsZero() || next.FinishedAt.Before(next.StartedAt) {
			return s.owner.recordError(ErrInvalidStage)
		}
		if !s.started.IsZero() && next.FinishedAt.Equal(now) {
			next.Elapsed = now.Sub(s.started)
		}
		next.Status, next.Error = result(stageErr)
		next.Revision++
		// spec: Freeze the first valid result, but only seal after queue admission.
		// Backpressure retries must submit the same terminal fact, not leave it running.
		s.terminal = &next
	}
	if err := s.owner.writer.Write(store.Update{Stages: []store.StageUpdate{*s.terminal}}); err != nil {
		return err
	}
	s.record, s.ended = *s.terminal, true
	return nil
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

func encodeAttributes(attributes []Attribute) (map[string]json.RawMessage, error) {
	if len(attributes) == 0 {
		return nil, nil
	}
	values := make(map[string]json.RawMessage, len(attributes))
	for _, attribute := range attributes {
		value, err := json.Marshal(attribute.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrInvalidAttribute, attribute.Key, err)
		}
		values[attribute.Key] = value
	}
	return values, nil
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

// Flush checkpoints the configured writer. A Manager-backed writer checkpoints
// its ID; a standalone writer checkpoints only its own buffer.
func (t *Recorder) Flush(ctx context.Context) error {
	return errors.Join(t.writer.Flush(ctx), t.Err())
}

// Err reports recording errors on this handle. Submission failures are returned
// immediately; accepted-data loss remains visible through the writer's checkpoint.
func (t *Recorder) Err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.attributeErr
}
func (t *Recorder) recordError(err error) error {
	if t.attributeErr == nil {
		t.attributeErr = err
	}
	t.writer.RecordError(err)
	return err
}

func (t *Recorder) Snapshot(ctx context.Context) (Snapshot, error) {
	flushErr := t.Flush(ctx)
	doc, readErr := t.store.Read(ctx, t.id)
	if doc.ID == "" {
		doc.ID, doc.RootStageID = t.id, model.RootID(t.id)
	}
	snapshot := doc.Snapshot(time.Now().UTC())
	snapshot.Collection = Collection{LocalFlushed: flushErr == nil, StoreRead: readErr == nil}
	return snapshot, errors.Join(flushErr, readErr)
}

// RecordFinish accepts an optional result even without RecordStart. It never
// ends stages or prevents late recording. Repeating the same result is idempotent.
func (t *Recorder) RecordFinish(operationErr error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	status, message := result(operationErr)
	if t.finished {
		if t.operation.Status != status || t.operation.Error != message {
			return store.ErrConflict
		}
		return nil
	}
	if t.terminal == nil {
		next := t.operation
		next.Revision++
		next.FinishedAt = time.Now().UTC()
		next.Status, next.Error = status, message
		t.terminal = &next
	}
	// Preserve the first result across rejected writes, including its timestamp.
	next := *t.terminal
	// A start/attribute update may have been accepted while the finish was pending.
	next.StartedAt, next.Operation, next.Attributes = t.operation.StartedAt, t.operation.Operation, t.operation.Attributes
	next.Revision = t.operation.Revision + 1
	if err := t.enqueueOperation(next); err != nil {
		return err
	}
	t.operation, t.finished = next, true
	return nil
}

// Finish records an optional result and reads a snapshot for standalone callers.
func (t *Recorder) Finish(ctx context.Context, operationErr error) (Snapshot, error) {
	finishErr := t.RecordFinish(operationErr)
	snapshot, err := t.Snapshot(ctx)
	return snapshot, errors.Join(finishErr, err)
}
func (t *Recorder) enqueueOperation(operation store.OperationRecord) error {
	operation.Attributes = model.CloneJSONAttributes(operation.Attributes)
	return t.writer.Write(store.Update{Operation: &operation})
}

var _ Timeline = (*Recorder)(nil)
