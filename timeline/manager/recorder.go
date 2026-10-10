package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/compforge/go-stdx/timeline/cache"
	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
	"github.com/google/uuid"
)

var ErrNotStarted = errors.New("timeline: operation attributes require a locally recorded start")

// factWriter accepts immutable recording facts and provides a persistence checkpoint.
// A rejected write must not be retained. Acceptance and retention depend on the
// implementation: Manager uses a best-effort loading cache; standalone recorder
// buffers until an explicit Flush. Implementations support concurrent callers.
type factWriter interface {
	Write(store.Update) error
	// RecordError retains collection loss independently of pending queue membership.
	RecordError(error)
	Flush(context.Context) error
}

// withWriter is internal wiring for cache-backed handles and one-fact builders.
func withWriter(writer factWriter) Option { return func(t *recorder) { t.writer = writer } }

// recorder owns one writer's revisions, operation facts and stage timing. It has
// no process cache, worker or expiry policy. Multiple writers contribute to one ID.
type recorder struct {
	id           string
	store        store.Store
	actor        Actor
	writer       factWriter
	mu           sync.Mutex
	operation    store.OperationRecord
	attributeErr error
	started      bool
	finished     bool
	terminal     *store.OperationRecord
}

// New binds a writer to an ID without recording a start or querying storage.
func newRecorder(id string, options ...Option) (*recorder, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	t := &recorder{id: id, store: store.NewNoopStore()}
	for _, option := range options {
		option(t)
	}
	if t.store == nil {
		return nil, errors.New("timeline: nil Store")
	}
	if t.writer == nil {
		c, err := cache.NewStandalone(t.store)
		if err != nil {
			return nil, err
		}
		t.writer = &cachedWriter{id: id, cache: c}
	}
	return t, nil
}
func (t *recorder) ID() string { return t.id }

// RecordStart accepts an optional beginning without forcing a flush. Repeating the
// same facts is idempotent and retains the original time. Finish may arrive first.
func (t *recorder) RecordStart(operation string, attributes ...Attribute) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	values, err := encodeAttributes(attributes)
	if err != nil {
		return t.recordError(err)
	}
	if t.started && t.operation.Operation == operation && model.SameJSON(t.operation.Attributes, values) {
		return nil
	}
	next := t.operation
	next.Revision++
	// spec: Start and Finish retain their independently observed times. A late
	// Start may follow FinishedAt; preserve that order even if Duration is negative.
	next.Operation, next.StartedAt, next.Attributes = operation, time.Now().UTC(), values
	if !t.finished {
		next.Status = Unknown
	}
	startUpdate := next
	startUpdate.FinishedAt, startUpdate.Status, startUpdate.Error = time.Time{}, Unknown, ""
	if err := t.enqueueOperation(startUpdate); err != nil {
		return err
	}
	t.operation, t.started = next, true
	return nil
}

// Start records the optional beginning and checkpoints this standalone writer.
// Manager's ID-based Start uses RecordStart and submits in the background.
func (t *recorder) Start(ctx context.Context, operation string, attributes ...Attribute) error {
	if err := t.RecordStart(operation, attributes...); err != nil {
		return err
	}
	return t.Flush(ctx)
}

// StageRef carries explicit stage identity as data. Context binding does not
// assign a parent to subsequent stages.
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
func (t *recorder) Begin(name string, opts ...StageOption) StageHandle {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	data := Stage{ID: StageID(uuid.NewString()), Name: name, StartedAt: now.UTC(), Status: Running, Actor: t.actor}
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

func (t *recorder) SetAttributes(attributes ...Attribute) {
	_ = t.UpdateAttributes(attributes...)
}

// UpdateAttributes updates a locally recorded start's attributes, returning
// admission errors immediately. It does not require an operation to remain open.
func (t *recorder) UpdateAttributes(attributes ...Attribute) error {
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
	startUpdate := next
	startUpdate.FinishedAt, startUpdate.Status, startUpdate.Error = time.Time{}, Unknown, ""
	if err := t.enqueueOperation(startUpdate); err != nil {
		return err
	}
	t.operation = next
	return nil
}

type recordedStage struct {
	id       StageID // immutable even while record is replaced under the owner lock
	owner    *recorder
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
		if next.Actor.Key() != s.record.Actor.Key() || next.FinishedAt.IsZero() || next.FinishedAt.Before(next.StartedAt) {
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
func (t *recorder) Flush(ctx context.Context) error {
	return errors.Join(t.writer.Flush(ctx), t.Err())
}

// Err reports recording errors on this handle. Submission failures are returned
// immediately; accepted-data loss remains visible through the writer's checkpoint.
func (t *recorder) Err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.attributeErr
}
func (t *recorder) recordError(err error) error {
	if t.attributeErr == nil {
		t.attributeErr = err
	}
	t.writer.RecordError(err)
	return err
}

func (t *recorder) Snapshot(ctx context.Context) (Snapshot, error) {
	flushErr := t.Flush(ctx)
	reader := t.writer.(interface {
		ReadSnapshot(context.Context) (Snapshot, error)
	})
	snapshot, readErr := reader.ReadSnapshot(ctx)
	snapshot.Collection.LocalFlushed = flushErr == nil && (snapshot.Collection.LocalFlushed || errors.Is(readErr, store.ErrNotFound))
	return snapshot, errors.Join(flushErr, readErr)
}

// RecordFinish accepts an optional result even without RecordStart. It never
// ends stages or prevents late recording. Repeating the same result is idempotent.
func (t *recorder) RecordFinish(operationErr error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	status, message := result(operationErr)
	if t.finished {
		if t.operation.Status == status && t.operation.Error == message {
			return nil
		}
		t.terminal = nil
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
	finishUpdate := next
	finishUpdate.StartedAt, finishUpdate.Operation, finishUpdate.Attributes = time.Time{}, "", nil
	if err := t.enqueueOperation(finishUpdate); err != nil {
		return err
	}
	t.operation, t.finished = next, true
	return nil
}

// Finish records an optional result and reads a snapshot for standalone callers.
func (t *recorder) Finish(ctx context.Context, operationErr error) (Snapshot, error) {
	finishErr := t.RecordFinish(operationErr)
	snapshot, err := t.Snapshot(ctx)
	return snapshot, errors.Join(finishErr, err)
}
func (t *recorder) enqueueOperation(operation store.OperationRecord) error {
	operation.Attributes = model.CloneJSONAttributes(operation.Attributes)
	return t.writer.Write(store.Update{Operation: &operation})
}

var _ Timeline = (*recorder)(nil)
