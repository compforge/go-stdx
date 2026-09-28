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
	id        string
	store     Store
	actor     Actor
	mu        sync.Mutex
	flushGate chan struct{}
	pending   []Record
	fieldErr  error
	started   bool
	finished  bool
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
// handle never resets existing records: the first accepted start wins. Retry a
// failed flush with Flush; do not invent a second business start timestamp.
func (t *Recorder) Start(ctx context.Context, operation string, fields ...Field) error {
	t.mu.Lock()
	if t.started || t.finished {
		t.mu.Unlock()
		return ErrAlreadyStarted
	}
	t.started = true
	t.pending = append(t.pending, Record{ID: "operation:start", Kind: OperationStarted,
		At: time.Now().UTC(), Operation: operation, Fields: t.encode(fields)})
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

func (t *Recorder) Begin(ctx context.Context, name string, fields ...Field) (context.Context, Stage) {
	t.mu.Lock()
	defer t.mu.Unlock()
	parent := rootID(t.id)
	if ref, ok := StageFromContext(ctx); ok && ref.TimelineID == t.id && ref.StageID != "" {
		parent = ref.StageID
	}
	now := time.Now()
	s := &recordedStage{owner: t, started: now, record: StageRecord{
		ID: StageID(uuid.NewString()), ParentID: parent, Name: name,
		StartedAt: now.UTC(), Status: Running, Actor: t.actor, Fields: t.encode(fields),
	}}
	// A business terminal record is not a distributed ingestion barrier. Late
	// workers retain their actual intervals instead of becoming silent no-ops.
	s.enqueue()
	return NewStageContext(ctx, StageRef{TimelineID: t.id, StageID: s.record.ID}), s
}

func (t *Recorder) SetFields(fields ...Field) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending = append(t.pending, Record{ID: uuid.NewString(), Kind: OperationFields,
		At: time.Now().UTC(), Fields: t.encode(fields)})
}

type recordedStage struct {
	owner    *Recorder
	record   StageRecord
	started  time.Time // retains the monotonic clock for the measured duration
	revision uint64
	ended    bool
}

func (s *recordedStage) enqueue() {
	s.revision++
	record := s.record
	record.Fields = cloneJSONFields(record.Fields)
	s.owner.pending = append(s.owner.pending, Record{ID: uuid.NewString(), Kind: StageUpdated,
		At: time.Now().UTC(), Stage: &record, Revision: s.revision})
}

func (s *recordedStage) SetFields(fields ...Field) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.ended {
		return
	}
	s.record.Fields = mergeFields(s.record.Fields, s.owner.encode(fields))
	s.enqueue()
}

func (s *recordedStage) End(err error, fields ...Field) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.ended {
		return
	}
	s.ended = true
	s.record.Fields = mergeFields(s.record.Fields, s.owner.encode(fields))
	s.record.FinishedAt = time.Now().UTC()
	s.record.Elapsed = time.Since(s.started)
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

func (t *Recorder) encode(fields []Field) map[string]json.RawMessage {
	if len(fields) == 0 {
		return nil
	}
	values := make(map[string]json.RawMessage, len(fields))
	for _, field := range fields {
		value, err := json.Marshal(field.Value)
		if err != nil {
			if t.fieldErr == nil {
				t.fieldErr = fmt.Errorf("%w: %s: %v", ErrInvalidField, field.Key, err)
			}
			continue
		}
		values[field.Key] = value
	}
	return values
}

func mergeFields(dst, src map[string]json.RawMessage) map[string]json.RawMessage {
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
// A failed/ambiguous append retains identical record IDs for a safe retry.
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
	records := append([]Record(nil), t.pending...)
	fieldErr := t.fieldErr
	t.mu.Unlock()
	if len(records) == 0 {
		return fieldErr
	}
	if err := t.store.Append(ctx, t.id, records); err != nil {
		return errors.Join(err, fieldErr)
	}
	t.mu.Lock()
	t.pending = append([]Record(nil), t.pending[len(records):]...)
	t.mu.Unlock()
	return fieldErr
}

func (t *Recorder) Snapshot(ctx context.Context) (Snapshot, error) {
	flushErr := t.Flush(ctx)
	records, readErr := t.store.Read(ctx, t.id)
	snapshot := Project(t.id, records, time.Now().UTC())
	snapshot.Collection = Collection{LocalFlushed: flushErr == nil, StoreRead: readErr == nil}
	return snapshot, errors.Join(flushErr, readErr)
}

// Finish records the business result once on this handle and returns the current
// shared snapshot. The first terminal record accepted by Store wins globally.
// It does not terminate stages, reject late observations, or close shared IO.
func (t *Recorder) Finish(ctx context.Context, operationErr error) (Snapshot, error) {
	t.mu.Lock()
	if !t.finished {
		t.finished = true
		status, message := result(operationErr)
		t.pending = append(t.pending, Record{ID: "operation:finish", Kind: OperationFinished,
			At: time.Now().UTC(), Status: status, Error: message})
	}
	t.mu.Unlock()
	return t.Snapshot(ctx)
}

var _ Timeline = (*Recorder)(nil)
