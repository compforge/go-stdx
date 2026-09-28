package timeline

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"
)

// Actor identifies the executor of a stage. Callers choose the identity domain:
// for example a Pod UID and name, a worker ID, or a service instance.
// Actor identifies the executor, not the authenticated user requesting work.
type Actor struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// Record is an immutable observation. ID is the idempotency key for transport
// retries. Stage updates contain the entire stage at Revision, not a patch.
// Operation start/finish are emitted only by the business coordinator.
type Record struct {
	ID        string                     `json:"id"`
	Kind      RecordKind                 `json:"kind"`
	At        time.Time                  `json:"at"`
	Operation string                     `json:"operation,omitempty"`
	Status    Status                     `json:"status,omitempty"`
	Error     string                     `json:"error,omitempty"`
	Fields    map[string]json.RawMessage `json:"fields,omitempty"`
	Stage     *StageRecord               `json:"stage,omitempty"`
	Revision  uint64                     `json:"revision,omitempty"`
}

type RecordKind string

const (
	OperationStarted  RecordKind = "operation_started"
	OperationFields   RecordKind = "operation_fields"
	OperationFinished RecordKind = "operation_finished"
	StageUpdated      RecordKind = "stage_updated"
)

var ErrRecordConflict = errors.New("timeline: record ID reused with different content")
var ErrAlreadyStarted = errors.New("timeline: operation already started on this handle")

// Store is shared by all handles of an operation, potentially in different
// processes. Implementations must support concurrent callers.
//
// Append acknowledges durable acceptance for persistent stores. A retry with
// the same timeline ID, record ID and content is a no-op; different content
// returns ErrRecordConflict. On error some records may have been accepted.
// Read returns a consistent, detached view in backend-defined stable order, with
// duplicate deliveries removed. This is a read boundary, not a distributed
// completion barrier. Implementations must respect context cancellation.
//
// Store connections and record retention are owned by the application. Ending
// an operation never closes the store or prevents late stage updates.
type Store interface {
	Append(context.Context, string, []Record) error
	Read(context.Context, string) ([]Record, error)
}

// MemoryStore collects records within one process. Share the same instance to
// join handles; use a persistent Store for independent processes.
type MemoryStore struct {
	mu      sync.Mutex
	records map[string][]Record
	ids     map[string]map[string]Record
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[string][]Record), ids: make(map[string]map[string]Record)}
}

func (s *MemoryStore) Append(ctx context.Context, id string, records []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.ids[id] == nil {
		s.ids[id] = make(map[string]Record)
	}
	for _, record := range records {
		if previous, ok := s.ids[id][record.ID]; ok {
			if !reflect.DeepEqual(previous, record) {
				return ErrRecordConflict
			}
			continue
		}
		record = cloneRecord(record)
		s.ids[id][record.ID] = record
		s.records[id] = append(s.records[id], record)
	}
	return nil
}

func (s *MemoryStore) Read(ctx context.Context, id string) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make([]Record, len(s.records[id]))
	for i, record := range s.records[id] {
		result[i] = cloneRecord(record)
	}
	return result, nil
}

func cloneRecord(record Record) Record {
	record.Fields = cloneJSONFields(record.Fields)
	if record.Stage != nil {
		stage := *record.Stage
		stage.Fields = cloneJSONFields(stage.Fields)
		record.Stage = &stage
	}
	return record
}

func cloneJSONFields(fields map[string]json.RawMessage) map[string]json.RawMessage {
	if fields == nil {
		return nil
	}
	result := make(map[string]json.RawMessage, len(fields))
	for key, value := range fields {
		result[key] = append(json.RawMessage(nil), value...)
	}
	return result
}
