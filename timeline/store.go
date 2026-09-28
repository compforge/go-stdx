package timeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"
)

// Actor optionally identifies the executor, not the requesting user.
// The caller chooses the identity domain; ID and Name are independently optional.
type Actor struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

var ErrConflict = errors.New("timeline: conflicting revision or immutable boundary")
var ErrAlreadyStarted = errors.New("timeline: operation already started on this handle")
var ErrNotStarted = errors.New("timeline: only the started coordinator may update the operation")
var ErrNotFound = errors.New("timeline: document not found")

// OperationRecord is the coordinator's latest state. StartedAt and Operation
// identify its immutable start; Revision orders full-state updates from that owner.
type OperationRecord struct {
	Revision   uint64                     `json:"revision"`
	Operation  string                     `json:"operation"`
	StartedAt  time.Time                  `json:"started_at"`
	FinishedAt time.Time                  `json:"finished_at,omitempty"`
	Status     Status                     `json:"status"`
	Error      string                     `json:"error,omitempty"`
	Fields     map[string]json.RawMessage `json:"fields,omitempty"`
}

// Document is the durable, current state of one timeline, without read-time
// collection metadata. It retains one state per stage, not an event history.
type Document struct {
	ID          string  `json:"id"`
	RootStageID StageID `json:"root_stage_id"`
	OperationRecord
	Stages []StageUpdate `json:"stages,omitempty"`
}

// StageUpdate is the storage envelope. Revision belongs to the writer protocol,
// not to the public stage data returned in snapshots.
type StageUpdate struct {
	Stage
	Revision uint64 `json:"revision"`
}

// Update contains only this writer's changes. Each value is a complete state
// at its revision. Only the coordinator writes Operation; stage owners write Stages.
type Update struct {
	Operation *OperationRecord
	Stages    []StageUpdate
	// Completed contains immutable imported intervals, with no caller-managed revision.
	Completed []Stage
}

// Store atomically merges updates into one document per ID. Older revisions are
// ignored; equal revisions with different content and changed immutable boundaries
// return ErrConflict. On error acceptance may be uncertain, so retry the same update.
// Read returns detached data or ErrNotFound. Implementations support concurrent
// callers and cancellation. Connections, schema and retention belong to the caller.
type Store interface {
	Merge(context.Context, string, Update) error
	Read(context.Context, string) (Document, error)
}

// MemoryStore shares documents among handles in one process.
type MemoryStore struct {
	mu        sync.Mutex
	documents map[string]Document
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{documents: make(map[string]Document)} }
func (s *MemoryStore) Merge(ctx context.Context, id string, update Update) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	doc, _, err := MergeDocument(id, s.documents[id], update)
	if err == nil {
		s.documents[id] = doc
	}
	return err
}
func (s *MemoryStore) Read(ctx context.Context, id string) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Document{}, err
	}
	doc, ok := s.documents[id]
	if !ok {
		return Document{}, ErrNotFound
	}
	return cloneDocument(doc), nil
}

// MergeDocument is the shared merge contract for storage implementations. It
// never mutates its inputs. A conflict rejects the entire update. changed=false
// lets durable stores acknowledge duplicate delivery without rewriting the row.
func MergeDocument(id string, current Document, update Update) (doc Document, changed bool, err error) {
	if id == "" {
		return Document{}, false, ErrEmptyID
	}
	if current.ID != "" && current.ID != id {
		return Document{}, false, ErrConflict
	}
	doc = cloneDocument(current)
	doc.ID, doc.RootStageID = id, rootID(id)
	if incoming := update.Operation; incoming != nil {
		old := doc.OperationRecord
		if incoming.Revision == 0 || incoming.StartedAt.IsZero() {
			return Document{}, false, ErrConflict
		}
		if old.Revision != 0 && (old.Operation != incoming.Operation || !old.StartedAt.Equal(incoming.StartedAt)) {
			return Document{}, false, ErrConflict
		}
		if incoming.Revision == old.Revision && !sameJSON(old, *incoming) {
			return Document{}, false, ErrConflict
		}
		if incoming.Revision > old.Revision {
			// A higher revision cannot reopen or replace an accepted business terminal.
			if !old.FinishedAt.IsZero() && !sameJSON(old, *incoming) {
				return Document{}, false, ErrConflict
			}
			doc.OperationRecord = *incoming
			doc.Fields = cloneJSONFields(incoming.Fields)
			changed = true
		}
	}
	indexes := make(map[StageID]int, len(doc.Stages))
	for i, stage := range doc.Stages {
		indexes[stage.ID] = i
	}
	for _, incoming := range update.Stages {
		if incoming.ID == "" || incoming.Revision == 0 || incoming.StartedAt.IsZero() {
			return Document{}, false, ErrConflict
		}
		if i, ok := indexes[incoming.ID]; ok {
			old := doc.Stages[i]
			if old.ParentID != incoming.ParentID || old.Name != incoming.Name || old.Actor != incoming.Actor || !old.StartedAt.Equal(incoming.StartedAt) {
				return Document{}, false, ErrConflict
			}
			if incoming.Revision < old.Revision {
				continue
			}
			if incoming.Revision == old.Revision {
				if !sameJSON(old, incoming) {
					return Document{}, false, ErrConflict
				}
				continue
			}
			if !old.FinishedAt.IsZero() {
				return Document{}, false, ErrConflict
			}
			incoming.Fields = cloneJSONFields(incoming.Fields)
			doc.Stages[i] = incoming
		} else {
			incoming.Fields = cloneJSONFields(incoming.Fields)
			indexes[incoming.ID] = len(doc.Stages)
			doc.Stages = append(doc.Stages, incoming)
		}
		changed = true
	}
	for _, incoming := range update.Completed {
		if err := validateCompleted(id, incoming); err != nil {
			return Document{}, false, err
		}
		if incoming.ParentID == "" {
			incoming.ParentID = rootID(id)
		}
		revision := uint64(2)
		if i, ok := indexes[incoming.ID]; ok {
			old := doc.Stages[i]
			if !old.FinishedAt.IsZero() {
				if !sameJSON(old.Stage, incoming) {
					return Document{}, false, ErrConflict
				}
				continue
			}
			if old.ParentID != incoming.ParentID || old.Name != incoming.Name || old.Actor != incoming.Actor || !old.StartedAt.Equal(incoming.StartedAt) {
				return Document{}, false, ErrConflict
			}
			revision = old.Revision + 1
			incoming.Fields = cloneJSONFields(incoming.Fields)
			doc.Stages[i] = StageUpdate{Stage: incoming, Revision: revision}
		} else {
			incoming.Fields = cloneJSONFields(incoming.Fields)
			indexes[incoming.ID] = len(doc.Stages)
			doc.Stages = append(doc.Stages, StageUpdate{Stage: incoming, Revision: revision})
		}
		changed = true
	}
	sortStageUpdates(doc.Stages)
	return doc, changed, nil
}

// SQL JSON types may normalize whitespace and object key order. Compare decoded
// JSON with Number so retries neither conflict on formatting nor round large ints.
func sameJSON(a, b any) bool {
	decode := func(v any) any {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var out any
		if decoder.Decode(&out) != nil {
			return nil
		}
		return out
	}
	return reflect.DeepEqual(decode(a), decode(b))
}
func cloneDocument(doc Document) Document {
	doc.Fields = cloneJSONFields(doc.Fields)
	doc.Stages = append([]StageUpdate(nil), doc.Stages...)
	for i := range doc.Stages {
		doc.Stages[i].Fields = cloneJSONFields(doc.Stages[i].Fields)
	}
	return doc
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
