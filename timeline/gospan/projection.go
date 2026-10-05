package gospantimeline

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/akmadian/gospan"
	"github.com/compforge/go-stdx/timeline"
	"github.com/google/uuid"
)

const boundaryKey = "go-stdx.timeline.boundary"

type boundary struct{ data timeline.Stage }

const checkpointKey = "go-stdx.timeline.checkpoint"

type checkpoint struct{ sequence uint64 }

// projection retains values from the event stream, not gospan spans or Batch
// slices. Its lock is independent of recorder.mu so a full producer queue can
// always be drained. Exporters only receive detached snapshots.
type projection struct {
	prefix     string
	id         string // immutable even before the root event is collected
	mu         sync.Mutex
	rootID     timeline.StageID
	records    map[timeline.StageID]*timeline.Stage
	identities map[int64]timeline.StageID
	boundaries map[timeline.StageID]timeline.Stage
	sequence   uint64
	changed    chan struct{}
}

func newProjection(id string) *projection {
	return &projection{prefix: uuid.NewString(), id: id, rootID: timeline.StageID("operation:" + id), identities: make(map[int64]timeline.StageID), boundaries: make(map[timeline.StageID]timeline.Stage), records: make(map[timeline.StageID]*timeline.Stage), changed: make(chan struct{})}
}

func (p *projection) WriteBatch(batch gospan.Batch) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, event := range batch.Events {
		if event.ParentID == 0 && event.Kind == gospan.EventStart {
			p.identities[event.SpanID] = p.rootID
		}
		for _, attr := range event.Attrs {
			if marker, ok := attr.Value.Any().(boundary); attr.Key == boundaryKey && ok {
				p.identities[event.SpanID] = marker.data.ID
				p.boundaries[marker.data.ID] = marker.data
			}
		}
		id := p.stageID(event.SpanID)
		record := p.records[id]
		if record == nil {
			record = &timeline.Stage{ID: id, Status: timeline.Running}
			p.records[id] = record
		}
		switch event.Kind {
		case gospan.EventStart:
			record.Name = event.Name
			record.ParentID = p.stageID(event.ParentID)
			record.StartedAt = time.Unix(0, event.StartNS).UTC()
		case gospan.EventEnd:
			record.FinishedAt = time.Unix(0, event.EndNS).UTC()
			record.Error = event.Error
			switch event.Status {
			case gospan.SpanStatusError:
				record.Status = timeline.Failed
			case gospan.SpanStatusCanceled:
				record.Status = timeline.Canceled
			default:
				record.Status = timeline.Succeeded
			}
		}
		for _, attr := range event.Attrs {
			if marker, ok := attr.Value.Any().(checkpoint); attr.Key == checkpointKey && ok {
				p.sequence = marker.sequence
				continue
			}
			if marker, ok := attr.Value.Any().(boundary); attr.Key == boundaryKey && ok {
				if record.Attributes == nil && len(marker.data.Attributes) > 0 {
					record.Attributes = make(map[string]json.RawMessage)
				}
				for key, value := range marker.data.Attributes {
					record.Attributes[key] = value
				}
				continue
			}
			value := attr.Value.Any().(attributeValue)
			if record.Attributes == nil {
				record.Attributes = make(map[string]json.RawMessage)
			}
			record.Attributes[attr.Key] = value.value
		}
		if data, ok := p.boundaries[id]; ok {
			record.ParentID, record.Actor, record.StartedAt = data.ParentID, data.Actor, data.StartedAt
			if !data.FinishedAt.IsZero() {
				record.FinishedAt, record.Elapsed = data.FinishedAt, data.Elapsed
				// The boundary precedes EventEnd; publish the code with its result.
				if record.Status != timeline.Running {
					record.Code = data.Code
				}
			}
		}
	}
	close(p.changed)
	p.changed = make(chan struct{})
	return nil
}

func (*projection) Flush() error { return nil }
func (*projection) Close() error { return nil }

func (p *projection) wait(ctx context.Context, sequence uint64) error {
	for {
		p.mu.Lock()
		ready, changed := p.sequence >= sequence, p.changed
		p.mu.Unlock()
		if ready {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *projection) snapshot(complete bool) timeline.Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := timeline.Snapshot{ID: p.id, RootStageID: p.rootID, CapturedAt: time.Now().UTC(), Collection: timeline.Collection{LocalFlushed: complete, StoreRead: complete}}
	if root := p.records[p.rootID]; root != nil {
		result.RootStageID, result.Operation = root.ID, root.Name
		result.StartedAt, result.FinishedAt = root.StartedAt, root.FinishedAt
		result.Status, result.Error = root.Status, root.Error
		result.Attributes = cloneAttributes(root.Attributes)
		if complete && !root.FinishedAt.IsZero() {
			result.CapturedAt = root.FinishedAt
		}
	}
	for id, record := range p.records {
		if id == p.rootID {
			continue
		}
		stage := *record
		stage.Attributes = cloneAttributes(record.Attributes)
		result.Stages = append(result.Stages, stage)
	}
	slices.SortFunc(result.Stages, func(a, b timeline.Stage) int {
		if order := a.StartedAt.Compare(b.StartedAt); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return result
}

// JSON bytes must be copied too: modifying an exported snapshot cannot alter
// the projection or another snapshot while other goroutines are recording.
func cloneAttributes(attributes map[string]json.RawMessage) map[string]json.RawMessage {
	if attributes == nil {
		return nil
	}
	result := make(map[string]json.RawMessage, len(attributes))
	for key, value := range attributes {
		result[key] = slices.Clone(value)
	}
	return result
}

func (p *projection) stageID(id int64) timeline.StageID {
	if id == 0 {
		return ""
	}
	if identity, ok := p.identities[id]; ok {
		return identity
	}
	return timeline.StageID(p.prefix + ":" + strconv.FormatInt(id, 10))
}
