package timeline

import (
	"context"
	"errors"
	"sync"
)

var ErrAlreadyExists = errors.New("timeline: ID is already registered")

// Factory creates a fresh Timeline with the supplied ID. It must not publish
// the instance elsewhere: the Registry owns registration and completion cleanup.
// On error it must release any resources it allocated.
type Factory func(ctx context.Context, id, operation string, attributes ...Attribute) (Timeline, error)

// Registry indexes active timelines within one process. Share an instance across
// components; business code owns ID generation, propagation and completion.
// Construct it with NewRegistry. It must not be copied after first use.
type Registry struct {
	mu      sync.RWMutex
	factory Factory
	active  map[string]*registeredTimeline
}

// NewRegistry uses factory to create timelines without binding them to context.
// The factory must be non-nil. A Registry does not store completed history or
// automatically finish operations when a request's context is canceled.
func NewRegistry(factory Factory) *Registry {
	return &Registry{factory: factory, active: make(map[string]*registeredTimeline)}
}

// Create reserves a non-empty ID and creates an operation. Concurrent creates
// for the same ID return ErrAlreadyExists, including while its factory is running.
// A failed creation releases the ID. The caller owns Finish on the returned
// Timeline; independent components should only end their own stages.
func (r *Registry) Create(ctx context.Context, id, operation string, attributes ...Attribute) (Timeline, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	r.mu.Lock()
	if _, exists := r.active[id]; exists {
		r.mu.Unlock()
		return nil, ErrAlreadyExists
	}
	entry := &registeredTimeline{registry: r, id: id}
	r.active[id] = entry
	r.mu.Unlock()

	// Reserve before calling the backend, but do not serialize unrelated IDs
	// or lookups behind backend initialization.
	tl, err := r.factory(ctx, id, operation, attributes...)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		delete(r.active, id)
		return nil, err
	}
	entry.Timeline = tl
	return entry, nil
}

// Lookup returns the instance published by Create. It returns false while the
// factory is running and after Finish has returned without ErrActiveStages.
// It never creates an operation. A concurrent Finish may seal a returned
// instance before its next Begin; recording then follows Timeline's inert rules.
func (r *Registry) Lookup(id string) (Timeline, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.active[id]
	if !ok || entry.Timeline == nil {
		return nil, false
	}
	return entry, true
}

type registeredTimeline struct {
	Timeline
	registry *Registry
	id       string
}

func (t *registeredTimeline) Finish(ctx context.Context, operationErr error) (Snapshot, error) {
	snapshot, err := t.Timeline.Finish(ctx, operationErr)
	if !errors.Is(err, ErrActiveStages) {
		// Finish seals the result even if collection times out. Retained handles
		// may retry collection, but must not remove a later registration of this ID.
		t.registry.mu.Lock()
		if t.registry.active[t.id] == t {
			delete(t.registry.active, t.id)
		}
		t.registry.mu.Unlock()
	}
	return snapshot, err
}
