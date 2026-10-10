package timeline

import (
	"context"
	"errors"

	"github.com/compforge/go-stdx/timeline/internal/cache"
	"github.com/compforge/go-stdx/timeline/store"
)

var ErrClosed = cache.ErrClosed
var ErrBufferFull = cache.ErrBufferFull
var ErrStageNotFound = errors.New("timeline manager: stage not found")
var ErrAmbiguousStage = errors.New("timeline manager: multiple running stages share this name")

type Config = cache.Config
type Stats = cache.Stats

// Manager joins ID-based recording with the shared loading and saving cache.
// Writes acknowledge memory acceptance; Flush optionally waits for Store submission.
type Manager struct {
	cache *cache.Cache
	store store.Store
}

// NewManager starts a Manager. A nil backend selects NoopStore for cache-only recording.
func NewManager(backend store.Store, config Config) (*Manager, error) {
	if backend == nil {
		backend = store.NewNoopStore()
	}
	c, err := cache.New(backend, config)
	if err != nil {
		return nil, err
	}
	return &Manager{cache: c, store: backend}, nil
}

// NewWriter creates an actor-scoped recorder. Its writes use the same cache;
// recording may load Store on a miss. Keep each stage owned by one recorder.
func (m *Manager) NewWriter(id string, actor Actor) (*Handle, error) {
	if m.cache.Closed() {
		return nil, ErrClosed
	}
	return New(id, WithStore(m.store), WithActor(actor), withWriter(&writer{manager: m, id: id}))
}
func (m *Manager) apply(id string, fn func(*recorder, store.Document) error) error {
	return m.cache.Update(context.Background(), id, func(doc store.Document) (store.Update, error) {
		w := &captureWriter{}
		r, err := restoreRecorder(doc, WithStore(m.store), withWriter(w))
		if err != nil {
			return store.Update{}, err
		}
		if err = fn(r, doc); err != nil {
			return store.Update{}, err
		}
		return w.update, nil
	})
}

// Each operation emits at most one fact. Manager.update serializes construction
// and admission, so a rejected write cannot advance the cached stage revision.
type captureWriter struct{ update store.Update }

func (w *captureWriter) Write(u store.Update) error { w.update = u; return nil }
func (*captureWriter) RecordError(error)            {}
func (*captureWriter) Flush(context.Context) error  { return nil }
