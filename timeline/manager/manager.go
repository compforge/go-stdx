package manager

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/compforge/go-stdx/timeline"
	"github.com/compforge/go-stdx/timeline/store"
	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/sync/singleflight"
)

var ErrClosed = errors.New("timeline manager: shut down")
var ErrBufferFull = errors.New("timeline manager: pending save limit exceeded")
var ErrStageNotFound = errors.New("timeline manager: stage not found")
var ErrAmbiguousStage = errors.New("timeline manager: multiple running stages share this name")

// Config bounds memory and backend calls. Zero fields select defaults. TTL is
// fixed from insertion; reading and changing a cached document do not renew it.
type Config struct {
	TTL                 time.Duration // default 1h
	MaxTimelines        int           // default 1024 cached documents
	FlushInterval       time.Duration // default 1s between background saves
	ExportTimeout       time.Duration // default 5s for each Store read/merge
	MaxPendingTimelines int           // default 1024, including evicted documents awaiting a final save
	MaxPendingUpdates   int           // default 1024 per document, before coalescing a save batch
	// OnError reports save failures outside locks. It must return promptly and
	// must not call Shutdown. Default: slog with the timeline ID, without data.
	OnError func(id string, err error)
}

// Manager owns the policy joining ttlcache and Store. Writes acknowledge
// memory acceptance; Flush is the optional persistence checkpoint. This is a
// best-effort cache, without durable delivery or cross-process consistency.
// spec: Eviction schedules one final bounded save. Failure is reported and the
// buffer is released, so an unavailable Store cannot pin memory indefinitely.
type Manager struct {
	store        store.Store
	config       Config
	items        *ttlcache.Cache[string, *entry]
	loads        singleflight.Group
	mu           sync.Mutex // lifecycle and pending membership; never held during Store IO
	pending      map[*entry]struct{}
	closed       bool
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	wake         chan struct{}
	stopEviction func()
	shutdownMu   sync.Mutex
	dropped      atomic.Uint64
	rejected     atomic.Uint64
}

type entry struct {
	mu         sync.Mutex
	id         string
	doc        store.Document
	exists     bool
	updates    []store.Update
	batch      *store.Update // immutable until a save succeeds or is deliberately dropped
	batchCount int
	evicted    bool
	err        error
	lossErr    error
	lost       bool
	gate       chan struct{}
}

func New(backend store.Store, config Config) (*Manager, error) {
	if backend == nil {
		return nil, errors.New("timeline manager: nil Store")
	}
	if config.TTL < 0 || config.MaxTimelines < 0 || config.FlushInterval < 0 || config.ExportTimeout < 0 || config.MaxPendingTimelines < 0 || config.MaxPendingUpdates < 0 {
		return nil, errors.New("timeline manager: negative configuration")
	}
	if config.TTL == 0 {
		config.TTL = time.Hour
	}
	if config.MaxTimelines == 0 {
		config.MaxTimelines = 1024
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = time.Second
	}
	if config.ExportTimeout == 0 {
		config.ExportTimeout = 5 * time.Second
	}
	if config.MaxPendingTimelines == 0 {
		config.MaxPendingTimelines = 1024
	}
	if config.MaxPendingUpdates == 0 {
		config.MaxPendingUpdates = 1024
	}
	if config.OnError == nil {
		config.OnError = func(id string, err error) { slog.Error("timeline save failed", "timeline_id", id, "error", err) }
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Manager{store: backend, config: config, pending: make(map[*entry]struct{}), ctx: ctx, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1)}
	c.items = ttlcache.New(ttlcache.WithTTL[string, *entry](config.TTL), ttlcache.WithCapacity[string, *entry](uint64(config.MaxTimelines)), ttlcache.WithDisableTouchOnHit[string, *entry]())
	c.stopEviction = c.items.OnEviction(func(_ context.Context, _ ttlcache.EvictionReason, item *ttlcache.Item[string, *entry]) {
		e := item.Value()
		e.mu.Lock()
		e.evicted = true
		e.mu.Unlock()
		select {
		case c.wake <- struct{}{}:
		default:
		}
	})
	go c.run()
	return c, nil
}

// NewWriter creates an actor-scoped recorder. Its writes use the same cache;
// recording may load Store on a miss. Keep each stage owned by one recorder.
func (m *Manager) NewWriter(id string, actor timeline.Actor) (*timeline.Recorder, error) {
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	return timeline.New(id, timeline.WithStore(m.store), timeline.WithActor(actor), timeline.WithWriter(&writer{manager: m, id: id}))
}
func (m *Manager) apply(id string, fn func(*timeline.Recorder, store.Document) error) error {
	return m.update(context.Background(), id, func(doc store.Document) (store.Update, error) {
		w := &captureWriter{}
		r, err := timeline.Restore(doc, timeline.WithStore(m.store), timeline.WithWriter(w))
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
