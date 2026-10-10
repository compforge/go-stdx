package timeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

var ErrManagerClosed = errors.New("timeline: manager is shut down")
var ErrBufferFull = errors.New("timeline: pending record limit exceeded")

// ManagerConfig controls background persistence. Zero values use the defaults
// below. Limits count updates/handles, not encoded bytes; attribute sizes remain
// the caller's responsibility.
type ManagerConfig struct {
	FlushInterval     time.Duration // default 1s; also the initial retry delay
	ExportTimeout     time.Duration // default 5s per handle
	MaxPendingHandles int           // default 1024
	MaxPendingUpdates int           // default 1024 per handle, including in-flight updates
	// OnError receives background errors once per failure streak per handle.
	// It runs on the worker, outside locks; it must return promptly and must not
	// call Shutdown. By default errors are logged with slog, without record data.
	OnError func(id string, err error)
}

// Manager owns background persistence for local recording handles. Mutations
// only change memory and signal the worker; callers need not Flush each stage.
// One worker processes dirty handles, with bounded IO and per-handle backoff.
// Store connections and durable retention remain owned by the application.
//
// spec: A notification is a hint. Dirty state survives coalesced notifications,
// failed writes, and callers discarding their handles. Success only acknowledges
// the captured update batch; concurrent mutations remain pending.
// link: ../docs/timeline.md#manager-与后台提交
type Manager struct {
	store   Store
	config  ManagerConfig
	mu      sync.Mutex
	dirty   map[*Recorder]*managedPending
	closed  bool
	dropped uint64
	wake    chan struct{}
	done    chan struct{}
	cancel  context.CancelFunc
}

type managedPending struct {
	generation uint64
	failures   int
	next       time.Time
}

type managedWork struct {
	recorder   *Recorder
	pending    *managedPending
	generation uint64
}

// NewManager starts one background worker. Stop business producers before
// calling Shutdown with an independent, bounded cleanup context.
func NewManager(store Store, config ManagerConfig) (*Manager, error) {
	if store == nil {
		return nil, errors.New("timeline: nil Store")
	}
	if config.FlushInterval < 0 || config.ExportTimeout < 0 || config.MaxPendingHandles < 0 || config.MaxPendingUpdates < 0 {
		return nil, errors.New("timeline: negative manager configuration")
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = time.Second
	}
	if config.ExportTimeout == 0 {
		config.ExportTimeout = 5 * time.Second
	}
	if config.MaxPendingHandles == 0 {
		config.MaxPendingHandles = 1024
	}
	if config.MaxPendingUpdates == 0 {
		config.MaxPendingUpdates = 1024
	}
	if config.OnError == nil {
		config.OnError = func(id string, err error) {
			slog.Error("timeline background persistence failed", "timeline_id", id, "error", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		store: store, config: config, dirty: make(map[*Recorder]*managedPending),
		wake: make(chan struct{}, 1), done: make(chan struct{}), cancel: cancel,
	}
	go m.run(ctx)
	return m, nil
}

// New returns an independent writer for id. Handles sharing an ID contribute to
// the same document but retain their own Actor and coordinator authority. Options
// may set WithActor; WithStore is invalid because the Manager owns the Store.
// Idle handles are not retained by the Manager and can become dirty again later.
func (m *Manager) New(id string, options ...Option) (*Recorder, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	r := &Recorder{id: id, manager: m, flushGate: make(chan struct{}, 1)}
	for _, option := range options {
		option(r)
	}
	if r.store != nil {
		return nil, errors.New("timeline: WithStore is not a Manager handle option")
	}
	r.store = m.store
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrManagerClosed
	}
	return r, nil
}

// schedule runs while the recorder is locked. The worker never holds m.mu while
// acquiring a recorder lock, so recording cannot wait for database IO.
func (m *Manager) schedule(r *Recorder) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrManagerClosed
	}
	p := m.dirty[r]
	if p == nil {
		if len(m.dirty) >= m.config.MaxPendingHandles {
			m.dropped++
			return ErrBufferFull
		}
		p = &managedPending{}
		m.dirty[r] = p
	}
	p.generation++
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return nil
}

// ManagerStats describes this process's buffers, never global completeness.
type ManagerStats struct {
	PendingHandles int
	DroppedUpdates uint64
}

func (m *Manager) Stats() ManagerStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return ManagerStats{PendingHandles: len(m.dirty), DroppedUpdates: m.dropped}
}

func (m *Manager) work(readyOnly bool) []managedWork {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	work := make([]managedWork, 0, len(m.dirty))
	for r, p := range m.dirty {
		if !readyOnly || !now.Before(p.next) {
			work = append(work, managedWork{recorder: r, pending: p, generation: p.generation})
		}
	}
	return work
}

func (m *Manager) run(ctx context.Context) {
	defer close(m.done)
	ticker := time.NewTicker(m.config.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-m.wake:
		}
		for _, w := range m.work(true) {
			if ctx.Err() != nil {
				return
			}
			writeCtx, cancel := context.WithTimeout(ctx, m.config.ExportTimeout)
			err := w.recorder.Flush(writeCtx)
			cancel()
			if m.complete(w, err) && ctx.Err() == nil {
				m.config.OnError(w.recorder.id, err)
			}
		}
	}
}

// complete releases only the dirty generation we processed. Checking the queue
// separately is safe: a new mutation also advances generation under m.mu.
func (m *Manager) complete(w managedWork, err error) bool {
	r := w.recorder
	r.mu.Lock()
	remaining := len(r.pending) != 0
	r.mu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.dirty[r]
	// A concurrent explicit Flush may retire this entry, and another mutation
	// may register the same handle again. Do not acknowledge that new entry.
	if p != w.pending {
		return false
	}
	report := err != nil && p.failures == 0
	if !remaining && p.generation == w.generation {
		delete(m.dirty, r)
	} else if err != nil {
		p.failures++
		delay := min(m.config.FlushInterval, 30*time.Second)
		for i := 1; i < p.failures && delay < 30*time.Second; i++ {
			delay = min(delay*2, 30*time.Second)
		}
		p.next = time.Now().Add(delay)
	} else {
		p.failures = 0
		p.next = time.Time{}
	}
	return report
}

// Flush submits records from the currently dirty local handles. It may also
// include concurrent updates, but does not wait for future writers or remote
// processes. Errors retain pending data for retry. The caller's context bounds
// the whole call; each handle also has the configured ExportTimeout.
func (m *Manager) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var errs []error
	for _, w := range m.work(false) {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		writeCtx, cancel := context.WithTimeout(ctx, m.config.ExportTimeout)
		err := w.recorder.Flush(writeCtx)
		cancel()
		m.complete(w, err)
		if err != nil {
			errs = append(errs, fmt.Errorf("timeline %s: %w", w.recorder.id, err))
		}
	}
	return errors.Join(errs...)
}

// Shutdown rejects new mutations, cancels the background worker, and attempts a
// final Flush with ctx. A failed/timed-out drain can be retried by calling
// Shutdown again. It never ends business operations or closes the shared Store.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	m.cancel()
	m.mu.Unlock()
	select {
	case <-m.done:
		return m.Flush(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}
