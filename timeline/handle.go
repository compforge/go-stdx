package timeline

import (
	"context"
	"github.com/compforge/go-stdx/timeline/store"
)

// Option configures a writer without performing remote IO.
type Option func(*recorder)

func WithStore(backend store.Store) Option { return func(t *recorder) { t.store = backend } }
func WithActor(actor Actor) Option         { return func(t *recorder) { t.actor = actor } }

// Handle groups the optional object-style API for one recording identity.
// The primary business API is Begin/End/Record/Read with optional Start/Finish.
// A standalone handle shares Manager's cache/Store implementation and checkpoints
// explicitly; NewWriter binds a handle to an existing Manager's background saves.
type Handle struct{ recorder *recorder }

// New creates a standalone handle. Without WithStore, its cache is the only
// document owner and NoopStore accepts checkpoints without retaining data.
func New(id string, options ...Option) (*Handle, error) {
	r, err := newRecorder(id, options...)
	if err != nil {
		return nil, err
	}
	return &Handle{recorder: r}, nil
}

func (h *Handle) ID() string { return h.recorder.ID() }
func (h *Handle) Begin(name string, options ...StageOption) StageHandle {
	return h.recorder.Begin(name, options...)
}
func (h *Handle) Record(stage Stage) error { return h.recorder.Record(stage) }
func (h *Handle) Start(ctx context.Context, operation string, attributes ...Attribute) error {
	return h.recorder.Start(ctx, operation, attributes...)
}
func (h *Handle) SetAttributes(attributes ...Attribute) { h.recorder.SetAttributes(attributes...) }
func (h *Handle) UpdateAttributes(attributes ...Attribute) error {
	return h.recorder.UpdateAttributes(attributes...)
}
func (h *Handle) Flush(ctx context.Context) error                { return h.recorder.Flush(ctx) }
func (h *Handle) Snapshot(ctx context.Context) (Snapshot, error) { return h.recorder.Snapshot(ctx) }
func (h *Handle) Finish(ctx context.Context, err error) (Snapshot, error) {
	return h.recorder.Finish(ctx, err)
}
func (h *Handle) Err() error { return h.recorder.Err() }

var _ Timeline = (*Handle)(nil)
