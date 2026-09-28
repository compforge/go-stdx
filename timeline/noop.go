package timeline

import "context"

// Noop returns a disabled timeline retaining id. It records no intervals or
// fields, and leaves contexts unchanged. An empty ID denotes untracked work.
func Noop(id string) Timeline { return noop{id: id} }

type noop struct{ id string }

func (t noop) ID() string { return t.id }

func (noop) SetFields(...Field)  {}
func (noop) End(error, ...Field) {}
func (noop) Begin(ctx context.Context, _ string, _ ...Field) (context.Context, Stage) {
	return ctx, noop{}
}
func (t noop) Snapshot(context.Context) (Snapshot, error) {
	return Snapshot{ID: t.id, Complete: true}, nil
}
func (t noop) Finish(context.Context, error) (Snapshot, error) {
	return Snapshot{ID: t.id, Complete: true}, nil
}
