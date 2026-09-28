package timeline

import "context"

// Noop returns a stateless disabled timeline. Its contexts retain their lifetime
// and values; snapshots are empty. Use it when recording is optional.
func Noop() Timeline { return noop{} }

type noop struct{}

func (noop) SetFields(...Field)  {}
func (noop) End(error, ...Field) {}
func (noop) Begin(ctx context.Context, _ string, _ ...Field) (context.Context, Stage) {
	return ctx, noop{}
}
func (noop) Snapshot(context.Context) (Snapshot, error)      { return Snapshot{Complete: true}, nil }
func (noop) Finish(context.Context, error) (Snapshot, error) { return Snapshot{Complete: true}, nil }
