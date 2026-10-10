package manager

import "context"

// Noop returns a disabled timeline retaining id. It records no intervals or
// attributes, and leaves contexts unchanged. An empty ID denotes untracked work.
func Noop(id string) Timeline { return noop{id: id} }

type noop struct{ id string }

func (t noop) ID() string { return t.id }

func (noop) SetAttributes(...Attribute)               {}
func (noop) Record(Stage) error                       { return nil }
func (noop) Begin(string, ...StageOption) StageHandle { return noopStage{} }
func (t noop) Snapshot(context.Context) (Snapshot, error) {
	return Snapshot{ID: t.id, Collection: Collection{LocalFlushed: true, StoreRead: true}}, nil
}
func (t noop) Finish(context.Context, error) (Snapshot, error) {
	return Snapshot{ID: t.id, Collection: Collection{LocalFlushed: true, StoreRead: true}}, nil
}

func (noop) Flush(context.Context) error { return nil }

type noopStage struct{}

func (noopStage) ID() StageID                      { return "" }
func (noopStage) SetAttributes(...Attribute) error { return nil }
func (noopStage) End(error, ...EndOption) error    { return nil }
