package store

import "context"

// NoopStore accepts submissions without retaining facts. Reads return ErrNotFound;
// the recording cache is the only document owner and eviction cannot be recovered.
type NoopStore struct{}

func NewNoopStore() *NoopStore { return &NoopStore{} }

func (*NoopStore) Merge(ctx context.Context, _ string, _ Update) error { return ctx.Err() }

func (*NoopStore) Read(ctx context.Context, _ string) (Document, error) {
	if err := ctx.Err(); err != nil {
		return Document{}, err
	}
	return Document{}, ErrNotFound
}
