package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/compforge/go-stdx/timeline/store"
)

func TestNoopStoreDoesNotRetainFacts(t *testing.T) {
	s := store.NewNoopStore()
	if err := s.Merge(context.Background(), "task", store.Update{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(context.Background(), "task"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("noop store retained a document: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Merge(ctx, "task", store.Update{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, "task"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
