package store

import (
	"context"
	"sync"
)

// MemoryStore shares documents among handles in one process.
type MemoryStore struct {
	mu        sync.Mutex
	documents map[string]Document
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{documents: make(map[string]Document)} }
func (s *MemoryStore) Merge(ctx context.Context, id string, update Update) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	doc, _, err := MergeDocument(id, s.documents[id], update)
	if err == nil {
		s.documents[id] = doc
	}
	return err
}
func (s *MemoryStore) Read(ctx context.Context, id string) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Document{}, err
	}
	doc, ok := s.documents[id]
	if !ok {
		return Document{}, ErrNotFound
	}
	return doc.Clone(), nil
}
