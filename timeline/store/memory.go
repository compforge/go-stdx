package store

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryStore shares documents among handles in one process.
type MemoryStore struct {
	mu        sync.Mutex
	documents map[string]Document
	updated   map[string]time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{documents: make(map[string]Document), updated: make(map[string]time.Time)}
}
func (s *MemoryStore) Merge(ctx context.Context, id string, update Update) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	doc, changed, err := MergeDocument(id, s.documents[id], update)
	if err == nil && changed {
		s.updated[id] = time.Now().UTC()
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

func (s *MemoryStore) MGet(ctx context.Context, ids []string) ([]Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result []Document
	seen := make(map[string]bool)
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		if d, ok := s.documents[id]; ok {
			result = append(result, d.Clone())
		}
	}
	return result, nil
}
func (s *MemoryStore) Latest(ctx context.Context, after time.Time, limit int) ([]Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	var ids []string
	for id, at := range s.updated {
		if at.After(after) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := s.updated[ids[i]], s.updated[ids[j]]
		if a.Equal(b) {
			return ids[i] > ids[j]
		}
		return a.After(b)
	})
	var result []Document
	for _, id := range ids[:min(limit, len(ids))] {
		result = append(result, s.documents[id].Clone())
	}
	return result, nil
}
