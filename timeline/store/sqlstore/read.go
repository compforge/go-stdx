package sqlstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/compforge/go-stdx/timeline/store"
)

func (s *Store) documents(ctx context.Context, query string, args ...any) ([]store.Document, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []store.Document
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var d store.Document
		if err := json.Unmarshal(raw, &d); err != nil {
			return nil, fmt.Errorf("decode timeline: %w", err)
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

func (s *Store) MGet(ctx context.Context, ids []string) ([]store.Document, error) {
	if len(ids) == 0 {
		return nil, ctx.Err()
	}
	args := make([]any, 0, len(ids))
	seen := make(map[string]bool)
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			args = append(args, id)
		}
	}
	return s.documents(ctx, "SELECT payload FROM timelines WHERE id IN ("+strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")+")", args...)
}

func (s *Store) Latest(ctx context.Context, after time.Time, limit int) ([]store.Document, error) {
	if limit <= 0 {
		return nil, ctx.Err()
	}
	return s.documents(ctx, "SELECT payload FROM timelines WHERE updated_at > ? ORDER BY updated_at DESC, id DESC LIMIT ?", after.UTC(), limit)
}
