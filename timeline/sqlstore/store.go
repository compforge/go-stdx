// Package sqlstore persists one document per timeline using an application-owned
// database/sql pool. It uses ? bind parameters and does not manage schema or GC.
package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/compforge/go-stdx/timeline"
)

// Store uses timelines(id, payload, version, created_at, updated_at). id is the
// primary key; version is incremented on every accepted change. JSON merging is
// done in Go, so backends need no vendor-specific JSON update expressions.
type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) load(ctx context.Context, id string) (timeline.Document, uint64, error) {
	var raw []byte
	var version uint64
	err := s.db.QueryRowContext(ctx, "SELECT payload, version FROM timelines WHERE id = ?", id).Scan(&raw, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return timeline.Document{}, 0, timeline.ErrNotFound
	}
	if err != nil {
		return timeline.Document{}, 0, err
	}
	var doc timeline.Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		return doc, 0, fmt.Errorf("decode timeline %s: %w", id, err)
	}
	return doc, version, nil
}
func (s *Store) Read(ctx context.Context, id string) (timeline.Document, error) {
	doc, _, err := s.load(ctx, id)
	return doc, err
}

// Merge retries only confirmed version races. An ambiguous commit returns an
// error; retrying the same update is safe even if the first commit succeeded.
func (s *Store) Merge(ctx context.Context, id string, update timeline.Update) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, version, err := s.load(ctx, id)
		if err != nil && !errors.Is(err, timeline.ErrNotFound) {
			return err
		}
		next, changed, err := timeline.MergeDocument(id, current, update)
		if err != nil || !changed {
			return err
		}
		raw, err := json.Marshal(next)
		if err != nil {
			return fmt.Errorf("encode timeline %s: %w", id, err)
		}
		now := time.Now().UTC()
		if version == 0 {
			_, err = s.db.ExecContext(ctx, "INSERT INTO timelines (id, payload, version, created_at, updated_at) VALUES (?, ?, ?, ?, ?)", id, string(raw), 1, now, now)
			if err == nil {
				return nil
			}
			// Detect competing insertion without depending on vendor error codes.
			_, observed, readErr := s.load(ctx, id)
			if readErr != nil || observed == 0 {
				return errors.Join(err, readErr)
			}
		} else {
			result, err := s.db.ExecContext(ctx, "UPDATE timelines SET payload = ?, version = ?, updated_at = ? WHERE id = ? AND version = ?", string(raw), version+1, now, id, version)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if rows == 1 {
				return nil
			}
		}
		// Bounded backoff avoids a hot spin while preserving the caller's deadline.
		delay := time.Duration(1<<min(attempt, 5)) * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

var _ timeline.Store = (*Store)(nil)
