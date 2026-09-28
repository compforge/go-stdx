// Package sqlstore persists timeline observations using an application-owned
// database/sql pool. It uses ? bind parameters (MySQL, SQLite and DM-style
// drivers); it does not open pools, create schemas or close the supplied DB.
package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/compforge/go-stdx/timeline"
)

// Store uses timeline_records(record_seq, timeline_id, record_id, payload).
// record_seq must be database-generated and ordered; (timeline_id, record_id)
// must have a unique constraint. See docs/timeline.md for schema and retention.
type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) Append(ctx context.Context, id string, records []timeline.Record) error {
	for _, record := range records {
		payload, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode timeline record: %w", err)
		}
		_, insertErr := s.db.ExecContext(ctx,
			"INSERT INTO timeline_records (timeline_id, record_id, payload) VALUES (?, ?, ?)",
			id, record.ID, string(payload))
		if insertErr == nil {
			continue
		}
		// A commit may have succeeded even if its acknowledgement was lost. Read
		// back the exact idempotency key rather than relying on dialect error codes.
		var existing []byte
		readErr := s.db.QueryRowContext(ctx,
			"SELECT payload FROM timeline_records WHERE timeline_id = ? AND record_id = ?",
			id, record.ID).Scan(&existing)
		if readErr != nil {
			return errors.Join(insertErr, readErr)
		}
		if !bytes.Equal(existing, payload) {
			return timeline.ErrRecordConflict
		}
	}
	return nil
}

func (s *Store) Read(ctx context.Context, id string) ([]timeline.Record, error) {
	// A single statement obtains one database read view. Do not page without a
	// read transaction: pages could mix different snapshots under concurrent IO.
	rows, err := s.db.QueryContext(ctx,
		"SELECT payload FROM timeline_records WHERE timeline_id = ? ORDER BY record_seq", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []timeline.Record
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return records, err
		}
		var record timeline.Record
		if err := json.Unmarshal(payload, &record); err != nil {
			return records, fmt.Errorf("decode timeline record: %w", err)
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

var _ timeline.Store = (*Store)(nil)
