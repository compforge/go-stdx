// Package model holds values shared by recording and storage, without either lifecycle.
package model

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrInvalidStage = errors.New("timeline: invalid stage")
var ErrEmptyID = errors.New("timeline: ID must not be empty")
var ErrInvalidAttribute = errors.New("timeline: attribute is not JSON serializable")

// Actor identifies the executor, not the requesting user.
// The zero value is the default executor for single-process recording.
// When provided, ID takes precedence over Name.
type Actor struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// StageID identifies a logical stage. Begin generates a UUID by default; multiple
// actors may contribute separate states under a caller-supplied shared ID.
type StageID string

type Status string

const (
	Unknown   Status = "unknown"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Canceled  Status = "canceled"
)

// Stage retains the interval and result of work or a wait. Leaf stages represent
// one component's execution flow; parent stages may group parallel child stages.
// ParentID refers to a logical stage group or Snapshot.RootStageID. FinishedAt is zero
// while work is running.
type Stage struct {
	ID         StageID                    `json:"id"`
	ParentID   StageID                    `json:"parent_id"`
	Actor      Actor                      `json:"actor,omitzero"`
	Elapsed    time.Duration              `json:"elapsed_ns,omitempty"`
	Name       string                     `json:"name"`
	StartedAt  time.Time                  `json:"started_at"`
	FinishedAt time.Time                  `json:"finished_at,omitempty"`
	Status     Status                     `json:"status"`
	Error      string                     `json:"error,omitempty"`
	Attributes map[string]json.RawMessage `json:"attributes,omitempty"`
	// Code is an optional caller-defined value, independent of Status and Error.
	// Timeline records it without inferring or interpreting its meaning.
	Code string `json:"code,omitempty"`
}

// Duration measures a finished interval, or a running interval at capturedAt.
func (s Stage) Duration(capturedAt time.Time) time.Duration {
	if !s.FinishedAt.IsZero() && s.Elapsed != 0 {
		return s.Elapsed
	}
	return interval(s.StartedAt, s.FinishedAt, capturedAt)
}

// Snapshot is a detached view. Stages are ordered by start time, ID, then actor;
// overlapping intervals stay overlapping rather than being added together.
// Collection describes the observations this reader can confirm. A successful
// read does not prove all processes have flushed or that no late stage remains.
// All slices and JSON attribute values belong to this snapshot. Standard json.Marshal
// and json.Unmarshal persist it without a backend or a caller-defined DTO.
// JSON stores actors once in a payload-local table; stage actor_ref values are
// resolved back to full Actors when decoding.
// Times use time.Time JSON encoding (RFC 3339); zero FinishedAt means running.
type Snapshot struct {
	ID          string                     `json:"id"`
	RootStageID StageID                    `json:"root_stage_id"`
	Operation   string                     `json:"operation"`
	StartedAt   time.Time                  `json:"started_at"`
	FinishedAt  time.Time                  `json:"finished_at,omitempty"`
	CapturedAt  time.Time                  `json:"captured_at"`
	Status      Status                     `json:"status"`
	Error       string                     `json:"error,omitempty"`
	Attributes  map[string]json.RawMessage `json:"attributes,omitempty"`
	Stages      []Stage                    `json:"stages,omitempty"`
	Collection  Collection                 `json:"collection"`
}

// Collection is deliberately scoped: neither field asserts that all distributed
// participants have reported. Remote crashed/unflushed producers are unknown.
type Collection struct {
	// LocalFlushed means no known pending local facts or save loss. A cached
	// read reports the current state without forcing a flush.
	LocalFlushed bool `json:"local_flushed"`
	// StoreRead means this read fetched persistent state; a cache hit is false.
	StoreRead bool `json:"store_read"`
}

// Duration returns the signed difference between the recorded boundaries, or
// between StartedAt and CapturedAt while unfinished. A Start observed after
// Finish intentionally produces a negative duration; timestamps are not clamped
// or reordered to infer an execution interval. An absent Start returns zero.
func (s Snapshot) Duration() time.Duration {
	return interval(s.StartedAt, s.FinishedAt, s.CapturedAt)
}

func interval(start, end, captured time.Time) time.Duration {
	if start.IsZero() {
		return 0
	}
	if end.IsZero() {
		end = captured
	}
	return end.Sub(start)
}
