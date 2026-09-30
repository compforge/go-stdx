package timeline

import (
	"encoding/json"
	"fmt"
	"time"
)

// StageOption sets the identity, source start time or initial attributes of a stage.
// Implementations apply options synchronously before returning a handle.
type StageOption func(*Stage) error

// EndOption supplies a source end time, final attributes or a code.
type EndOption func(*Stage) error

func WithStageID(id StageID) StageOption { return func(s *Stage) error { s.ID = id; return nil } }
func WithParent(id StageID) StageOption  { return func(s *Stage) error { s.ParentID = id; return nil } }
func WithStartTime(at time.Time) StageOption {
	return func(s *Stage) error { s.StartedAt = at; return nil }
}
func WithStageActor(actor Actor) StageOption {
	return func(s *Stage) error { s.Actor = actor; return nil }
}
func WithEndTime(at time.Time) EndOption {
	return func(s *Stage) error { s.FinishedAt = at; return nil }
}
func WithFields(fields ...Field) StageOption {
	return func(s *Stage) error { return setStageFields(s, fields) }
}

// WithCode records a caller-defined code independently of the stage result.
// An empty code is omitted from JSON.
func WithCode(code string) EndOption {
	return func(s *Stage) error { s.Code = code; return nil }
}

func WithEndFields(fields ...Field) EndOption {
	return func(s *Stage) error { return setStageFields(s, fields) }
}

func setStageFields(s *Stage, fields []Field) error {
	for _, field := range fields {
		raw, err := json.Marshal(field.Value)
		if err != nil {
			return fmt.Errorf("%w: %s: %v", ErrInvalidField, field.Key, err)
		}
		if s.Fields == nil {
			s.Fields = make(map[string]json.RawMessage)
		}
		s.Fields[field.Key] = raw
	}
	return nil
}
