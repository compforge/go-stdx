package manager

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

// WithParent records an association without looking up or requiring the parent.
// The ID is preserved even when the parent is absent or arrives later; callers
// can link available stages when reading and display unresolved stages at the root.
func WithParent(id StageID) StageOption { return func(s *Stage) error { s.ParentID = id; return nil } }
func WithStartTime(at time.Time) StageOption {
	return func(s *Stage) error { s.StartedAt = at; return nil }
}
func WithStageActor(actor Actor) StageOption {
	return func(s *Stage) error { s.Actor = actor; return nil }
}

// WithEndActor selects the actor for a named End; it cannot change a handle identity.
func WithEndActor(actor Actor) EndOption { return func(s *Stage) error { s.Actor = actor; return nil } }

func WithEndTime(at time.Time) EndOption {
	return func(s *Stage) error { s.FinishedAt = at; return nil }
}
func WithAttributes(attributes ...Attribute) StageOption {
	return func(s *Stage) error { return setStageAttributes(s, attributes) }
}

// WithCode records a caller-defined code independently of the stage result.
// An empty code is omitted from JSON.
func WithCode(code string) EndOption {
	return func(s *Stage) error { s.Code = code; return nil }
}

func WithEndAttributes(attributes ...Attribute) EndOption {
	return func(s *Stage) error { return setStageAttributes(s, attributes) }
}

func setStageAttributes(s *Stage, attributes []Attribute) error {
	for _, attribute := range attributes {
		raw, err := json.Marshal(attribute.Value)
		if err != nil {
			return fmt.Errorf("%w: %s: %v", ErrInvalidAttribute, attribute.Key, err)
		}
		if s.Attributes == nil {
			s.Attributes = make(map[string]json.RawMessage)
		}
		s.Attributes[attribute.Key] = raw
	}
	return nil
}
