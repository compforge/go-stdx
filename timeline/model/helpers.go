package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
)

// SQL JSON types may normalize whitespace and object key order. Compare decoded
// JSON with Number so retries neither conflict on formatting nor round large ints.
func SameJSON(a, b any) bool {
	decode := func(v any) any {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var out any
		if decoder.Decode(&out) != nil {
			return nil
		}
		return out
	}
	return reflect.DeepEqual(decode(a), decode(b))
}
func CloneJSONAttributes(attributes map[string]json.RawMessage) map[string]json.RawMessage {
	if attributes == nil {
		return nil
	}
	result := make(map[string]json.RawMessage, len(attributes))
	for key, value := range attributes {
		result[key] = append(json.RawMessage(nil), value...)
	}
	return result
}
func ValidateCompleted(id string, stage Stage) error {
	if stage.Actor.Key() == "" || stage.ID == "" || stage.Name == "" || stage.StartedAt.IsZero() || stage.FinishedAt.IsZero() || stage.FinishedAt.Before(stage.StartedAt) || stage.Elapsed < 0 || stage.ID == stage.ParentID || stage.ID == RootID(id) {
		return ErrInvalidStage
	}
	switch stage.Status {
	case Succeeded, Failed, Canceled:
	default:
		return ErrInvalidStage
	}
	for key, value := range stage.Attributes {
		if !json.Valid(value) {
			return fmt.Errorf("%w: %s", ErrInvalidAttribute, key)
		}
	}
	return nil
}

func RootID(id string) StageID { return StageID("operation:" + id) }
