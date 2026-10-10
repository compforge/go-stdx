package timeline

import "encoding/json"

// AttributeValue decodes the most recent value for key using encoding/json.
// It returns false for a missing key or a JSON value incompatible with T.
func AttributeValue[T any](attributes map[string]json.RawMessage, key string) (T, bool) {
	var value T
	raw, ok := attributes[key]
	if !ok || json.Unmarshal(raw, &value) != nil {
		var zero T
		return zero, false
	}
	return value, true
}
