package timeline

import "encoding/json"

// Decode legacy attributes at the wire boundary only. Aliases keep standalone
// stages and update revisions intact without promoting Stage.UnmarshalJSON to
// the whole StageUpdate. Encoding always uses the public attributes key.
type stageData Stage
type stageAttributesJSON struct {
	stageData
	LegacyAttributes map[string]json.RawMessage `json:"fields"`
}

func (s stageAttributesJSON) stage() Stage {
	result := Stage(s.stageData)
	if result.Attributes == nil {
		result.Attributes = s.LegacyAttributes
	}
	return result
}

func (s *Stage) UnmarshalJSON(raw []byte) error {
	var wire stageAttributesJSON
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	*s = wire.stage()
	return nil
}

func (s *StageUpdate) UnmarshalJSON(raw []byte) error {
	var wire struct {
		stageAttributesJSON
		Revision uint64 `json:"revision"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	*s = StageUpdate{Stage: wire.stage(), Revision: wire.Revision}
	return nil
}
