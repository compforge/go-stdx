package model

import "encoding/json"

// Decode legacy attributes at the wire boundary only. Aliases keep standalone
// stages and update revisions intact without promoting Stage.UnmarshalJSON to
// the whole StageUpdate. Encoding always uses the public attributes key.
type stageData Stage
type StageAttributesJSON struct {
	stageData
	LegacyAttributes map[string]json.RawMessage `json:"fields"`
}

func (s StageAttributesJSON) Stage() Stage {
	result := Stage(s.stageData)
	if result.Attributes == nil {
		result.Attributes = s.LegacyAttributes
	}
	return result
}

func (s *Stage) UnmarshalJSON(raw []byte) error {
	var wire StageAttributesJSON
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	*s = wire.Stage()
	return nil
}
