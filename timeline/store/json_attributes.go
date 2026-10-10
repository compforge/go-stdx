package store

import (
	"encoding/json"

	"github.com/compforge/go-stdx/timeline/model"
)

func (s *StageUpdate) UnmarshalJSON(raw []byte) error {
	var wire struct {
		model.StageAttributesJSON
		Revision uint64 `json:"revision"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	*s = StageUpdate{Stage: wire.Stage(), Revision: wire.Revision}
	return nil
}
