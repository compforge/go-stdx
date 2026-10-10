package model

import (
	"fmt"
	"strings"
)

// Summary returns a human-readable plain-text view of the operation and its
// stages in snapshot order. Repeated logical IDs include actor labels; arbitrary
// attributes and stage IDs are omitted. It is a
// display format, not a round-trip serialization or a machine-readable contract.
// Durations use the snapshot capture time, so repeated calls are deterministic.
// Operation status and collection outcomes remain independent of stage results.
func (s Snapshot) Summary() string {
	var out strings.Builder
	fmt.Fprintf(&out, "%s: %s (%s)", s.Operation, s.Status, s.Duration())
	if s.Error != "" {
		fmt.Fprintf(&out, ": %s", s.Error)
	}
	counts := make(map[StageID]int)
	for _, stage := range s.Stages {
		counts[stage.ID]++
	}
	for _, stage := range s.Stages {
		name := stage.Name
		if counts[stage.ID] > 1 && stage.Actor.Key() != "" {
			name += " [" + stage.Actor.Key() + "]"
		}
		fmt.Fprintf(&out, "\n  %s: %s (%s)", name, stage.Status, stage.Duration(s.CapturedAt))
		if stage.Error != "" {
			fmt.Fprintf(&out, ": %s", stage.Error)
		}
	}
	fmt.Fprintf(&out, "\ncollection: local_flushed=%t, store_read=%t", s.Collection.LocalFlushed, s.Collection.StoreRead)
	return out.String()
}
