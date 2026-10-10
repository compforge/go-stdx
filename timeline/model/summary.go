package model

import (
	"fmt"
	"strings"
)

// Summary returns a human-readable plain-text view of the operation and its
// stages in snapshot order. It omits IDs, actors and arbitrary attributes. It is a
// display format, not a round-trip serialization or a machine-readable contract.
// Durations use the snapshot capture time, so repeated calls are deterministic.
// Operation status and collection outcomes remain independent of stage results.
func (s Snapshot) Summary() string {
	var out strings.Builder
	fmt.Fprintf(&out, "%s: %s (%s)", s.Operation, s.Status, s.Duration())
	if s.Error != "" {
		fmt.Fprintf(&out, ": %s", s.Error)
	}
	for _, stage := range s.Stages {
		fmt.Fprintf(&out, "\n  %s: %s (%s)", stage.Name, stage.Status, stage.Duration(s.CapturedAt))
		if stage.Error != "" {
			fmt.Fprintf(&out, ": %s", stage.Error)
		}
	}
	fmt.Fprintf(&out, "\ncollection: local_flushed=%t, store_read=%t", s.Collection.LocalFlushed, s.Collection.StoreRead)
	return out.String()
}
