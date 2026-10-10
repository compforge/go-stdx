package manager

import (
	"context"
)

// Read returns the current cached view, loading Store on a miss. It does not save
// pending facts or refresh a warm cache from another process.
func (m *Manager) Read(ctx context.Context, id string) (Snapshot, error) {
	return m.cache.Read(ctx, id)
}

// Evict releases a cached document and schedules a final best-effort save.
// NoopStore cannot recover an evicted document.
func (m *Manager) Evict(id string) { m.cache.Evict(id) }

// Flush optionally triggers or waits for persistence. Long-running applications
// normally do not need to call it: Manager saves pending facts in the background.
// Use Flush for an explicit persistence checkpoint or to request an earlier save;
// use Shutdown to drain pending facts when stopping the application.
//
// Flush waits for the ID's pending Store submissions when wait is true. With
// wait=false it coalesces a wakeup of the shared save worker and returns immediately;
// save failures are reported through Config.OnError. The worker may save other IDs.
// +spec=`Nonblocking Flush uses the Manager lifetime and IO timeout independently of the caller context.`
func (m *Manager) Flush(ctx context.Context, id string, wait bool) error {
	return m.cache.Flush(ctx, id, wait)
}

// Shutdown stops recording and tries to save remaining facts with the caller's
// context. The application owns the Store connection lifetime.
func (m *Manager) Shutdown(ctx context.Context) error { return m.cache.Shutdown(ctx) }

// Stats reports cache residency, pending submissions and known rejected/lost saves.
func (m *Manager) Stats() Stats { return m.cache.Stats() }
