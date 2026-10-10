package cache

import (
	"fmt"
	"sort"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// ttlcache.Get always promotes LRU, including WithDisableTouchOnHit. Use its
// detached Items view for maintenance reads so background polling stays cold.
func (c *Cache) peek(id string) *ttlcache.Item[string, *entry] {
	return c.items.Items()[id]
}
func (c *Cache) requestSave(id string) {
	select {
	case c.saveCh <- id:
	default:
	}
}
func (c *Cache) requestLoad(id string) {
	if c.background {
		select {
		case c.loadCh <- id:
		default:
		}
	}
}

// drainIDs bounds queue work too: duplicate producers cannot starve Latest or
// the periodic persistence scan by keeping a channel permanently nonempty.
func drainIDs(ch <-chan string, first string, limit int) []string {
	ids := make([]string, 0, limit)
	seen := make(map[string]bool)
	if first != "" {
		ids = append(ids, first)
		seen[first] = true
	}
	for n := len(ids); n < limit; n++ {
		select {
		case id := <-ch:
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		default:
			return ids
		}
	}
	return ids
}

// pace bounds back-to-back channel wakes without another public tuning knob.
// The first wake is immediate; subsequent batches are at least 10ms apart (or
// one configured interval, if shorter). Periodic scheduling is independent.
func (c *Cache) pace(last time.Time, interval time.Duration) bool {
	delay := time.Until(last.Add(min(interval, 10*time.Millisecond)))
	if delay <= 0 {
		return c.ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-c.ctx.Done():
		return false
	}
}

func (c *Cache) saveIDs(explicit []string, periodic bool) []string {
	var due []*entry
	if periodic {
		c.mu.Lock()
		for e := range c.pending {
			due = append(due, e)
		}
		sort.Slice(due, func(i, j int) bool { return due[i].scheduled < due[j].scheduled })
		c.mu.Unlock()
	}
	ids := make([]string, 0, c.config.BatchLimit)
	seen := make(map[string]bool)
	add := func(id string) {
		if !seen[id] && len(ids) < c.config.BatchLimit {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	// Reserve a periodic slot even under continuous explicit traffic. The other
	// slots prefer requested IDs; a limit of one gives the due scan this turn.
	if len(due) > 0 {
		add(due[0].id)
	}
	for _, id := range explicit {
		add(id)
	}
	for _, e := range due {
		add(e.id)
	}
	for _, id := range explicit {
		if !seen[id] {
			c.requestSave(id)
		}
	}
	c.mu.Lock()
	for e := range c.pending {
		if seen[e.id] {
			c.schedule++
			e.scheduled = c.schedule
		}
	}
	c.mu.Unlock()
	return ids
}
func (c *Cache) runSave() {
	defer close(c.done)
	timer := time.NewTicker(c.config.FlushInterval)
	defer timer.Stop()
	next := time.Now().Add(c.config.FlushInterval)
	var last time.Time
	for {
		first := ""
		periodic := false
		select {
		case <-c.ctx.Done():
			return
		case first = <-c.saveCh:
		case <-timer.C:
			periodic = true
		}
		if !c.pace(last, c.config.FlushInterval) {
			return
		}
		periodic = periodic || !time.Now().Before(next)
		if periodic {
			next = time.Now().Add(c.config.FlushInterval)
		}
		ids := c.saveIDs(drainIDs(c.saveCh, first, c.config.BatchLimit), periodic)
		for _, id := range ids {
			for _, e := range c.entries(id) {
				if c.ctx.Err() != nil {
					return
				}
				_ = c.save(c.ctx, e)
			}
		}
		last = time.Now()
	}
}

func (c *Cache) runLoad() {
	defer close(c.loadDone)
	ticker := time.NewTicker(c.config.LoadInterval)
	defer ticker.Stop()
	var after, last time.Time
	var mgetFailed, latestFailed bool
	for {
		first := ""
		select {
		case <-c.ctx.Done():
			return
		case first = <-c.loadCh:
		case <-ticker.C:
		}
		if !c.pace(last, c.config.LoadInterval) {
			return
		}
		ids := drainIDs(c.loadCh, first, c.config.BatchLimit)
		if len(ids) > 0 {
			err := c.refreshMany(c.ctx, ids)
			if err != nil && !mgetFailed && c.ctx.Err() == nil {
				c.config.OnError("", fmt.Errorf("timeline MGet: %w", err))
			}
			mgetFailed = err != nil
		}
		started := time.Now().UTC()
		err := c.refreshLatest(c.ctx, after)
		if err != nil && !latestFailed && c.ctx.Err() == nil {
			c.config.OnError("", fmt.Errorf("timeline Latest: %w", err))
		}
		latestFailed = err != nil
		if err == nil {
			after = started.Add(-c.config.LoadInterval)
		}
		last = time.Now()
	}
}
