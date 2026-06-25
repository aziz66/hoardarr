package torbox

import (
	"sync"
	"time"
)

// createRateLimiter is a non-blocking rolling-window limiter for TorBox's per-token
// create-endpoint limits: createtorrent and createusenetdownload are each 60/hour. The
// regular per-minute API limiter + retryablehttp can't absorb an hour-long window, and
// the *arr add is synchronous (blocking would time it out), so when the hourly budget is
// spent we let the add fail FAST with a clear "retry later" error. The *arr then re-queues
// the grab over time, which paces a bulk re-acquisition under the limit instead of
// hammering TorBox into a 429 storm.
type createRateLimiter struct {
	mu     sync.Mutex
	window time.Duration
	max    int
	times  []time.Time
}

func newCreateRateLimiter(max int, window time.Duration) *createRateLimiter {
	return &createRateLimiter{max: max, window: window}
}

// Allow records and permits a create when fewer than max have occurred within the rolling
// window; otherwise it returns false without recording.
func (l *createRateLimiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.evict(now)
	if len(l.times) >= l.max {
		return false
	}
	l.times = append(l.times, now)
	return true
}

// RetryAfter reports how long until the oldest in-window create ages out (when a slot
// frees up). Zero when a slot is already available.
func (l *createRateLimiter) RetryAfter() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.evict(time.Now())
	if len(l.times) < l.max {
		return 0
	}
	return time.Until(l.times[0].Add(l.window))
}

func (l *createRateLimiter) evict(now time.Time) {
	cutoff := now.Add(-l.window)
	i := 0
	for i < len(l.times) && l.times[i].Before(cutoff) {
		i++
	}
	l.times = l.times[i:]
}
