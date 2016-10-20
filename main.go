package limiter

import (
	"sync"
	"time"
)

// Limiter holds time bucket
// event counts and rate tresholds.
type Limiter struct {
	sync.Mutex
	counts    map[int64]int64
	hardLimit int64
	softLimit int64
}

// Config parameters.
type Config struct {
	HardLimit int64
	SoftLimit int64
	GcInt     int
}

// NewLimiter takes a *Config and returns a *Limiter.
func NewLimiter(config *Config) *Limiter {
	limiter := &Limiter{
		counts:    make(map[int64]int64),
		hardLimit: config.HardLimit,
		softLimit: config.SoftLimit,
	}

	// Are we running time bucket gc
	// on the Limiter?
	if config.GcInt != 0 {
		go intervalGc(limiter, time.Duration(config.GcInt)*time.Second)
	}

	return limiter
}

// IncrCount increments the count in the appropriate time
// bucket by i and returns a single integer referencing whether
// or not any limits were met.
// 0: No limits were hit.
// 1: The soft limit was hit.
// 2: The hard limit was hit.
func (limiter *Limiter) IncrCount(i int64) int {
	limiter.Lock()
	defer limiter.Unlock()

	now := time.Now().Unix()
	limiter.counts[now] += i

	switch count := limiter.counts[now]; {
	case count >= limiter.hardLimit:
		return 2
	case count >= limiter.softLimit:
		return 1
	default:
		return 0
	}
}

// Incr increments the count in the appropriate time
// bucket by 1 and returns a single integer referencing whether
// or not any limits were met.
func (limiter *Limiter) Incr() int {
	limiter.Lock()
	defer limiter.Unlock()

	now := time.Now().Unix()
	limiter.counts[now]++

	switch count := limiter.counts[now]; {
	case count >= limiter.hardLimit:
		return 2
	case count >= limiter.softLimit:
		return 1
	default:
		return 0
	}
}

// gc locks the Limiter and traverses all time bucket
// keys older than now minus a time.Duration p.
func (limiter *Limiter) gc(p time.Duration) {
	limiter.Lock()

	// cutoff is a timestamp in the past;
	// now minus the period p.
	cutoff := time.Now().Add(-p)
	for k, _ := range limiter.counts {
		keyTimeStamp := time.Unix(k, 0)
		if keyTimeStamp.Before(cutoff) {
			delete(limiter.counts, k)
		}
	}

	limiter.Unlock()
}

// intervalGc calls gc() on a time interval.
func intervalGc(limiter *Limiter, interval time.Duration) {
	loop := time.NewTicker(interval)

	for _ = range loop.C {
		limiter.gc(5 * time.Second)
	}
}
