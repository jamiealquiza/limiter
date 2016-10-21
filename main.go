package limiter

import (
	"sync"
	"time"
)

// TokenBucket uses a buffered channel as
// a bucket, where the depth equals the channel
// buffer size.
type TokenBucket struct {
	tokens chan int
	refill *time.Ticker
}

// TokenBucketConfig holds TokenBucket parameters.
type TokenBucketConfig struct {
	RefillRate int
	Capacity int
}

// Limiter uses a soft/hard threshold
// with a static (TODO exponential) timeout.
type Limiter struct {
	sync.Mutex
	counts    map[int64]int64
	hardLimit int64
	softLimit int64
}

// LimiterConfig holds Limiter parameters.
type LimiterConfig struct {
	HardLimit int64
	SoftLimit int64
	GcInt     int
}

// NewTokenBucket takes a *TokenBucketConfig
// and returns a *TokenBucket.
func NewTokenBucket(config *TokenBucketConfig) *TokenBucket {
	tokenBucket := &TokenBucket{
		tokens: make(chan int, config.Capacity),
		refill: time.NewTicker(time.Second*time.Duration(config.RefillRate)),
	}

	go bucketRefiller(tokenBucket)

	return tokenBucket
}

// bucketRefiller empties a TokenBucket.tokens
// channel at the TokenBucketConfig.RefillRate.
func bucketRefiller(tokenBucket *TokenBucket) {
	for _ = range tokenBucket.refill.C {
		<- tokenBucket.tokens
	}
}

// GetToken simulates a token request by
// attempting a send on a buffered channel,
// which will block when full.
func (tokenBucket *TokenBucket) GetToken() {
	tokenBucket.tokens <- 0
}

// NewLimiter takes a *Config and returns a *Limiter.
func NewLimiter(config *LimiterConfig) *Limiter {
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
