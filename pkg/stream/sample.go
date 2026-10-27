package stream

import (
	"context"
	"hash/fnv"
	"sync"
	"time"
)

// Sampler implements deterministic hash-based sampling using Record.ID.
type Sampler struct {
	rate float64 // 0.0 to 1.0
}

func NewSampler(rate float64) *Sampler {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	return &Sampler{rate: rate}
}

func (s *Sampler) Match(r Record) (bool, error) {
	if s.rate >= 1 {
		return true, nil
	}
	if s.rate <= 0 {
		return false, nil
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(r.ID))
	v := float64(h.Sum64()) / float64(^uint64(0))
	return v < s.rate, nil
}

func (s *Sampler) Rate() float64 { return s.rate }

// RateLimiter implements a token bucket rate limiter.
type RateLimiter struct {
	rate     float64 // tokens per second
	burst    int     // max tokens
	mu       sync.Mutex
	tokens   float64
	lastFill time.Time
}

func NewRateLimiter(rate float64) *RateLimiter {
	if rate <= 0 {
		return &RateLimiter{rate: 0}
	}
	return &RateLimiter{
		rate:     rate,
		burst:    int(rate) + 1, // allow burst of ~1 second
		tokens:   float64(int(rate) + 1),
		lastFill: time.Now(),
	}
}

func (r *RateLimiter) Allow() bool {
	if r.rate <= 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(r.lastFill).Seconds()
	r.tokens += elapsed * r.rate
	if r.tokens > float64(r.burst) {
		r.tokens = float64(r.burst)
	}
	r.lastFill = now
	if r.tokens >= 1 {
		r.tokens--
		return true
	}
	return false
}

func (r *RateLimiter) Wait(ctx interface{}) error {
	// Not used in current implementation; Allow() is used for non-blocking check.
	return nil
}

// SampleFilter wraps a Source to apply sampling and rate limiting.
type SampleFilter struct {
	sampler     *Sampler
	rateLimiter *RateLimiter
	source      Source
	sampled     int
	rateLimited int
}

func NewSampleFilter(source Source, sampleRate float64, maxRate float64) *SampleFilter {
	var sampler *Sampler
	if sampleRate > 0 {
		sampler = NewSampler(sampleRate)
	}
	var rateLimiter *RateLimiter
	if maxRate > 0 {
		rateLimiter = NewRateLimiter(maxRate)
	}
	return &SampleFilter{
		source:      source,
		sampler:     sampler,
		rateLimiter: rateLimiter,
	}
}

func (f *SampleFilter) Name() string {
	return f.source.Name() + " (sampled)"
}

func (f *SampleFilter) Fetch(ctx context.Context, cursor string) (Batch, error) {
	batch, err := f.source.Fetch(ctx, cursor)
	if err != nil {
		return batch, err
	}
	if f.sampler == nil && f.rateLimiter == nil {
		return batch, nil
	}
	filtered := make([]Record, 0, len(batch.Records))
	for _, r := range batch.Records {
		keep := true
		if f.sampler != nil {
			matched, err := f.sampler.Match(r)
			if err != nil {
				return Batch{}, err
			}
			if !matched {
				f.sampled++
				keep = false
			}
		}
		if keep && f.rateLimiter != nil {
			if !f.rateLimiter.Allow() {
				f.rateLimited++
				keep = false
			}
		}
		if keep {
			filtered = append(filtered, r)
		}
	}
	batch.Records = filtered
	return batch, nil
}

func (f *SampleFilter) CursorLedger(cursor string) (uint32, bool) {
	return f.source.CursorLedger(cursor)
}

func (f *SampleFilter) Sampled() int     { return f.sampled }
func (f *SampleFilter) RateLimited() int { return f.rateLimited }
