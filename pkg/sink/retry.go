package sink

import (
	"math/rand/v2"
	"time"
)

// backoffDelay returns the pause before retry number attempt (1-based):
// initial doubled once per previous attempt, capped at max, optionally
// jittered by +/- jitter (a fraction, so 0.2 means +/-20%).
//
// Jitter keeps many streams that failed together from retrying in lockstep
// against an endpoint that is already struggling.
func backoffDelay(initial, maxDelay time.Duration, attempt int, jitter float64) time.Duration {
	delay := initial
	for i := 1; i < attempt; i++ {
		if delay >= maxDelay/2 {
			delay = maxDelay
			break
		}
		delay *= 2
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	if jitter > 0 {
		factor := 1 + (rand.Float64()*2-1)*jitter
		delay = time.Duration(float64(delay) * factor)
	}
	if delay < 0 {
		return 0
	}
	return delay
}
