package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// heartbeat prints a line on stderr whenever a follow stream waits an
// interval without delivering a record, so a quiet contract is
// distinguishable from a hung process.
//
// The reporter is a plain value with no goroutine and no timer: the
// heartbeat clock below slices the tailer's own waits and beats between
// them, and OnPage marks delivered pages, so the beat runs on the tailer's
// goroutine and there is nothing of its own to clean up at termination.
// That is deliberate — a liveness signal must not become part of what has
// to be kept alive.
//
// Every method is nil-safe, and newHeartbeat returns nil when --heartbeat
// was not passed, so the wiring in runStream reads the same either way.
type heartbeat struct {
	// out is where the line goes: the run's serialised stderr.
	out io.Writer
	// interval is how long the stream may wait in silence before a line
	// is printed.
	interval time.Duration
	// idle counts the time waited since a record was last delivered.
	idle time.Duration
}

// newHeartbeat starts a reporter, or returns nil when heartbeats were not
// asked for or the interval was nonsense.
func newHeartbeat(interval time.Duration, out io.Writer) *heartbeat {
	if interval <= 0 {
		return nil
	}
	return &heartbeat{out: out, interval: interval}
}

// mark records that a record was delivered, restarting the idle window.
//
// It is wired to OnPage: a page was emitted in full, so the stream is not
// idle however long its next poll takes.
func (h *heartbeat) mark() {
	if h == nil {
		return
	}
	h.idle = 0
}

// slept accounts for a stretch of waiting, printing one line per interval
// it crosses. The remainder carries over, so consecutive short waits beat
// at their cumulative total, and a wait of several intervals beats once per
// interval rather than once at the end.
func (h *heartbeat) slept(d time.Duration) {
	if h == nil || d <= 0 {
		return
	}
	h.idle += d
	if h.idle < h.interval {
		return
	}
	idle := h.idle
	h.idle = 0
	fmt.Fprintf(h.out, "prism: heartbeat: idle for %s with no records\n", idle)
}

// heartbeatClock wraps the tailer's Clock so that each wait is sliced at
// the heartbeat interval.
//
// Slicing is the point: a line printed only after the wait ends would say
// nothing about a process stuck inside one. The wrapped clock's semantics
// are otherwise preserved — cancellation still interrupts a slice, and a
// non-positive wait is passed through untouched so it reports cancellation
// exactly as the clock underneath would.
type heartbeatClock struct {
	clock stream.Clock
	beat  *heartbeat
}

// Now implements stream.Clock.
func (c *heartbeatClock) Now() time.Time { return c.clock.Now() }

// Sleep implements stream.Clock, beating at every interval boundary of the
// wait.
func (c *heartbeatClock) Sleep(ctx context.Context, d time.Duration) error {
	if c.beat == nil || d <= 0 {
		return c.clock.Sleep(ctx, d)
	}
	remaining := d
	for remaining > 0 {
		slice := remaining
		if slice > c.beat.interval {
			slice = c.beat.interval
		}
		if err := c.clock.Sleep(ctx, slice); err != nil {
			return err
		}
		remaining -= slice
		c.beat.slept(slice)
	}
	return nil
}
