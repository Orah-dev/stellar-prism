package sink

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// StatusBar renders a compact live stream summary to diagnostics while
// forwarding records unchanged. Disabled bars are transparent.
type StatusBar struct {
	next        stream.Sink
	diagnostics io.Writer
	enabled     bool
	now         func() time.Time
	mu          sync.Mutex
	started     time.Time
	lastLedger  time.Time
	events      uint64
	ledger      uint32
}

// StatusBarOptions configures a StatusBar.
type StatusBarOptions struct {
	Enabled bool
	Now     func() time.Time
}

// NewStatusBar wraps next. The status line is suppressed unless Enabled.
func NewStatusBar(next stream.Sink, diagnostics io.Writer, opts StatusBarOptions) *StatusBar {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &StatusBar{next: next, diagnostics: diagnostics, enabled: opts.Enabled, now: now, started: now()}
}

// Emit forwards r and updates the display after a successful delivery.
func (s *StatusBar) Emit(r stream.Record) error {
	if err := s.next.Emit(r); err != nil {
		return err
	}
	if !s.enabled {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if r.Kind == stream.KindEvent {
		s.events++
	}
	block := time.Duration(0)
	if r.Kind == stream.KindLedger && !s.lastLedger.IsZero() {
		block = r.Time.Sub(s.lastLedger)
	}
	if r.Kind == stream.KindLedger {
		s.lastLedger, s.ledger = r.Time, r.Sequence
	}
	elapsed := now.Sub(s.started).Seconds()
	rate := float64(s.events)
	if elapsed > 0 {
		rate /= elapsed
	}
	lag := now.Sub(r.Time)
	if lag < 0 {
		lag = 0
	}
	_, _ = fmt.Fprintf(s.diagnostics, "\rLedger %d | Block Time: %s | Ingest Lag: %s | Events/sec: %.1f", s.ledger, block.Round(100*time.Millisecond), lag.Round(time.Second), rate)
	return nil
}

// Flush forwards Flush and terminates the status line when active.
func (s *StatusBar) Flush() error {
	if s.enabled {
		_, _ = fmt.Fprintln(s.diagnostics)
	}
	return s.next.Flush()
}
