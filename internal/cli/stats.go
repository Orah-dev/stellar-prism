package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// runStats accumulates what a stream run did, so --stats can report it when
// the command exits.
//
// The reporter is a plain value with no goroutine and no timer of its own:
// phases fold their stream.Stats in as they finish, and one deferred call
// prints the line. That is deliberate — a summary of the run must not become
// part of what has to be cleaned up at termination.
//
// Every method is nil-safe, and newRunStats returns nil when --stats was not
// passed, so the wiring in runStream reads the same either way.
type runStats struct {
	// now reads the clock so tests can pin the elapsed time.
	now func() time.Time
	// startedAt is when the reporter was created; elapsed time is measured
	// from there, so it covers the whole run including setup.
	startedAt time.Time
	// streaming marks that the first phase has started fetching. Until
	// then there is no run to summarise, and a failure during setup
	// should report only its own error.
	streaming bool

	// processed counts records handed to the sinks, summed over phases.
	processed int
	// duplicates counts records dropped as duplicates: the tailer's
	// high-water mark plus the --dedup-window ring.
	duplicates int
	// retries counts fetches that failed and were retried.
	retries int
	// sampled counts records dropped by deterministic sampling.
	sampled int
	// rateLimited counts records dropped by the token bucket rate limiter.
	rateLimited int
}

// newRunStats starts a reporter for a run, or returns nil when --stats was
// not requested.
func newRunStats(enabled bool) *runStats {
	if !enabled {
		return nil
	}
	now := time.Now
	return &runStats{now: now, startedAt: now()}
}

// start marks the point where the run begins fetching, arming the report.
//
// It is called once the start ledger is resolved and the first phase is
// about to run, so a failure earlier — an unresolvable head, a sink that
// cannot be built — exits without printing a summary of a run that never
// streamed.
func (s *runStats) start() {
	if s == nil {
		return
	}
	s.streaming = true
}

// add folds one phase's stats into the totals. The concurrent backfill and
// the follow tailer each report their own, and a run that spans both is one
// run to the reader of the summary.
func (s *runStats) add(stats stream.Stats) {
	if s == nil {
		return
	}
	s.processed += stats.Emitted
	s.duplicates += stats.Duplicates
	s.retries += stats.Retries
	s.sampled += stats.Sampled
	s.rateLimited += stats.RateLimited
}

// addDuplicates folds in duplicates dropped by the --dedup-window ring.
//
// The ring suppresses a record at the output seam, after the tailer counted
// it as emitted, so the tailer's counters cannot see it — only the dedup
// sink knows that number.
func (s *runStats) addDuplicates(n int) {
	if s == nil {
		return
	}
	s.duplicates += n
}

// addSampled adds records dropped by deterministic sampling.
func (s *runStats) addSampled(n int) {
	if s == nil {
		return
	}
	s.sampled += n
}

// addRateLimited adds records dropped by the token bucket rate limiter.
func (s *runStats) addRateLimited(n int) {
	if s == nil {
		return
	}
	s.rateLimited += n
}

// dedupDropped reports how many records the --dedup-window ring has
// suppressed so far, or zero when the run has no ring.
//
// The count is read from live sink state, so the caller must only read it
// while no goroutine is still emitting through the sink: runStream takes
// its snapshot before the drain goroutine is started, or after a drain that
// completed and was joined, and never after a drain abandoned to its
// goroutine.
func dedupDropped(dedup *stream.DedupSink) int {
	if dedup == nil {
		return 0
	}
	return dedup.Dropped()
}

// report prints the one-line summary on w.
//
// It is a deferred call at the end of runStream, so it runs after the drain,
// the cursor commit and the sink close: the numbers describe what actually
// reached the sinks. It is silent when --stats was not passed, and when the
// run failed before it started fetching.
func (s *runStats) report(w io.Writer) {
	if s == nil || !s.streaming {
		return
	}
	elapsed := s.now().Sub(s.startedAt).Round(time.Millisecond)
	fmt.Fprintf(w, "prism: stats: processed=%d duplicates=%d retries=%d sampled=%d rateLimited=%d elapsed=%s\n",
		s.processed, s.duplicates, s.retries, s.sampled, s.rateLimited, elapsed)
}
