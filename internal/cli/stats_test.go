package cli

import (
	"bytes"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// TestRunStatsDisabledIsSilent covers the flag being off: the reporter is
// nil, every method is a no-op, and nothing is printed.
func TestRunStatsDisabledIsSilent(t *testing.T) {
	var buf bytes.Buffer
	summary := newRunStats(false)
	if summary != nil {
		t.Fatalf("newRunStats(false) = %v, want nil", summary)
	}

	// Every method must be nil-safe, so the wiring in runStream does not
	// branch on whether --stats was passed.
	summary.start()
	summary.add(stream.Stats{Emitted: 5, Duplicates: 1, Retries: 2})
	summary.addDuplicates(3)
	summary.report(&buf)

	if buf.Len() != 0 {
		t.Errorf("report() wrote %q, want nothing", buf.String())
	}
}

// TestRunStatsReportLine pins the clock and checks the exact line: totals
// summed over phases, elapsed measured from the start of the run and
// rounded to milliseconds.
func TestRunStatsReportLine(t *testing.T) {
	summary := newRunStats(true)
	base := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	summary.startedAt = base
	summary.now = func() time.Time { return base.Add(1500 * time.Millisecond) }

	summary.start()
	summary.add(stream.Stats{Emitted: 10, Duplicates: 2, Retries: 1})
	summary.add(stream.Stats{Emitted: 5, Duplicates: 1})
	summary.addDuplicates(3)

	var buf bytes.Buffer
	summary.report(&buf)

	const want = "prism: stats: processed=15 duplicates=6 retries=1 sampled=0 rateLimited=0 elapsed=1.5s\n"
	if got := buf.String(); got != want {
		t.Errorf("report() = %q, want %q", got, want)
	}
}

// TestRunStatsSilentUntilStreaming covers a run that failed before it
// started fetching: the error stands alone, with no summary of a run that
// never streamed.
func TestRunStatsSilentUntilStreaming(t *testing.T) {
	summary := newRunStats(true)
	base := time.Now()
	summary.startedAt = base
	summary.now = func() time.Time { return base }

	var buf bytes.Buffer
	summary.report(&buf)

	if buf.Len() != 0 {
		t.Errorf("report() wrote %q before start(), want nothing", buf.String())
	}
}

// TestDedupDroppedCountsRingSuppressions checks the third number's source:
// the --dedup-window ring counts its own suppressions, and that count folds
// into the summary through dedupDropped. A nil ring reads as zero.
func TestDedupDroppedCountsRingSuppressions(t *testing.T) {
	if got := dedupDropped(nil); got != 0 {
		t.Errorf("dedupDropped(nil) = %d, want 0", got)
	}

	ring := stream.NewDedupSink(stream.SinkFunc(func(stream.Record) error { return nil }), 4)
	rec := stream.Record{ID: "0000000001-0001-0001", Kind: stream.KindEvent}
	if err := ring.Emit(rec); err != nil {
		t.Fatalf("first Emit() error = %v", err)
	}
	if err := ring.Emit(rec); err != nil {
		t.Fatalf("second Emit() error = %v", err)
	}
	if got := dedupDropped(ring); got != 1 {
		t.Fatalf("dedupDropped() = %d, want 1", got)
	}

	summary := newRunStats(true)
	base := time.Now()
	summary.startedAt = base
	summary.now = func() time.Time { return base }
	summary.start()
	summary.addDuplicates(dedupDropped(ring))

	var buf bytes.Buffer
	summary.report(&buf)
	const want = "prism: stats: processed=0 duplicates=1 retries=0 sampled=0 rateLimited=0 elapsed=0s\n"
	if got := buf.String(); got != want {
		t.Errorf("report() = %q, want %q", got, want)
	}
}

// TestEventsCommandStatsReportsOnExit is the feature's contract: with
// --stats, exactly one summary line lands on stderr as the command exits,
// and the record stream on stdout is untouched.
func TestEventsCommandStatsReportsOnExit(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 4), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, stderr, err := run(t, "events", "--from", "1000", "--json", "--stats")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	const prefix = "prism: stats: processed=4 duplicates=0 retries=0 sampled=0 rateLimited=0 elapsed="
	if !strings.Contains(stderr, prefix) {
		t.Errorf("stderr does not contain %q\n%s", prefix, stderr)
	}
	if n := strings.Count(stderr, "prism: stats:"); n != 1 {
		t.Errorf("stderr holds %d summary lines, want exactly 1\n%s", n, stderr)
	}
	if strings.Contains(stdout, "prism: stats:") {
		t.Errorf("the summary leaked into stdout\n%s", stdout)
	}
	if lines := nonEmptyLines(stdout); len(lines) != 4 {
		t.Errorf("stdout has %d records, want 4\n%s", len(lines), stdout)
	}
}

// TestEventsCommandStatsCountsDuplicates scripts a server that re-sends the
// page it just served — the replay the tailer's high-water mark suppresses —
// and checks both totals in the summary.
func TestEventsCommandStatsCountsDuplicates(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 4),
		fakerpc.WithPageLimit(2),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {ReplayLast: true},
		}),
	)
	withFake(t, srv)

	stdout, stderr, err := run(t, "events", "--from", "1000", "--json",
		"--stats", "--page-size", "2")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	const prefix = "prism: stats: processed=4 duplicates=1 retries=0 sampled=0 rateLimited=0 elapsed="
	if !strings.Contains(stderr, prefix) {
		t.Errorf("stderr does not contain %q\n%s", prefix, stderr)
	}
	if lines := nonEmptyLines(stdout); len(lines) != 4 {
		t.Errorf("stdout has %d records, want 4 (a replay must not reach it)\n%s",
			len(lines), stdout)
	}
}

// TestEventsCommandStatsCountsRetries scripts one dropped connection and
// checks that the retried fetch shows up in the summary.
func TestEventsCommandStatsCountsRetries(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 4),
		fakerpc.WithPageLimit(10),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			0: {Err: fakerpc.ErrDropped},
		}),
	)
	withFake(t, srv)

	_, stderr, err := run(t, "events", "--from", "1000", "--json", "--stats")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	const prefix = "prism: stats: processed=4 duplicates=0 retries=1 sampled=0 rateLimited=0 elapsed="
	if !strings.Contains(stderr, prefix) {
		t.Errorf("stderr does not contain %q\n%s", prefix, stderr)
	}
}

// TestEventsCommandStatsAfterBackfill covers a run whose totals come from
// the concurrent backfill workers rather than from the follow tailer, and
// whose report runs on the early return path.
func TestEventsCommandStatsAfterBackfill(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(9000, 30), fakerpc.WithPageLimit(5))
	withFake(t, srv)

	stdout, stderr, err := run(t, "events", "--from", "9000", "--json",
		"--page-size", "5", "--backfill-workers", "3", "--stats")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	const prefix = "prism: stats: processed=30 duplicates=0 retries=0 sampled=0 rateLimited=0 elapsed="
	if !strings.Contains(stderr, prefix) {
		t.Errorf("stderr does not contain %q\n%s", prefix, stderr)
	}
	if lines := nonEmptyLines(stdout); len(lines) != 30 {
		t.Errorf("stdout has %d records, want 30\n%s", len(lines), stdout)
	}
}

// TestEventsCommandStatsOffByDefault guards backward compatibility: a run
// that did not ask for the summary prints nothing new.
func TestEventsCommandStatsOffByDefault(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 4), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	_, stderr, err := run(t, "events", "--from", "1000", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if strings.Contains(stderr, "prism: stats:") {
		t.Errorf("stderr holds a summary without --stats\n%s", stderr)
	}
}

// TestEventsCommandStatsSilentOnSetupFailure covers the other half of the
// contract: a run that failed before it started fetching reports only its
// error, not a summary of a run that never streamed.
func TestEventsCommandStatsSilentOnSetupFailure(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 2))
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "missing", "cursor.jsonl")
	_, stderr, err := run(t, "events", "--from", "100", "--json",
		"--stats", "--state-file", path)
	if err == nil {
		t.Fatal("Execute() error = nil, want a missing-directory failure")
	}
	if strings.Contains(stderr, "prism: stats:") {
		t.Errorf("stderr holds a summary for a run that never streamed\n%s", stderr)
	}
}

// TestEventsCommandStatsSurvivesDrainTimeout covers the termination hazard
// the summary has to respect: a drain that overruns --drain-timeout is
// abandoned to its goroutine, which may still be emitting while the summary
// prints. The summary must appear exactly once either way — from the
// pre-drain snapshot when the drain was abandoned, or from a fresh read
// after a drain that completed and was joined.
//
// stdout is discarded rather than buffered: the abandoned goroutine may
// still write to it after Execute returns, so this test must not read it.
func TestEventsCommandStatsSurvivesDrainTimeout(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 4), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	var stderr safeBuffer
	err := Execute([]string{
		"events", "--from", "1000", "--json", "--stats",
		"--drain-timeout", "1ns",
	}, io.Discard, &stderr)

	// Either the drain lost the race and was abandoned ("timed out"), or it
	// completed with its own deadline already expired ("context deadline
	// exceeded"), or it beat both timers. All are the drain reporting its
	// own bound; anything else is a run failure the summary should not be
	// masking.
	if err != nil &&
		!strings.Contains(err.Error(), "timed out") &&
		!strings.Contains(err.Error(), "deadline exceeded") {
		t.Errorf("Execute() error = %v, want a drain-timeout error", err)
	}

	text := stderr.String()
	const prefix = "prism: stats: processed=4 duplicates=0 retries=0 sampled=0 rateLimited=0 elapsed="
	if !strings.Contains(text, prefix) {
		t.Errorf("stderr does not contain %q\n%s", prefix, text)
	}
	if n := strings.Count(text, "prism: stats:"); n != 1 {
		t.Errorf("stderr holds %d summary lines, want exactly 1\n%s", n, text)
	}
}

// TestEventsCommandStatsOnInterrupt covers the summary on the signal path:
// Ctrl-C stops the fetching, the drain flushes what is left, and the
// deferred report prints the totals as the command exits.
func TestEventsCommandStatsOnInterrupt(t *testing.T) {
	// The signal is delivered to this process, so its disposition is
	// claimed before anything else happens: without a handler the runtime
	// would terminate the test binary.
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, syscall.SIGTERM)
	defer signal.Stop(interrupts)

	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout := &safeBuffer{}
	stderr := &safeBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- Execute([]string{
			"events",
			"--from", "1000",
			"--json",
			"--follow",
			"--poll", "10ms",
			"--stats",
		}, stdout, stderr)
	}()

	// The first record proves the command is running, and therefore that
	// its signal handler is already in place.
	waitForOutput(t, stdout, `"ledger":1000`)

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("Kill() error = %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute() error = %v, want a clean shutdown", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the command did not stop after the signal")
	}

	// Two records were ready when the signal landed and the third was
	// still in the reorder buffer: the drain flushed it, so all three
	// were processed.
	const prefix = "prism: stats: processed=3 duplicates=0 retries=0 sampled=0 rateLimited=0 elapsed="
	if text := stderr.String(); !strings.Contains(text, prefix) {
		t.Errorf("stderr does not contain %q\n%s", prefix, text)
	}
	if text := stdout.String(); strings.Contains(text, "prism: stats:") {
		t.Errorf("the summary leaked into stdout\n%s", text)
	}
}

// TestEventsHelpAdvertisesTheStatsFlag checks the flag is discoverable.
func TestEventsHelpAdvertisesTheStatsFlag(t *testing.T) {
	stdout, _, err := run(t, "events", "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, want := range []string{"--stats", "run summary on exit"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("help does not mention %q", want)
		}
	}
}
