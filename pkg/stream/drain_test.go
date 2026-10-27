package stream_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// flushSink records what it is given and can be made to block on flush, so
// the drain's timeout can be exercised against a sink that never answers.
type flushSink struct {
	mu  sync.Mutex
	ids []string
	// flushes counts the flushes the sink was asked for.
	flushes atomic.Int64
	// block, when set, holds every flush until it is closed.
	block chan struct{}
}

func (s *flushSink) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = append(s.ids, r.ID)
	return nil
}

func (s *flushSink) Flush() error {
	s.flushes.Add(1)
	if s.block != nil {
		<-s.block
	}
	return nil
}

func (s *flushSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ids)
}

func (s *flushSink) flushCount() int64 { return s.flushes.Load() }

// newDrainSource returns an event source over a small fixture.
func newDrainSource(t *testing.T) stream.Source {
	t.Helper()
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 5), fakerpc.WithPageLimit(10))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}
	return src
}

// TestTailerDrainFlushesTheSink covers the ordinary drain: the sink is
// flushed and the drain reports success.
func TestTailerDrainFlushesTheSink(t *testing.T) {
	t.Parallel()

	sink := &flushSink{}
	tailer := stream.NewTailer(newDrainSource(t), sink, stream.Options{Clock: newFakeClock()})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	before := sink.flushCount()
	if err := tailer.Drain(context.Background(), time.Second); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	if sink.flushCount() <= before {
		t.Error("Drain() did not flush the sink")
	}
}

// TestTailerDrainRespectsTheLimit checks that the drain does not push the
// stream past the number of records that were asked for: a request for N
// records must not quietly become N plus whatever the run was holding.
func TestTailerDrainRespectsTheLimit(t *testing.T) {
	t.Parallel()

	sink := &flushSink{}
	tailer := stream.NewTailer(newDrainSource(t), sink, stream.Options{
		Limit: 2,
		Clock: newFakeClock(),
	})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if err := tailer.Drain(context.Background(), time.Second); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	if got := sink.count(); got != 2 {
		t.Errorf("the drain emitted %d records, want the 2 the limit asked for", got)
	}
}

// TestTailerDrainTimesOutOnAStuckSink covers the reason the drain takes a
// timeout at all: a sink that never answers must not hold the shutdown
// open for ever.
func TestTailerDrainTimesOutOnAStuckSink(t *testing.T) {
	t.Parallel()

	sink := &flushSink{block: make(chan struct{})}
	tailer := stream.NewTailer(newDrainSource(t), sink, stream.Options{Clock: newFakeClock()})

	start := time.Now()
	err := tailer.Drain(context.Background(), 50*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Drain() error = nil, want the timeout to be reported")
	}
	if !strings.Contains(err.Error(), "draining") {
		t.Errorf("Drain() error = %q, want it to say the drain timed out", err.Error())
	}
	if elapsed > 10*time.Second {
		t.Errorf("Drain() took %s, want it to give up at its deadline", elapsed)
	}
}

// TestTailerDrainTimeoutBoundsTheFinalFlush covers the same guarantee for
// the flush the run itself performs when it stops: with a drain timeout
// configured, a sink that never answers cannot hold the run open.
func TestTailerDrainTimeoutBoundsTheFinalFlush(t *testing.T) {
	t.Parallel()

	sink := &flushSink{block: make(chan struct{})}
	tailer := stream.NewTailer(newDrainSource(t), sink, stream.Options{
		Clock:        newFakeClock(),
		DrainTimeout: 50 * time.Millisecond,
	})

	start := time.Now()
	err := tailer.Run(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Run() error = nil, want the failed flush to be reported")
	}
	if !strings.Contains(err.Error(), "flushing output") {
		t.Errorf("Run() error = %q, want it to report the failed flush", err.Error())
	}
	if elapsed > 10*time.Second {
		t.Errorf("Run() took %s, want it to give up at the drain timeout", elapsed)
	}
}

// TestTailerDrainCommitsTheFinalCursor checks that the drain reports the
// pages that were delivered, which is what commits the final cursor.
func TestTailerDrainCommitsTheFinalCursor(t *testing.T) {
	t.Parallel()

	sink := &flushSink{}
	var pages []stream.Page
	tailer := stream.NewTailer(newDrainSource(t), sink, stream.Options{
		Clock: newFakeClock(),
		OnPage: func(page stream.Page) {
			pages = append(pages, page)
		},
	})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := tailer.Drain(context.Background(), time.Second); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	if len(pages) == 0 {
		t.Fatal("no page was reported, so the final cursor was never committed")
	}
	last := pages[len(pages)-1]
	if last.Cursor == "" {
		t.Error("the last reported page carries no cursor")
	}
	if tailer.Cursor() != last.Cursor {
		t.Errorf("Cursor() = %q, want the last reported cursor %q",
			tailer.Cursor(), last.Cursor)
	}
}
