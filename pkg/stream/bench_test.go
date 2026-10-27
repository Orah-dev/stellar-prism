package stream_test

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

const (
	benchmarkEvents    = 50_000
	benchmarkPageSize  = 500
	benchmarkLedgerGap = 10
)

func benchmarkRecords(n int) []stream.Record {
	records := make([]stream.Record, n)
	base := time.Unix(1_700_000_000, 0).UTC()
	for i := range records {
		ledger := uint32(1_000_000 + i/benchmarkLedgerGap)
		txIndex := uint32(i % benchmarkLedgerGap)
		records[i] = stream.Record{
			ID:       fmt.Sprintf("%010d-%06d-%06d", ledger, txIndex, 0),
			Sequence: ledger,
			Time:     base.Add(time.Duration(i) * time.Second),
			Kind:     stream.KindEvent,
			Event: &stream.Event{
				Type:       "contract",
				ContractID: "CBGSB5J4Z3R6Y5YQ5J4Z3R6Y5YQ5J4Z3R6Y5YQ5J4Z3R6Y5YQ5J4Z3R",
				TxHash:     fmt.Sprintf("%064x", i),
				TxIndex:    txIndex,
				OpIndex:    0,
				EventIndex: 0,
				TopicXDR:   []string{"AAAAAQ=="},
				ValueXDR:   "AAAAAgAAAAE=",
			},
		}
	}
	return records
}

// benchmarkSource serves a fixed corpus in pages and uses the record index as
// its cursor, so a benchmark exercises the real Source contract without an RPC
// or a network.
type benchmarkSource struct {
	records  []stream.Record
	pageSize int
}

func (s *benchmarkSource) Name() string { return "benchmark" }

func (s *benchmarkSource) Fetch(_ context.Context, cursor string) (stream.Batch, error) {
	start := 0
	if cursor != "" {
		parsed, err := strconv.Atoi(cursor)
		if err != nil {
			return stream.Batch{}, fmt.Errorf("benchmark cursor %q: %w", cursor, err)
		}
		start = parsed
	}
	if len(s.records) == 0 {
		return stream.Batch{Records: []stream.Record{}}, nil
	}
	if start < 0 || start > len(s.records) {
		return stream.Batch{}, fmt.Errorf("benchmark cursor %q is out of range", cursor)
	}
	end := start + s.pageSize
	if end > len(s.records) {
		end = len(s.records)
	}
	return stream.Batch{
		Records:    s.records[start:end],
		NextCursor: strconv.Itoa(end),
		Latest:     s.records[len(s.records)-1].Sequence,
		Oldest:     s.records[0].Sequence,
		Full:       end < len(s.records),
	}, nil
}

func (s *benchmarkSource) CursorLedger(cursor string) (uint32, bool) {
	if cursor == "" {
		return 0, false
	}
	index, err := strconv.Atoi(cursor)
	if err != nil || index < 0 || index > len(s.records) || len(s.records) == 0 {
		return 0, false
	}
	if index == 0 {
		return s.records[0].Sequence, true
	}
	return s.records[index-1].Sequence, true
}

type benchmarkSink struct {
	emitted atomic.Int64
}

func (s *benchmarkSink) Emit(stream.Record) error {
	s.emitted.Add(1)
	return nil
}

func (s *benchmarkSink) Flush() error { return nil }

func reportEventsPerSecond(b *testing.B, events int) {
	b.Helper()
	b.ReportMetric(float64(events), "events/op")
	b.ReportMetric(float64(events)*float64(b.N)/b.Elapsed().Seconds(), "events/s")
}

type benchmarkAllocs struct {
	before runtime.MemStats
}

func startBenchmarkAllocs() *benchmarkAllocs {
	a := &benchmarkAllocs{}
	runtime.GC()
	runtime.ReadMemStats(&a.before)
	return a
}

func (a *benchmarkAllocs) report(b *testing.B, events int) {
	b.Helper()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	total := float64(events) * float64(b.N)
	b.ReportMetric(float64(after.Mallocs-a.before.Mallocs)/total, "allocs/event")
	b.ReportMetric(float64(after.TotalAlloc-a.before.TotalAlloc)/total, "bytes/event")
}

func benchmarkTailer(b *testing.B, records []stream.Record, dedup bool) {
	b.Helper()
	b.ReportAllocs()
	allocs := startBenchmarkAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		raw := &benchmarkSink{}
		var sink stream.Sink = raw
		if dedup {
			sink = stream.NewDedupSink(raw, stream.DefaultDedupWindow)
		}
		tailer := stream.NewTailer(
			&benchmarkSource{records: records, pageSize: benchmarkPageSize},
			sink,
			stream.Options{},
		)
		if err := tailer.Run(context.Background()); err != nil {
			b.Fatalf("Run() error = %v", err)
		}
		if got := raw.emitted.Load(); got != int64(len(records)) {
			b.Fatalf("emitted %d records, want %d", got, len(records))
		}
	}

	b.StopTimer()
	allocs.report(b, len(records))
	reportEventsPerSecond(b, len(records))
}

func BenchmarkTailerFiftyThousandEvents(b *testing.B) {
	records := benchmarkRecords(benchmarkEvents)
	b.Run("raw", func(b *testing.B) {
		benchmarkTailer(b, records, false)
	})
	b.Run("dedup", func(b *testing.B) {
		benchmarkTailer(b, records, true)
	})
}

func BenchmarkDedupSinkFiftyThousandEvents(b *testing.B) {
	records := benchmarkRecords(benchmarkEvents)
	b.ReportAllocs()
	allocs := startBenchmarkAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		raw := &benchmarkSink{}
		sink := stream.NewDedupSink(raw, stream.DefaultDedupWindow)
		for _, record := range records {
			if err := sink.Emit(record); err != nil {
				b.Fatalf("Emit() error = %v", err)
			}
		}
		if got := raw.emitted.Load(); got != int64(len(records)) {
			b.Fatalf("emitted %d records, want %d", got, len(records))
		}
	}

	b.StopTimer()
	allocs.report(b, len(records))
	reportEventsPerSecond(b, len(records))
}

func BenchmarkReorderBufferFiftyThousandEvents(b *testing.B) {
	records := benchmarkRecords(benchmarkEvents)
	b.ReportAllocs()
	allocs := startBenchmarkAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffer := stream.NewReorderBuffer()
		ready := 0
		for start := 0; start < len(records); start += benchmarkPageSize {
			end := start + benchmarkPageSize
			if end > len(records) {
				end = len(records)
			}
			ready += len(buffer.Add(records[start:end]))
		}
		ready += len(buffer.Flush())
		if ready != len(records) {
			b.Fatalf("released %d records, want %d", ready, len(records))
		}
	}

	b.StopTimer()
	allocs.report(b, len(records))
	reportEventsPerSecond(b, len(records))
}

func BenchmarkEventKeyFiftyThousandEvents(b *testing.B) {
	records := benchmarkRecords(benchmarkEvents)
	b.ReportAllocs()
	allocs := startBenchmarkAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, record := range records {
			if key := stream.EventKey(record); key == "" {
				b.Fatal("EventKey() returned an empty key")
			}
		}
	}

	b.StopTimer()
	allocs.report(b, len(records))
	reportEventsPerSecond(b, len(records))
}

// TestBenchmarkSourceHonoursTheSourceContract checks the fixture used by the
// benchmarks: every record is served exactly once, in order, and a malformed
// cursor is reported rather than silently interpreted as the start.
func TestBenchmarkSourceHonoursTheSourceContract(t *testing.T) {
	t.Parallel()

	records := benchmarkRecords(25)
	src := &benchmarkSource{records: records, pageSize: 10}
	ctx := context.Background()
	cursor := ""
	seen := 0
	last := ""
	for {
		batch, err := src.Fetch(ctx, cursor)
		if err != nil {
			t.Fatalf("Fetch(%q) error = %v", cursor, err)
		}
		for _, record := range batch.Records {
			if last != "" && record.ID <= last {
				t.Fatalf("ID %q did not increase after %q", record.ID, last)
			}
			last = record.ID
			seen++
		}
		if !batch.Full {
			break
		}
		cursor = batch.NextCursor
	}
	if seen != len(records) {
		t.Fatalf("served %d records, want %d", seen, len(records))
	}

	if _, err := src.Fetch(ctx, "not-a-number"); err == nil {
		t.Fatal("Fetch() error = nil, want a malformed-cursor failure")
	}
	if _, err := src.Fetch(ctx, "999"); err == nil {
		t.Fatal("Fetch() error = nil, want an out-of-range cursor failure")
	}
	if _, ok := src.CursorLedger("not-a-number"); ok {
		t.Fatal("CursorLedger() accepted a malformed cursor")
	}
}

// TestStreamingFiftyThousandEvents covers the benchmark's target load as an
// ordinary integration test: the full corpus is delivered in order, memory is
// bounded per event, throughput clears the 50k events/s budget, and the run
// leaves no goroutine behind.
func TestStreamingFiftyThousandEvents(t *testing.T) {
	records := benchmarkRecords(benchmarkEvents)
	runtime.GC()
	beforeGoroutines := runtime.NumGoroutine()

	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	raw := &benchmarkSink{}
	sink := stream.NewDedupSink(raw, stream.DefaultDedupWindow)
	tailer := stream.NewTailer(
		&benchmarkSource{records: records, pageSize: benchmarkPageSize},
		sink,
		stream.Options{},
	)

	start := time.Now()
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	elapsed := time.Since(start)
	drainDeadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > beforeGoroutines {
		if time.Now().After(drainDeadline) {
			t.Fatalf("goroutines = %d, want no more than %d",
				runtime.NumGoroutine(), beforeGoroutines)
		}
		time.Sleep(time.Millisecond)
	}

	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	if got := raw.emitted.Load(); got != benchmarkEvents {
		t.Fatalf("emitted %d records, want %d", got, benchmarkEvents)
	}
	stats := tailer.Stats()
	if stats.Emitted != benchmarkEvents {
		t.Errorf("Stats().Emitted = %d, want %d", stats.Emitted, benchmarkEvents)
	}
	if stats.Duplicates != 0 {
		t.Errorf("Stats().Duplicates = %d, want 0", stats.Duplicates)
	}

	eventsPerSecond := float64(benchmarkEvents) / elapsed.Seconds()
	t.Logf("delivered %d events in %s (%.0f events/s, %d bytes/event allocated)",
		benchmarkEvents, elapsed.Round(time.Millisecond), eventsPerSecond,
		(memAfter.TotalAlloc-memBefore.TotalAlloc)/benchmarkEvents)

	minEventsPerSecond := 50_000.0
	if raceBuild {
		// The race detector adds substantial per-operation overhead. Keep
		// the check meaningful under -race without making it timing-flaky;
		// the benchmarks measure and report the real throughput.
		minEventsPerSecond = 20_000
	}
	if eventsPerSecond < minEventsPerSecond {
		t.Errorf("throughput = %.0f events/s, want at least %.0f",
			eventsPerSecond, minEventsPerSecond)
	}
	const allocationBudget = 8192
	bytesPerEvent := (memAfter.TotalAlloc - memBefore.TotalAlloc) / benchmarkEvents
	if bytesPerEvent > allocationBudget {
		t.Errorf("allocated %d bytes/event, want at most %d", bytesPerEvent, allocationBudget)
	}
}
