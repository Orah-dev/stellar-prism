package sink_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type recordingSink struct {
	records []stream.Record
	batches [][]stream.Record
	flushes int
	closes  int
	err     error
}

func (r *recordingSink) Emit(record stream.Record) error {
	r.records = append(r.records, record)
	return r.err
}

func (r *recordingSink) Flush() error { r.flushes++; return r.err }

func (r *recordingSink) Close() error { r.closes++; return r.err }

type batchRecordingSink struct{ recordingSink }

func (b *batchRecordingSink) EmitBatch(records []stream.Record) error {
	b.batches = append(b.batches, records)
	return b.err
}

func TestFanoutEmitAndFlush(t *testing.T) {
	t.Parallel()

	first := &recordingSink{}
	second := &batchRecordingSink{}
	fanout := sink.NewFanout(first, second)

	r := eventRecord()
	if err := fanout.Emit(r); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if len(first.records) != 1 || len(second.records) != 1 {
		t.Errorf("Emit reached %d/%d sinks, want both", len(first.records), len(second.records))
	}

	if err := fanout.EmitBatch([]stream.Record{r, ledgerRecord()}); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	if len(second.batches) != 1 || len(second.batches[0]) != 2 {
		t.Errorf("batch-capable sink did not receive the batch: %+v", second.batches)
	}
	if len(first.records) != 3 {
		t.Errorf("plain sink received %d records, want 3 via per-record fallback", len(first.records))
	}

	if err := fanout.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if first.flushes != 1 || second.flushes != 1 {
		t.Errorf("flushes = %d/%d, want 1 each", first.flushes, second.flushes)
	}
}

func TestFanoutStopsAtFirstError(t *testing.T) {
	t.Parallel()

	first := &recordingSink{err: errors.New("broken")}
	second := &recordingSink{}
	fanout := sink.NewFanout(first, second)

	err := fanout.Emit(eventRecord())
	if err == nil {
		t.Fatal("Emit() error = nil, want the sink failure")
	}
	if !strings.Contains(err.Error(), "fanout: sink 0") {
		t.Errorf("error = %q, want it to identify the failing sink", err)
	}
	if len(second.records) != 0 {
		t.Errorf("a sink after the failure still received %d records, want 0", len(second.records))
	}
}

func TestFanoutFlushesAndClosesEverySink(t *testing.T) {
	t.Parallel()

	broken := &recordingSink{err: errors.New("broken")}
	healthy := &recordingSink{}
	fanout := sink.NewFanout(broken, healthy)

	if err := fanout.Flush(); err == nil {
		t.Fatal("Flush() error = nil, want the failure to be reported")
	}
	if healthy.flushes != 1 {
		t.Errorf("healthy sink flushed %d times, want 1 despite the earlier failure", healthy.flushes)
	}

	if err := fanout.Close(); err == nil {
		t.Fatal("Close() error = nil, want the failure to be reported")
	}
	if broken.closes != 1 || healthy.closes != 1 {
		t.Errorf("closes = %d/%d, want both sinks closed", broken.closes, healthy.closes)
	}
}

func TestFanoutWithoutSinksIsANoOp(t *testing.T) {
	t.Parallel()

	fanout := sink.NewFanout()
	if err := fanout.Emit(eventRecord()); err != nil {
		t.Errorf("Emit() error = %v", err)
	}
	if err := fanout.EmitBatch([]stream.Record{eventRecord()}); err != nil {
		t.Errorf("EmitBatch() error = %v", err)
	}
	if err := fanout.Flush(); err != nil {
		t.Errorf("Flush() error = %v", err)
	}
	if err := fanout.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}
