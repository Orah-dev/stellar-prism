package sink

import (
	"errors"
	"fmt"
	"io"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// Fanout delivers every record to each of several sinks.
//
// It is how a stream writes to stdout while also posting to a webhook,
// archiving object blocks or sampling metrics without giving up the existing
// output. A failure in any sink is returned: silently dropping a delivery is
// worse than stopping a stream that has an operator watching it.
type Fanout struct {
	sinks []stream.Sink
}

// NewFanout returns a sink that forwards to sinks in order.
func NewFanout(sinks ...stream.Sink) *Fanout {
	return &Fanout{sinks: sinks}
}

// Emit implements stream.Sink.
func (f *Fanout) Emit(r stream.Record) error {
	for i, s := range f.sinks {
		if err := s.Emit(r); err != nil {
			return fmt.Errorf("fanout: sink %d: %w", i, err)
		}
	}
	return nil
}

// EmitBatch implements stream.BatchSink, forwarding the whole batch to sinks
// that can consume one and falling back to per-record emission otherwise.
func (f *Fanout) EmitBatch(records []stream.Record) error {
	for i, s := range f.sinks {
		if batch, ok := s.(stream.BatchSink); ok {
			if err := batch.EmitBatch(records); err != nil {
				return fmt.Errorf("fanout: sink %d: %w", i, err)
			}
			continue
		}
		for _, r := range records {
			if err := s.Emit(r); err != nil {
				return fmt.Errorf("fanout: sink %d: emitting record %s: %w", i, r.ID, err)
			}
		}
	}
	return nil
}

// Flush implements stream.Sink. Every sink is flushed even when an earlier one
// fails, so one broken destination cannot leave another with buffered data.
func (f *Fanout) Flush() error {
	errs := make([]error, 0, len(f.sinks))
	for i, s := range f.sinks {
		if err := s.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("fanout: sink %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// Close flushes and closes every sink that owns a resource. It is safe to call
// more than once only if the children are; the sinks in this package are.
func (f *Fanout) Close() error {
	errs := make([]error, 0, len(f.sinks))
	for i, s := range f.sinks {
		closer, ok := s.(io.Closer)
		if !ok {
			continue
		}
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("fanout: sink %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}
