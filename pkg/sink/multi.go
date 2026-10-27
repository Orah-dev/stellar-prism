package sink

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type MultiSinkPolicy string

const (
	MultiSinkAbortAll       MultiSinkPolicy = "abort-all"
	MultiSinkContinueAndDLQ MultiSinkPolicy = "continue-and-dlq"
)

type MultiSinkOptions struct {
	Policy MultiSinkPolicy
	// OnFailure persists a failed delivery when Policy is ContinueAndDLQ.
	// Returning an error keeps the record pending and prevents cursor advance.
	OnFailure func(stream.Record, error) error
}

// MultiSink delivers a record to every sink exactly once per successful sink.
// A retry of the same record only calls sinks that have not acknowledged it,
// avoiding duplicate writes to successful sinks while retaining the cursor
// until each required delivery has completed.
type MultiSink struct {
	sinks   []stream.Sink
	opts    MultiSinkOptions
	mu      sync.Mutex
	pending map[string]map[int]struct{}
}

func NewMultiSink(sinks []stream.Sink, opts MultiSinkOptions) (*MultiSink, error) {
	if opts.Policy == "" {
		opts.Policy = MultiSinkAbortAll
	}
	if opts.Policy != MultiSinkAbortAll && opts.Policy != MultiSinkContinueAndDLQ {
		return nil, fmt.Errorf("multi sink: invalid policy %q", opts.Policy)
	}
	return &MultiSink{sinks: append([]stream.Sink(nil), sinks...), opts: opts, pending: make(map[string]map[int]struct{})}, nil
}

func (m *MultiSink) Emit(r stream.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := r.ID
	if key == "" {
		key = fmt.Sprintf("%p", &r)
	}
	pending := m.pending[key]
	if pending == nil {
		pending = make(map[int]struct{}, len(m.sinks))
		for i := range m.sinks {
			pending[i] = struct{}{}
		}
	}
	var failures []error
	for i := range pending {
		if err := m.sinks[i].Emit(r); err != nil {
			failures = append(failures, fmt.Errorf("sink %d: %w", i, err))
			continue
		}
		delete(pending, i)
	}
	if len(failures) == 0 {
		delete(m.pending, key)
		return nil
	}
	err := errors.Join(failures...)
	if m.opts.Policy == MultiSinkContinueAndDLQ && m.opts.OnFailure != nil {
		dlqErr := m.opts.OnFailure(r, err)
		if dlqErr == nil {
			delete(m.pending, key)
			return nil
		}
		return errors.Join(err, fmt.Errorf("dead letter queue: %w", dlqErr))
	}
	m.pending[key] = pending
	return fmt.Errorf("multi sink: %w", err)
}

func (m *MultiSink) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var errs []error
	for i, s := range m.sinks {
		if err := s.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("sink %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// Close releases every child that owns a resource. Tailer.Drain has already
// flushed the chain by the time Close is called; individual sinks such as the
// Elasticsearch sink also flush on Close, which makes this safe for callers
// that use MultiSink directly.
func (m *MultiSink) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var errs []error
	for i, s := range m.sinks {
		closer, ok := s.(io.Closer)
		if !ok {
			continue
		}
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("sink %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}
