// Package streamtest provides small, deterministic test doubles for programs
// built on prism's public stream package.
package streamtest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// EventBuilder constructs event records without requiring RPC fixtures.
type EventBuilder struct{ record stream.Record }

// Event returns a builder with a stable default event record.
func Event() *EventBuilder {
	return &EventBuilder{record: stream.Record{
		ID: "0000000001", Kind: stream.KindEvent, Time: time.Unix(0, 0).UTC(), Event: &stream.Event{},
	}}
}

// ID sets the record identifier.
func (b *EventBuilder) ID(id string) *EventBuilder { b.record.ID = id; return b }

// Ledger sets the record's ledger sequence.
func (b *EventBuilder) Ledger(sequence uint32) *EventBuilder { b.record.Sequence = sequence; return b }

// At sets the ledger close time.
func (b *EventBuilder) At(at time.Time) *EventBuilder { b.record.Time = at; return b }

// Contract sets the emitting contract ID.
func (b *EventBuilder) Contract(id string) *EventBuilder { b.record.Event.ContractID = id; return b }

// Topic appends one base64-encoded topic value.
func (b *EventBuilder) Topic(value string) *EventBuilder {
	b.record.Event.TopicXDR = append(b.record.Event.TopicXDR, value)
	return b
}

// Value sets the base64-encoded event value.
func (b *EventBuilder) Value(value string) *EventBuilder { b.record.Event.ValueXDR = value; return b }

// Build returns an independent record safe for the caller to modify.
func (b *EventBuilder) Build() stream.Record {
	r := b.record
	e := *r.Event
	e.TopicXDR = append([]string(nil), e.TopicXDR...)
	r.Event = &e
	return r
}

// RecordingSink captures records for assertions. It is safe to inspect while
// a tailer is running.
type RecordingSink struct {
	mu      sync.Mutex
	records []stream.Record
	err     error
}

// NewRecordingSink returns an empty recording sink.
func NewRecordingSink() *RecordingSink { return &RecordingSink{} }

// Fail makes subsequent Emit calls return err.
func (s *RecordingSink) Fail(err error) { s.mu.Lock(); s.err = err; s.mu.Unlock() }

// Emit records r.
func (s *RecordingSink) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.records = append(s.records, r)
	return nil
}

// Flush implements stream.Sink.
func (*RecordingSink) Flush() error { return nil }

// Records returns a snapshot of captured records.
func (s *RecordingSink) Records() []stream.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]stream.Record(nil), s.records...)
}

// FakeSource returns pre-programmed batches in order. Once exhausted it
// returns an empty batch, making it suitable for bounded tailer tests.
type FakeSource struct {
	NameValue string
	Batches   []stream.Batch
	Err       error
	mu        sync.Mutex
	next      int
}

// NewFakeSource creates a source from batches.
func NewFakeSource(batches ...stream.Batch) *FakeSource {
	return &FakeSource{NameValue: "fake", Batches: batches}
}

// Name implements stream.Source.
func (s *FakeSource) Name() string {
	if s.NameValue == "" {
		return "fake"
	}
	return s.NameValue
}

// Fetch implements stream.Source.
func (s *FakeSource) Fetch(context.Context, string) (stream.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return stream.Batch{}, s.Err
	}
	if s.next >= len(s.Batches) {
		return stream.Batch{}, nil
	}
	b := s.Batches[s.next]
	s.next++
	return b, nil
}

// CursorLedger parses the decimal ledger prefix convention used by test IDs.
func (*FakeSource) CursorLedger(string) (uint32, bool) { return 0, false }

// AssertOrderedIDs reports an error unless records are strictly ordered by ID.
func AssertOrderedIDs(records []stream.Record) error {
	for i := 1; i < len(records); i++ {
		if records[i-1].ID >= records[i].ID {
			return fmt.Errorf("record IDs are not strictly ordered at %d: %q then %q", i, records[i-1].ID, records[i].ID)
		}
	}
	return nil
}

// AssertDeduplicated reports an error when an ID appears more than once.
func AssertDeduplicated(records []stream.Record) error {
	seen := make(map[string]struct{}, len(records))
	for _, r := range records {
		if _, ok := seen[r.ID]; ok {
			return fmt.Errorf("duplicate record ID %q", r.ID)
		}
		seen[r.ID] = struct{}{}
	}
	return nil
}
