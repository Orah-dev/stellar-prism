package sink

import (
	"fmt"
	"io"
	"sync"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// DryRunSink validates the stream path without delivering records to an
// external destination. It writes compact diagnostics to diagnostics.
type DryRunSink struct {
	diagnostics io.Writer
	mu          sync.Mutex
	matched     uint64
}

// NewDryRunSink returns a side-effect-free sink.
func NewDryRunSink(diagnostics io.Writer) *DryRunSink { return &DryRunSink{diagnostics: diagnostics} }

// Emit records a successful filter match without forwarding it.
func (s *DryRunSink) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.matched++
	_, err := fmt.Fprintf(s.diagnostics, "dry-run: match id=%s ledger=%d kind=%s\n", r.ID, r.Sequence, r.Kind)
	return err
}

// Flush implements stream.Sink.
func (*DryRunSink) Flush() error { return nil }

// Matched returns the number of records that reached the sink.
func (s *DryRunSink) Matched() uint64 { s.mu.Lock(); defer s.mu.Unlock(); return s.matched }
