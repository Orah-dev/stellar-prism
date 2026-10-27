package sink_test

import (
	"errors"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestMultiSinkRetriesOnlyUnacknowledgedSinks(t *testing.T) {
	healthy := &recordingSink{}
	failing := &recordingSink{err: errors.New("temporary")}
	m, err := sink.NewMultiSink([]stream.Sink{healthy, failing}, sink.MultiSinkOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r := eventRecord()
	if err := m.Emit(r); err == nil {
		t.Fatal("first Emit error = nil")
	}
	if len(healthy.records) != 1 || len(failing.records) != 1 {
		t.Fatalf("first delivery = %d/%d", len(healthy.records), len(failing.records))
	}
	failing.err = nil
	if err := m.Emit(r); err != nil {
		t.Fatalf("retry error = %v", err)
	}
	if len(healthy.records) != 1 {
		t.Errorf("healthy sink received %d records, want no duplicate", len(healthy.records))
	}
	if len(failing.records) != 2 {
		t.Errorf("failing sink received %d records, want retry", len(failing.records))
	}
}

func TestMultiSinkClosesEveryChild(t *testing.T) {
	first := &recordingSink{}
	broken := &recordingSink{err: errors.New("close failed")}
	m, err := sink.NewMultiSink([]stream.Sink{first, broken}, sink.MultiSinkOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err == nil {
		t.Fatal("Close() error = nil, want child close failure")
	}
	if first.closes != 1 || broken.closes != 1 {
		t.Errorf("closes = %d/%d, want 1/1", first.closes, broken.closes)
	}
}
