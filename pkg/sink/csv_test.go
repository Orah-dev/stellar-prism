package sink_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestCSVSinkHeaderEscapingAndTSV(t *testing.T) {
	var out bytes.Buffer
	s, err := sink.NewCSVSink(&out, sink.CSVOptions{Fields: []string{"ledger", "contract", "value"}})
	if err != nil {
		t.Fatal(err)
	}
	r := stream.Record{Sequence: 42, Event: &stream.Event{ContractID: "C,quoted\"", ValueXDR: "line one\nline two"}}
	if err := s.Emit(r); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "ledger,contract,value\n42,\"C,quoted\"\"\",\"line one\nline two\"\n"; got != want {
		t.Errorf("CSV = %q, want %q", got, want)
	}

	out.Reset()
	tsv, err := sink.NewCSVSink(&out, sink.CSVOptions{Fields: []string{"time", "kind"}, Delimiter: '\t'})
	if err != nil {
		t.Fatal(err)
	}
	if err := tsv.Emit(stream.Record{Time: time.Date(2026, 10, 22, 1, 2, 3, 0, time.UTC), Kind: stream.KindEvent}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "time\tkind\n2026-10-22T01:02:03Z\tevent\n") {
		t.Errorf("TSV = %q", out.String())
	}
}
