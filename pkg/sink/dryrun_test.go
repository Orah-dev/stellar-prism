package sink

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestDryRunSinkDoesNotDeliverAndReportsMatch(t *testing.T) {
	var diagnostics bytes.Buffer
	sink := NewDryRunSink(&diagnostics)
	if err := sink.Emit(stream.Record{ID: "0001", Sequence: 7, Kind: stream.KindEvent}); err != nil {
		t.Fatal(err)
	}
	if sink.Matched() != 1 {
		t.Fatalf("Matched() = %d, want 1", sink.Matched())
	}
	if got := diagnostics.String(); !strings.Contains(got, "id=0001") {
		t.Fatalf("diagnostic %q", got)
	}
}
