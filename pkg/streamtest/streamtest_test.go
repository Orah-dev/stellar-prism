package streamtest

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestRecordingSinkAndAssertions(t *testing.T) {
	sink := NewRecordingSink()
	first := Event().ID("01").Contract("C").Build()
	second := Event().ID("02").Build()
	if err := sink.Emit(first); err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit(second); err != nil {
		t.Fatal(err)
	}
	if err := AssertOrderedIDs(sink.Records()); err != nil {
		t.Fatal(err)
	}
	if err := AssertDeduplicated(sink.Records()); err != nil {
		t.Fatal(err)
	}
	sink.Fail(errors.New("stop"))
	if err := sink.Emit(stream.Record{}); err == nil {
		t.Fatal("expected configured failure")
	}
}

func TestFakeSource(t *testing.T) {
	source := NewFakeSource(stream.Batch{Records: []stream.Record{Event().Build()}})
	batch, err := source.Fetch(t.Context(), "")
	if err != nil || len(batch.Records) != 1 {
		t.Fatalf("Fetch() = %#v, %v", batch, err)
	}
}

func ExampleEventBuilder() {
	record := Event().ID("0000000042").Ledger(42).Contract("CABC").Build()
	fmt.Printf("%s %d %s\n", record.ID, record.Sequence, record.Event.ContractID)
	// Output: 0000000042 42 CABC
}
