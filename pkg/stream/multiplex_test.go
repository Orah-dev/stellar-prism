package stream

import (
	"context"
	"testing"
)

func TestMultiplexSource(t *testing.T) {
	// Create two mock sources with different records
	records1 := []Record{
		{ID: "00000000000000000001", Sequence: 1, Kind: KindEvent},
		{ID: "00000000000000000003", Sequence: 1, Kind: KindEvent},
		{ID: "00000000000000000005", Sequence: 1, Kind: KindEvent},
	}
	records2 := []Record{
		{ID: "00000000000000000002", Sequence: 1, Kind: KindEvent},
		{ID: "00000000000000000004", Sequence: 1, Kind: KindEvent},
		{ID: "00000000000000000006", Sequence: 1, Kind: KindEvent},
	}

	mockSrc1 := &mockSource{records: records1, name: "source1"}
	mockSrc2 := &mockSource{records: records2, name: "source2"}

	mux := NewMultiplexSource(mockSrc1, mockSrc2)

	ctx := context.Background()
	batch, err := mux.Fetch(ctx, "")
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	if len(batch.Records) != 6 {
		t.Errorf("expected 6 merged records, got %d", len(batch.Records))
	}

	// Check that records are in order
	for i := 1; i < len(batch.Records); i++ {
		if batch.Records[i-1].ID >= batch.Records[i].ID {
			t.Errorf("records not in order at index %d: %s >= %s",
				i, batch.Records[i-1].ID, batch.Records[i].ID)
		}
	}

	// Check exact order
	expectedIDs := []string{
		"00000000000000000001",
		"00000000000000000002",
		"00000000000000000003",
		"00000000000000000004",
		"00000000000000000005",
		"00000000000000000006",
	}
	for i, id := range expectedIDs {
		if batch.Records[i].ID != id {
			t.Errorf("record %d: expected ID %s, got %s", i, id, batch.Records[i].ID)
		}
	}
}

func TestMultiplexSourceSingleSource(t *testing.T) {
	records := []Record{
		{ID: "00000000000000000001", Sequence: 1, Kind: KindEvent},
		{ID: "00000000000000000002", Sequence: 1, Kind: KindEvent},
	}

	mockSrc := &mockSource{records: records, name: "source1"}
	mux := NewMultiplexSource(mockSrc)

	ctx := context.Background()
	batch, err := mux.Fetch(ctx, "")
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	if len(batch.Records) != 2 {
		t.Errorf("expected 2 records, got %d", len(batch.Records))
	}
	if batch.Records[0].ID != "00000000000000000001" {
		t.Errorf("first record ID mismatch")
	}
	if batch.Records[1].ID != "00000000000000000002" {
		t.Errorf("second record ID mismatch")
	}
}

func TestMultiplexSourceEmptySource(t *testing.T) {
	records1 := []Record{
		{ID: "00000000000000000001", Sequence: 1, Kind: KindEvent},
	}
	records2 := []Record{} // Empty source

	mockSrc1 := &mockSource{records: records1, name: "source1"}
	mockSrc2 := &mockSource{records: records2, name: "source2"}

	mux := NewMultiplexSource(mockSrc1, mockSrc2)

	ctx := context.Background()
	batch, err := mux.Fetch(ctx, "")
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	if len(batch.Records) != 1 {
		t.Errorf("expected 1 record, got %d", len(batch.Records))
	}
	if batch.Records[0].ID != "00000000000000000001" {
		t.Errorf("record ID mismatch")
	}
}
