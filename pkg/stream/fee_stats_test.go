package stream

import "testing"

func TestAverageFeesUsesMostRecentLedgers(t *testing.T) {
	samples := map[uint32][]int64{10: {100, 200}, 11: {300}, 12: {900}}
	if got := averageFees(samples, 2); got != 600 {
		t.Fatalf("two-ledger average = %v, want 600", got)
	}
	if got := averageFees(samples, 10); got != 375 {
		t.Fatalf("all-ledger average = %v, want 375", got)
	}
}
