package sink

import (
	"bytes"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestStatusBarReportsAndForwards(t *testing.T) {
	var display bytes.Buffer
	var received int
	bar := NewStatusBar(stream.SinkFunc(func(stream.Record) error { received++; return nil }), &display, StatusBarOptions{Enabled: true, Now: func() time.Time { return time.Unix(20, 0) }})
	if err := bar.Emit(stream.Record{Kind: stream.KindLedger, Sequence: 8, Time: time.Unix(10, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := bar.Emit(stream.Record{Kind: stream.KindEvent, Time: time.Unix(11, 0)}); err != nil {
		t.Fatal(err)
	}
	if received != 2 {
		t.Fatalf("received = %d", received)
	}
	if display.Len() == 0 {
		t.Fatal("expected status display")
	}
}
