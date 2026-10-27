package stream

import "testing"

func TestWALReplayAndCommit(t *testing.T) {
	w, err := OpenWAL(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Append(Record{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	got, err := w.Replay()
	if err != nil || len(got) != 1 {
		t.Fatalf("%v %#v", err, got)
	}
	if err = w.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err = w.Replay()
	if err != nil || len(got) != 0 {
		t.Fatalf("%v %#v", err, got)
	}
}
