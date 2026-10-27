package cli

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestRunWithTimeoutWrapsErrTimedOut covers the contract runStream relies on
// to tell an abandoned goroutine from a completed one: the timeout error
// must be recognisable with errors.Is, because the --stats summary reads
// stream and sink counters only when it is not there.
func TestRunWithTimeoutWrapsErrTimedOut(t *testing.T) {
	released := make(chan struct{})
	err := runWithTimeout(time.Nanosecond, func() error {
		<-released
		return nil
	})
	// Release the abandoned worker so the test leaves no goroutine behind.
	close(released)

	if !errors.Is(err, errTimedOut) {
		t.Fatalf("runWithTimeout() error = %v, want it to match errTimedOut", err)
	}
	if want := "timed out after 1ns"; !strings.Contains(err.Error(), want) {
		t.Errorf("error = %q, want it to contain %q", err, want)
	}
}

// TestRunWithTimeoutPassesWorkErrorThrough checks the other side of that
// contract: a work error that completed in time is returned as itself, so a
// caller reading counters after it is safe.
func TestRunWithTimeoutPassesWorkErrorThrough(t *testing.T) {
	sentinel := errors.New("work failed")

	err := runWithTimeout(time.Minute, func() error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Errorf("runWithTimeout() error = %v, want it to wrap the work error", err)
	}
	if errors.Is(err, errTimedOut) {
		t.Errorf("runWithTimeout() error = %v, must not match errTimedOut", err)
	}

	// A non-positive timeout runs the work inline, on the caller's
	// goroutine, so it can never be abandoned.
	if err := runWithTimeout(0, func() error { return sentinel }); !errors.Is(err, sentinel) {
		t.Errorf("runWithTimeout(0) error = %v, want the work error", err)
	}
}
