package cli

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

type fakeProfiler struct {
	closes atomic.Int64
	err    error
}

func (f *fakeProfiler) Close() error {
	f.closes.Add(1)
	return f.err
}

func TestPProfPortStartsAndStopsProfiler(t *testing.T) {
	var stdout, stderr bytes.Buffer
	profiler := &fakeProfiler{}
	var gotPort int

	err := ExecuteWithProfiler([]string{"version", "--pprof-port", "6060"},
		&stdout, &stderr,
		func(port int, _ io.Writer) (Profiler, error) {
			gotPort = port
			return profiler, nil
		})
	if err != nil {
		t.Fatalf("ExecuteWithProfiler() error = %v", err)
	}
	if gotPort != 6060 {
		t.Errorf("factory port = %d, want 6060", gotPort)
	}
	if got := profiler.closes.Load(); got != 1 {
		t.Errorf("Close() calls = %d, want 1", got)
	}
	if !strings.Contains(stdout.String(), Version) {
		t.Errorf("version output = %q, want %q", stdout.String(), Version)
	}
}

func TestPProfProfilerStopsWhenTheCommandFails(t *testing.T) {
	profiler := &fakeProfiler{}
	err := ExecuteWithProfiler([]string{"events", "--format", "nonsense", "--pprof-port", "6060"},
		io.Discard, io.Discard,
		func(int, io.Writer) (Profiler, error) { return profiler, nil })
	if err == nil {
		t.Fatal("ExecuteWithProfiler() error = nil, want the validation failure")
	}
	if got := profiler.closes.Load(); got != 1 {
		t.Errorf("Close() calls = %d, want 1 after a failed command", got)
	}
}

func TestPProfPortZeroDoesNotStartAProfiler(t *testing.T) {
	called := false
	err := ExecuteWithProfiler([]string{"version", "--pprof-port", "0"},
		io.Discard, io.Discard,
		func(int, io.Writer) (Profiler, error) {
			called = true
			return &fakeProfiler{}, nil
		})
	if err != nil {
		t.Fatalf("ExecuteWithProfiler() error = %v", err)
	}
	if called {
		t.Error("the profiler factory was called for --pprof-port 0")
	}
}

func TestPProfPortRejectsInvalidRange(t *testing.T) {
	for _, port := range []string{"-1", "70000"} {
		called := false
		err := ExecuteWithProfiler([]string{"version", "--pprof-port", port},
			io.Discard, io.Discard,
			func(int, io.Writer) (Profiler, error) {
				called = true
				return &fakeProfiler{}, nil
			})
		if err == nil {
			t.Errorf("--pprof-port %s error = nil, want a range failure", port)
		}
		if called {
			t.Errorf("--pprof-port %s started a profiler", port)
		}
		if err != nil && !strings.Contains(err.Error(), "between 0 and 65535") {
			t.Errorf("--pprof-port %s error = %q, want the range", port, err)
		}
	}
}

func TestPProfPortRequiresAProfilerImplementation(t *testing.T) {
	err := Execute([]string{"version", "--pprof-port", "6060"}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("Execute() error = nil, want a missing profiling implementation")
	}
	if !strings.Contains(err.Error(), "not supported") {
		t.Errorf("error = %q, want it to name the missing implementation", err)
	}
}

func TestPProfFactoryFailuresAreReportedAndCleanedUp(t *testing.T) {
	startErr := errors.New("bind failed")
	err := ExecuteWithProfiler([]string{"version", "--pprof-port", "6060"},
		io.Discard, io.Discard,
		func(int, io.Writer) (Profiler, error) { return nil, startErr })
	if !errors.Is(err, startErr) {
		t.Errorf("error = %v, want it to wrap %v", err, startErr)
	}

	err = ExecuteWithProfiler([]string{"version", "--pprof-port", "6060"},
		io.Discard, io.Discard,
		func(int, io.Writer) (Profiler, error) { return nil, nil })
	if err == nil || !strings.Contains(err.Error(), "no profiler") {
		t.Errorf("nil profiler error = %v, want a clear failure", err)
	}
}

func TestPProfCloseFailureIsReturned(t *testing.T) {
	closeErr := errors.New("shutdown failed")
	profiler := &fakeProfiler{err: closeErr}
	err := ExecuteWithProfiler([]string{"version", "--pprof-port", "6060"},
		io.Discard, io.Discard,
		func(int, io.Writer) (Profiler, error) { return profiler, nil })
	if !errors.Is(err, closeErr) {
		t.Errorf("error = %v, want it to wrap %v", err, closeErr)
	}
}

func TestRootHelpAdvertisesPProfPort(t *testing.T) {
	var stdout bytes.Buffer
	if err := Execute([]string{"--help"}, &stdout, io.Discard); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout.String(), "--pprof-port") {
		t.Errorf("help does not advertise --pprof-port\n%s", stdout.String())
	}
}

func TestCloseProfilerWithoutAProfilerIsANoOp(t *testing.T) {
	g := &globalFlags{}
	if err := g.closeProfiler(); err != nil {
		t.Errorf("closeProfiler() error = %v, want nil", err)
	}
}
