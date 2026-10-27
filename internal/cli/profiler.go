package cli

import (
	"fmt"
	"io"
)

// Profiler is a runtime profiling server that runs for the lifetime of one
// command.
type Profiler interface {
	// Close stops the profiler and waits for its goroutines to finish.
	Close() error
}

// ProfilerFactory starts a profiler on port and returns its handle. The CLI
// owns the flag and the lifecycle; cmd/prism supplies the pprof-backed
// implementation so the streaming library does not depend on net/http/pprof.
type ProfilerFactory func(port int, stderr io.Writer) (Profiler, error)

func validateProfilerPort(port int) error {
	if port < 0 || port > 65535 {
		return fmt.Errorf("--pprof-port must be between 0 and 65535")
	}
	return nil
}

// startProfiler starts the profiler when --pprof-port asked for one. Port
// zero means profiling is disabled, so the factory is not consulted and no
// listener is opened.
func (g *globalFlags) startProfiler(stderr io.Writer) error {
	if err := validateProfilerPort(g.pprofPort); err != nil {
		return err
	}
	if g.pprofPort == 0 {
		return nil
	}
	if g.profilerFactory == nil {
		return fmt.Errorf("--pprof-port is not supported by this build")
	}
	profiler, err := g.profilerFactory(g.pprofPort, stderr)
	if err != nil {
		return fmt.Errorf("starting the pprof server: %w", err)
	}
	if profiler == nil {
		return fmt.Errorf("starting the pprof server: no profiler was returned")
	}
	g.profiler = profiler
	return nil
}

// closeProfiler stops the profiler and reports a failure to shut down. It is
// safe to call when no profiler was started.
func (g *globalFlags) closeProfiler() error {
	if g.profiler == nil {
		return nil
	}
	profiler := g.profiler
	g.profiler = nil
	if err := profiler.Close(); err != nil {
		return fmt.Errorf("closing the pprof server: %w", err)
	}
	return nil
}
