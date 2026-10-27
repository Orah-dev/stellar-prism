package stream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DefaultHealthStaleAfter is how long the health monitor may go without an
// update before the process is considered stuck.
//
// A stream that is wedged behind an unresponsive endpoint stops making
// progress, and a liveness probe that notices is what lets Kubernetes
// restart the pod instead of leaving it running but useless.
const DefaultHealthStaleAfter = 30 * time.Second

// defaultHealthNamespace prefixes every metric the health server exposes.
const defaultHealthNamespace = "prism"

// HealthSnapshot is the state the probe endpoints report.
type HealthSnapshot struct {
	// Alive reports whether the process is running and still making
	// progress.
	Alive bool
	// Ready reports whether the stream is fit to keep serving.
	Ready bool
	// Reason explains why the process is not ready. It is empty when the
	// process is ready.
	Reason string
	// CurrentLedger is the newest ledger the stream has emitted.
	CurrentLedger uint32
	// LatestLedger is the newest ledger the server has.
	LatestLedger uint32
	// Lag is how many ledgers the stream is behind the server.
	Lag uint32
	// RPCReachable reports whether the last RPC call succeeded. It is
	// unknown until the first call, which is not treated as a failure.
	RPCReachable bool
	// RPCKnown reports whether an RPC call has been made at all.
	RPCKnown bool
	// Gapped reports whether the stream fell out of the retention window.
	Gapped bool
	// Active reports whether this instance is the one that is streaming.
	// A standby is alive but not ready: it is waiting for the lease, not
	// serving.
	Active bool
	// LastError is the last failure the stream reported.
	LastError string
	// UpdatedAt is when the monitor was last refreshed.
	UpdatedAt time.Time
}

// HealthMonitorOptions configures a HealthMonitor.
type HealthMonitorOptions struct {
	// MaxLag is how many ledgers the stream may be behind before it stops
	// being ready. Zero means no limit.
	MaxLag uint32
	// StaleAfter is how long the monitor may go without an update before
	// the process stops being alive. Zero uses DefaultHealthStaleAfter.
	StaleAfter time.Duration
	// Clock is injectable for tests. Defaults to RealClock.
	Clock Clock
}

// HealthMonitor tracks the health of a running stream.
//
// It is fed by the tailer: every successful fetch reports how far the
// stream has got, and every failure marks the endpoint as unreachable. The
// probe endpoints read the result.
//
// The monitor is safe for concurrent use: the stream writes to it from its
// own goroutine while the HTTP server reads it from another.
type HealthMonitor struct {
	opts  HealthMonitorOptions
	clock Clock

	mu            sync.Mutex
	currentLedger uint32
	latestLedger  uint32
	rpcReachable  bool
	rpcKnown      bool
	gapped        bool
	active        bool
	lastError     string
	updatedAt     time.Time
}

// NewHealthMonitor returns a monitor configured by opts.
func NewHealthMonitor(opts HealthMonitorOptions) *HealthMonitor {
	if opts.StaleAfter <= 0 {
		opts.StaleAfter = DefaultHealthStaleAfter
	}
	if opts.Clock == nil {
		opts.Clock = RealClock{}
	}
	return &HealthMonitor{opts: opts, clock: opts.Clock, active: true}
}

// MaxLag returns the lag limit the monitor was configured with.
func (m *HealthMonitor) MaxLag() uint32 { return m.opts.MaxLag }

// RecordProgress reports how far the stream has got.
//
// A successful fetch is also proof that the endpoint answered, so the
// monitor marks the RPC reachable.
func (m *HealthMonitor) RecordProgress(p Progress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p.CurrentLedger > m.currentLedger {
		m.currentLedger = p.CurrentLedger
	}
	if p.LatestLedger > m.latestLedger {
		m.latestLedger = p.LatestLedger
	}
	m.rpcReachable = true
	m.rpcKnown = true
	m.updatedAt = m.clock.Now()
}

// SetRPCReachable records whether the endpoint answered.
func (m *HealthMonitor) SetRPCReachable(reachable bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rpcReachable = reachable
	m.rpcKnown = true
	m.updatedAt = m.clock.Now()
}

// SetGapped records that the stream fell out of the retention window.
//
// A gapped stream is not ready: it has lost records, and continuing to
// serve from it would hide the loss.
func (m *HealthMonitor) SetGapped(gapped bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gapped = gapped
	m.updatedAt = m.clock.Now()
}

// SetActive records whether this instance is the one that is streaming.
//
// A standby is alive but not ready: it is waiting for the lease rather than
// serving, so it must not be reported as stuck, and it must not be sent
// traffic.
func (m *HealthMonitor) SetActive(active bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active = active
	m.updatedAt = m.clock.Now()
}

// SetError records the last failure the stream reported.
func (m *HealthMonitor) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		m.lastError = ""
	} else {
		m.lastError = err.Error()
	}
	m.updatedAt = m.clock.Now()
}

// Snapshot returns the current health of the stream.
func (m *HealthMonitor) Snapshot() HealthSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	snapshot := HealthSnapshot{
		CurrentLedger: m.currentLedger,
		LatestLedger:  m.latestLedger,
		RPCReachable:  m.rpcReachable,
		RPCKnown:      m.rpcKnown,
		Gapped:        m.gapped,
		Active:        m.active,
		LastError:     m.lastError,
		UpdatedAt:     m.updatedAt,
	}
	if m.latestLedger > m.currentLedger {
		snapshot.Lag = m.latestLedger - m.currentLedger
	}

	// A process that has never reported anything has only just started, so
	// it is alive; one that stopped reporting is stuck.
	snapshot.Alive = m.updatedAt.IsZero() || now.Sub(m.updatedAt) <= m.opts.StaleAfter

	switch {
	case !m.active:
		// A standby is waiting for the lease, not stuck, so it stays
		// alive whatever it last reported.
		snapshot.Alive = true
		snapshot.Reason = "this instance is a standby; another one is the leader"
	case m.gapped:
		snapshot.Reason = "the stream gapped: records were lost to the retention window"
	case m.rpcKnown && !m.rpcReachable:
		snapshot.Reason = "the RPC endpoint is not answering"
	case m.opts.MaxLag > 0 && snapshot.Lag > m.opts.MaxLag:
		snapshot.Reason = fmt.Sprintf("the stream is %d ledgers behind, more than the %d allowed",
			snapshot.Lag, m.opts.MaxLag)
	}
	snapshot.Ready = snapshot.Alive && snapshot.Reason == ""
	return snapshot
}

// HealthServerOptions configures a HealthServer.
type HealthServerOptions struct {
	// Addr is the address to listen on, for example ":8086". Required.
	Addr string
	// Monitor is the state the endpoints report. Required.
	Monitor *HealthMonitor
	// Namespace prefixes every metric name. Empty uses "prism".
	Namespace string
	// ReadHeaderTimeout bounds how long a client may take to send its
	// headers. Zero uses five seconds.
	ReadHeaderTimeout time.Duration
}

// HealthServer serves the liveness and readiness probes a container
// orchestrator asks for.
//
// Three endpoints are served:
//
//   - /healthz answers whether the process is alive and still making
//     progress. It is the liveness probe: a 503 tells the orchestrator to
//     restart a process that is running but stuck.
//   - /readyz answers whether the stream is fit to serve: the endpoint
//     answers, the stream has not gapped, and it is not too far behind.
//     It is the readiness probe.
//   - /metrics exposes the same state in the Prometheus exposition format.
//
// The server owns one goroutine, so Close leaves nothing running.
type HealthServer struct {
	monitor   *HealthMonitor
	namespace string
	addr      string
	server    *http.Server
	listener  net.Listener
	done      chan struct{}

	// stop is closed when the server is closed. The goroutine that watches
	// the context uses it to leave once the server is gone, so a server
	// that is closed does not leave a goroutine behind.
	stop     chan struct{}
	stopOnce sync.Once

	mu       sync.Mutex
	serveErr error
}

// NewHealthServer returns a server configured by opts.
//
// The listener is not bound until Start is called, so a caller can report a
// bind failure before the stream starts.
func NewHealthServer(opts HealthServerOptions) (*HealthServer, error) {
	if opts.Addr == "" {
		return nil, errors.New("health server: a listen address is required")
	}
	if opts.Monitor == nil {
		return nil, errors.New("health server: a health monitor is required")
	}
	namespace := opts.Namespace
	if namespace == "" {
		namespace = defaultHealthNamespace
	}
	timeout := opts.ReadHeaderTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	s := &HealthServer{
		monitor:   opts.Monitor,
		namespace: namespace,
		addr:      opts.Addr,
		done:      make(chan struct{}),
		stop:      make(chan struct{}),
	}
	s.server = &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: timeout,
	}
	return s, nil
}

// Start binds the address and serves in the background.
//
// The server stops when the context is cancelled, so a probe endpoint does
// not outlive the run it reports on.
func (s *HealthServer) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("health server: listening on %s: %w", s.addr, err)
	}
	s.listener = listener
	go func() {
		defer close(s.done)
		if err := s.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.mu.Lock()
			s.serveErr = err
			s.mu.Unlock()
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
		case <-s.stop:
			// The server was closed; nothing left to watch.
			return
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			s.mu.Lock()
			s.serveErr = errors.Join(s.serveErr, err)
			s.mu.Unlock()
		}
	}()
	return nil
}

// Addr returns the address the server is listening on, or an empty string
// when it is not serving.
func (s *HealthServer) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Close stops the server and waits for it to finish.
//
// It is safe to call more than once.
func (s *HealthServer) Close() error {
	s.stopOnce.Do(func() {
		close(s.stop)
	})

	s.mu.Lock()
	listener := s.listener
	s.listener = nil
	s.mu.Unlock()

	if listener == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownErr := s.server.Shutdown(ctx)
	<-s.done

	s.mu.Lock()
	serveErr := s.serveErr
	s.mu.Unlock()
	return errors.Join(shutdownErr, serveErr)
}

// Handler returns the handler serving the probe endpoints.
func (s *HealthServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return mux
}

// handleHealthz answers the liveness probe.
func (s *HealthServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if !allowRead(w, r) {
		return
	}
	snapshot := s.monitor.Snapshot()
	if !snapshot.Alive {
		writePlain(w, http.StatusServiceUnavailable, "stuck: the stream has stopped making progress")
		return
	}
	writePlain(w, http.StatusOK, "ok")
}

// handleReadyz answers the readiness probe.
func (s *HealthServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !allowRead(w, r) {
		return
	}
	snapshot := s.monitor.Snapshot()
	if !snapshot.Ready {
		writePlain(w, http.StatusServiceUnavailable, snapshot.Reason)
		return
	}
	writePlain(w, http.StatusOK, "ready")
}

// handleMetrics exposes the health state in the Prometheus exposition
// format.
func (s *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if !allowRead(w, r) {
		return
	}
	snapshot := s.monitor.Snapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")

	var b strings.Builder
	namespace := s.namespace
	fmt.Fprintf(&b, "# HELP %s_health_alive Whether the stream is alive and making progress.\n", namespace)
	fmt.Fprintf(&b, "# TYPE %s_health_alive gauge\n", namespace)
	fmt.Fprintf(&b, "%s_health_alive %d\n", namespace, boolValue(snapshot.Alive))

	fmt.Fprintf(&b, "# HELP %s_health_ready Whether the stream is fit to serve.\n", namespace)
	fmt.Fprintf(&b, "# TYPE %s_health_ready gauge\n", namespace)
	fmt.Fprintf(&b, "%s_health_ready %d\n", namespace, boolValue(snapshot.Ready))

	fmt.Fprintf(&b, "# HELP %s_health_up Whether the RPC endpoint is answering.\n", namespace)
	fmt.Fprintf(&b, "# TYPE %s_health_up gauge\n", namespace)
	fmt.Fprintf(&b, "%s_health_up %d\n", namespace, boolValue(snapshot.RPCReachable))

	fmt.Fprintf(&b, "# HELP %s_health_active Whether this instance is the one that is streaming.\n", namespace)
	fmt.Fprintf(&b, "# TYPE %s_health_active gauge\n", namespace)
	fmt.Fprintf(&b, "%s_health_active %d\n", namespace, boolValue(snapshot.Active))

	fmt.Fprintf(&b, "# HELP %s_stream_lag Ledgers the stream is behind the server.\n", namespace)
	fmt.Fprintf(&b, "# TYPE %s_stream_lag gauge\n", namespace)
	fmt.Fprintf(&b, "%s_stream_lag %d\n", namespace, snapshot.Lag)

	fmt.Fprintf(&b, "# HELP %s_stream_current_ledger Newest ledger the stream has emitted.\n", namespace)
	fmt.Fprintf(&b, "# TYPE %s_stream_current_ledger gauge\n", namespace)
	fmt.Fprintf(&b, "%s_stream_current_ledger %d\n", namespace, snapshot.CurrentLedger)

	fmt.Fprintf(&b, "# HELP %s_stream_latest_ledger Newest ledger the server has.\n", namespace)
	fmt.Fprintf(&b, "# TYPE %s_stream_latest_ledger gauge\n", namespace)
	fmt.Fprintf(&b, "%s_stream_latest_ledger %d\n", namespace, snapshot.LatestLedger)

	if _, err := w.Write([]byte(b.String())); err != nil {
		return
	}
}

// allowRead rejects anything but a read, and reports whether the handler
// may continue.
func allowRead(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

// writePlain writes a short text response.
func writePlain(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	// A failed write means the probe client hung up, which is not
	// something the server can act on.
	if _, err := w.Write([]byte(message + "\n")); err != nil {
		return
	}
}

// boolValue renders a boolean as a Prometheus gauge value.
func boolValue(v bool) int {
	if v {
		return 1
	}
	return 0
}
