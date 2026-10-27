package stream_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// get performs a GET against a handler and returns the status and the body.
func get(t *testing.T, handler http.Handler, path string) (int, string) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.String()
}

// TestHealthServerLiveness covers the liveness probe: a stream that is
// making progress is alive, and one that stopped is not.
func TestHealthServerLiveness(t *testing.T) {
	t.Parallel()

	monitor := stream.NewHealthMonitor(stream.HealthMonitorOptions{
		StaleAfter: 50 * time.Millisecond,
	})
	server, err := stream.NewHealthServer(stream.HealthServerOptions{
		Addr:    "127.0.0.1:0",
		Monitor: monitor,
	})
	if err != nil {
		t.Fatalf("NewHealthServer() error = %v", err)
	}

	status, body := get(t, server.Handler(), "/healthz")
	if status != http.StatusOK {
		t.Errorf("/healthz status = %d, want %d (%s)", status, http.StatusOK, body)
	}

	// The stream reports progress, and then stops.
	monitor.RecordProgress(stream.Progress{CurrentLedger: 10, LatestLedger: 10})
	status, _ = get(t, server.Handler(), "/healthz")
	if status != http.StatusOK {
		t.Errorf("/healthz status = %d after progress, want %d", status, http.StatusOK)
	}

	time.Sleep(80 * time.Millisecond)
	status, body = get(t, server.Handler(), "/healthz")
	if status != http.StatusServiceUnavailable {
		t.Errorf("/healthz status = %d after the stream stalled, want %d (%s)",
			status, http.StatusServiceUnavailable, body)
	}
}

// TestHealthServerReadiness covers the readiness probe: the stream has to be
// reachable, not gapped and not too far behind.
func TestHealthServerReadiness(t *testing.T) {
	t.Parallel()

	monitor := stream.NewHealthMonitor(stream.HealthMonitorOptions{MaxLag: 5})
	server, err := stream.NewHealthServer(stream.HealthServerOptions{
		Addr:    "127.0.0.1:0",
		Monitor: monitor,
	})
	if err != nil {
		t.Fatalf("NewHealthServer() error = %v", err)
	}
	handler := server.Handler()

	// A stream that is caught up is ready.
	monitor.RecordProgress(stream.Progress{CurrentLedger: 100, LatestLedger: 102})
	status, body := get(t, handler, "/readyz")
	if status != http.StatusOK {
		t.Errorf("/readyz status = %d, want %d (%s)", status, http.StatusOK, body)
	}

	// A stream that is too far behind is not.
	monitor.RecordProgress(stream.Progress{CurrentLedger: 100, LatestLedger: 200})
	status, body = get(t, handler, "/readyz")
	if status != http.StatusServiceUnavailable {
		t.Errorf("/readyz status = %d with a lag of 100, want %d (%s)",
			status, http.StatusServiceUnavailable, body)
	}
	if !strings.Contains(body, "behind") {
		t.Errorf("/readyz body %q does not explain the lag", body)
	}

	// An endpoint that stopped answering is not ready either.
	monitor.SetRPCReachable(false)
	status, body = get(t, handler, "/readyz")
	if status != http.StatusServiceUnavailable {
		t.Errorf("/readyz status = %d with an unreachable RPC, want %d (%s)",
			status, http.StatusServiceUnavailable, body)
	}
	if !strings.Contains(body, "not answering") {
		t.Errorf("/readyz body %q does not explain the unreachable endpoint", body)
	}

	// A gapped stream is not ready: it has lost records.
	monitor.SetRPCReachable(true)
	monitor.SetGapped(true)
	status, body = get(t, handler, "/readyz")
	if status != http.StatusServiceUnavailable {
		t.Errorf("/readyz status = %d for a gapped stream, want %d (%s)",
			status, http.StatusServiceUnavailable, body)
	}
	if !strings.Contains(body, "gapped") {
		t.Errorf("/readyz body %q does not mention the gap", body)
	}
}

// TestHealthServerMetrics covers the Prometheus endpoint: the lag and the
// health of the stream have to be there to be scraped.
func TestHealthServerMetrics(t *testing.T) {
	t.Parallel()

	monitor := stream.NewHealthMonitor(stream.HealthMonitorOptions{MaxLag: 100})
	server, err := stream.NewHealthServer(stream.HealthServerOptions{
		Addr:    "127.0.0.1:0",
		Monitor: monitor,
	})
	if err != nil {
		t.Fatalf("NewHealthServer() error = %v", err)
	}
	monitor.RecordProgress(stream.Progress{CurrentLedger: 90, LatestLedger: 100})

	status, body := get(t, server.Handler(), "/metrics")
	if status != http.StatusOK {
		t.Fatalf("/metrics status = %d, want %d", status, http.StatusOK)
	}
	for _, want := range []string{
		"prism_stream_lag 10",
		"prism_stream_current_ledger 90",
		"prism_stream_latest_ledger 100",
		"prism_health_alive 1",
		"prism_health_ready 1",
		"prism_health_up 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics does not contain %q\n%s", want, body)
		}
	}
}

// TestHealthServerRejectsWrites checks that the probe endpoints only answer
// reads, so a stray POST cannot be mistaken for a probe.
func TestHealthServerRejectsWrites(t *testing.T) {
	t.Parallel()

	monitor := stream.NewHealthMonitor(stream.HealthMonitorOptions{})
	server, err := stream.NewHealthServer(stream.HealthServerOptions{
		Addr:    "127.0.0.1:0",
		Monitor: monitor,
	})
	if err != nil {
		t.Fatalf("NewHealthServer() error = %v", err)
	}

	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s status = %d, want %d", path, recorder.Code, http.StatusMethodNotAllowed)
		}
	}
}

// TestHealthServerServesOverTheNetwork checks that the server really binds
// and answers, which is what an orchestrator's probe does.
func TestHealthServerServesOverTheNetwork(t *testing.T) {
	t.Parallel()

	monitor := stream.NewHealthMonitor(stream.HealthMonitorOptions{})
	server, err := stream.NewHealthServer(stream.HealthServerOptions{
		Addr:    "127.0.0.1:0",
		Monitor: monitor,
	})
	if err != nil {
		t.Fatalf("NewHealthServer() error = %v", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	addr := server.Addr()
	if addr == "" {
		t.Fatal("Addr() is empty while the server is serving")
	}

	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz status = %d, want %d (%s)", resp.StatusCode, http.StatusOK, body)
	}
}

// TestHealthMonitorStandbyIsAliveButNotReady covers the active-passive case:
// a standby is waiting for the lease, not stuck, so it must not be reported
// as dead, and it must not be sent traffic.
func TestHealthMonitorStandbyIsAliveButNotReady(t *testing.T) {
	t.Parallel()

	monitor := stream.NewHealthMonitor(stream.HealthMonitorOptions{
		MaxLag:     10,
		StaleAfter: 10 * time.Millisecond,
	})
	monitor.RecordProgress(stream.Progress{CurrentLedger: 10, LatestLedger: 10})
	monitor.SetActive(false)

	time.Sleep(30 * time.Millisecond)
	snapshot := monitor.Snapshot()
	if !snapshot.Alive {
		t.Error("a standby was reported as not alive")
	}
	if snapshot.Ready {
		t.Error("a standby was reported as ready")
	}
	if !strings.Contains(snapshot.Reason, "standby") {
		t.Errorf("Reason = %q, want it to say the instance is a standby", snapshot.Reason)
	}
}

// TestHealthServerNeedsAnAddressAndAMonitor checks the construction guards.
func TestHealthServerNeedsAnAddressAndAMonitor(t *testing.T) {
	t.Parallel()

	if _, err := stream.NewHealthServer(stream.HealthServerOptions{
		Monitor: stream.NewHealthMonitor(stream.HealthMonitorOptions{}),
	}); err == nil {
		t.Error("NewHealthServer() error = nil without an address")
	}
	if _, err := stream.NewHealthServer(stream.HealthServerOptions{
		Addr: ":8086",
	}); err == nil {
		t.Error("NewHealthServer() error = nil without a monitor")
	}
}
