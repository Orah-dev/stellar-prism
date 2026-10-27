package main

import (
	"bytes"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"
)

func getProfile(t *testing.T, client *http.Client, url string) (int, []byte) {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("closing %s response: %v", url, err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", url, err)
	}
	return resp.StatusCode, body
}

func TestStartPProfServesRuntimeProfiles(t *testing.T) {
	var stderr bytes.Buffer
	srv, err := startPProf(0, &stderr)
	if err != nil {
		t.Fatalf("startPProf() error = %v", err)
	}
	defer func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	if srv.Addr() == "" {
		t.Fatal("Addr() is empty")
	}
	if !strings.Contains(stderr.String(), "serving pprof on http://") {
		t.Errorf("stderr does not announce the pprof server: %q", stderr.String())
	}

	client := &http.Client{Timeout: 10 * time.Second}
	tests := []struct {
		path string
		want string
	}{
		{path: "/debug/pprof/", want: "heap"},
		{path: "/debug/pprof/heap?debug=1", want: "heap"},
		{path: "/debug/pprof/allocs?debug=1", want: "allocs"},
		{path: "/debug/pprof/goroutine?debug=1", want: "goroutine"},
		{path: "/debug/pprof/block?debug=1", want: ""},
		{path: "/debug/pprof/mutex?debug=1", want: ""},
		{path: "/debug/pprof/threadcreate?debug=1", want: ""},
		{path: "/debug/pprof/cmdline", want: ""},
		{path: "/debug/pprof/symbol", want: "num_symbols"},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			status, body := getProfile(t, client, "http://"+srv.Addr()+tc.path)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", status, body)
			}
			if len(body) == 0 {
				t.Fatal("profile response is empty")
			}
			if tc.want != "" && !bytes.Contains(body, []byte(tc.want)) {
				t.Errorf("response does not contain %q\n%s", tc.want, body)
			}
		})
	}
}

func TestNewPProfProfiler(t *testing.T) {
	profiler, err := newPProfProfiler(0, io.Discard)
	if err != nil {
		t.Fatalf("newPProfProfiler() error = %v", err)
	}
	if profiler == nil {
		t.Fatal("newPProfProfiler() returned nil")
	}
	if err := profiler.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestPProfServerServesCPUProfile(t *testing.T) {
	srv, err := startPProf(0, io.Discard)
	if err != nil {
		t.Fatalf("startPProf() error = %v", err)
	}
	defer func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	client := &http.Client{Timeout: 10 * time.Second}
	status, body := getProfile(t, client, "http://"+srv.Addr()+"/debug/pprof/profile?seconds=1")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", status, body)
	}
	if len(body) == 0 {
		t.Fatal("CPU profile is empty")
	}
}

func TestPProfServerCloseIsIdempotentAndStopsServing(t *testing.T) {
	srv, err := startPProf(0, io.Discard)
	if err != nil {
		t.Fatalf("startPProf() error = %v", err)
	}
	addr := srv.Addr()

	if err := srv.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	client := &http.Client{Timeout: 250 * time.Millisecond}
	resp, err := client.Get("http://" + addr + "/debug/pprof/")
	if err == nil {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Errorf("closing the response body: %v", closeErr)
		}
		t.Fatal("the pprof server still answered after Close")
	}
}

func TestStartPProfRejectsInvalidPorts(t *testing.T) {
	for _, port := range []int{-1, 70000} {
		if _, err := startPProf(port, io.Discard); err == nil {
			t.Errorf("startPProf(%d) error = nil, want a range failure", port)
		}
	}
}

func TestPProfServerLeavesNoGoroutineBehind(t *testing.T) {
	before := runtime.NumGoroutine()
	srv, err := startPProf(0, io.Discard)
	if err != nil {
		t.Fatalf("startPProf() error = %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if runtime.NumGoroutine() <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines = %d, want no more than %d", runtime.NumGoroutine(), before)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
