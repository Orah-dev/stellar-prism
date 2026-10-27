package sink_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestElasticsearchSinkBulkUsesDailyIndexAndRecordID(t *testing.T) {
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_bulk" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "ApiKey test-key" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		data, _ := io.ReadAll(r.Body)
		body = string(data)
		_, _ = w.Write([]byte(`{"errors":false,"items":[]}`))
	}))
	defer server.Close()
	s, err := sink.NewElasticsearchSink(sink.ElasticsearchOptions{URL: server.URL, Index: "stellar-events", APIKey: "test-key", BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 10, 22, 23, 0, 0, 0, time.FixedZone("late", -5*3600))
	if err := s.Emit(stream.Record{ID: "fixed-id", Time: day, Kind: stream.KindEvent}); err != nil {
		t.Fatal(err)
	}
	if err := s.Emit(stream.Record{ID: "next", Time: day, Kind: stream.KindEvent}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 4 {
		t.Fatalf("bulk lines = %d, want 4: %q", len(lines), body)
	}
	var action map[string]map[string]string
	if err := json.Unmarshal([]byte(lines[0]), &action); err != nil {
		t.Fatal(err)
	}
	if action["index"]["_id"] != "fixed-id" || action["index"]["_index"] != "stellar-events-2026.10.23" {
		t.Errorf("action = %#v", action)
	}
}

func TestElasticsearchSinkRetainsBatchAfterFailure(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer server.Close()
	s, err := sink.NewElasticsearchSink(sink.ElasticsearchOptions{URL: server.URL, Index: "events", BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Emit(stream.Record{ID: "one", Kind: stream.KindEvent}); err == nil {
		t.Fatal("first Emit error = nil")
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush retry error = %v", err)
	}
	if calls != 2 {
		t.Errorf("calls = %d, want 2", calls)
	}
}
