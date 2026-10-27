package sink_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
)

func TestGCSStorePutObject(t *testing.T) {
	t.Parallel()

	type captured struct {
		method string
		path   string
		query  string
		header http.Header
		body   []byte
	}
	got := make(chan captured, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading body: %v", err)
		}
		got <- captured{
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.RawQuery,
			header: r.Header.Clone(),
			body:   body,
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	store, err := sink.NewGCSStore(sink.GCSOptions{
		Endpoint:    srv.URL,
		Bucket:      "examplebucket",
		AccessToken: "ya29.token",
	})
	if err != nil {
		t.Fatalf("NewGCSStore() error = %v", err)
	}

	body := []byte(`{"kind":"event"}` + "\n")
	if err := store.PutObject(context.Background(), "2023/11/14/chunk.ndjson", body, "application/x-ndjson"); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	req := <-got
	if req.method != http.MethodPost {
		t.Errorf("method = %s, want POST for a simple media upload", req.method)
	}
	if req.path != "/upload/storage/v1/b/examplebucket/o" {
		t.Errorf("path = %q, want the JSON upload endpoint", req.path)
	}
	query := req.query
	for _, want := range []string{"uploadType=media", "name=2023%2F11%2F14%2Fchunk.ndjson"} {
		if !strings.Contains(query, want) {
			t.Errorf("query = %q, want it to contain %q", query, want)
		}
	}
	if got := req.header.Get("Authorization"); got != "Bearer ya29.token" {
		t.Errorf("Authorization = %q, want a bearer token", got)
	}
	if got := req.header.Get("Content-Type"); got != "application/x-ndjson" {
		t.Errorf("Content-Type = %q, want application/x-ndjson", got)
	}
	if string(req.body) != string(body) {
		t.Errorf("body = %q, want %q", req.body, body)
	}
}

func TestGCSStoreReportsHTTPFailure(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "permission denied", http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)

	store, err := sink.NewGCSStore(sink.GCSOptions{
		Endpoint:    srv.URL,
		Bucket:      "bucket",
		AccessToken: "token",
	})
	if err != nil {
		t.Fatalf("NewGCSStore() error = %v", err)
	}

	err = store.PutObject(context.Background(), "chunk.ndjson", []byte("x"), "application/x-ndjson")
	if err == nil {
		t.Fatal("PutObject() error = nil, want a failure")
	}
	for _, want := range []string{"Forbidden", "permission denied"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
}

func TestGCSStoreValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts sink.GCSOptions
	}{
		{name: "missing bucket", opts: sink.GCSOptions{AccessToken: "token"}},
		{name: "missing token", opts: sink.GCSOptions{Bucket: "bucket"}},
		{name: "non-http endpoint", opts: sink.GCSOptions{Endpoint: "gs://bucket", Bucket: "bucket", AccessToken: "token"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sink.NewGCSStore(tc.opts); err == nil {
				t.Fatal("NewGCSStore() error = nil, want a validation failure")
			}
		})
	}

	if _, err := sink.NewGCSStore(sink.GCSOptions{Bucket: "bucket", AccessToken: "token"}); err != nil {
		t.Fatalf("NewGCSStore(default endpoint) error = %v", err)
	}
}
