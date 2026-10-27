package sink_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
)

func TestS3StorePutObject(t *testing.T) {
	t.Parallel()

	type captured struct {
		method string
		path   string
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
			path:   r.URL.EscapedPath(),
			header: r.Header.Clone(),
			body:   body,
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	store, err := sink.NewS3Store(sink.S3Options{
		Endpoint:  srv.URL,
		Bucket:    "examplebucket",
		Region:    "us-east-1",
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Now:       func() time.Time { return time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("NewS3Store() error = %v", err)
	}

	body := []byte("Welcome to Amazon S3.")
	if err := store.PutObject(context.Background(), "dir/test$file.ndjson", body, "application/x-ndjson"); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	req := <-got
	if req.method != http.MethodPut {
		t.Errorf("method = %s, want PUT", req.method)
	}
	if req.path != "/examplebucket/dir/test%24file.ndjson" {
		t.Errorf("path = %q, want the object key with %%24 escaped", req.path)
	}
	if string(req.body) != string(body) {
		t.Errorf("body = %q, want %q", req.body, body)
	}
	if got := req.header.Get("Content-Type"); got != "application/x-ndjson" {
		t.Errorf("Content-Type = %q, want application/x-ndjson", got)
	}
	if got := req.header.Get("X-Amz-Date"); got != "20130524T000000Z" {
		t.Errorf("X-Amz-Date = %q, want the injected clock", got)
	}

	sum := sha256.Sum256(body)
	if got := req.header.Get("X-Amz-Content-Sha256"); got != hex.EncodeToString(sum[:]) {
		t.Errorf("X-Amz-Content-Sha256 = %q, want the payload hash", got)
	}

	auth := req.header.Get("Authorization")
	wantPrefix := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date,Signature="
	if !strings.HasPrefix(auth, wantPrefix) {
		t.Errorf("Authorization = %q, want prefix %q", auth, wantPrefix)
	}
	if signature := strings.TrimPrefix(auth, wantPrefix); len(signature) != 64 {
		t.Errorf("signature = %q, want 64 hex characters", signature)
	}
}

func TestS3StoreVirtualHostedStyle(t *testing.T) {
	t.Parallel()

	var (
		gotURL  string
		gotHost string
		gotAuth string
	)

	client := &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		gotURL = req.URL.String()
		gotHost = req.Host
		gotAuth = req.Header.Get("Authorization")
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})}

	store, err := sink.NewS3Store(sink.S3Options{
		Endpoint:      "https://s3.us-east-1.amazonaws.com",
		Bucket:        "examplebucket",
		AccessKey:     "AKIAIOSFODNN7EXAMPLE",
		SecretKey:     "secret",
		Client:        client,
		VirtualHosted: true,
		Now:           func() time.Time { return time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("NewS3Store() error = %v", err)
	}
	if err := store.PutObject(context.Background(), "test.txt", []byte("x"), "text/plain"); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	if gotURL != "https://examplebucket.s3.us-east-1.amazonaws.com/test.txt" {
		t.Errorf("URL = %q, want the bucket in the host", gotURL)
	}
	if gotHost != "examplebucket.s3.us-east-1.amazonaws.com" {
		t.Errorf("Host = %q, want the virtual-hosted host", gotHost)
	}
	if !strings.Contains(gotAuth, "SignedHeaders=content-type;host;") {
		t.Errorf("Authorization = %q, want host signed", gotAuth)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestS3StoreReportsHTTPFailure(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "<Error>AccessDenied</Error>", http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)

	store, err := sink.NewS3Store(sink.S3Options{
		Endpoint:  srv.URL,
		Bucket:    "bucket",
		AccessKey: "key",
		SecretKey: "secret",
	})
	if err != nil {
		t.Fatalf("NewS3Store() error = %v", err)
	}

	err = store.PutObject(context.Background(), "test.txt", []byte("x"), "text/plain")
	if err == nil {
		t.Fatal("PutObject() error = nil, want a failure")
	}
	for _, want := range []string{"Forbidden", "AccessDenied"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
}

func TestS3StoreValidation(t *testing.T) {
	t.Parallel()

	valid := sink.S3Options{
		Endpoint:  "https://s3.us-east-1.amazonaws.com",
		Bucket:    "bucket",
		AccessKey: "key",
		SecretKey: "secret",
	}

	tests := []struct {
		name   string
		mutate func(*sink.S3Options)
	}{
		{name: "missing endpoint", mutate: func(o *sink.S3Options) { o.Endpoint = "" }},
		{name: "non-http endpoint", mutate: func(o *sink.S3Options) { o.Endpoint = "s3://bucket" }},
		{name: "missing host", mutate: func(o *sink.S3Options) { o.Endpoint = "https://" }},
		{name: "missing bucket", mutate: func(o *sink.S3Options) { o.Bucket = "" }},
		{name: "missing access key", mutate: func(o *sink.S3Options) { o.AccessKey = "" }},
		{name: "missing secret", mutate: func(o *sink.S3Options) { o.SecretKey = "" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := valid
			tc.mutate(&opts)
			if _, err := sink.NewS3Store(opts); err == nil {
				t.Fatal("NewS3Store() error = nil, want a validation failure")
			}
		})
	}

	if _, err := sink.NewS3Store(valid); err != nil {
		t.Fatalf("NewS3Store(valid) error = %v", err)
	}
}
