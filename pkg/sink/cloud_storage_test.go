package sink_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type storedObject struct {
	key         string
	body        []byte
	contentType string
}

// fakeObjectStore records uploads and can be told to fail a number of initial
// calls, which is what exercises the retry path without a network.
type fakeObjectStore struct {
	mu        sync.Mutex
	objects   []storedObject
	calls     int
	failFirst int
	notify    chan struct{}
}

func (f *fakeObjectStore) PutObject(_ context.Context, key string, body []byte, contentType string) error {
	f.mu.Lock()
	f.calls++
	call := f.calls
	fail := call <= f.failFirst
	notify := f.notify
	if !fail {
		f.objects = append(f.objects, storedObject{
			key:         key,
			body:        append([]byte(nil), body...),
			contentType: contentType,
		})
	}
	f.mu.Unlock()

	if notify != nil {
		select {
		case notify <- struct{}{}:
		default:
		}
	}
	if fail {
		return errors.New("injected upload failure")
	}
	return nil
}

func (f *fakeObjectStore) stopFailing() {
	f.mu.Lock()
	f.failFirst = 0
	f.mu.Unlock()
}

func (f *fakeObjectStore) uploaded() []storedObject {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]storedObject(nil), f.objects...)
}

func (f *fakeObjectStore) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func cloudLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}

func cloudRecords(n int) []stream.Record {
	records := make([]stream.Record, 0, n)
	for i := range n {
		r := eventRecord()
		r.ID = fmt.Sprintf("%020d", i+1)
		r.Sequence = uint32(100 + i)
		r.Time = time.Unix(1_700_000_000+int64(i), 0).UTC()
		records = append(records, r)
	}
	return records
}

func TestCloudStorageUploadsChunks(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{}
	s, err := sink.NewCloudStorage(sink.CloudStorageOptions{
		Store:        store,
		BatchRecords: 2,
	})
	if err != nil {
		t.Fatalf("NewCloudStorage() error = %v", err)
	}

	records := cloudRecords(5)
	if err := s.EmitBatch(records); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	if got := len(store.uploaded()); got != 2 {
		t.Fatalf("uploaded %d objects, want 2 full chunks", got)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	objects := store.uploaded()
	if len(objects) != 3 {
		t.Fatalf("uploaded %d objects, want 3 after Flush", len(objects))
	}

	wantLines := []int{2, 2, 1}
	for i, obj := range objects {
		if !strings.HasPrefix(obj.key, "2023/11/14/ledger-") {
			t.Errorf("object %d key = %q, want a dated ledger prefix", i, obj.key)
		}
		if !strings.HasSuffix(obj.key, ".ndjson") {
			t.Errorf("object %d key = %q, want an .ndjson suffix", i, obj.key)
		}
		if obj.contentType != "application/x-ndjson" {
			t.Errorf("object %d content type = %q, want application/x-ndjson", i, obj.contentType)
		}
		lines := cloudLines(string(obj.body))
		if len(lines) != wantLines[i] {
			t.Errorf("object %d has %d lines, want %d\n%s", i, len(lines), wantLines[i], obj.body)
		}
		for _, line := range lines {
			if !strings.Contains(line, `"kind":"event"`) {
				t.Errorf("object %d line is not a record: %s", i, line)
			}
		}
	}

	if objects[0].key == objects[1].key {
		t.Errorf("two chunks share the key %q; a retry would overwrite a different chunk", objects[0].key)
	}
}

func TestCloudStorageCompressesChunks(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{}
	s, err := sink.NewCloudStorage(sink.CloudStorageOptions{
		Store:    store,
		Compress: true,
	})
	if err != nil {
		t.Fatalf("NewCloudStorage() error = %v", err)
	}
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	objects := store.uploaded()
	if len(objects) != 1 {
		t.Fatalf("uploaded %d objects, want 1", len(objects))
	}
	obj := objects[0]
	if !strings.HasSuffix(obj.key, ".ndjson.gz") {
		t.Errorf("key = %q, want .ndjson.gz", obj.key)
	}
	if obj.contentType != "application/gzip" {
		t.Errorf("content type = %q, want application/gzip", obj.contentType)
	}

	reader, err := gzip.NewReader(bytes.NewReader(obj.body))
	if err != nil {
		t.Fatalf("body is not gzip: %v", err)
	}
	defer func() { _ = reader.Close() }()
	plain, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading gzip body: %v", err)
	}
	if !strings.Contains(string(plain), `"kind":"event"`) {
		t.Errorf("compressed object does not contain the record\n%s", plain)
	}
}

func TestCloudStorageSplitsByBytes(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{}
	s, err := sink.NewCloudStorage(sink.CloudStorageOptions{
		Store:        store,
		BatchRecords: 1000,
		BatchBytes:   400,
	})
	if err != nil {
		t.Fatalf("NewCloudStorage() error = %v", err)
	}

	records := cloudRecords(4)
	if err := s.EmitBatch(records); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	objects := store.uploaded()
	if len(objects) != len(records) {
		t.Fatalf("uploaded %d objects at a 400-byte cap, want one per oversized record", len(objects))
	}
	total := 0
	for i, obj := range objects {
		total += len(cloudLines(string(obj.body)))
		if len(obj.body) > 400 {
			t.Errorf("object %d is %d bytes, over the 400-byte cap", i, len(obj.body))
		}
	}
	if total != len(records) {
		t.Errorf("objects contain %d records, want %d", total, len(records))
	}
}

func TestCloudStorageRetriesUploads(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{failFirst: 2}
	var delays []time.Duration
	s, err := sink.NewCloudStorage(sink.CloudStorageOptions{
		Store:          store,
		BatchRecords:   1,
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		Sleep:          func(d time.Duration) { delays = append(delays, d) },
	})
	if err != nil {
		t.Fatalf("NewCloudStorage() error = %v", err)
	}

	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v, want the upload to recover", err)
	}
	if got := store.attempts(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	if len(delays) != len(want) {
		t.Fatalf("delays = %v, want %v", delays, want)
	}
	for i := range want {
		if delays[i] != want[i] {
			t.Errorf("delay[%d] = %s, want %s", i, delays[i], want[i])
		}
	}
	if got := len(store.uploaded()); got != 1 {
		t.Errorf("uploaded %d objects, want 1 after recovery", got)
	}
}

func TestCloudStorageKeepsChunkAfterFailure(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{failFirst: 1 << 20}
	s, err := sink.NewCloudStorage(sink.CloudStorageOptions{
		Store:        store,
		BatchRecords: 1,
		MaxRetries:   -1,
		Sleep:        func(time.Duration) {},
	})
	if err != nil {
		t.Fatalf("NewCloudStorage() error = %v", err)
	}

	if err := s.Emit(eventRecord()); err == nil {
		t.Fatal("Emit() error = nil, want the failed upload")
	}
	before := store.attempts()

	store.stopFailing()
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() after recovery error = %v", err)
	}
	if got := store.attempts(); got != before+1 {
		t.Errorf("attempts = %d, want one more than %d", got, before)
	}

	objects := store.uploaded()
	if len(objects) != 1 {
		t.Fatalf("uploaded %d objects, want the retained chunk", len(objects))
	}
	if !strings.Contains(string(objects[0].body), `"kind":"event"`) {
		t.Errorf("retained chunk lost the record\n%s", objects[0].body)
	}
}

func TestCloudStorageFlushesOnDelay(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{notify: make(chan struct{}, 1)}
	s, err := sink.NewCloudStorage(sink.CloudStorageOptions{
		Store:        store,
		BatchRecords: 1000,
		MaxDelay:     10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewCloudStorage() error = %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	select {
	case <-store.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("partial chunk was not uploaded after MaxDelay")
	}
	if got := len(store.uploaded()); got != 1 {
		t.Errorf("uploaded %d objects, want 1", got)
	}
}

func TestCloudStorageCloseFlushes(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{}
	s, err := sink.NewCloudStorage(sink.CloudStorageOptions{Store: store})
	if err != nil {
		t.Fatalf("NewCloudStorage() error = %v", err)
	}
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := len(store.uploaded()); got != 1 {
		t.Errorf("uploaded %d objects on close, want 1", got)
	}
	if err := s.Emit(eventRecord()); err == nil {
		t.Error("Emit() after Close error = nil, want an error")
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}

func TestCloudStoragePrefix(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{}
	s, err := sink.NewCloudStorage(sink.CloudStorageOptions{
		Store:  store,
		Prefix: "/prism/mainnet/",
	})
	if err != nil {
		t.Fatalf("NewCloudStorage() error = %v", err)
	}
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	objects := store.uploaded()
	if len(objects) != 1 {
		t.Fatalf("uploaded %d objects, want 1", len(objects))
	}
	if !strings.HasPrefix(objects[0].key, "prism/mainnet/") {
		t.Errorf("key = %q, want the configured prefix", objects[0].key)
	}
	if strings.Contains(objects[0].key, "//") {
		t.Errorf("key = %q, want no doubled separator", objects[0].key)
	}
}

func TestCloudStorageRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts sink.CloudStorageOptions
	}{
		{name: "missing store"},
		{name: "negative records", opts: sink.CloudStorageOptions{Store: &fakeObjectStore{}, BatchRecords: -1}},
		{name: "negative bytes", opts: sink.CloudStorageOptions{Store: &fakeObjectStore{}, BatchBytes: -1}},
		{name: "negative delay", opts: sink.CloudStorageOptions{Store: &fakeObjectStore{}, MaxDelay: -time.Second}},
		{name: "jitter out of range", opts: sink.CloudStorageOptions{Store: &fakeObjectStore{}, Jitter: 1.5}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sink.NewCloudStorage(tc.opts); err == nil {
				t.Fatal("NewCloudStorage() error = nil, want a validation failure")
			}
		})
	}
}

func TestCloudStorageRejectsUnknownKind(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{}
	s, err := sink.NewCloudStorage(sink.CloudStorageOptions{Store: store})
	if err != nil {
		t.Fatalf("NewCloudStorage() error = %v", err)
	}
	if err := s.Emit(stream.Record{ID: "x", Kind: stream.Kind("nonsense")}); err == nil {
		t.Fatal("Emit(unknown kind) error = nil, want an encoding failure")
	}
	if got := len(store.uploaded()); got != 0 {
		t.Errorf("uploaded %d objects for an invalid record, want 0", got)
	}
}
