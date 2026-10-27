package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// ObjectStore uploads an immutable object to a bucket.
//
// It is the seam that keeps the chunking logic testable without a network:
// S3Store and GCSStore implement it, and tests substitute an in-memory one.
// Implementations must treat key as an opaque path and overwrite an existing
// object with the same key, since a retried upload reuses it.
type ObjectStore interface {
	PutObject(ctx context.Context, key string, body []byte, contentType string) error
}

const (
	defaultCloudBatchRecords  = 1000
	defaultCloudBatchBytes    = 8 << 20
	defaultCloudUploadTimeout = 30 * time.Second
)

// CloudStorageOptions configures a CloudStorage sink.
type CloudStorageOptions struct {
	// Store receives the uploaded chunks. Required.
	Store ObjectStore
	// Prefix is the key prefix every object shares, for example
	// "prism/mainnet". An empty prefix writes at the bucket root.
	Prefix string
	// BatchRecords flushes a chunk after this many records. Zero uses 1000.
	BatchRecords int
	// BatchBytes flushes before a write would push the chunk past this many
	// bytes. A single record larger than the cap is still uploaded whole.
	// Zero uses 8MiB.
	BatchBytes int
	// MaxDelay uploads a partial chunk after this long, so a quiet stream
	// does not sit on data indefinitely. Zero disables the timer.
	MaxDelay time.Duration
	// Compress gzips each chunk and names it .ndjson.gz.
	Compress bool
	// Decode embeds decoded XDR in the archived records, matching a stream
	// run without --raw.
	Decode bool
	// MaxRetries is how many times a failed upload is retried after the first
	// attempt. Zero uses 3; a negative value disables retries.
	MaxRetries int
	// InitialBackoff is the delay before the first retry, doubling each
	// attempt. Zero uses 100ms.
	InitialBackoff time.Duration
	// MaxBackoff caps a single retry delay. Zero uses 5s.
	MaxBackoff time.Duration
	// Jitter randomises retry delays by this fraction. Zero disables it.
	Jitter float64
	// UploadTimeout bounds a single upload. Zero uses 30s.
	UploadTimeout time.Duration
	// Now is the clock used to partition keys by date. Tests set it; nil means
	// time.Now.
	Now func() time.Time
	// Sleep waits between retries. Tests replace it to avoid real delays; nil
	// means time.Sleep.
	Sleep func(time.Duration)
	// OnError observes failures from the MaxDelay timer, which cannot return
	// an error to Emit. The same error is surfaced by the next Emit, Flush or
	// Close.
	OnError func(error)
}

// CloudStorage archives stream records as NDJSON "blocks" in an object store.
//
// A chunk is assembled in memory and uploaded once it reaches BatchRecords,
// BatchBytes or MaxDelay, whichever comes first. Object keys derive only from
// the records themselves, so a retried upload overwrites the same key instead
// of creating a duplicate, and an interrupted process leaves at most one
// partial chunk behind.
//
// Like the webhook sink, delivery is at-least-once: a failed upload stays
// buffered and is retried by a later Flush or Close.
type CloudStorage struct {
	opts  CloudStorageOptions
	now   func() time.Time
	sleep func(time.Duration)

	mu       sync.Mutex
	buf      bytes.Buffer
	count    int
	first    stream.Record
	last     stream.Record
	timer    *time.Timer
	closed   bool
	asyncErr error
}

// NewCloudStorage validates opts and builds a chunked object store sink.
func NewCloudStorage(opts CloudStorageOptions) (*CloudStorage, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("cloud storage: Store is required")
	}
	if opts.BatchRecords < 0 {
		return nil, fmt.Errorf("cloud storage: BatchRecords must not be negative")
	}
	if opts.BatchRecords == 0 {
		opts.BatchRecords = defaultCloudBatchRecords
	}
	if opts.BatchBytes < 0 {
		return nil, fmt.Errorf("cloud storage: BatchBytes must not be negative")
	}
	if opts.BatchBytes == 0 {
		opts.BatchBytes = defaultCloudBatchBytes
	}
	if opts.MaxDelay < 0 {
		return nil, fmt.Errorf("cloud storage: MaxDelay must not be negative")
	}
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	} else if opts.MaxRetries == 0 {
		opts.MaxRetries = defaultWebhookRetries
	}
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = defaultWebhookInitial
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = defaultWebhookMaxBackoff
	}
	if opts.MaxBackoff < opts.InitialBackoff {
		opts.MaxBackoff = opts.InitialBackoff
	}
	if opts.Jitter < 0 || opts.Jitter > 1 {
		return nil, fmt.Errorf("cloud storage: Jitter must be between 0 and 1")
	}
	if opts.UploadTimeout <= 0 {
		opts.UploadTimeout = defaultCloudUploadTimeout
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}
	sleep := opts.Sleep
	if sleep == nil {
		sleep = time.Sleep
	}
	return &CloudStorage{opts: opts, now: now, sleep: sleep}, nil
}

// Emit implements stream.Sink.
func (c *CloudStorage) Emit(r stream.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.emitLocked(r)
}

// EmitBatch implements stream.BatchSink.
func (c *CloudStorage) EmitBatch(records []stream.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, r := range records {
		if err := c.emitLocked(r); err != nil {
			return err
		}
	}
	return nil
}

func (c *CloudStorage) emitLocked(r stream.Record) error {
	if c.closed {
		return fmt.Errorf("cloud storage: emit after close")
	}
	if c.asyncErr != nil {
		return c.asyncErr
	}

	line, err := MarshalRecord(r, c.opts.Decode)
	if err != nil {
		return fmt.Errorf("cloud storage: encoding record %s: %w", r.ID, err)
	}
	line = append(line, '\n')

	// Flush before adding when this record would break the byte cap, so a
	// chunk never exceeds BatchBytes unless one record alone does.
	if c.count > 0 && c.opts.BatchBytes > 0 && c.buf.Len()+len(line) > c.opts.BatchBytes {
		if err := c.flushLocked(); err != nil {
			return err
		}
	}

	if c.count == 0 {
		c.first = r
	}
	c.last = r
	c.count++
	_, writeErr := c.buf.Write(line)
	if writeErr != nil {
		return fmt.Errorf("cloud storage: buffering record %s: %w", r.ID, writeErr)
	}

	if c.count >= c.opts.BatchRecords || c.buf.Len() >= c.opts.BatchBytes {
		return c.flushLocked()
	}
	if c.opts.MaxDelay > 0 && c.timer == nil {
		c.timer = time.AfterFunc(c.opts.MaxDelay, c.flushOnTimer)
	}
	return nil
}

// Flush implements stream.Sink by uploading the current partial chunk.
func (c *CloudStorage) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.flushLocked()
}

// Close flushes the partial chunk and stops the timer. It is safe to call more
// than once.
func (c *CloudStorage) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Keep retrying a chunk a previous Close could not upload, while still
	// rejecting new records through Emit.
	c.closed = true
	return c.flushLocked()
}

func (c *CloudStorage) flushOnTimer() {
	c.mu.Lock()
	if c.closed || c.timer == nil {
		c.mu.Unlock()
		return
	}
	err := c.flushLocked()
	onError := c.opts.OnError
	c.mu.Unlock()

	if err != nil && onError != nil {
		onError(err)
	}
}

func (c *CloudStorage) flushLocked() error {
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	if c.count == 0 {
		return nil
	}

	body := append([]byte(nil), c.buf.Bytes()...)
	key := c.objectKey()
	first, last := c.first, c.last
	count := c.count

	c.buf.Reset()
	c.count = 0
	c.first = stream.Record{}
	c.last = stream.Record{}

	if err := c.upload(key, body); err != nil {
		// Put the chunk back so a transient failure does not lose data; the
		// key is derived from the records, so a retry overwrites rather than
		// duplicating the object.
		c.buf.Write(body)
		c.count = count
		c.first = first
		c.last = last
		c.asyncErr = err
		return err
	}
	c.asyncErr = nil
	return nil
}

func (c *CloudStorage) objectKey() string {
	at := c.first.Time
	if at.IsZero() {
		at = c.now()
	}
	date := at.UTC().Format("2006/01/02")

	first := sanitizeKeySegment(c.first.ID)
	last := sanitizeKeySegment(c.last.ID)
	name := fmt.Sprintf("%s-%s.ndjson", first, last)
	if c.opts.Compress {
		name += ".gz"
	}

	key := path.Join(date, fmt.Sprintf("ledger-%010d-%010d", c.first.Sequence, c.last.Sequence), name)
	if prefix := strings.Trim(strings.TrimSpace(c.opts.Prefix), "/"); prefix != "" {
		key = path.Join(prefix, key)
	}
	return key
}

// sanitizeKeySegment keeps a record id from escaping its path segment. Record
// ids are fixed-width numeric today, but a future source must not be able to
// write outside the configured prefix.
func sanitizeKeySegment(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	if s == "" {
		return "record"
	}
	return s
}

func (c *CloudStorage) upload(key string, body []byte) error {
	contentType := "application/x-ndjson"
	if c.opts.Compress {
		compressed, err := gzipBytes(body)
		if err != nil {
			return err
		}
		body = compressed
		contentType = "application/gzip"
	}

	var lastErr error
	for attempt := 0; attempt <= c.opts.MaxRetries; attempt++ {
		if attempt > 0 {
			c.sleep(backoffDelay(c.opts.InitialBackoff, c.opts.MaxBackoff, attempt, c.opts.Jitter))
		}

		ctx, cancel := context.WithTimeout(context.Background(), c.opts.UploadTimeout)
		err := c.opts.Store.PutObject(ctx, key, body, contentType)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf("cloud storage: uploading %s: %w", key, lastErr)
}

// gzipBytes compresses body at the default level. The archive is small and
// built in memory: a chunk is already bounded by BatchBytes.
func gzipBytes(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(body); err != nil {
		closeErr := gz.Close()
		return nil, errors.Join(
			fmt.Errorf("cloud storage: compressing chunk: %w", err),
			closeErr,
		)
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("cloud storage: compressing chunk: %w", err)
	}
	return buf.Bytes(), nil
}
