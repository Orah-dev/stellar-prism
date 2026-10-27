package sink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type ElasticsearchOptions struct {
	URL       string
	Index     string
	Username  string
	Password  string
	APIKey    string
	BatchSize int
	MaxDelay  time.Duration
	// Decode controls whether XDR fields are rendered using the same schema as
	// --json. The CLI sets it to !--raw.
	Decode bool
	Client *http.Client
	Now    func() time.Time
}

// ElasticsearchSink indexes records through the Elasticsearch/OpenSearch bulk
// API. Each record's stable ID is the document ID, making retries idempotent.
type ElasticsearchSink struct {
	opts     ElasticsearchOptions
	http     *http.Client
	now      func() time.Time
	mu       sync.Mutex
	buf      []stream.Record
	timer    *time.Timer
	closed   bool
	asyncErr error
}

func NewElasticsearchSink(opts ElasticsearchOptions) (*ElasticsearchSink, error) {
	u, err := url.Parse(strings.TrimSpace(opts.URL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("elasticsearch: URL must be an absolute http(s) URL")
	}
	if strings.TrimSpace(opts.Index) == "" {
		return nil, fmt.Errorf("elasticsearch: index is required")
	}
	if opts.BatchSize < 0 || opts.MaxDelay < 0 {
		return nil, fmt.Errorf("elasticsearch: batch size and delay must not be negative")
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 100
	}
	if opts.Client == nil {
		opts.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &ElasticsearchSink{opts: opts, http: opts.Client, now: opts.Now}, nil
}

func (s *ElasticsearchSink) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("elasticsearch: emit after close")
	}
	if s.asyncErr != nil {
		return s.asyncErr
	}
	s.buf = append(s.buf, r)
	if len(s.buf) >= s.opts.BatchSize {
		return s.flushLocked()
	}
	if s.opts.MaxDelay > 0 && s.timer == nil {
		s.timer = time.AfterFunc(s.opts.MaxDelay, s.flushOnTimer)
	}
	return nil
}
func (s *ElasticsearchSink) EmitBatch(records []stream.Record) error {
	for _, r := range records {
		if err := s.Emit(r); err != nil {
			return err
		}
	}
	return nil
}
func (s *ElasticsearchSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flushLocked(); err != nil {
		return err
	}
	return s.asyncErr
}
func (s *ElasticsearchSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.flushLocked()
}
func (s *ElasticsearchSink) flushOnTimer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if err := s.flushLocked(); err != nil {
		s.asyncErr = err
	}
}
func (s *ElasticsearchSink) flushLocked() error {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	if len(s.buf) == 0 {
		return nil
	}
	records := s.buf
	if err := s.postBulk(records); err != nil {
		return err
	}
	s.buf = nil
	return nil
}
func (s *ElasticsearchSink) postBulk(records []stream.Record) error {
	var body bytes.Buffer
	for _, r := range records {
		day := r.Time
		if day.IsZero() {
			day = s.now()
		}
		meta, err := json.Marshal(map[string]any{"index": map[string]any{"_index": s.opts.Index + "-" + day.UTC().Format("2006.01.02"), "_id": r.ID}})
		if err != nil {
			return fmt.Errorf("elasticsearch: marshal bulk action: %w", err)
		}
		doc, err := MarshalRecord(r, s.opts.Decode)
		if err != nil {
			return fmt.Errorf("elasticsearch: marshal record: %w", err)
		}
		body.Write(meta)
		body.WriteByte('\n')
		body.Write(doc)
		body.WriteByte('\n')
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(s.opts.URL, "/")+"/_bulk", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if s.opts.APIKey != "" {
		req.Header.Set("Authorization", "ApiKey "+s.opts.APIKey)
	} else if s.opts.Username != "" {
		req.SetBasicAuth(s.opts.Username, s.opts.Password)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("elasticsearch: bulk request: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("elasticsearch: reading bulk response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("elasticsearch: bulk request returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	var reply struct {
		Errors bool `json:"errors"`
	}
	if err := json.Unmarshal(data, &reply); err != nil {
		return fmt.Errorf("elasticsearch: malformed bulk response: %w", err)
	}
	if reply.Errors {
		return fmt.Errorf("elasticsearch: bulk response contains failed items")
	}
	return nil
}
