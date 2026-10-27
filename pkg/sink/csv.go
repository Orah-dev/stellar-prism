package sink

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// CSVOptions configures a CSV or TSV stream. Fields uses the stable names
// documented by the CLI: id, ledger, time, kind, contract, tx, topic0 and
// value. Values that do not apply to a record are emitted as empty cells.
type CSVOptions struct {
	Fields    []string
	Delimiter rune
}

// CSVSink writes records as a header followed by one safely-escaped row per
// record. csv.Writer handles embedded delimiters, quotes and newlines.
type CSVSink struct {
	mu     sync.Mutex
	w      *csv.Writer
	fields []string
}

func NewCSVSink(w io.Writer, opts CSVOptions) (*CSVSink, error) {
	// Do not normalize the caller's slice in place: flag parsing and tests may
	// reuse it to construct another sink.
	fields := append([]string(nil), opts.Fields...)
	if len(fields) == 0 {
		fields = []string{"id", "ledger", "time", "kind", "contract", "tx", "topic0", "value"}
	}
	for i, field := range fields {
		fields[i] = strings.ToLower(strings.TrimSpace(field))
		switch fields[i] {
		case "id", "ledger", "time", "kind", "contract", "tx", "topic0", "value":
		default:
			return nil, fmt.Errorf("csv: unsupported field %q", field)
		}
	}
	if opts.Delimiter == 0 {
		opts.Delimiter = ','
	}
	if opts.Delimiter == '"' || opts.Delimiter == '\r' || opts.Delimiter == '\n' || opts.Delimiter == utf8.RuneError {
		return nil, fmt.Errorf("csv: invalid delimiter %q", opts.Delimiter)
	}
	cw := csv.NewWriter(w)
	cw.Comma = opts.Delimiter
	if err := cw.Write(fields); err != nil {
		return nil, err
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return nil, err
	}
	return &CSVSink{w: cw, fields: fields}, nil
}

func (s *CSVSink) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := make([]string, len(s.fields))
	for i, field := range s.fields {
		row[i] = csvField(r, field)
	}
	if err := s.w.Write(row); err != nil {
		return err
	}
	s.w.Flush()
	return s.w.Error()
}

func (s *CSVSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.w.Flush()
	return s.w.Error()
}

func csvField(r stream.Record, field string) string {
	switch field {
	case "id":
		return r.ID
	case "ledger":
		return strconv.FormatUint(uint64(r.Sequence), 10)
	case "time":
		return r.Time.UTC().Format("2006-01-02T15:04:05Z07:00")
	case "kind":
		return string(r.Kind)
	}
	if r.Event == nil {
		return ""
	}
	switch field {
	case "contract":
		return r.Event.ContractID
	case "tx":
		return r.Event.TxHash
	case "topic0":
		if len(r.Event.TopicXDR) > 0 {
			return r.Event.TopicXDR[0]
		}
	case "value":
		return r.Event.ValueXDR
	}
	return ""
}
