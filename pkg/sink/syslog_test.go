package sink_test

import (
	"bytes"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func syslogNow() time.Time {
	return time.Date(2023, 11, 14, 22, 15, 23, 0, time.UTC)
}

func newTestSyslog(t *testing.T, opts sink.SyslogOptions) *sink.Syslog {
	t.Helper()
	if opts.Now == nil {
		opts.Now = syslogNow
	}
	s, err := sink.NewSyslog(opts)
	if err != nil {
		t.Fatalf("NewSyslog() error = %v", err)
	}
	return s
}

func TestSyslogRFC5424Shape(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := newTestSyslog(t, sink.SyslogOptions{
		Writer:   &buf,
		Hostname: "node-1",
	})
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	line := buf.String()
	if strings.Contains(strings.TrimSuffix(line, "\n"), "\n") {
		t.Errorf("one record produced more than one syslog line\n%s", line)
	}
	// local0 (16) * 8 + info (6) = 134.
	if !strings.HasPrefix(line, "<134>1 2023-11-14T22:15:23.000Z node-1 prism - event ") {
		t.Errorf("header is wrong\n%s", line)
	}
	for _, want := range []string{
		`[prism@32473 kind="event" ledger="100" id="0000000429496729600-0000000000"]`,
		`"kind":"event"`,
		`"valueXdr"`,
	} {
		if !strings.Contains(line, want) {
			t.Errorf("message does not contain %q\n%s", want, line)
		}
	}
}

func TestSyslogSeverityByRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		build func() stream.Record
		want  string
	}{
		{
			name: "successful transaction is info",
			build: func() stream.Record {
				return stream.Record{ID: "1", Kind: stream.KindTransaction,
					Transaction: &stream.Transaction{Successful: true}}
			},
			want: "<134>1 ",
		},
		{
			name: "failed transaction is warning",
			build: func() stream.Record {
				return stream.Record{ID: "1", Kind: stream.KindTransaction,
					Transaction: &stream.Transaction{Successful: false}}
			},
			want: "<132>1 ",
		},
		{
			name: "failed deployment is warning",
			build: func() stream.Record {
				return stream.Record{ID: "1", Kind: stream.KindWASMDeployment,
					WASMDeployment: &stream.WASMEvent{Successful: false}}
			},
			want: "<132>1 ",
		},
		{
			name: "diagnostic event is debug",
			build: func() stream.Record {
				r := eventRecord()
				r.Event.Type = "diagnostic"
				return r
			},
			want: "<135>1 ",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			s := newTestSyslog(t, sink.SyslogOptions{Writer: &buf, Hostname: "node"})
			if err := s.Emit(tc.build()); err != nil {
				t.Fatalf("Emit() error = %v", err)
			}
			if !strings.HasPrefix(buf.String(), tc.want) {
				t.Errorf("message = %q, want prefix %q", buf.String(), tc.want)
			}
		})
	}
}

func TestSyslogEscapesStructuredData(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := newTestSyslog(t, sink.SyslogOptions{Writer: &buf, Hostname: "node"})
	r := eventRecord()
	r.ID = `bad"id]with\slash`
	if err := s.Emit(r); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if !strings.Contains(buf.String(), `id="bad\"id\]with\\slash"`) {
		t.Errorf("structured data was not escaped\n%s", buf.String())
	}
	if strings.Count(buf.String(), "\n") != 1 {
		t.Errorf("escaped message spans more than one line\n%s", buf.String())
	}
}

func TestSyslogJournaldShape(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := newTestSyslog(t, sink.SyslogOptions{
		Writer: &buf,
		Format: sink.SyslogJournald,
	})
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := s.EmitBatch([]stream.Record{ledgerRecord()}); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}

	out := buf.String()
	if !strings.HasSuffix(out, "\n\n") {
		t.Errorf("journald entries must end with a blank line\n%q", out)
	}
	for _, want := range []string{
		"PRIORITY=6\n",
		"SYSLOG_IDENTIFIER=prism\n",
		"SYSLOG_FACILITY=16\n",
		"PRISM_KIND=event\n",
		"PRISM_LEDGER=100\n",
		"PRISM_RECORD_ID=0000000429496729600-0000000000\n",
		"PRISM_LEDGER_TIME=2023-11-14T22:15:00Z\n",
		"MESSAGE={",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("journald output does not contain %q\n%s", want, out)
		}
	}
	if got := strings.Count(out, "PRIORITY=6"); got != 2 {
		t.Errorf("got %d journald entries, want 2", got)
	}
}

func TestSyslogRejectsBadRecord(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := newTestSyslog(t, sink.SyslogOptions{Writer: &buf})
	if err := s.Emit(stream.Record{ID: "x", Kind: stream.Kind("nonsense")}); err == nil {
		t.Fatal("Emit(unknown kind) error = nil, want an encoding failure")
	}
	if buf.Len() != 0 {
		t.Errorf("a failed record was still written\n%s", buf.String())
	}
}

func TestSyslogPropagatesWriterError(t *testing.T) {
	t.Parallel()

	s := newTestSyslog(t, sink.SyslogOptions{Writer: syslogFailingWriter{}})
	err := s.Emit(eventRecord())
	if err == nil {
		t.Fatal("Emit() error = nil, want the writer failure")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error = %q, want it to wrap the writer failure", err)
	}
}

type syslogFailingWriter struct{}

func (syslogFailingWriter) Write([]byte) (int, error) { return 0, errors.New("boom") }

type closableBuffer struct {
	bytes.Buffer
	closed bool
}

func (c *closableBuffer) Close() error {
	c.closed = true
	return nil
}

func TestSyslogCloseClosesWriter(t *testing.T) {
	t.Parallel()

	writer := &closableBuffer{}
	s := newTestSyslog(t, sink.SyslogOptions{Writer: writer})
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !writer.closed {
		t.Error("Close() did not close the underlying writer")
	}
	if err := s.Emit(eventRecord()); err == nil {
		t.Error("Emit() after Close error = nil, want an error")
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}

func TestSyslogValidation(t *testing.T) {
	t.Parallel()

	valid := sink.SyslogOptions{Writer: &bytes.Buffer{}}

	tests := []struct {
		name   string
		mutate func(*sink.SyslogOptions)
	}{
		{name: "missing writer", mutate: func(o *sink.SyslogOptions) { o.Writer = nil }},
		{name: "unknown format", mutate: func(o *sink.SyslogOptions) { o.Format = "cef" }},
		{name: "unknown facility", mutate: func(o *sink.SyslogOptions) { o.Facility = "nowhere" }},
		{name: "space in app name", mutate: func(o *sink.SyslogOptions) { o.AppName = "prism app" }},
		{name: "newline in message id", mutate: func(o *sink.SyslogOptions) { o.MessageID = "a\nb" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := valid
			tc.mutate(&opts)
			if _, err := sink.NewSyslog(opts); err == nil {
				t.Fatal("NewSyslog() error = nil, want a validation failure")
			}
		})
	}

	valid.Format = sink.SyslogJournald
	valid.Facility = "local3"
	valid.AppName = "prism-test"
	if _, err := sink.NewSyslog(valid); err != nil {
		t.Fatalf("NewSyslog(valid) error = %v", err)
	}
}

func TestParseSyslogFacility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    sink.SyslogFacility
		wantErr bool
	}{
		{in: "local0", want: sink.FacilityLocal0},
		{in: "LOCAL5", want: sink.FacilityLocal5},
		{in: "user", want: sink.FacilityUser},
		{in: "23", want: sink.FacilityLocal7},
		{in: "", want: sink.FacilityLocal0},
		{in: "24", wantErr: true},
		{in: "nonsense", wantErr: true},
	}
	for _, tc := range tests {
		got, err := sink.ParseSyslogFacility(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseSyslogFacility(%q) error = nil, want a failure", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSyslogFacility(%q) error = %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseSyslogFacility(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestDialSyslogUnixgram(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "log.sock")
	listener, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Skipf("unixgram listener unavailable: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	conn, err := sink.DialSyslog("unixgram://" + path)
	if err != nil {
		t.Fatalf("DialSyslog() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if err := listener.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	buf := make([]byte, 32)
	n, _, err := listener.ReadFromUnix(buf)
	if err != nil {
		t.Fatalf("ReadFromUnix() error = %v", err)
	}
	if got := string(buf[:n]); got != "hello" {
		t.Errorf("received %q, want hello", got)
	}
}

func TestDialSyslogUDP(t *testing.T) {
	t.Parallel()

	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket() error = %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	conn, err := sink.DialSyslog("udp://" + listener.LocalAddr().String())
	if err != nil {
		t.Fatalf("DialSyslog() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if err := listener.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	buf := make([]byte, 32)
	n, _, err := listener.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom() error = %v", err)
	}
	if got := string(buf[:n]); got != "hello" {
		t.Errorf("received %q, want hello", got)
	}
}

func TestDialSyslogValidation(t *testing.T) {
	t.Parallel()

	for _, address := range []string{"", "http://example.com", "example.com:514", "unixgram://"} {
		if _, err := sink.DialSyslog(address); err == nil {
			t.Errorf("DialSyslog(%q) error = nil, want a validation failure", address)
		}
	}
}
