package sink

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/nats-io/nats.go"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// NATSPublisher is kept small so the delivery and subject rules can be tested
// without a broker. Implementations return only after a broker acknowledgement
// when JetStream is enabled.
type NATSPublisher interface {
	Publish(subject string, data []byte) error
	Close() error
}

type NATSOptions struct {
	URL       string
	Subject   string
	JetStream bool
	// Decode controls whether XDR fields use the regular --json representation.
	Decode    bool
	Publisher NATSPublisher
}

type NATSSink struct {
	publisher NATSPublisher
	subject   *template.Template
	decode    bool
}

func NewNATSSink(opts NATSOptions) (*NATSSink, error) {
	if strings.TrimSpace(opts.Subject) == "" {
		return nil, fmt.Errorf("nats: subject is required")
	}
	t, err := template.New("subject").Option("missingkey=error").Parse(opts.Subject)
	if err != nil {
		return nil, fmt.Errorf("nats: invalid subject template: %w", err)
	}
	p := opts.Publisher
	if p == nil {
		if strings.TrimSpace(opts.URL) == "" {
			return nil, fmt.Errorf("nats: URL is required")
		}
		nc, err := nats.Connect(opts.URL)
		if err != nil {
			return nil, fmt.Errorf("nats: connect: %w", err)
		}
		if opts.JetStream {
			js, err := nc.JetStream()
			if err != nil {
				nc.Close()
				return nil, fmt.Errorf("nats: JetStream: %w", err)
			}
			p = jetStreamPublisher{nc: nc, js: js}
		} else {
			p = coreNATSPublisher{nc: nc}
		}
	}
	return &NATSSink{publisher: p, subject: t, decode: opts.Decode}, nil
}

func (s *NATSSink) Emit(r stream.Record) error {
	var subject bytes.Buffer
	data := subjectTemplateData(r)
	if err := s.subject.Execute(&subject, data); err != nil {
		return fmt.Errorf("nats: render subject: %w", err)
	}
	if err := validateNATSSubject(subject.String()); err != nil {
		return err
	}
	payload, err := MarshalRecord(r, s.decode)
	if err != nil {
		return fmt.Errorf("nats: marshal record: %w", err)
	}
	if err := s.publisher.Publish(subject.String(), payload); err != nil {
		return fmt.Errorf("nats: publish: %w", err)
	}
	return nil
}

func subjectTemplateData(r stream.Record) struct {
	ID         string
	Ledger     uint32
	ContractID string
	Topic0     string
} {
	data := struct {
		ID         string
		Ledger     uint32
		ContractID string
		Topic0     string
	}{ID: r.ID, Ledger: r.Sequence}
	if r.Event != nil {
		data.ContractID = r.Event.ContractID
		if len(r.Event.TopicXDR) > 0 {
			data.Topic0 = r.Event.TopicXDR[0]
		}
	}
	return data
}
func (s *NATSSink) Flush() error { return nil }
func (s *NATSSink) Close() error { return s.publisher.Close() }

func validateNATSSubject(subject string) error {
	if subject == "" || strings.ContainsAny(subject, " \t\r\n") {
		return fmt.Errorf("nats: rendered subject is empty or contains whitespace")
	}
	for _, token := range strings.Split(subject, ".") {
		if token == "" || token == "*" || token == ">" {
			return fmt.Errorf("nats: rendered subject %q contains an invalid publish token", subject)
		}
	}
	return nil
}

type coreNATSPublisher struct{ nc *nats.Conn }

func (p coreNATSPublisher) Publish(subject string, data []byte) error {
	if err := p.nc.Publish(subject, data); err != nil {
		return err
	}
	return p.nc.Flush()
}
func (p coreNATSPublisher) Close() error { p.nc.Close(); return nil }

type jetStreamPublisher struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

func (p jetStreamPublisher) Publish(subject string, data []byte) error {
	_, err := p.js.Publish(subject, data)
	return err
}
func (p jetStreamPublisher) Close() error { p.nc.Close(); return nil }
