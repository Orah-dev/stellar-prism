package sink_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type testPublisher struct {
	subject string
	data    []byte
	err     error
}

func (p *testPublisher) Publish(subject string, data []byte) error {
	p.subject, p.data = subject, data
	return p.err
}
func (p *testPublisher) Close() error { return nil }

func TestNATSSinkRendersSubjectAndPropagatesPublishFailure(t *testing.T) {
	p := &testPublisher{}
	s, err := sink.NewNATSSink(sink.NATSOptions{Subject: "stellar.events.{{.ContractID}}.{{.Topic0}}", Publisher: p})
	if err != nil {
		t.Fatal(err)
	}
	r := stream.Record{ID: "r", Kind: stream.KindEvent, Event: &stream.Event{ContractID: "CDL", TopicXDR: []string{"trade"}}}
	if err := s.Emit(r); err != nil {
		t.Fatal(err)
	}
	if p.subject != "stellar.events.CDL.trade" || len(p.data) == 0 {
		t.Errorf("publish = %q %q", p.subject, p.data)
	}
	p.err = errors.New("offline")
	if err := s.Emit(r); err == nil {
		t.Fatal("Emit error = nil")
	}
}

func TestNATSSinkRejectsInvalidRenderedSubject(t *testing.T) {
	p := &testPublisher{}
	s, err := sink.NewNATSSink(sink.NATSOptions{Subject: "stellar.{{.ContractID}}", Publisher: p})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Emit(stream.Record{ID: "r"})
	if err == nil || !strings.Contains(err.Error(), "invalid publish token") {
		t.Fatalf("Emit() error = %v, want invalid subject", err)
	}
	if p.subject != "" {
		t.Errorf("publisher called with subject %q", p.subject)
	}
}
