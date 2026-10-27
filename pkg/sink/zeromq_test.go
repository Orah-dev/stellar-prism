package sink_test

import (
	"errors"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestZeroMQSinkRendersTopicAndPropagatesPublishFailure(t *testing.T) {
	p := &testPublisher{}
	s, err := sink.NewZeroMQSink(sink.ZeroMQOptions{
		Topic:     "stellar.events.{{.ContractID}}.{{.Topic0}}",
		Publisher: p,
	})
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

func TestZeroMQSinkRejectsEmptyRenderedTopic(t *testing.T) {
	p := &testPublisher{}
	s, err := sink.NewZeroMQSink(sink.ZeroMQOptions{Topic: "{{.ContractID}}", Publisher: p})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Emit(stream.Record{ID: "r", Kind: stream.KindEvent}); err == nil {
		t.Fatal("Emit error = nil")
	}
}
