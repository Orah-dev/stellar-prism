package sink

import (
	"context"
	"testing"

	"cloud.google.com/go/pubsub/v2"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type fakePublishResult struct{}

func (fakePublishResult) Get(context.Context) (string, error) { return "id", nil }

type fakePublisher struct{ message *pubsub.Message }

func (f *fakePublisher) Publish(_ context.Context, m *pubsub.Message) PublishResult {
	f.message = m
	return fakePublishResult{}
}
func TestPubSubSinkUsesContractOrderingKey(t *testing.T) {
	f := &fakePublisher{}
	s, e := NewPubSubSink(f, false)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Emit(stream.Record{ID: "1", Kind: stream.KindEvent, Event: &stream.Event{ContractID: "C"}}); e != nil {
		t.Fatal(e)
	}
	if f.message.OrderingKey != "C" {
		t.Fatalf("key %q", f.message.OrderingKey)
	}
}
