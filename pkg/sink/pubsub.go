package sink

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub/v2"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// PubSubPublisher is the small seam used by PubSubSink; tests supply it so no
// Application Default Credentials or network connection is needed.
type PubSubPublisher interface {
	Publish(context.Context, *pubsub.Message) PublishResult
}
type PublishResult interface {
	Get(context.Context) (string, error)
}
type pubsubAdapter struct{ publisher *pubsub.Publisher }
type pubsubResult struct{ result *pubsub.PublishResult }

func (a pubsubAdapter) Publish(ctx context.Context, m *pubsub.Message) PublishResult {
	return pubsubResult{a.publisher.Publish(ctx, m)}
}
func (r pubsubResult) Get(ctx context.Context) (string, error) { return r.result.Get(ctx) }
func AdaptPubSubPublisher(publisher *pubsub.Publisher) PubSubPublisher {
	return pubsubAdapter{publisher}
}

type PubSubSink struct {
	publisher PubSubPublisher
	decode    bool
}

func NewPubSubSink(publisher PubSubPublisher, decode bool) (*PubSubSink, error) {
	if publisher == nil {
		return nil, fmt.Errorf("pubsub: publisher is required")
	}
	return &PubSubSink{publisher: publisher, decode: decode}, nil
}

func (s *PubSubSink) Emit(r stream.Record) error {
	b, err := MarshalRecord(r, s.decode)
	if err != nil {
		return fmt.Errorf("pubsub: marshal record: %w", err)
	}
	ordering := fmt.Sprintf("%010d", r.Sequence)
	if r.Event != nil && r.Event.ContractID != "" {
		ordering = r.Event.ContractID
	}
	_, err = s.publisher.Publish(context.Background(), &pubsub.Message{Data: b, OrderingKey: ordering, Attributes: map[string]string{"record_id": r.ID, "kind": string(r.Kind)}}).Get(context.Background())
	if err != nil {
		return fmt.Errorf("pubsub: publish: %w", err)
	}
	return nil
}
func (*PubSubSink) Flush() error { return nil }
func (s *PubSubSink) Close() error {
	if publisher, ok := s.publisher.(interface{ Stop() }); ok {
		publisher.Stop()
	}
	return nil
}
