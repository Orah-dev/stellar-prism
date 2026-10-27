package sink

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type fakeSQS struct{ calls []*sqs.SendMessageBatchInput }

func (f *fakeSQS) SendMessageBatch(_ context.Context, in *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	f.calls = append(f.calls, in)
	return &sqs.SendMessageBatchOutput{}, nil
}

type fakeSNS struct{ calls []*sns.PublishInput }

func (f *fakeSNS) Publish(_ context.Context, in *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	f.calls = append(f.calls, in)
	return &sns.PublishOutput{}, nil
}

func TestSQSSinkBatchesAndDeduplicatesFIFO(t *testing.T) {
	f := &fakeSQS{}
	s, err := NewSQSSink(f, "https://x/q.fifo", true, false)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]stream.Record, 11)
	for i := range rows {
		rows[i] = stream.Record{ID: "0000000001", Kind: stream.KindLedger}
	}
	if err = s.EmitBatch(rows); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 2 || len(f.calls[0].Entries) != 10 || f.calls[0].Entries[0].MessageDeduplicationId == nil {
		t.Fatal("expected FIFO batches with deduplication IDs")
	}
}
func TestSNSSinkPublishes(t *testing.T) {
	f := &fakeSNS{}
	s, err := NewSNSSink(f, "arn:topic", false)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Emit(stream.Record{ID: "1", Kind: stream.KindLedger}); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 1 {
		t.Fatal("expected publish")
	}
}
