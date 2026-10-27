package sink

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// SQSAPI and SNSAPI keep AWS delivery testable without credentials or a network.
type SQSAPI interface {
	SendMessageBatch(context.Context, *sqs.SendMessageBatchInput, ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}
type SNSAPI interface {
	Publish(context.Context, *sns.PublishInput, ...func(*sns.Options)) (*sns.PublishOutput, error)
}

type SQSSink struct {
	client       SQSAPI
	queue        string
	fifo, decode bool
}

func NewSQSSink(client SQSAPI, queue string, fifo, decode bool) (*SQSSink, error) {
	if client == nil || queue == "" {
		return nil, fmt.Errorf("sqs: client and queue URL are required")
	}
	return &SQSSink{client, queue, fifo, decode}, nil
}
func (s *SQSSink) Emit(r stream.Record) error { return s.EmitBatch([]stream.Record{r}) }
func (s *SQSSink) EmitBatch(rs []stream.Record) error {
	for len(rs) > 0 {
		n := len(rs)
		if n > 10 {
			n = 10
		}
		entries := make([]types.SendMessageBatchRequestEntry, 0, n)
		for i, r := range rs[:n] {
			b, e := MarshalRecord(r, s.decode)
			if e != nil {
				return e
			}
			x := types.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), MessageBody: aws.String(string(b))}
			if s.fifo {
				x.MessageDeduplicationId = aws.String(r.ID)
				group := string(r.Kind)
				if r.Event != nil && r.Event.ContractID != "" {
					group = r.Event.ContractID
				}
				x.MessageGroupId = aws.String(group)
			}
			entries = append(entries, x)
		}
		out, e := s.client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: aws.String(s.queue), Entries: entries})
		if e != nil {
			return fmt.Errorf("sqs: send batch: %w", e)
		}
		if len(out.Failed) > 0 {
			return fmt.Errorf("sqs: %d messages failed", len(out.Failed))
		}
		rs = rs[n:]
	}
	return nil
}
func (*SQSSink) Flush() error { return nil }
func (*SQSSink) Close() error { return nil }

type SNSSink struct {
	client SNSAPI
	topic  string
	decode bool
}

func NewSNSSink(client SNSAPI, topic string, decode bool) (*SNSSink, error) {
	if client == nil || topic == "" {
		return nil, fmt.Errorf("sns: client and topic ARN are required")
	}
	return &SNSSink{client, topic, decode}, nil
}
func (s *SNSSink) Emit(r stream.Record) error {
	b, e := MarshalRecord(r, s.decode)
	if e != nil {
		return e
	}
	_, e = s.client.Publish(context.Background(), &sns.PublishInput{TopicArn: aws.String(s.topic), Message: aws.String(string(b))})
	if e != nil {
		return fmt.Errorf("sns: publish: %w", e)
	}
	return nil
}
func (*SNSSink) Flush() error { return nil }
func (*SNSSink) Close() error { return nil }
