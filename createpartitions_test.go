package kafka

import (
	"context"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientCreatePartitions(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("1.0.1") {
		return
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	createTopic(ctx, topic, 1)
	defer deleteTopic(ctx, topic)

	res, err := client.CreatePartitions(ctx, &CreatePartitionsRequest{
		Topics: []TopicPartitionsConfig{
			{
				Name:  topic,
				Count: 2,
				TopicPartitionAssignments: []TopicPartitionAssignment{
					{
						BrokerIDs: []int32{1},
					},
				},
			},
		},
		ValidateOnly: false,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := res.Errors[topic]; err != nil {
		t.Error(err)
	}
}

func TestClientCreatePartitionsNoAssignments(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("1.0.1") {
		return
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	createTopic(ctx, topic, 1)
	defer deleteTopic(ctx, topic)

	res, err := client.CreatePartitions(ctx, &CreatePartitionsRequest{
		Topics: []TopicPartitionsConfig{
			{
				Name:  topic,
				Count: 2,
			},
		},
		ValidateOnly: false,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := res.Errors[topic]; err != nil {
		t.Error(err)
	}
}
