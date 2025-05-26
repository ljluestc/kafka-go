package kafka

import (
	"context"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
	"github.com/stretchr/testify/assert"
)

func TestClientDescribeConfigs(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("0.11.0") {
		return
	}

	const (
		MaxMessageBytes      = "max.message.bytes"
		MaxMessageBytesValue = "200000"
	)

	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	createTopic(ctx, topic, 1)
	defer deleteTopic(ctx, topic)

	_, err := client.AlterConfigs(context.Background(), &AlterConfigsRequest{
		Resources: []AlterConfigRequestResource{{
			ResourceType: ResourceTypeTopic,
			ResourceName: topic,
			Configs: []AlterConfigRequestConfig{{
				Name:  MaxMessageBytes,
				Value: MaxMessageBytesValue,
			},
			},
		}},
	})

	if err != nil {
		t.Fatal(err)
	}

	describeResp, err := client.DescribeConfigs(context.Background(), &DescribeConfigsRequest{
		Resources: []DescribeConfigRequestResource{{
			ResourceType: ResourceTypeTopic,
			ResourceName: topic,
			ConfigNames:  []string{MaxMessageBytes},
		}},
	})

	if err != nil {
		t.Fatal(err)
	}

	maxMessageBytesValue := "0"
	for _, resource := range describeResp.Resources {
		if resource.ResourceType == int8(ResourceTypeTopic) && resource.ResourceName == topic {
			for _, entry := range resource.ConfigEntries {
				if entry.ConfigName == MaxMessageBytes {
					maxMessageBytesValue = entry.ConfigValue
				}
			}
		}
	}
	assert.Equal(t, maxMessageBytesValue, MaxMessageBytesValue)
}
