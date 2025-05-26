package kafka

import (
	"context"
	"fmt"
	"time"
)

// createTopic creates a Kafka topic with the specified configuration.
func createTopic(ctx context.Context, client *Client, topic string, numPartitions int) error {
	_, err := client.CreateTopics(ctx, &CreateTopicsRequest{
		Topics: []TopicConfig{
			{Topic: topic, NumPartitions: numPartitions, ReplicationFactor: 1},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create topic %s: %w", topic, err)
	}
	return nil
}

// deleteTopic deletes a Kafka topic.
func deleteTopic(ctx context.Context, client *Client, topic string) error {
	_, err := client.DeleteTopics(ctx, &DeleteTopicsRequest{
		Topics: []string{topic},
	})
	if err != nil {
		return fmt.Errorf("failed to delete topic %s: %w", topic, err)
	}
	return nil
}

// waitForTopic waits until the specified topic is available in Kafka.
func waitForTopic(ctx context.Context, client *Client, topic string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		metadata, err := client.Metadata(ctx, &MetadataRequest{
			Topics: []string{topic},
		})
		if err == nil && len(metadata.Topics) > 0 && metadata.Topics[0].Name == topic { // Use "Name" instead of "Topic"
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for topic %s", topic)
}
