package kafka

import (
	"context"
	"testing"
	"time"
)

func TestAddOffsetsToTxn(t *testing.T) {
	client := &Client{Addr: TCP("localhost:9092")}
	ctx := context.Background()
	topic := "test-topic"

	// Create the topic
	err := createTopic(ctx, topic, 1)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	defer deleteTopic(ctx, topic)

	// Wait for the topic to be available
	err = waitForTopic(ctx, client, topic, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to wait for topic: %v", err)
	}

	// ...existing test logic...
}
