package kafka

import (
	"context"
	"testing"
	"time"
)

func TestAlterPartitionReassignments(t *testing.T) {
	client := &Client{Addr: TCP("localhost:9092")}
	ctx := context.Background()
	topic1 := "test-topic1"
	topic2 := "test-topic2"

	// Create the topics
	err := createTopic(ctx, topic1, 1)
	if err != nil {
		t.Fatalf("failed to create topic1: %v", err)
	}
	defer deleteTopic(ctx, topic1)

	err = createTopic(ctx, topic2, 1)
	if err != nil {
		t.Fatalf("failed to create topic2: %v", err)
	}
	defer deleteTopic(ctx, topic2)

	// Wait for the topics to be available
	err = waitForTopic(ctx, client, topic1, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to wait for topic1: %v", err)
	}

	err = waitForTopic(ctx, client, topic2, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to wait for topic2: %v", err)
	}

	// ...existing test logic...
}
