package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUnsubscribeFromTopic(t *testing.T) {
	// Create test topics
	topic1 := "test-topic1-" + time.Now().String()
	topic2 := "test-topic2-" + time.Now().String()
	brokers := []string{"localhost:9092"}

	// Create topics
	client := &Client{Addr: TCP(brokers...)}
	_, err := client.CreateTopics(context.Background(), &CreateTopicsRequest{
		Topics: []TopicConfig{
			{Topic: topic1, NumPartitions: 1, ReplicationFactor: 1},
			{Topic: topic2, NumPartitions: 1, ReplicationFactor: 1},
		},
	})
	require.NoError(t, err)

	// Produce messages to both topics
	producer, err := DialLeader(context.Background(), "tcp", brokers[0], topic1, 0)
	require.NoError(t, err)
	_, err = producer.WriteMessages(Message{Key: []byte("key1"), Value: []byte("value1")})
	require.NoError(t, err)
	producer.Close()

	producer, err = DialLeader(context.Background(), "tcp", brokers[0], topic2, 0)
	require.NoError(t, err)
	_, err = producer.WriteMessages(Message{Key: []byte("key2"), Value: []byte("value2")})
	require.NoError(t, err)
	producer.Close()

	// Create reader
	reader := NewReader(ReaderConfig{
		Brokers: brokers,
		GroupID: "test-group-" + time.Now().String(),
		MaxWait: 1 * time.Second,
	})
	defer reader.Close()

	// Subscribe to both topics
	err = reader.SubscribeTopics([]string{topic1, topic2})
	require.NoError(t, err)

	// Consume messages from both topics
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	messages := make(map[string]int)
	for i := 0; i < 2; i++ {
		msg, err := reader.ReadMessage(ctx)
		require.NoError(t, err)
		messages[msg.Topic]++
	}
	require.Equal(t, 1, messages[topic1], "expected message from topic1")
	require.Equal(t, 1, messages[topic2], "expected message from topic2")

	// Unsubscribe from topic1
	err = reader.UnsubscribeFromTopic(topic1)
	require.NoError(t, err)

	// Produce new messages
	producer, err = DialLeader(context.Background(), "tcp", brokers[0], topic1, 0)
	require.NoError(t, err)
	_, err = producer.WriteMessages(Message{Key: []byte("key1"), Value: []byte("value1-new")})
	require.NoError(t, err)
	producer.Close()

	producer, err = DialLeader(context.Background(), "tcp", brokers[0], topic2, 0)
	require.NoError(t, err)
	_, err = producer.WriteMessages(Message{Key: []byte("key2"), Value: []byte("value2-new")})
	require.NoError(t, err)
	producer.Close()

	// Consume messages (should only get from topic2)
	messages = make(map[string]int)
	for i := 0; i < 1; i++ {
		msg, err := reader.ReadMessage(ctx)
		require.NoError(t, err)
		messages[msg.Topic]++
	}
	require.Equal(t, 0, messages[topic1], "expected no messages from topic1")
	require.Equal(t, 1, messages[topic2], "expected message from topic2")

	// Test unsubscribing from non-subscribed topic
	err = reader.UnsubscribeFromTopic("non-existent-topic")
	require.EqualError(t, err, "topic non-existent-topic not found in subscriptions")

	// Test unsubscribing from last topic
	err = reader.UnsubscribeFromTopic(topic2)
	require.NoError(t, err)

	// Verify no messages are received
	_, err = reader.ReadMessage(context.Background())
	require.Error(t, err, "expected timeout due to no subscriptions")
}
