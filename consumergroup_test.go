package kafka

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConsumerGroupCoordinatorServerUnavailable(t *testing.T) {
	// Mock dialer that fails with io.EOF
	failingDialer := &Dialer{
		Timeout: 1 * time.Second,
		DialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			return nil, io.EOF
		},
	}

	config := ConsumerGroupConfig{
		ID:      "test-group",
		Brokers: []string{"localhost:9092"},
		Topics:  []string{"test-topic"},
		Dialer:  failingDialer,
		Timeout: 1 * time.Second,
	}
	cg, err := NewConsumerGroup(config)
	assert.NoError(t, err)
	defer cg.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cg.Next(ctx)
	assert.ErrorIs(t, err, ErrServerUnavailable, "expected ErrServerUnavailable, got %v", err)
}
