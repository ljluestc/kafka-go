package kafka

import (
    "context"
    "testing"
)

func TestTransactionalWriter(t *testing.T) {
    ctx := context.Background()
    config := WriterConfig{
        Addr:         TCP("localhost:9092"),
        Topic:        "test-topic",
        Balancer:     &RoundRobin{},
        RequiredAcks: RequireAll,
    }

    t.Run("Invalid TransactionalID", func(t *testing.T) {
        _, err := NewTransactionalWriter(config, "")
        if err == nil {
            t.Errorf("Expected error for empty transactionalID")
        }
    })

    t.Run("Successful Transaction", func(t *testing.T) {
        writer, err := NewTransactionalWriter(config, "test-id")
        if err != nil {
            t.Skipf("Skipping due to Kafka broker not available: %v", err)
        }
        defer writer.Close()

        if err := writer.BeginTransaction(ctx); err != nil {
            t.Fatalf("Failed to begin transaction: %v", err)
        }

        messages := []Message{
            {Value: []byte("test message")},
        }
        if err := writer.WriteMessages(ctx, messages...); err != nil {
            t.Fatalf("Failed to write messages: %v", err)
        }

        if err := writer.CommitTransaction(ctx, nil); err != nil {
            t.Fatalf("Failed to commit transaction: %v", err)
        }
    })
}
