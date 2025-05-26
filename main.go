//go:build ignore
// +build ignore

package main

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func main() {
	ctx := context.Background()
	address := "localhost:9092"
	topic := "test-topic"
	sasl := plain.Mechanism{
		Username: "user",
		Password: "pass",
	}

	config := kafka.WriterConfig{
		Addr:         kafka.TCP(address),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		Async:        true,
		BatchSize:    100_000,
		BatchBytes:   100_000 * 1024,
		RequiredAcks: kafka.RequireAll,
		Compression:  kafka.Lz4,
		Transport: &kafka.Transport{
			SASL: sasl,
			TLS: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	transactionalID := "my-transactional-id"
	writer, err := kafka.NewTransactionalWriter(config, transactionalID)
	if err != nil {
		fmt.Printf("Failed to create transactional writer: %v\n", err)
		return
	}
	defer writer.Close()

	if err := writer.BeginTransaction(ctx); err != nil {
		fmt.Printf("Failed to begin transaction: %v\n", err)
		return
	}

	messages := []kafka.Message{
		{Value: []byte("message 1")},
		{Value: []byte("message 2")},
	}
	if err := writer.WriteMessages(ctx, messages...); err != nil {
		fmt.Printf("Failed to write messages: %v\n", err)
		_ = writer.AbortTransaction(ctx)
		return
	}

	if err := writer.CommitTransaction(ctx, nil); err != nil {
		fmt.Printf("Failed to commit transaction: %v\n", err)
		_ = writer.AbortTransaction(ctx)
		return
	}

	fmt.Println("Transaction committed successfully")
}
