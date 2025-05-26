package kafka

import (
	"context"
	"errors"
	"sync"
)

// TransactionalWriter is a no-op stub so that the core builds cleanly.
type TransactionalWriter struct {
	mutex sync.Mutex
}

func NewTransactionalWriter(config WriterConfig, transactionalID string) (*TransactionalWriter, error) {
	if transactionalID == "" {
		return nil, errors.New("transactionalID must not be empty")
	}
	return &TransactionalWriter{}, nil
}

func (tw *TransactionalWriter) BeginTransaction(ctx context.Context) error            { return nil }
func (tw *TransactionalWriter) WriteMessages(ctx context.Context, m ...Message) error { return nil }
func (tw *TransactionalWriter) CommitTransaction(ctx context.Context, _ map[string][]OffsetCommit) error {
	return nil
}
func (tw *TransactionalWriter) AbortTransaction(ctx context.Context) error { return nil }
func (tw *TransactionalWriter) Close() error                               { return nil }
