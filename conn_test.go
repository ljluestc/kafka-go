package kafka

import (
	"bufio"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestConnShortReadError(t *testing.T) {
	conn := &Conn{
		rbuf:    bufio.NewReader(strings.NewReader("short")),
		raddr:   "localhost:9092",
		rbufBuf: make([]byte, 1024),
	}
	ctx := context.Background()
	buf := make([]byte, 10)
	n, err := conn.readFull(ctx, buf)
	if n != 5 {
		t.Errorf("expected 5 bytes read, got %d", n)
	}
	if !errors.Is(err, errShortRead) {
		t.Errorf("expected errShortRead, got %v", err)
	}
	if !strings.Contains(err.Error(), "failed to read 10 bytes") {
		t.Errorf("expected contextual error, got %v", err)
	}
}
