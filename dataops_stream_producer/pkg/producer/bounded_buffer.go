package producer

import (
    "errors"
    "sync"

    "github.com/IBM/sarama"
)

var ErrBufferFull = errors.New("resilience buffer is full")

type BoundedBuffer struct {
    mu       sync.Mutex
    items    []*sarama.ProducerMessage
    capacity int
    dropped  uint64
}

func NewBoundedBuffer(capacity int) *BoundedBuffer {
    if capacity <= 0 {
        capacity = 10000
    }

    return &BoundedBuffer{
        items:    make([]*sarama.ProducerMessage, 0, capacity),
        capacity: capacity,
    }
}

func (b *BoundedBuffer) Push(msg *sarama.ProducerMessage) bool {
    b.mu.Lock()
    defer b.mu.Unlock()

    if len(b.items) >= b.capacity {
        b.dropped++
        return false
    }

    b.items = append(b.items, msg)
    return true
}

// PushFront được dùng khi Kafka chưa sẵn sàng khi flusher retry.
func (b *BoundedBuffer) PushFront(msg *sarama.ProducerMessage) bool {
    b.mu.Lock()
    defer b.mu.Unlock()

    if len(b.items) >= b.capacity {
        b.dropped++
        return false
    }

    b.items = append([]*sarama.ProducerMessage{msg}, b.items...)
    return true
}

func (b *BoundedBuffer) Pop() (*sarama.ProducerMessage, bool) {
    b.mu.Lock()
    defer b.mu.Unlock()

    if len(b.items) == 0 {
        return nil, false
    }

    msg := b.items[0]
    b.items[0] = nil
    b.items = b.items[1:]
    return msg, true
}

func (b *BoundedBuffer) Len() int {
    b.mu.Lock()
    defer b.mu.Unlock()
    return len(b.items)
}

func (b *BoundedBuffer) Dropped() uint64 {
    b.mu.Lock()
    defer b.mu.Unlock()
    return b.dropped
}