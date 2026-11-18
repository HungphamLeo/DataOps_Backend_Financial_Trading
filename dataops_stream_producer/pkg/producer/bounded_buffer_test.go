package producer

import (
    "testing"

    "github.com/IBM/sarama"
)

func TestBoundedBufferCapacity(t *testing.T) {
    buffer := NewBoundedBuffer(2)

    if !buffer.Push(&sarama.ProducerMessage{Topic: "one"}) {
        t.Fatal("first push failed")
    }

    if !buffer.Push(&sarama.ProducerMessage{Topic: "two"}) {
        t.Fatal("second push failed")
    }

    if buffer.Push(&sarama.ProducerMessage{Topic: "three"}) {
        t.Fatal("buffer should reject overflow")
    }

    if buffer.Len() != 2 {
        t.Fatalf("want len=2, got=%d", buffer.Len())
    }

    if buffer.Dropped() != 1 {
        t.Fatalf("want dropped=1, got=%d", buffer.Dropped())
    }
}

func TestBoundedBufferFIFO(t *testing.T) {
    buffer := NewBoundedBuffer(10)

    _ = buffer.Push(&sarama.ProducerMessage{Topic: "first"})
    _ = buffer.Push(&sarama.ProducerMessage{Topic: "second"})

    first, ok := buffer.Pop()
    if !ok || first.Topic != "first" {
        t.Fatalf("first FIFO item = %#v, ok=%v", first, ok)
    }

    second, ok := buffer.Pop()
    if !ok || second.Topic != "second" {
        t.Fatalf("second FIFO item = %#v, ok=%v", second, ok)
    }
}

func TestPushFrontPreservesRetryOrder(t *testing.T) {
    buffer := NewBoundedBuffer(10)

    _ = buffer.Push(&sarama.ProducerMessage{Topic: "first"})
    _ = buffer.Push(&sarama.ProducerMessage{Topic: "second"})
    _ = buffer.PushFront(&sarama.ProducerMessage{Topic: "retry"})

    first, _ := buffer.Pop()
    if first.Topic != "retry" {
        t.Fatalf("want retry first, got=%s", first.Topic)
    }
}