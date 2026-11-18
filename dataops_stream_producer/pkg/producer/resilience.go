package producer

import (
    "log"
    "sync"
    "sync/atomic"
    "time"

    "github.com/IBM/sarama"
    "github.com/sony/gobreaker"
)

const DefaultBufferCapacity = 10000

type KafkaResilience struct {
    producer     sarama.SyncProducer
    breaker      *gobreaker.CircuitBreaker
    buffer       *BoundedBuffer
    flushInterval time.Duration
    stopCh       chan struct{}
    stopOnce     sync.Once
    wg           sync.WaitGroup
    buffered     atomic.Uint64
    flushed      atomic.Uint64
}

func NewKafkaResilience(producer sarama.SyncProducer, capacity int) *KafkaResilience {
    r := &KafkaResilience{
        producer:      producer,
        buffer:        NewBoundedBuffer(capacity),
        flushInterval: time.Second,
        stopCh:        make(chan struct{}),
    }

    r.breaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
        Name:        "kafka-main-producer",
        MaxRequests: 2,
        Interval:    10 * time.Second,
        Timeout:     15 * time.Second,
        ReadyToTrip: func(counts gobreaker.Counts) bool {
            return counts.ConsecutiveFailures >= 5
        },
        OnStateChange: func(name string, from, to gobreaker.State) {
            log.Printf("[Resilience] Circuit %s changed: %s -> %s", name, from, to)
        },
    })

    r.wg.Add(1)
    go r.flusher()

    return r
}

// Send gửi trực tiếp khi Kafka hoạt động.
// Khi Kafka lỗi hoặc circuit đang open, message được lưu FIFO trong buffer.
func (r *KafkaResilience) Send(msg *sarama.ProducerMessage) error {
    _, err := r.breaker.Execute(func() (interface{}, error) {
        _, _, sendErr := r.producer.SendMessage(msg)
        return nil, sendErr
    })

    if err == nil {
        return nil
    }

    if r.buffer.Push(msg) {
        r.buffered.Add(1)
        log.Printf(
            "[Resilience] Kafka unavailable; buffered topic=%s key=%v state=%s buffer=%d",
            msg.Topic,
            msg.Key,
            r.breaker.State(),
            r.buffer.Len(),
        )
        return nil
    }

    log.Printf(
        "[Resilience] CRITICAL: buffer full topic=%s capacity=%d dropped=%d",
        msg.Topic,
        r.buffer.capacity,
        r.buffer.Dropped(),
    )
    return ErrBufferFull
}

func (r *KafkaResilience) flusher() {
    defer r.wg.Done()

    ticker := time.NewTicker(r.flushInterval)
    defer ticker.Stop()

    for {
        select {
        case <-r.stopCh:
            return
        case <-ticker.C:
            r.flushOneBatch()
        }
    }
}

func (r *KafkaResilience) flushOneBatch() {
    if r.breaker.State() == gobreaker.StateOpen {
        return
    }

    for r.buffer.Len() > 0 {
        if r.breaker.State() == gobreaker.StateOpen {
            return
        }

        msg, ok := r.buffer.Pop()
        if !ok {
            return
        }

        _, err := r.breaker.Execute(func() (interface{}, error) {
            _, _, sendErr := r.producer.SendMessage(msg)
            return nil, sendErr
        })

        if err != nil {
            r.buffer.PushFront(msg)
            log.Printf(
                "[Resilience] Retry failed; message kept in buffer topic=%s buffer=%d",
                msg.Topic,
                r.buffer.Len(),
            )
            return
        }

        r.flushed.Add(1)
        log.Printf(
            "[Resilience] Buffered message flushed topic=%s buffer=%d",
            msg.Topic,
            r.buffer.Len(),
        )
    }
}

func (r *KafkaResilience) Close() {
    r.stopOnce.Do(func() {
        close(r.stopCh)
        r.wg.Wait()
    })
}

func (r *KafkaResilience) Stats() (buffered, flushed, bufferedNow, dropped uint64) {
    return r.buffered.Load(),
        r.flushed.Load(),
        uint64(r.buffer.Len()),
        r.buffer.Dropped()
}