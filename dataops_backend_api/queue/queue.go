// REUSE: 100% copied from trading-bot-api/queue/queue.go
// LEARN: Priority Queue pattern — bài học về data structures trong Go
//
// Queue là một hàng đợi (FIFO) hỗ trợ:
//   - Thêm item ngay lập tức (Add)
//   - Thêm item sau một khoảng delay (AddAfter) — dùng cho retry logic
//   - Exponential backoff (When) — càng fail nhiều, delay càng tăng theo lũy thừa 2
//
// WHY: Dùng trong analytics-api để queue các alert jobs, retry failed notifications

package queue

import (
	"container/heap"
	"math"
	"sync"
	"time"
)

// Queue là interface — bất kỳ struct nào implement đủ methods này đều có thể dùng như Queue
// LEARN: Interface = contract, không phải inheritance như Java/C++
type Queue interface {
	Add(item interface{})
	Get() (item interface{}, close bool)
	Close()
	Closing() bool

	Done(item interface{})

	AddAfter(item interface{}, duration time.Duration)
	Forget(item interface{})
	NumReQueues(item interface{}) int

	When(item interface{}) time.Duration
}

// queue là unexported struct (chữ thường) — chỉ có thể tạo qua New()
// LEARN: Encapsulation trong Go = lowercase struct + exported constructor
type queue struct {
	queue []interface{}

	processing set // set chứa các items đang được xử lý (tránh duplicate)

	failuresLock sync.Mutex
	failures     map[interface{}]int // đếm số lần fail của mỗi item

	cond    *sync.Cond // condition variable — block goroutine khi queue trống
	closing bool

	waitItemCh chan *Item // channel nhận delayed items

	stopCh   chan struct{}
	stopOnce sync.Once // đảm bảo Close() chỉ chạy 1 lần dù gọi nhiều lần
}

// New tạo queue mới và bắt đầu goroutine loop() ngầm xử lý delayed items
// LEARN: Constructor pattern trong Go = exported function trả về interface
func New(name string) Queue {
	q := &queue{
		processing: set{},
		cond:       sync.NewCond(&sync.Mutex{}),
		waitItemCh: make(chan *Item),
		stopCh:     make(chan struct{}),
		failures:   make(map[interface{}]int),
	}

	// LEARN: go keyword = spawn goroutine (lightweight thread, ~2KB stack vs ~2MB OS thread)
	go q.loop()
	return q
}

func (q *queue) Add(item interface{}) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.closing {
		return
	}

	// Skip nếu item đang được xử lý — tránh race condition
	if q.processing.has(item) {
		return
	}

	q.queue = append(q.queue, item)
	q.cond.Signal() // đánh thức goroutine đang block ở Get()
}

// loop chạy ngầm, quản lý delayed items và chuyển vào queue khi đến giờ
// LEARN: Event loop pattern — 1 goroutine duy nhất quản lý state = tránh data race
func (q *queue) loop() {
	waitingForQueue := &PriorityQueue{}
	timeCh := make(<-chan time.Time)
	heap.Init(waitingForQueue) // khởi tạo heap invariant

	var nextTimer *time.Timer
	for {
		if q.Closing() {
			return
		}

		// Pop các items đã hết thời gian chờ vào queue chính
		for len(*waitingForQueue) > 0 {
			item := waitingForQueue.Peek().(*Item)
			if item.readyAt.After(time.Now()) {
				break
			}
			item = heap.Pop(waitingForQueue).(*Item)
			q.Add(item.value)
		}

		// Tính thời điểm item tiếp theo cần được thêm vào
		nextReadyAt := timeCh
		if len(*waitingForQueue) > 0 {
			if nextTimer != nil {
				nextTimer.Stop()
			}
			item := waitingForQueue.Peek().(*Item)
			nextTimer = time.NewTimer(time.Until(item.readyAt))
			nextReadyAt = nextTimer.C
		}

		// LEARN: select = switch cho channels, block cho đến khi 1 channel có data
		select {
		case <-q.stopCh:
			return
		case <-nextReadyAt:
		case waitObj := <-q.waitItemCh:
			if waitObj.readyAt.After(time.Now()) {
				heap.Push(waitingForQueue, waitObj)
			} else {
				q.Add(waitObj.value)
			}
		}
	}
}

// AddAfter thêm item vào queue sau một khoảng thời gian — dùng cho retry
func (q *queue) AddAfter(item interface{}, duration time.Duration) {
	if q.Closing() {
		return
	}
	if duration <= 0 {
		q.Add(item)
		return
	}
	q.waitItemCh <- &Item{value: item, readyAt: time.Now().Add(duration)}
}

// Get block cho đến khi có item trong queue (hoặc queue đóng)
// LEARN: Blocking call = goroutine hiệu quả hơn busy-waiting (polling liên tục)
func (q *queue) Get() (item interface{}, close bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	for len(q.queue) == 0 && !q.closing {
		q.cond.Wait() // nhả lock và block, tái acquire khi được Signal()
	}

	if len(q.queue) == 0 {
		return nil, true
	}

	item = q.queue[0]
	q.queue[0] = nil // clear reference để GC thu hồi memory
	q.queue = q.queue[1:]

	q.processing.insert(item)
	return item, false
}

// When tính exponential backoff delay: delay = baseDelay * 2^(số lần fail)
// LEARN: Exponential backoff = pattern cơ bản của retry trong distributed systems
func (q *queue) When(item interface{}) time.Duration {
	q.failuresLock.Lock()
	defer q.failuresLock.Unlock()

	exp := q.failures[item]
	q.failures[item]++

	backoff := float64(baseDelay.Nanoseconds()) * math.Pow(2, float64(exp))
	if backoff > math.MaxInt64 {
		return maxDelay
	}

	calculated := time.Duration(backoff)
	if calculated > maxDelay {
		return maxDelay
	}
	return calculated
}

func (q *queue) NumReQueues(item interface{}) int {
	q.failuresLock.Lock()
	defer q.failuresLock.Unlock()
	return q.failures[item]
}

func (q *queue) Forget(item interface{}) {
	q.failuresLock.Lock()
	defer q.failuresLock.Unlock()
	delete(q.failures, item)
}

func (q *queue) Closing() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.closing
}

func (q *queue) Done(item interface{}) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.processing.delete(item)
}

// Close đóng queue và giải phóng tất cả goroutines đang block
// LEARN: sync.Once đảm bảo close channel chỉ xảy ra 1 lần (đóng 2 lần = panic)
func (q *queue) Close() {
	q.stopOnce.Do(func() {
		q.cond.L.Lock()
		defer q.cond.L.Unlock()
		q.closing = true
		q.cond.Broadcast() // đánh thức TẤT CẢ goroutines đang Wait()
		close(q.stopCh)
	})
}
