// REUSE: 100% copied from trading-bot-api/queue/types.go
// LEARN: Constants và types trong Go
//
// baseDelay và maxDelay kiểm soát exponential backoff:
//   attempt 1:  1µs
//   attempt 2:  2µs
//   attempt 3:  4µs
//   ...
//   max:        1000s (cap để tránh delay vô hạn)

package queue

import "time"

const (
	baseDelay = time.Microsecond  // delay khởi đầu = 1 microsecond
	maxDelay  = 1000 * time.Second // delay tối đa = 1000 giây
)

// Item là một job trong delayed queue
// LEARN: struct trong Go — không có class, chỉ có struct + methods
type Item struct {
	value   interface{} // payload (bất kỳ kiểu gì)
	readyAt time.Time   // thời điểm item sẵn sàng để xử lý
	index   int         // vị trí trong heap (quản lý bởi PriorityQueue)
}
