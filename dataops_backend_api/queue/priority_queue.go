// REUSE: 100% copied from trading-bot-api/queue/priority_queue.go
// LEARN: Heap interface — container/heap yêu cầu implement 5 methods
//
// Min-Heap: phần tử có readyAt nhỏ nhất (sớm nhất) luôn ở top
// Dùng để quản lý timed delayed jobs: O(log n) insert, O(log n) remove

package queue

// PriorityQueue là min-heap sắp xếp theo thời gian readyAt
// LEARN: Implement container/heap.Interface = Len + Less + Swap + Push + Pop
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

// Less: item sớm hơn (readyAt nhỏ hơn) có priority CAO hơn
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].readyAt.After(pq[j].readyAt)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Peek xem phần tử đầu heap mà không xóa (không có trong container/heap mặc định)
func (pq PriorityQueue) Peek() interface{} {
	return pq[0]
}

// Push thêm item vào heap (gọi qua heap.Push, không gọi trực tiếp)
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

// Pop lấy item ưu tiên cao nhất ra khỏi heap (gọi qua heap.Pop, không gọi trực tiếp)
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // nil để GC có thể thu hồi memory — important!
	item.index = -1 // đánh dấu không còn trong heap
	*pq = old[0 : n-1]
	return item
}
