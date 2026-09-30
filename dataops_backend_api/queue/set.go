// REUSE: 100% copied from trading-bot-api/queue/set.go
// LEARN: Set data structure trong Go — Go không có built-in set, dùng map[T]struct{}
//
// map[T]empty{} là idiomatic Go cho Set:
//   - O(1) lookup (has)
//   - O(1) insert
//   - Zero memory overhead: empty struct chiếm 0 bytes

package queue

type empty struct{}
type t interface{}
type set map[t]empty

func (s set) has(item t) bool {
	_, exists := s[item]
	return exists
}

func (s set) insert(item t) {
	s[item] = empty{}
}

func (s set) delete(item t) {
	delete(s, item)
}

func (s set) len() int {
	return len(s)
}
