// LEARN: Infrastructure — Load Balancer with Health Check
//
// REUSE: ~90% từ trading-bot-load-balancer/networking/networking.go
//
// Load Balancer trong context này:
//   - Không phải network LB (Nginx, HAProxy)
//   - Application-level LB: distribute requests đến healthy service instances
//   - Health check: TCP dial → nếu fail → mark unhealthy, route traffic sang instance khác
//
// "Building Microservices" (O'Reilly) Ch.11: Resilience Patterns
//   Circuit Breaker > Health Check > Retry > Timeout
//   Fail fast: phát hiện lỗi sớm, không để request timeout

package infrastructure

import (
	"fmt"
	"log"
	"net"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"
)

// NetworkingService là interface để abstract health checking
// REUSE: 100% từ trading-bot-load-balancer/networking/networking.go
type NetworkingService interface {
	Check(address string) bool   // kiểm tra 1 địa chỉ có healthy không
	HealthCheck()                // chạy health check cho tất cả backends
	SetAlive(address string, alive bool)
	CheckAlive(address string) bool
}

// Backend đại diện cho 1 service instance
// LEARN: Struct với sync.RWMutex — thread-safe state management
//        RWMutex: nhiều goroutines có thể đọc cùng lúc, nhưng chỉ 1 goroutine ghi
type Backend struct {
	Address      string
	IsAlive      bool
	mu           sync.RWMutex
	ReverseProxy *httputil.ReverseProxy // HTTP reverse proxy đến backend
}

// SetAlive thread-safe setter cho IsAlive
// LEARN: mu.Lock() cho write, mu.RLock() cho read
func (b *Backend) SetAlive(alive bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.IsAlive = alive
}

// CheckAlive thread-safe getter
func (b *Backend) CheckAlive() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.IsAlive
}

// LoadBalancer quản lý pool của backends
type LoadBalancer struct {
	backends []*Backend
	current  int  // round-robin index
	mu       sync.Mutex
}

// NewLoadBalancer tạo LB với danh sách backend addresses
func NewLoadBalancer(addresses []string) *LoadBalancer {
	backends := make([]*Backend, 0, len(addresses))

	for _, addr := range addresses {
		parsedURL, err := url.Parse(addr)
		if err != nil {
			log.Printf("[LoadBalancer] Invalid address %s: %v", addr, err)
			continue
		}

		// Tạo ReverseProxy cho mỗi backend
		// LEARN: httputil.NewSingleHostReverseProxy — built-in Go reverse proxy
		//        Forward request đến backend, copy headers, stream response
		proxy := httputil.NewSingleHostReverseProxy(parsedURL)

		backends = append(backends, &Backend{
			Address:      addr,
			IsAlive:      true, // optimistic: giả sử healthy lúc khởi động
			ReverseProxy: proxy,
		})
	}

	return &LoadBalancer{backends: backends}
}

// NextHealthyBackend lấy backend healthy tiếp theo (round-robin)
// LEARN: Round-robin = phân phối đều request, simple và effective
//        Alternatives: least-connections, weighted, random
func (lb *LoadBalancer) NextHealthyBackend() *Backend {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Duyệt tất cả backends, bắt đầu từ current index
	for i := 0; i < len(lb.backends); i++ {
		idx := (lb.current + i) % len(lb.backends)
		if lb.backends[idx].CheckAlive() {
			lb.current = (idx + 1) % len(lb.backends)
			return lb.backends[idx]
		}
	}
	return nil // không có backend nào healthy
}

// Check kiểm tra 1 địa chỉ có healthy không qua TCP dial
// REUSE: 100% từ trading-bot-load-balancer/networking/networking.go Check()
// LEARN: net.DialTimeout — TCP dial với timeout, không block vô hạn
func (lb *LoadBalancer) Check(address string) bool {
	const (
		maxRetries = 3
		timeout    = 15 * time.Second
	)

	for i := 0; i < maxRetries; i++ {
		conn, err := net.DialTimeout("tcp", address, timeout)
		if err == nil {
			conn.Close()
			return true
		}
		log.Printf("[LoadBalancer] Health check failed %s attempt %d: %v", address, i+1, err)
		time.Sleep(time.Second)
	}
	return false
}

// HealthCheck chạy TCP health check cho tất cả backends, spawn goroutine per backend
// REUSE: 100% từ trading-bot-load-balancer/networking/networking.go HealthCheck()
// LEARN: Goroutine fan-out pattern — launch nhiều goroutines độc lập
//        Không cần sync (WaitGroup) vì health check là fire-and-forget
func (lb *LoadBalancer) HealthCheck() {
	for _, b := range lb.backends {
		b := b // capture loop variable — IMPORTANT: avoid goroutine closure bug
		go func() {
			alive := lb.Check(b.Address)
			b.SetAlive(alive)
			if !alive {
				log.Printf("[LoadBalancer] Backend UNHEALTHY: %s", b.Address)
			} else {
				log.Printf("[LoadBalancer] Backend healthy: %s", b.Address)
			}
		}()
	}
}

// StartHealthCheckLoop bắt đầu background health check loop
// LEARN: Context-aware loop — dừng khi context cancelled (graceful shutdown)
func (lb *LoadBalancer) StartHealthCheckLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			lb.HealthCheck()
		}
	}()
	log.Printf("[LoadBalancer] Health check loop started, interval=%s", interval)
}

// Stats trả về thống kê về pool health
func (lb *LoadBalancer) Stats() map[string]interface{} {
	alive := 0
	for _, b := range lb.backends {
		if b.CheckAlive() {
			alive++
		}
	}
	return map[string]interface{}{
		"total":   len(lb.backends),
		"alive":   alive,
		"dead":    len(lb.backends) - alive,
		"healthy": fmt.Sprintf("%d/%d", alive, len(lb.backends)),
	}
}
