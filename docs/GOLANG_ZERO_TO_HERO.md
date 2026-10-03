
# Golang Zero to Hero — Từ Sinh Viên Đến Backend Engineer
> Học Golang thông qua các dự án thực tế: Financial Trading Platform (Market Maker, Trading Bot, Payment Gateway)
>
> **Tham khảo sách O'Reilly:** _Go Concurrency in Practice_ · _Fundamentals of Software Architecture_ · _Designing Data-Intensive Applications_ · _Building Microservices_ · _gRPC: Up and Running_ · _RESTful Web API Patterns and Practices_

## Mục lục

1. [Tại sao chọn Golang?](#1-tại-sao-chọn-golang)
2. [Kiến trúc tổng thể của hệ thống](#2-kiến-trúc-tổng-thể-của-hệ-thống)
3. [Go Fundamentals — Nền tảng ngôn ngữ](#3-go-fundamentals--nền-tảng-ngôn-ngữ)
4. [Con trỏ (Pointers)](#4-con-trỏ-pointers)
5. [Struct, Interface và Composition](#5-struct-interface-và-composition)
6. [Concurrency — Goroutine & Channel](#6-concurrency--goroutine--channel)
7. [Error Handling trong Go](#7-error-handling-trong-go)
8. [Package & Module System](#8-package--module-system)
9. [RESTful API với Gin](#9-restful-api-với-gin)
10. [gRPC & Protocol Buffers](#10-grpc--protocol-buffers)
11. [Database — GORM & MySQL](#11-database--gorm--mysql)
12. [Redis — Cache & Pub/Sub](#12-redis--cache--pubsub)
13. [Kafka — Message Queue](#13-kafka--message-queue)
14. [Design Patterns trong Go](#14-design-patterns-trong-go)
15. [Layered Architecture — Kiến trúc phân lớp](#15-layered-architecture--kiến-trúc-phân-lớp)
16. [Microservices Architecture](#16-microservices-architecture)
17. [Authentication & Security](#17-authentication--security)
18. [Background Jobs & Cron](#18-background-jobs--cron)
19. [Priority Queue & Data Structures](#19-priority-queue--data-structures)
20. [Configuration Management](#20-configuration-management)
21. [Graceful Shutdown](#21-graceful-shutdown)
22. [Lộ trình học tập](#22-lộ-trình-học-tập)

## 1. Tại sao chọn Golang?

### 1.1 Golang là gì?

Go (Golang) là ngôn ngữ lập trình được Google tạo ra năm 2007, ra mắt công khai năm 2009. Được thiết kế bởi Rob Pike, Ken Thompson và Robert Griesemer — những người đã viết Unix và C.

**Triết lý thiết kế:** _"Less is more"_ — ít tính năng, nhưng mỗi tính năng đều rất mạnh.

### 1.2 Ưu điểm so với các ngôn ngữ khác

| Tính năng | Go | Python | Java | Node.js |
|-----------|----|----|----|----|
| **Tốc độ** | ⚡ Compiled, gần C | 🐢 Interpreted | 🔶 JVM overhead | 🔶 Single-thread |
| **Concurrency** | ✅ Goroutines (lightweight) | ❌ GIL | ✅ Threads (heavy) | ✅ Async/Await |
| **Memory** | ✅ GC + stack auto-grow | ✅ GC | ✅ GC | ✅ GC |
| **Type Safety** | ✅ Static + inferred | ❌ Dynamic | ✅ Static | ❌ Dynamic |
| **Deployment** | ✅ Single binary | ❌ Runtime cần | ❌ JVM cần | ❌ Node cần |
| **Learning Curve** | 🟢 Thấp | 🟢 Rất thấp | 🔴 Cao | 🟡 Trung bình |

### 1.3 Tại sao dự án trading platform dùng Go?

Trong các repo này, Go được chọn vì:
- **Latency thấp**: Trading yêu cầu xử lý lệnh trong microseconds
- **High throughput**: Xử lý hàng nghìn lệnh/giây từ nhiều sàn (Binance, BingX, Bybit, OKX)
- **Goroutines**: Mỗi bot trading chạy song song không block nhau
- **Single binary**: Deploy lên AWS/EC2 chỉ cần copy 1 file
- **gRPC**: Giao tiếp giữa microservices cực nhanh (binary protocol)

> 📖 **Đọc thêm:** _"Go in Action"_ (O'Reilly) — Chapter 1: Introducing Go

## 2. Kiến trúc tổng thể của hệ thống

### 2.1 Sơ đồ kiến trúc

---
```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT (Frontend)                         │
└─────────────────────┬───────────────────────────────────────────┘
                       │ HTTP/REST
          ┌────────────▼────────────┐
          │   trading-bot-api       │  ← REST API Gateway (port 8080)
          │   market-maker-api      │    Gin Framework + JWT Auth
          └──┬──────────┬──────────┘
             │ gRPC     │ gRPC
    ┌─────────▼──┐  ┌───▼──────────────┐
    │ trading-bot │  │ trading-bot-     │
    │ trading-    │  │ load-balancer    │  ← Phân phối bot sang servers
    │ gateway     │  └──────────────────┘
    └─────────────┘
             │ gRPC
    ┌─────────▼──────────┐
    │ trading-bot-        │  ← Kế toán / Ví tiền
    │ accounting          │
    └─────────────────────┘

    ┌─────────────────────┐    ┌─────────────────────┐
    │ market-maker-       │    │ trading-bot-         │
    │ background          │    │ finance-service      │  ← Kafka Consumer
    │ (Cronjob)           │    └─────────────────────┘
    └─────────────────────┘

    ┌──────────────────────────────────────────────┐
    │              Infrastructure                   │
    │   MySQL (GORM) │ Redis │ Kafka │ gRPC         │
    └──────────────────────────────────────────────┘
```
---
### 2.2 Vai trò từng repo

| Repo | Vai trò | Công nghệ chính |
|------|---------|-----------------|
| `trading-bot-api` | REST API Gateway cho người dùng | Gin, JWT, gRPC Client |
| `market-maker-api` | REST API cho Market Maker | Gin, JWT, GORM |
| `trading-bot-trading-gateway` | gRPC Server quản lý jobs trading | gRPC, GORM |
| `trading-bot-load-balancer` | Phân tải bot sang trading servers | gRPC |
| `trading-bot-accounting` | Quản lý giao dịch tài chính | gRPC, GORM |
| `market-maker-background` | Cronjob thu thập dữ liệu asset | Cron, Redis, Python interop |
| `trading-bot-finance-service` | Kafka consumer xử lý tài chính | Kafka, gRPC |
| `trading-bot-protobuf` | Shared protobuf definitions | Protocol Buffers |
| `payment-gateway` | Xử lý thanh toán | Go |

## 3. Go Fundamentals — Nền tảng ngôn ngữ

### 3.1 Cấu trúc một chương trình Go

---
```go
// File: trading-bot-api/main.go
package main              // ← Mỗi file Go phải có package

import (
    "gateway-api/app"     // ← Import package nội bộ
    "log"                 // ← Import standard library
)

func main() {             // ← Entry point, không có return type
    err := app.NewServer()
    if err != nil {
        log.Printf("[NewServer] [Panic]: %s\n", err.Error())
        panic(err)
    }
}
```
---
**Khái niệm quan trọng:**
- `package main` — Package đặc biệt, chứa hàm `main()` để chạy chương trình
- `import` — Khai báo dependencies, Go tự động quản lý qua `go.mod`
- Không có `try/catch` — Go dùng multiple return values để xử lý lỗi

### 3.2 Các kiểu dữ liệu cơ bản

---
```go
// Số nguyên
var age int = 25
var count int64 = 1000000
var botId uint = 42           // Unsigned (không âm)

// Số thực
var price float64 = 0.0015    // Giá crypto

// Boolean
var isActive bool = true

// String
var symbol string = "BTCUSDT"

// Short declaration (phổ biến nhất)
name := "BingX"               // Go tự suy ra type là string
port := 8080                  // Go suy ra type là int
```
---
### 3.3 Zero Values — Giá trị mặc định

Go KHÔNG có `null` hay `undefined`. Mỗi type có giá trị mặc định (zero value):

---
```go
var i int      // = 0
var f float64  // = 0.0
var b bool     // = false
var s string   // = ""
var p *int     // = nil (con trỏ)
```
---
### 3.4 Functions — Hàm

---
```go
// Hàm với multiple return values — ĐẶC TRƯNG của Go
func InitializeDatabase(cfg *configuration.Config) (*Gorm, error) {
    // ...
    return &Gorm{Database: DB}, nil  // Trả về 2 giá trị
}

// Gọi hàm
repo, err := repository.InitializeDatabase(cfg)
if err != nil {
    return err  // Xử lý lỗi ngay lập tức
}

// Hàm không return gì
func Cronjob(svc background.Service) {
    // ...
}
```
---
### 3.5 Constants — Hằng số

---
```go
// Trong dự án: trading-bot-api/constants/
const port = 8080

// iota — tự động tăng giá trị
type AUTH_ROLE int
const (
    USER        AUTH_ROLE = iota + 1  // = 1
    ADMIN       AUTH_ROLE = iota + 1  // = 2
    SUPER_ADMIN AUTH_ROLE = iota + 1  // = 3
)
```
---
## 4. Con trỏ (Pointers)

> 📖 **Đọc thêm:** _"The Go Programming Language"_ (O'Reilly) — Chapter 2: Program Structure

### 4.1 Con trỏ là gì?

Con trỏ là **địa chỉ ô nhớ** của một biến. Thay vì copy toàn bộ dữ liệu, bạn truyền địa chỉ — tiết kiệm memory và cho phép modify trực tiếp.

---
```
Stack Memory:
┌──────────────────────────┐
│  cfg (địa chỉ: 0xC00012) │ ← Con trỏ, chỉ lưu địa chỉ (8 bytes)
└──────────┬───────────────┘
           │ trỏ đến
           ▼
Heap Memory:
┌──────────────────────────────────────────┐
│  Config{MySQL: ..., Redis: ..., Web: ...} │ ← Dữ liệu thực (có thể KB)
└──────────────────────────────────────────┘
```
---
### 4.2 Ký hiệu con trỏ trong Go

---
```go
var x int = 42
var p *int = &x     // & = lấy địa chỉ của x
fmt.Println(*p)     // * = dereferencing, lấy giá trị tại địa chỉ → in ra 42
*p = 100            // Sửa giá trị tại địa chỉ → x cũng thành 100
```
---
### 4.3 Con trỏ trong dự án thực tế

**Ví dụ 1: Truyền Config bằng con trỏ (không copy toàn bộ struct)**
---
---
---
```go
// market-maker-api/configuration/configuration.go
func FromEnv() (*Config, error) {   // ← Trả về *Config (con trỏ)
    cfg := &Config{}                // ← & tạo con trỏ đến Config mới trên heap
    cfg.MySQL.DNS = "..."
    return cfg, nil
}

// Sử dụng:
cfg, err := configuration.FromEnv()
// cfg là *Config — nếu truyền cfg đi nơi khác, không copy toàn bộ Config
repo, err := repository.InitializeDatabase(cfg)  // Truyền con trỏ, không copy
```
---
**Ví dụ 2: Pointer receiver — Method trên struct**
---
---
---
```go
// BackgroundService implement interface Service
func (s *BackgroundService) GetAsset() error {
//    ^ s là con trỏ đến BackgroundService
//    Nếu dùng (s BackgroundService) thì mỗi lần gọi sẽ copy toàn bộ struct
    s.repo.CreateAssetInfo(arg)  // Modify được field của struct gốc
    return nil
}
```
---
**Ví dụ 3: Nil pointer — Kiểm tra trước khi dùng**
---
---
---
```go
func (s *Server) CreateJob(ctx context.Context, req *tradinggatewayProto.CreateJobRequest) (*tradinggatewayProto.CreateJobResponse, error) {
    if req == nil {                  // ← Luôn kiểm tra nil trước khi dùng
        return nil, fmt.Errorf("invalid request")
    }
    // ...
}
```
---
### 4.4 Khi nào dùng pointer, khi nào không?

| Dùng `*T` (pointer) | Dùng `T` (value) |
|---------------------|------------------|
| Struct lớn (Config, DB) | Primitive types (int, bool, string) |
| Cần modify struct gốc | Chỉ đọc dữ liệu |
| Có thể nil (optional) | Luôn có giá trị |
| Interface implementation | Simple function args |

## 5. Struct, Interface và Composition

> 📖 **Đọc thêm:** _"Fundamentals of Software Architecture"_ (O'Reilly) — Chapter 10: Layered Architecture

### 5.1 Struct — Cấu trúc dữ liệu

---
```go
// market-maker-api/models/auth.go
type User struct {
    gorm.Model              // ← Embedding: "kế thừa" fields từ gorm.Model
    Username string         `gorm:"size:255;not null;unique" json:"username"`
    Password string         `gorm:"size:255;not null;" json:"password"`
    Email    string         `gorm:"not null;unique" json:"email"`
    RoleId   constants.AUTH_ROLE `gorm:"default:1" json:"role_id"`
}
// gorm.Model tự động thêm: ID, CreatedAt, UpdatedAt, DeletedAt
```
---
**Struct tags (backtick):** Metadata cho các library khác đọc:
- `` `gorm:"size:255"` `` → GORM biết tạo column VARCHAR(255)
- `` `json:"username"` `` → encoding/json dùng key "username" thay vì "Username"

### 5.2 Interface — Hợp đồng hành vi

Interface là **tập hợp method signatures**. Bất kỳ type nào implement đủ method đó → tự động satisfy interface (không cần `implements` như Java).

---
```go
// market-maker-background/pkg/background-mm/services/background/background.go

// Định nghĩa interface — "Service phải có 2 method này"
type Service interface {
    GetAsset() error
    CheckProviderSlowResponse() error
}

// Struct implement interface
type BackgroundService struct {
    repo     Repository
    cfg      *configuration.Config
    redisDB0 redis.Service
}

// Implement method 1
func (s *BackgroundService) GetAsset() error { ... }

// Implement method 2
func (s *BackgroundService) CheckProviderSlowResponse() error { ... }

// Constructor — trả về interface, không trả về concrete type
func NewService(...) Service {
    return &BackgroundService{...}  // ← *BackgroundService implement Service
}
```
---
**Tại sao trả về Interface thay vì Struct?**

---
```go
// ❌ Sai — phụ thuộc vào concrete implementation
func NewService() *BackgroundService { ... }

// ✅ Đúng — phụ thuộc vào abstraction (Dependency Inversion Principle)
func NewService() Service { ... }

// Lợi ích: Có thể swap implementation mà không sửa code caller
// Dùng mock trong testing:
type MockService struct{}
func (m *MockService) GetAsset() error { return nil }  // Fake cho test
```
---
### 5.3 Redis Interface — Ví dụ điển hình

---
```go
// trading-bot-api/redis/redis.go

type Service interface {         // ← Interface
    Set(key string, value interface{}, time time.Duration) error
    Get(key string) string
    Scan(key string) []string
    Del(key string) error
    Ping() bool
    Publish(channel string, payload []byte) error
    Subscribe(channel string) *redis.PubSub
}

type RedisStore struct {         // ← Concrete implementation
    Client *redis.Client
}

func (r *RedisStore) Get(key string) string {
    // ... real Redis call
}
```
---
**Tại sao hay?** Bạn có thể tạo `MockRedis` cho unit test mà không cần Redis thật.

### 5.4 Repository Interface Pattern

---
```go
// Tách biệt "contract" với "implementation"

type Repository interface {
    CreateAssetInfo(args *models.AssetInfo) error
}

// Service chỉ biết "Repository có thể CreateAssetInfo"
// Không quan tâm là MySQL, PostgreSQL hay MongoDB
type BackgroundService struct {
    repo Repository   // ← Nhận vào interface, không phải concrete type
}
```
---
## 6. Concurrency — Goroutine & Channel

> 📖 **Đọc thêm:** _"Concurrency in Go"_ (O'Reilly) — Cả cuốn sách quan trọng nhất cho Go

### 6.1 Goroutine — "Thread siêu nhẹ"

---
```go
// Thread hệ điều hành: ~1MB stack, tốn kém khi tạo
// Goroutine: ~2KB stack, Go runtime quản lý, có thể tạo hàng triệu cái

go func() {                              // ← Từ khóa "go" = chạy async
    if err := server.ListenAndServe(); err != nil {
        log.Fatalf("listen: %s\n", err)
    }
}()
// Code tiếp theo chạy ngay, không đợi goroutine trên
```
---
### 6.2 Channel — Giao tiếp giữa Goroutines

---
```go
// "Don't communicate by sharing memory; share memory by communicating"
// — Rob Pike, Go creator

// Channel là pipe để goroutine gửi/nhận data an toàn
stopCh := make(chan struct{})   // Channel không có dữ liệu, chỉ signal

// Goroutine lắng nghe signal
go func() {
    <-stopCh               // Block tại đây, chờ signal
    server.GracefulStop()  // Khi nhận signal, dừng gracefully
}()
```
---
### 6.3 select — Chờ nhiều channel cùng lúc

---
```go
// trading-bot-api/queue/queue.go

for {
    select {
    case <-q.stopCh:           // Nếu stopCh có data → stop
        return
    case <-nextReadyAt:        // Nếu timer expire → process
        // handle timer
    case waitObj := <-q.waitItemCh:   // Nếu có item mới → enqueue
        if waitObj.readyAt.After(time.Now()) {
            heap.Push(waitingForQueue, waitObj)
        } else {
            q.Add(waitObj.value)
        }
    }
}

```
---
---
```

---

## Tóm tắt các syntax cần nhớ

| Syntax | Ý nghĩa | Tương đương |
|---|---|---|
| `for { }` | Lặp vô tận | `while True:` (Python) |
| `select { case }` | Canh nhiều channel | Không có trong Python/Java |
| `<-ch` | Nhận từ channel | `ch.get()` |
| `ch <- x` | Gửi vào channel | `ch.put(x)` |
| `x := <-ch` | Nhận + lưu vào biến | `x = ch.get()` |
| `obj.field` | Lấy field của struct | `obj.field` (giống mọi ngôn ngữ) |
| `obj.Method()` | Gọi method | `obj.method()` (giống mọi ngôn ngữ) |
```
---
### 6.4 sync.Mutex — Bảo vệ dữ liệu chia sẻ

---
```go
// Định nghĩa struct "queue" — cái hộp chứa mọi thứ của queue
type queue struct {
    failuresLock sync.Mutex          // Ổ khóa bảo vệ failures bên dưới
    failures     map[interface{}]int // Bảng đếm: item → số lần thất bại
    cond         *sync.Cond          // Chuông báo cho goroutine đang chờ
    closing      bool                // Queue đang đóng không?
}

// Method "When" của queue — trả về: phải chờ bao lâu trước khi retry
func (q *queue) When(item interface{}) time.Duration {
    q.failuresLock.Lock()            // Khóa cửa (chỉ 1 goroutine vào)
    defer q.failuresLock.Unlock()    // Hứa: khi hàm xong, mở cửa

    exp := q.failures[item]          // Đọc số lần item này đã fail
    q.failures[item]++               // Tăng số lần fail lên 1
    // (sau đó tính 2^exp để ra thời gian chờ)
}
```
---
### 6.5 Context — Quản lý lifecycle

---
```go
// trading-bot-api/app/server.go

// Context lan truyền cancel signal xuống toàn bộ goroutine tree
ctx, stop := signal.NotifyContext(context.Background(), 
                    syscall.SIGINT, syscall.SIGTERM)
defer stop()   // Gọi stop() khi function return

go background.RunBackgroundTasks(ctx, svc)  // Nhận ctx, biết khi nào cần dừng

server := &http.Server{...}
go func() {
    server.ListenAndServe()
}()

<-ctx.Done()   // Block ở đây, chờ SIGINT (Ctrl+C) hoặc SIGTERM (kill)

// Graceful shutdown với timeout
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
defer cancel()
server.Shutdown(ctx)  // Dừng nhận request mới, chờ tối đa 3 giây
```
---
**Context Flow:**
---
---
---
```
main() context.Background()
  └── signal.NotifyContext()      ← Cancel khi nhận OS signal
        ├── background goroutine  ← Nhận ctx, dừng khi ctx.Done()
        └── HTTP server           ← Graceful shutdown
```
---
### 6.6 Pub/Sub với Redis Channel

---
```go
// trading-bot-api/redis/redis.go

// Publisher
func (r *RedisStore) Publish(channel string, payload []byte) error {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    return r.Client.Publish(ctx, channel, payload).Err()
}

// Subscriber
func (r *RedisStore) Subscribe(channel string) *redis.PubSub {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    return r.Client.Subscribe(ctx, channel)
}
Được, giải thích từng dòng nhé!

---

## Bức tranh tổng thể — Pub/Sub là gì?

**Pub/Sub** (Publish/Subscribe) = mô hình **kênh phát thanh**:

```
---
Publisher (người phát)          Channel (kênh)         Subscriber (người nghe)
      │                              │                        │
      │  Publish("orders", data) ───►│                        │
      │                              │──── nhận data ────────►│
      │                              │──── nhận data ────────►│ (nhiều người nghe)
---
```

Trong trading bot, ví dụ:
- **Publisher**: service tạo lệnh mua/bán → gửi vào channel `"orders"`
- **Subscriber**: service xử lý lệnh → lắng nghe channel `"orders"` để nhận và thực thi

---

## Phần 1: `func (r *RedisStore) Publish(...)` — Gửi dữ liệu vào Redis channel

```
---
func (r *RedisStore) Publish(channel string, payload []byte) error {
---
```

Phân tích từng phần:

| Phần | Ý nghĩa |
|---|---|
| `(r *RedisStore)` | Receiver — method này thuộc struct `RedisStore`, dùng `r` để truy cập |
| `channel string` | Tham số 1: tên kênh, kiểu string (ví dụ: `"orders"`, `"prices"`) |
| `payload []byte` | Tham số 2: dữ liệu cần gửi, kiểu **mảng byte** (raw binary / JSON đã encode) |
| `error` | Kiểu trả về — trả về lỗi nếu có, `nil` nếu thành công |

> 💡 `[]byte` = mảng byte — cách Go lưu dữ liệu nhị phân hoặc text đã được encode. Tương đương `bytes` trong Python.

---

## Phần 2: `context.WithCancel(...)` — Context có thể hủy thủ công

```
---
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
---
```

Bạn đã biết `context.WithTimeout` (tự hủy sau N giây). `context.WithCancel` thì **không có timeout** — chỉ hủy khi bạn **tự gọi `cancel()`**.

```
---
context.WithTimeout  → tự hủy sau 3 giây
context.WithCancel   → chỉ hủy khi gọi cancel() thủ công
---
```

Ở đây dùng `defer cancel()` → ngay khi hàm `Publish` return xong, `cancel()` được gọi → **context bị hủy → Redis client dọn dẹp tài nguyên liên quan**.

> 💡 Đây là pattern **bắt buộc** trong Go: tạo context → luôn `defer cancel()` ngay dòng tiếp theo, không bao giờ quên.

---

## Phần 3: `return r.Client.Publish(ctx, channel, payload).Err()`

```
---
return r.Client.Publish(ctx, channel, payload).Err()
---
```

Đọc từ trái sang phải:

```
---
r.Client          .Publish(ctx, channel, payload)        .Err()        return
    ↑                          ↑                            ↑              ↑
Redis client       Gọi lệnh PUBLISH của Redis        Lấy lỗi ra      Trả về lỗi đó
(kết nối Redis)    trả về một *Cmd object             (nil nếu OK)
---
```

Đây là **method chaining** — gọi hàm liên tiếp trên kết quả trả về:

```
---
// Viết tách ra cho dễ hiểu:
cmd := r.Client.Publish(ctx, channel, payload)  // Gửi lệnh PUBLISH tới Redis
err := cmd.Err()                                // Kiểm tra có lỗi không
return err                                      // Trả lỗi về cho người gọi
---
```

---

## Phần 4: `func (r *RedisStore) Subscribe(...)` — Lắng nghe Redis channel

```
---
func (r *RedisStore) Subscribe(channel string) *redis.PubSub {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    return r.Client.Subscribe(ctx, channel)
}
---
```

Khác với `Publish`:
- **Kiểu trả về**: `*redis.PubSub` — trả về một **object subscription** để caller dùng tiếp (nhận message từ channel)
- Không trả về `error` vì kết nối subscribe là **liên tục** — lỗi xảy ra khi đọc message, không phải khi subscribe

Người gọi hàm này sẽ dùng object trả về như sau:
```
---
sub := redisStore.Subscribe("orders")
msg := <-sub.Channel()   // Block, chờ message mới từ kênh "orders"
fmt.Println(msg.Payload) // In nội dung message
---
```

---

## Luồng dữ liệu hoàn chỉnh trong trading bot

```
---
Trading Bot Service                          Finance Service
       │                                           │
       │  redisStore.Publish(                      │
       │    "order_executed",                      │
       │    orderJSON,                             │
       │  )                                        │
       │ ──────────────────── Redis ──────────────►│
       │                   "order_executed"        │ sub := Subscribe("order_executed")
       │                      channel              │ msg := <-sub.Channel()
       │                                           │ // xử lý thanh toán...
---
```

---

## So sánh 2 hàm

| | `Publish` | `Subscribe` |
|---|---|---|
| **Vai trò** | Gửi message | Nhận message |
| **Trả về** | `error` | `*redis.PubSub` |
| **Blocking?** | Không — gửi xong là xong | Có — caller dùng object trả về để chờ |
| **Dùng khi** | Có event mới cần thông báo | Cần lắng nghe event từ service khác |

---

## Tóm tắt syntax mới

| Syntax | Ý nghĩa |
|---|---|
| `[]byte` | Mảng byte (raw binary / JSON bytes) |
| `context.WithCancel(ctx)` | Tạo context hủy thủ công |
| `cancel()` | Hủy context |
| `a.B().C()` | Method chaining — gọi liên tiếp trên kết quả |
| `.Err()` | Lấy lỗi từ Redis command result |
| `*redis.PubSub` | Con trỏ tới object subscription của Redis |
```
---
## 7. Error Handling trong Go

> Go không có exceptions. **Lỗi là giá trị** (errors are values).

### 7.1 Idiomatic Error Handling

---
```go
// Pattern chuẩn trong mọi file của dự án:
repo, err := repository.InitializeDatabase(cfg)
if err != nil {
    return err   // Bubble up lỗi
}

// Wrap lỗi với context
if !redisDB0.Ping() {
    return fmt.Errorf("[RedisServer] connection was down")
    //     ^^^^^^^^^ Thêm context vào lỗi
}

// Wrap lỗi với %w (unwrappable)
return fmt.Errorf("[CreateAssetInfo] failed: %w", err)
```
---
### 7.2 Multiple Return Values

---
```go
// Hàm luôn trả về (result, error)
func InitializeDatabase(cfg *Config) (*Gorm, error) {
    db, err := gorm.Open(...)
    if err != nil {
        return nil, err    // ← Trả về nil cho result, lỗi thực
    }
    return &Gorm{db}, nil  // ← Trả về result, nil cho error
}
```
---
### 7.3 defer — Cleanup đảm bảo

---
```go
// market-maker-background/.../asset.go
func (s *BackgroundService) CheckProviderSlowResponse() error {
    orderBookFile, err := os.Open(path)
    if err != nil { return err }
    defer orderBookFile.Close()   // ← Luôn chạy khi function return, dù lỗi hay không
    // Không cần lo file leak
}
```
---
## 8. Package & Module System

### 8.1 go.mod — Quản lý dependencies

---
```go
// trading-bot-api/go.mod
module gateway-api         // ← Tên module (import path prefix)

go 1.21.6                  // ← Phiên bản Go tối thiểu

require (
    github.com/PrimeNumber108/trading-bot-protobuf v1.1.18  // ← Protobuf shared
    github.com/gin-gonic/gin v1.9.1                         // ← HTTP framework
    github.com/go-redis/redis/v8 v8.11.5                    // ← Redis client
    github.com/segmentio/kafka-go v0.4.47                   // ← Kafka client
    google.golang.org/grpc v1.62.1                          // ← gRPC
    gorm.io/gorm v1.25.8                                    // ← ORM
)
```
---
### 8.2 Package Organization trong dự án

---
```
trading-bot-api/
├── main.go              ← package main (entry point)
├── app/                 ← package app (server setup)
│   ├── server.go
│   ├── client.go       ← gRPC client connections
│   └── signal.go       ← OS signal handling
├── api/rest/            ← package rest (HTTP handlers)
├── services/            ← package services (business logic)
│   ├── services.go     ← DI container
│   └── authentication/ ← package authentication
├── repository/          ← package repository (data access)
├── models/              ← package models (data structures)
├── kafka/               ← package kafka (message queue)
├── redis/               ← package redis (cache)
└── configuration/       ← package configuration (config)
```
---
**Quy tắc:**
- Tên package = tên thư mục (lowercase)
- Chỉ export symbol bắt đầu bằng chữ HOA: `NewService()` vs `newService()`
- Circular imports bị cấm — phải thiết kế dependency flow rõ ràng

## 9. RESTful API với Gin

> 📖 **Đọc thêm:** _"RESTful Web API Patterns and Practices Cookbook"_ (O'Reilly)

### 9.1 Gin Framework Setup

---
```go
// market-maker-api/api/rest.go

func NewAPI(svc *services.Services, 
            cfg *configuration.Config, 
            redisDB0 redis.Service) *gin.Engine {

    router := gin.Default()     // ← Logger + Recovery middleware tự động

    // Security middleware theo môi trường
    switch cfg.Web.Mode {
    case "dev":
        router.Use(middlewares.SecureNoRestrict(cfg))
    default:
        router.Use(middlewares.Secure(cfg))     // ← CORS, HSTS, etc.
    }

    // Route Groups — phân cấp URL
    serverRouter  := router.Group("/api")
    publicRouter  := serverRouter.Group("/v1")
    apiRouter     := publicRouter.Group("/fin20")
    apiRouter.Use(middlewares.JwtAuthMiddleware(cfg, redisDB0))  // ← Auth middleware

    adminRouter := serverRouter.Group("/v1/admin/fin20")
    adminRouter.Use(middlewares.Admin(cfg, redisDB0))            // ← Admin only

    return router
}
```
---
**URL Structure:**
---
---
---
```
/api/v1/login              ← Public (không cần token)
/api/v1/fin20/user         ← Authenticated user
/api/v1/admin/fin20/user   ← Admin only
```
---
### 9.2 HTTP Handler Pattern

---
```go
// market-maker-api/api/authentication.go

type AuthenticationHandler struct {
    AuthenticationSvc authentication.Service  // ← Inject service
}

func (a *AuthenticationHandler) Login(ctx *gin.Context) {
    // 1. Parse request body
    args := &requests.LoginRequest{}
    if err := ctx.ShouldBindJSON(args); err != nil {
        ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    // 2. Validate
    if err := args.Validate(); err != nil {
        ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    // 3. Business logic
    result, err := a.AuthenticationSvc.Login(args)
    if err != nil {
        ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    // 4. Response
    ctx.JSON(http.StatusOK, result)
}
POST /login
{ "email": "trader@gmail.com", "password": "mypass123" }
        │
        ▼
┌─────────────────────────────────────────────────┐
│  ShouldBindJSON                                 │
│  args.Email = "trader@gmail.com"                │
│  args.Password = "mypass123"                    │
│  Lỗi? → 400 Bad Request                        │
└────────────────────┬────────────────────────────┘
                     ▼
┌─────────────────────────────────────────────────┐
│  args.Validate()                                │
│  Email đúng format chưa? Password đủ dài chưa? │
│  Lỗi? → 400 Bad Request                        │
└────────────────────┬────────────────────────────┘
                     ▼
┌─────────────────────────────────────────────────┐
│  AuthenticationSvc.Login(args)                  │
│  Tìm user trong DB...                           │
│  So sánh password hash...                       │
│  Tạo JWT token...                               │
│  Lỗi? → 400 Bad Request                        │
└────────────────────┬────────────────────────────┘
                     ▼
         200 OK { "token": "eyJhbG..." }
```
---
### 9.3 RESTful HTTP Status Codes

| Code | Ý nghĩa | Khi nào dùng |
|------|---------|--------------|
| `200 OK` | Thành công | GET, PUT, DELETE thành công |
| `201 Created` | Tạo thành công | POST tạo resource mới |
| `400 Bad Request` | Request sai | Validation error, bad JSON |
| `401 Unauthorized` | Chưa xác thực | Không có token |
| `403 Forbidden` | Không có quyền | Có token nhưng không đủ role |
| `404 Not Found` | Không tìm thấy | Resource không tồn tại |
| `500 Internal Server Error` | Lỗi server | Unexpected error |

### 9.4 Middleware Chain

---
```
Request
   │
   ▼
[Gin Default]    ← Logger (log mọi request) + Recovery (panic → 500)
   │
   ▼
[Secure]         ← CORS, security headers
   │
   ▼
[JwtAuth]        ← Verify JWT token
   │
   ▼
[Handler]        ← Business logic
   │
   ▼
Response
```
---
## 10. gRPC & Protocol Buffers

> 📖 **Đọc thêm:** _"gRPC: Up and Running"_ (O'Reilly)

### 10.1 tại sao gRPC thay vì REST?

| | REST (JSON) | gRPC (Protobuf) |
|--|-----|------|
| **Format** | Text JSON | Binary |
| **Size** | Lớn hơn | ~3-10x nhỏ hơn |
| **Speed** | Chậm hơn | Nhanh hơn |
| **Type Safety** | Không | Có (generated code) |
| **Versioning** | Manual | Built-in field numbers |
| **Use case** | Client-Server public API | Service-to-Service internal |

**Trong dự án:** REST cho Frontend ↔ Backend. gRPC cho Backend service ↔ Backend service.

### 10.2 Định nghĩa Proto File

---
```protobuf
// trading-bot-protobuf/trading-gateway/trading-gateway.proto
syntax = "proto3";

package protobuf;
option go_package = "./protobuf/trading-gateway";

// Service định nghĩa các RPC methods
service Job {
    rpc CreateJob (CreateJobRequest) returns (CreateJobResponse) {}
    rpc GetJob    (GetJobRequest)    returns (GetJobResponse) {}
    rpc UpdateJob (UpdateJobRequest) returns (UpdateJobResponse) {}
    rpc DeleteJob (DeleteJobRequest) returns (DeleteJobResponse) {}
}

// Message = struct data
message CreateJobRequest {
    int64  bot_id       = 1;  // ← Field number (quan trọng cho versioning)
    string body         = 2;
    string from_service = 3;
}

message CreateJobResponse {
    string message = 1;
}
```
---
**Compile proto → Go code:**
---
---
---
```bash
protoc --go_out=. --go-grpc_out=. trading-gateway.proto
# Tạo ra: trading-gateway.pb.go + trading-gateway_grpc.pb.go
```
---
### 10.3 gRPC Server Implementation

---
```go
// trading-bot-trading-gateway/pkg/trading-gateway/api/rpc/rpc.go

// Embed generated interface để "implement" server
type Server struct {
    tradinggatewayProto.UnimplementedJobServer     // ← Unimplemented = safe defaults
    tradinggatewayProto.UnimplementedTokenServer
    tradinggatewayProto.UnimplementedLoggingServer
    Services *Services
}

func (s *Server) ListenAndServe(stopCh <-chan struct{}) error {
    // 1. Tạo TCP listener
    listen, err := net.Listen("tcp", s.Services.Cfg.Server.GRPC)

    // 2. Tạo gRPC server
    server = grpc.NewServer()

    // 3. Đăng ký services
    tradinggatewayProto.RegisterJobServer(server, s)
    tradinggatewayProto.RegisterTokenServer(server, s)

    // 4. Graceful stop khi nhận signal
    go func() {
        <-stopCh
        server.GracefulStop()
    }()

    // 5. Bắt đầu serve
    return server.Serve(listen)
}
```
---
### 10.4 gRPC Method Implementation

---
```go
// trading-bot-trading-gateway/pkg/trading-gateway/api/rpc/job.go

func (s *Server) CreateJob(ctx context.Context, req *tradinggatewayProto.CreateJobRequest) (*tradinggatewayProto.CreateJobResponse, error) {

    if req == nil {
        return nil, fmt.Errorf("invalid request")
    }

    // Route theo service name
    switch req.FromService {
    case constants.ACTION_SERVICE_NAME:
        args := &models.Action{BotId: uint(req.BotId), Body: req.Body}
        if err := s.Services.ActionSvc.CreateJob(args); err != nil {
            return nil, err
        }

    case constants.DATA_WORKER_SERVICE_NAME:
        // ...

    default:
        return nil, fmt.Errorf("[CreateJob] unknown service: %v", req.FromService)
    }

    return &tradinggatewayProto.CreateJobResponse{Message: "Success"}, nil
}
```
---
### 10.5 gRPC Client (Caller side)

---
```go
// trading-bot-api/app/client.go

func TradingGatewayTokenGrpcServer(cfg *configuration.Config) (tradinggatewayProto.TokenClient, error) {
    // Tạo connection đến gRPC server
    conn, err := grpc.Dial(
        cfg.Server.TradingGateway,
        grpc.WithTransportCredentials(insecure.NewCredentials()),  // ← Internal: không TLS
    )
    if err != nil {
        return nil, err
    }
    // Trả về client stub (auto-generated)
    return tradinggatewayProto.NewTokenClient(conn), nil
}

// Dùng trong service:
result, err := tradingGatewayClient.CreateToken(ctx, &proto.CreateTokenRequest{
    UserId:   userId,
    TokenKey: key,
})
```
```output
..\..\..\..\AppData\Local\Temp\main.go:20:1: syntax error: non-declaration statement outside function body
```
---

---
```go

```
---

### 10.6 Multi-Service gRPC Architecture

---
```
trading-bot-api (gRPC Client)
    │
    ├──→ trading-bot-trading-gateway :50001  (Job, Token, Logging)
    ├──→ trading-bot-load-balancer   :50002  (Server, SwitchBot)
    └──→ trading-bot-accounting      :50003  (AccountTransaction, ValidateTransaction)
```
---
## 11. Database — GORM & MySQL

> 📖 **Đọc thêm:** _"Designing Data-Intensive Applications"_ (O'Reilly) — Part I: Foundations

### 11.1 GORM Setup

---
```go
// market-maker-api/repository/repository.go

func InitializeDatabase(cfg *configuration.Config) (*Gorm, error) {

    // Open connection
    DB, err = gorm.Open(mysql.Open(cfg.MySQL.DNS), &gorm.Config{
        Logger: logger.Default.LogMode(logger.Error),
    })

    // AutoMigrate — tự động tạo/update table theo struct
    err = DB.AutoMigrate(
        &models.User{},
        &models.UserSession{},
        &models.AssetInfo{},
        &models.FinalOrder{},
        // ...
    )

    return &Gorm{Database: DB}, err
}
```
---
**MySQL DNS format:**
---
---
---
```
user:password@tcp(host:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local
```
---
### 11.2 Model với GORM

---
```go
type FinalOrder struct {
    gorm.Model                                    // ID, CreatedAt, UpdatedAt, DeletedAt
    OrderId       string `gorm:"primary_key;unique;not null" json:"order_id"`
    Symbol        string `json:"symbol"`
    Exchange      string `json:"exchange"`
    Quantity      string `json:"quantity"`
    Status        string `json:"status"`
    Price         string `json:"price"`
    Side          string `json:"side"`            // "buy" hoặc "sell"
    FilledPrice   string `json:"filled_price"`
    FilledSize    string `json:"filled_size"`
    Fee           string `json:"fee"`
}
```
---
### 11.3 GORM Soft Delete

`gorm.Model` có field `DeletedAt *gorm.DeletedAt`. Khi gọi `DB.Delete(&record)`:
- Không xóa khỏi database
- Chỉ set `deleted_at = NOW()`
- Query mặc định luôn thêm `WHERE deleted_at IS NULL`

### 11.4 CRUD Operations

---
```go
// Create
DB.Create(&models.User{Username: "alice", Email: "alice@example.com"})

// Read
var user models.User
DB.First(&user, userId)                    // Lấy theo ID
DB.Where("email = ?", email).First(&user)  // Lấy theo condition

// Update
DB.Model(&user).Update("status", "active")
DB.Model(&user).Updates(map[string]interface{}{"status": "active", "role_id": 2})

// Delete (soft delete)
DB.Delete(&user, userId)

// Query với pagination
var orders []models.FinalOrder
DB.Where("exchange = ?", exchange).
    Order("created_at DESC").
    Limit(20).Offset(page * 20).
    Find(&orders)
```
---
## 12. Redis — Cache & Pub/Sub

> Redis = **Re**mote **Di**ctionary **S**erver — In-memory data store

### 12.1 Redis Interface trong dự án

---
```go
// trading-bot-api/redis/redis.go

type Service interface {
    Set(key string, value interface{}, time time.Duration) error
    Get(key string) string
    Scan(key string) []string
    Del(key string) error
    Ping() bool
    Publish(channel string, payload []byte) error
    Subscribe(channel string) *redis.PubSub
}
```
---
### 12.2 Cách dùng Redis

---
```go
// Cache session user (TTL = 24 giờ)
err = redisDB0.Set(session.Session, sessionData, 24*time.Hour)

// Lấy session
cached := redis.Get(user.Session)
if cached == "" {
    return models.UserSession{}, fmt.Errorf("session not found")
}

// Delete khi logout
redis.Del(session)
```
---
### 12.3 Redis Pipeline

---
```go
// Xóa nhiều key cùng lúc (batch)
func (r *RedisStore) Dels(keys []string) error {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    pipe := r.Client.Pipeline()     // ← Gom nhiều lệnh, gửi 1 lần
    for _, key := range keys {
        pipe.Del(ctx, key)
    }
    _, err := pipe.Exec(ctx)        // ← Execute tất cả
    return err
}
```
---
**Tại sao Pipeline?** Thay vì N round-trips network, chỉ cần 1. Giảm latency đáng kể.

### 12.4 Multi-Database Redis

---
```go
// trading-bot-api/app/server.go
redisDB0 := redis.NewServer(cfg, cfg.Redis.DB_0)  // DB 0: session/auth
redisDB9 := redis.NewServer(cfg, cfg.Redis.DB_9)  // DB 9: feature flags

// Set feature flag
err = redisDB9.Set(constants.IsSwitchBalance, false, 0)  // TTL=0 = không expire
```
---
Redis hỗ trợ 16 DB (0-15), mỗi DB là namespace độc lập.

### 12.5 Redis cho Market Data Monitoring

---
```go
// market-maker-background/.../asset.go
keys, err := s.redisDB0.GetAllKeys()

for _, key := range keys {
    if utils.CheckProviderData(key) {
        value := s.redisDB0.Get(key)

        var data map[string]interface{}
        json.Unmarshal([]byte(value), &data)

        // Kiểm tra timestamp — provider chậm > 15 giây?
        timestamp := data["ts"].(float64)
        convertedTime := time.Unix(0, int64(timestamp)*int64(time.Millisecond))

        if time.Now().Sub(convertedTime).Seconds() >= 15 {
            // Alert: data provider slow response!
            log.Printf("Key: %v gap: %v seconds", key, ...)
        }
    }
}
```
---
## 13. Kafka — Message Queue

> 📖 **Đọc thêm:** _"Kafka: The Definitive Guide"_ (O'Reilly)

### 13.1 Kafka là gì?

Kafka là distributed event streaming platform. Dùng để:
- **Decouple services**: Service A publish event, Service B consume async
- **Buffering**: Xử lý traffic spike (pub nhiều hơn consumer xử lý được)
- **Replay**: Events được lưu trữ, có thể replay lại

### 13.2 Kafka Producer (Writer)

---
```go
// trading-bot-api/kafka/writer.go

func GetWriter(kafkaURL string) *kafka.Writer {
    return &kafka.Writer{
        Addr:     kafka.TCP(kafkaURL),
        Balancer: &kafka.LeastBytes{},  // ← Gửi message đến partition ít data nhất
    }
}

// Gửi message
writer := kafka.GetWriter(cfg.Kafka.URL)
err = writer.WriteMessages(ctx,
    kafka.Message{
        Topic: "bot.started",
        Value: []byte(jsonPayload),
    },
)
```
---
### 13.3 Kafka Consumer (Reader)

---
```go
// trading-bot-api/kafka/reader.go

func GetReader(kafkaURL string, topics []string) *kafka.Reader {
    return kafka.NewReader(kafka.ReaderConfig{
        Brokers:     []string{kafkaURL},
        GroupID:     "gateway-api-consumer",  // ← Consumer Group: đảm bảo mỗi message chỉ xử lý 1 lần
        GroupTopics: topics,
        MaxBytes:    10e6,  // 10MB max per message
    })
}

// Consume messages
reader := kafka.GetReader(kafkaURL, []string{"bot.started", "bot.stopped"})
for {
    msg, err := reader.ReadMessage(ctx)
    if err != nil { break }
    // Process message
    processMessage(msg.Value)
}
```
---
### 13.4 Kafka vs Redis Pub/Sub

| | Kafka | Redis Pub/Sub |
|--|-------|---------------|
| **Persistence** | ✅ Lưu trên disk | ❌ Mất khi không có subscriber |
| **Replay** | ✅ Có thể replay | ❌ Không |
| **Scale** | ✅ Phân tán | 🟡 Single node |
| **Latency** | 🟡 Thấp (ms) | ✅ Rất thấp (µs) |
| **Use case** | Event sourcing, audit log | Real-time notification |

## 14. Design Patterns trong Go

> 📖 **Đọc thêm:** _"Fundamentals of Software Architecture"_ (O'Reilly) — Chapter 5: Identifying Architecture Characteristics

### 14.1 Dependency Injection Pattern

Toàn bộ dự án dùng pattern này. Thay vì tạo dependency bên trong, **nhận vào từ bên ngoài**:

---
```go
// market-maker-api/services/services.go — DI Container

type Services struct {
    AuthenticationSvc authentication.Service   // ← Interface, không phải concrete
    StrategiesSvc     strategies.Service
    UserSvc           user.Service
}

func NewServices(
    repo *repository.Gorm,      // ← Inject database
    cfg  *configuration.Config, // ← Inject config
    redisDB6 redis.Service,     // ← Inject Redis
    redisDB7 redis.Service,
) *Services {
    // Mỗi service tạo ở đây và inject dependencies cần thiết
    userSvc := user.NewService(repo, redisDB7, cfg)
    authSvc := authentication.NewService(repo, cfg, redisDB6, userSvc)
    // ...
    return &Services{AuthenticationSvc: authSvc, UserSvc: userSvc, ...}
}
```
---
**Lợi ích DI:**
1. **Testability**: Thay Redis thật bằng mock trong test
2. **Flexibility**: Đổi MySQL sang PostgreSQL chỉ cần sửa 1 nơi
3. **Single Responsibility**: Mỗi service chỉ biết làm việc của mình

### 14.2 Repository Pattern

Tách biệt business logic khỏi database access:

---
```go
// Layer 1: Handler (HTTP) → chỉ biết về Service
func (a *AuthHandler) Login(ctx *gin.Context) {
    result, err := a.AuthSvc.Login(args)  // Không biết DB gì
}

// Layer 2: Service → chỉ biết về Repository interface
type authService struct {
    repo AuthRepository  // ← Interface
}
func (s *authService) Login(args *LoginRequest) (*LoginResponse, error) {
    user, err := s.repo.GetUserByEmail(args.Email)  // Gọi qua interface
}

// Layer 3: Repository → biết về Database cụ thể
type authRepository struct {
    db *gorm.DB  // ← Concrete GORM
}
func (r *authRepository) GetUserByEmail(email string) (*models.User, error) {
    var user models.User
    err := r.db.Where("email = ?", email).First(&user).Error
    return &user, err
}
```
---
### 14.3 Strategy Pattern

---
```go
// trading-bot-trading-gateway/pkg/trading-gateway/api/rpc/job.go
// Chọn strategy (service) dựa trên runtime value

switch req.FromService {
case constants.ACTION_SERVICE_NAME:    // Strategy 1
    s.Services.ActionSvc.CreateJob(args)
case constants.DATA_WORKER_SERVICE_NAME:  // Strategy 2
    s.Services.DataWorkerSvc.CreateJob(args)
case constants.FINANCE_SERVICE_NAME:   // Strategy 3
    s.Services.FinanceSvc.CreateJob(args)
}
```
---
### 14.4 Observer Pattern (Pub/Sub)

---
```go
// Publisher — Trading Bot gửi event
redisDB0.Publish("bot.trade.executed", tradePayload)

// Subscriber — Finance service nhận và xử lý
subscription := redisDB0.Subscribe("bot.trade.executed")
ch := subscription.Channel()
for msg := range ch {
    processTradeEvent(msg.Payload)
}
```
---
### 14.5 Options Pattern (gRPC Dial)

---
```go
// Cấu hình gRPC connection linh hoạt
func TradingGatewayTokenGrpcServer(cfg *Config, opts ...grpc.DialOption) (proto.TokenClient, error) {
    conn, err := grpc.Dial(cfg.Server.TradingGateway, opts...)
    // ...
}

// Gọi với các options khác nhau:
// Internal: không TLS
client, err := TradingGatewayTokenGrpcServer(cfg, grpc.WithTransportCredentials(insecure.NewCredentials()))

// External: có TLS
client, err := TradingGatewayTokenGrpcServer(cfg, grpc.WithTransportCredentials(creds))
```
---
## 15. Layered Architecture — Kiến trúc phân lớp

> 📖 **Đọc thêm:** _"Building Microservices"_ (O'Reilly) — Chapter 6: Workflow

### 15.1 Các lớp trong dự án

---
```
┌─────────────────────────────────┐
│  Transport Layer (HTTP/gRPC)    │  api/rest/, api/rpc/
│  ← Parse request, format resp  │  Handler structs
├─────────────────────────────────┤
│  Service Layer (Business Logic) │  services/*/
│  ← Business rules, validation  │  Service interfaces
├─────────────────────────────────┤
│  Repository Layer (Data Access) │  repository/
│  ← SQL queries, caching        │  Repository interfaces
├─────────────────────────────────┤
│  Infrastructure Layer           │  redis/, kafka/
│  ← External dependencies       │  models/
└─────────────────────────────────┘
```
---
### 15.2 Data flow ví dụ: User Login

---
```
HTTP POST /api/v1/login
    │
    ▼
[JwtAuthMiddleware]       ← Skip (public route)
    │
    ▼
[AuthenticationHandler.Login]
    │ ctx.ShouldBindJSON(&args)
    │ args.Validate()
    ▼
[AuthenticationService.Login]
    │ businessLogic: check password, create token
    ▼
[AuthRepository.GetUserByEmail]
    │ DB.Where("email=?").First(&user)
    ▼
[MySQL] → return user
    │
    ▼
[Service] → bcrypt verify password
    │        jwt.NewWithClaims(...)
    │        redis.Set(session, token)
    ▼
[Handler] → ctx.JSON(200, token)
    │
    ▼
HTTP Response 200 {"access_token": "..."}
```
---
## 16. Microservices Architecture

> 📖 **Đọc thêm:** _"Building Microservices"_ (O'Reilly) — Sam Newman

### 16.1 Nguyên tắc thiết kế Microservices

**Single Responsibility:** Mỗi service làm đúng 1 việc:
- `trading-bot-api`: Nhận request từ user, routing
- `trading-bot-trading-gateway`: Quản lý trạng thái trading job
- `trading-bot-accounting`: Xử lý tài chính, giao dịch
- `trading-bot-load-balancer`: Phân phối bot sang servers

**Loose Coupling:** Services không biết implementation details của nhau, chỉ biết interface (protobuf).

**High Cohesion:** Code liên quan đến nhau nằm cùng service.

### 16.2 Service Communication

---
```
Synchronous (cần kết quả ngay):
  REST API   → Gin HTTP (external clients)
  gRPC       → Protocol Buffers (internal services)

Asynchronous (không cần kết quả ngay):
  Kafka      → Event streaming (bot events, transactions)
  Redis PubSub → Real-time notifications (market data alerts)
```
---
### 16.3 Shared Protobuf Repo

Đây là pattern hay: `trading-bot-protobuf` là repo riêng chứa `.proto` files, được compile và publish như Go module:

---
```go
// Mọi service import cùng 1 protobuf package
require (
    github.com/PrimeNumber108/trading-bot-protobuf v1.1.18
)

// trading-bot-api sử dụng:
import tradinggatewayProto "github.com/PrimeNumber108/trading-bot-protobuf/trading-gateway"
import loadbalancerProto   "github.com/PrimeNumber108/trading-bot-protobuf/load-balancer"
import accountingProto     "github.com/PrimeNumber108/trading-bot-protobuf/accounting"
```
---
**Lợi ích:** Thay đổi API chỉ cần update version trong `go.mod`, không copy-paste code.

### 16.4 Service Discovery Pattern

---
```go
// Config-based service discovery (đơn giản cho internal)
type ServerConfig struct {
    TradingGateway string  // "trading-gateway:50001"
    LoadBalancer   string  // "load-balancer:50002"
    Accounting     string  // "accounting:50003"
}

// Khi deploy trên Kubernetes/Docker: dùng DNS service name
// Khi deploy trên EC2: dùng IP tĩnh hoặc internal load balancer
```
---
### 16.5 Phân tích accounting.proto — Domain-Driven Design

---
```protobuf
// Ví dụ về bounded context rõ ràng
service AccountTransaction {
    rpc CreateTransaction (...)       ← Tạo giao dịch mới
    rpc CreateTransactionLog (...)    ← Log trạng thái giao dịch
    rpc GetByTransactionId (...)      ← Query 1 giao dịch
    rpc GetAllTransactionInSubscriber ← Query giao dịch của subscriber
    rpc UpdateTransaction (...)       ← Cập nhật trạng thái
}

// Tách biệt company wallet khỏi user transaction
service ValidateTransaction {
    rpc CheckCryptoCurrencyTransaction ← Validate on-chain transaction
}
```
---
## 17. Authentication & Security

### 17.1 JWT (JSON Web Token) Flow

---
```
1. Login:
   Client → POST /login {username, password}
   Server → bcrypt.Compare(hash, password)
   Server → jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
                id: user.ID,
                session: sessionToken,
                username: user.Username,
                exp: time.Now().Add(24*time.Hour),
            })
   Server → redis.Set(sessionToken, roleInfo)
   Server → Response {access_token, refresh_token}

2. Authorized Request:
   Client → GET /api/fin20/user (Header: "Authorization: Bearer <token>")
   Middleware → jwt.Parse(token, secret)
   Middleware → redis.Get(claims.session)  ← Validate session still active
   Middleware → ctx.Set("user", userSession)
   Handler    → userId := ctx.GetUint("userId")

3. Logout:
   Server → redis.Del(session)  ← Invalidate session
```
---
### 17.2 JWT Middleware Code

---
```go
// market-maker-api/middlewares/auth.go

func JwtAuthMiddleware(cfg *configuration.Config, redis redis.Service) gin.HandlerFunc {
    return func(ctx *gin.Context) {
        // 1. Extract token from header
        token := utils.GetTokenFromHeader(ctx)
        if token == "" {
            ctx.String(http.StatusUnauthorized, "unauthorized")
            ctx.Abort()   // ← Dừng chain, không gọi handler
            return
        }

        // 2. Parse & validate JWT signature
        user, err := extractTokenToUser(token, cfg)
        if err != nil {
            ctx.String(http.StatusUnauthorized, "unauthorized")
            ctx.Abort()
            return
        }

        // 3. Set user info vào context để handler dùng
        ctx.Set("user", &user)
        ctx.Set("userId", user.ID)
        ctx.Next()  // ← Gọi handler tiếp theo
    }
}
```
---
### 17.3 Role-Based Access Control (RBAC)

---
```go
// Admin middleware — chỉ SUPER_ADMIN được qua
func Admin(cfg *configuration.Config, redis redis.Service) gin.HandlerFunc {
    return func(ctx *gin.Context) {
        user, err := GetUser(token, cfg, redis)

        if user.RoleId != constants.SUPER_ADMIN {
            ctx.String(http.StatusUnauthorized, "unauthorized")
            ctx.Abort()
            return
        }
        ctx.Next()
    }
}
```
---
### 17.4 AES Encryption cho Password

---
```go
// Password không truyền plaintext từ client
// Client encrypt bằng AES key, server decrypt

args.Password, err = utils.AesDecrypt(args.Password, cfg.Authentication.SecretKeyPasswordDecrypt)
// Sau đó mới bcrypt verify
bcrypt.CompareHashAndPassword(hashedPassword, []byte(decryptedPassword))
```
---
### 17.5 OAuth2 (Google/Facebook)

---
```go
// trading-bot-api/app/server.go

googleAuth := oauth.GoogleLogin(cfg)     // ← Google OAuth2 config

oauthFb := &oauth.OauthFacebook{Cfg: cfg}
facebookAuth := oauth.New(oauthFb)      // ← Facebook OAuth2 pattern
```
---
## 18. Background Jobs & Cron

### 18.1 Cronjob Pattern

---
```go
// market-maker-background/app/background-tradebot/server/background.go

func Cronjob(svc background.Service) {
    // 1. Setup context với signal handling
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    // 2. Tạo cron scheduler
    c := cron.New()

    // 3. Đăng ký jobs
    c.AddFunc("@every 60s", func() { svc.GetAsset() })              // Mỗi 60 giây
    c.AddFunc("@every 15s", func() { svc.CheckProviderSlowResponse() })  // Mỗi 15 giây

    // 4. Start async
    go c.Start()

    // 5. Block cho đến khi nhận shutdown signal
    <-ctx.Done()

    log.Printf("[Cronjob] stop: %v", stop)
}
```
---
### 18.2 Cron Expressions

---
```
@every 60s    = Mỗi 60 giây
@every 15m    = Mỗi 15 phút
@hourly       = Mỗi giờ
@daily        = Mỗi ngày lúc 00:00
@midnight     = Mỗi ngày lúc 00:00

Standard cron: "*/5 * * * *" = Mỗi 5 phút
               "0 */2 * * *" = Mỗi 2 giờ
               "0 9 * * 1"   = 9:00 sáng thứ Hai
```
---
### 18.3 Background Tasks với Context

---
```go
// trading-bot-api/background/...

func RunBackgroundTasks(ctx context.Context, svc *services.Services) {
    // Chạy nhiều background tasks song song
    go func() {
        for {
            select {
            case <-ctx.Done():   // ← Dừng khi server shutdown
                return
            case <-time.After(30 * time.Second):
                svc.CheckExpiredJobs()
            }
        }
    }()

    go func() {
        for {
            select {
            case <-ctx.Done():
                return
            case <-time.After(5 * time.Second):
                svc.SyncMarketData()
            }
        }
    }()
}
```
---
## 19. Priority Queue & Data Structures

### 19.1 heap.Interface trong Go

---
```go
// trading-bot-api/queue/priority_queue.go

// Implement heap.Interface để dùng container/heap
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int  { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
    return pq[i].readyAt.After(pq[j].readyAt)  // ← Max-heap theo thời gian
}

func (pq PriorityQueue) Swap(i, j int) {
    pq[i], pq[j] = pq[j], pq[i]
    pq[i].index = i
    pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
    n := len(*pq)
    item := x.(*Item)   // ← Type assertion: interface{} → *Item
    item.index = n
    *pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
    old := *pq
    n := len(old)
    item := old[n-1]
    old[n-1] = nil  // ← Tránh memory leak
    item.index = -1
    *pq = old[0 : n-1]
    return item
}
```
---
### 19.2 Retry với Exponential Backoff

---
```go
// trading-bot-api/queue/queue.go

func (q *queue) When(item interface{}) time.Duration {
    q.failuresLock.Lock()
    defer q.failuresLock.Unlock()

    exp := q.failures[item]
    q.failures[item]++

    // Exponential backoff: baseDelay * 2^failures
    backoff := float64(baseDelay.Nanoseconds()) * math.Pow(2, float64(exp))

    if backoff > math.MaxInt64 {
        return maxDelay  // Cap tại maxDelay
    }

    calculated := time.Duration(backoff)
    if calculated > maxDelay {
        return maxDelay
    }
    return calculated
}

// Ví dụ: failures=0 → 1µs, failures=5 → 32µs, failures=20 → ~1s, cap tại 1000s
```
---
### 19.3 Concurrent Queue với sync.Cond

---
```go
func (q *queue) Get() (item interface{}, close bool) {
    q.cond.L.Lock()
    defer q.cond.L.Unlock()

    // Block goroutine nếu queue rỗng (không busy-wait)
    for len(q.queue) == 0 && !q.closing {
        q.cond.Wait()  // ← Release lock, sleep cho đến khi Signal() được gọi
    }

    if len(q.queue) == 0 {
        return nil, true  // Queue đã close
    }

    item = q.queue[0]
    q.queue[0] = nil      // ← Xóa reference, GC có thể thu hồi
    q.queue = q.queue[1:]
    q.processing.insert(item)
    return item, false
}

func (q *queue) Add(item interface{}) {
    q.cond.L.Lock()
    defer q.cond.L.Unlock()
    q.queue = append(q.queue, item)
    q.cond.Signal()  // ← Wake up goroutine đang chờ ở Get()
}
```
---
## 20. Configuration Management

### 20.1 12-Factor App — Config từ Environment

---
```go
// market-maker-api/configuration/configuration.go

func FromEnv() (*Config, error) {
    err := godotenv.Load(".env")  // ← Load file .env vào os.Environ
    if err != nil {
        return nil, err
    }

    cfg := &Config{}
    cfg.MySQL.DNS = utils.GetEnvAsString("MYSQL_DNS", defaultMySQLDNS)
    cfg.Redis.Address = utils.GetEnvAsString("REDIS_ADDRESS", "localhost:6379")
    cfg.Authentication.Secret = utils.GetEnvAsString("SECRET", "")
    // ...
    return cfg, nil
}
```
---
**Pattern:** Env var → fallback to default constant.

### 20.2 .env file structure

---
```bash
# .env (không commit vào git!)
MYSQL_DNS=user:pass@tcp(db:3306)/trading?parseTime=True
REDIS_ADDRESS=redis:6379
SECRET=super-secret-jwt-key-256-bits
LIFESPAN=86400              # 24 giờ tính bằng giây
MODE=prod                   # dev | prod
GRPC_TRADING_GATEWAY=trading-gateway:50001
GRPC_LOAD_BALANCER=load-balancer:50002
GRPC_ACCOUNTING=accounting:50003
```
---
### 20.3 Config Struct Hierarchy

---
```go
type Config struct {
    MySQL struct {
        DNS string
    }
    Redis struct {
        Address  string
        Password string
        DB_0     int   // Session
        DB_9     int   // Feature flags
    }
    Server struct {
        TradingGateway string   // gRPC endpoints
        LoadBalancer   string
        Accounting     string
        GRPC           string   // This service's gRPC port
    }
    Authentication struct {
        Secret                   string
        SecretRefreshToken       string
        Lifespan                 int    // Access token TTL (seconds)
        LifespanRefreshToken     int    // Refresh token TTL
    }
}
```
---
## 21. Graceful Shutdown

Đây là pattern cực quan trọng trong production. Khi server nhận `SIGTERM` (k8s, `docker stop`):
1. **Dừng nhận request mới**
2. **Đợi request đang xử lý hoàn thành** (max timeout)
3. **Đóng connections** (DB, Redis, Kafka)
4. **Exit**

---
```go
// trading-bot-api/app/server.go

func RunApi(svc *services.Services, cfg *configuration.Config, ...) {

    // 1. Setup context react với OS signals
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    // 2. Start background tasks (nhận ctx để biết khi nào dừng)
    go background.RunBackgroundTasks(ctx, svc)

    // 3. Start HTTP server trong goroutine
    server := &http.Server{Addr: ":8080", Handler: handler}
    go func() {
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("listen: %s\n", err)
        }
    }()

    // 4. Block ở đây cho đến khi Ctrl+C hoặc SIGTERM
    <-ctx.Done()

    // 5. Graceful shutdown với timeout 3 giây
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()
    if err := server.Shutdown(ctx); err != nil {
        log.Fatal("Server forced to shutdown: ", err)
    }

    fmt.Println("Server exiting")
}
```
---
**Signal flow:**
---
---
---
```
OS: kill -SIGTERM PID
    │
    ▼
signal.NotifyContext → ctx.Done() channel nhận signal
    │
    ▼
<-ctx.Done() unblocks
    │
    ▼
server.Shutdown(ctxWithTimeout)
    │
    ├── Dừng nhận HTTP request mới
    ├── Chờ in-flight requests complete
    └── Return sau khi xong hoặc timeout
```
---
## 22. Lộ trình học tập

### Tuần 1-2: Go Basics
- [ ] Cài Go: https://go.dev/doc/install
- [ ] Đọc `A Tour of Go`: https://go.dev/tour/
- [ ] Viết lại `main.go` và `server.go` từ dự án
- [ ] Hiểu: package, import, types, functions, errors

### Tuần 3-4: Structs & Interfaces
- [ ] Viết `User`, `Config` struct với tags
- [ ] Tạo `Service` interface và implement
- [ ] Hiểu pointer receivers vs value receivers
- [ ] Đọc: _"The Go Programming Language"_ Chapter 4-7

### Tuần 5-6: Concurrency
- [ ] Viết goroutine + channel đơn giản
- [ ] Implement timeout với `context.WithTimeout`
- [ ] Implement graceful shutdown
- [ ] Đọc: _"Concurrency in Go"_ Chapter 1-4

### Tuần 7-8: HTTP API
- [ ] Build REST API với Gin
- [ ] Implement JWT middleware
- [ ] Implement Repository + Service + Handler layers
- [ ] Kết nối MySQL với GORM

### Tuần 9-10: Advanced
- [ ] Implement Redis cache
- [ ] Implement Kafka producer/consumer
- [ ] Build simple gRPC server + client
- [ ] Đọc: _"gRPC: Up and Running"_ Chapter 1-4

### Tuần 11-12: Microservices
- [ ] Tách project thành 2 services giao tiếp qua gRPC
- [ ] Implement shared protobuf module
- [ ] Deploy với Docker Compose
- [ ] Đọc: _"Building Microservices"_ Chapter 1, 4-6

## Tổng kết — Mental Model

---
```
Golang = Simple + Explicit + Concurrent

1. Đơn giản:     Ít keywords, ít magic, đọc code = hiểu code
2. Explicit:     Không có hidden errors (checked errors)
                 Không có hidden imports
                 Không có inheritance magic
3. Concurrent:   Goroutines = concurrency rẻ
                 Channels = communication safe
                 Context = lifecycle management

Kiến trúc dự án = Separation of Concerns
   Transport  →  Service  →  Repository  →  Database
   (HTTP/gRPC)   (Logic)     (Interface)    (MySQL/Redis)

Microservices = Single Responsibility at scale
   REST API  →  gRPC  →  Kafka  →  gRPC  →  Database
   (sync)       (sync)   (async)   (sync)    (persist)
```
---
> **Lời khuyên cuối:** Đừng cố học tất cả cùng lúc. Clone `market-maker-api`, chạy nó lên local, đọc từng file theo thứ tự: `main.go` → `app/server.go` → `api/rest.go` → `services/services.go` → `repository/repository.go`. Code không nói dối — mỗi dòng đều có lý do tồn tại.

*References: Go Concurrency in Practice (O'Reilly) · Fundamentals of Software Architecture (O'Reilly) · Designing Data-Intensive Applications (O'Reilly) · Building Microservices 2nd Ed. (O'Reilly) · gRPC: Up and Running (O'Reilly) · RESTful Web API Patterns and Practices Cookbook (O'Reilly) · The Go Programming Language (Addison-Wesley)*
