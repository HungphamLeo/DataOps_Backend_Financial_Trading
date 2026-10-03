# Golang Learning Session — Q&A Notes
> Giải thích các đoạn code từ `GOLANG_ZERO_TO_HERO.md` theo từng concept, dành cho người mới bắt đầu.

---

## Mục lục

1. [select + channel — Delayed Queue Loop](#1-select--channel--delayed-queue-loop)
2. [Syntax cơ bản: for, select, <-, heap](#2-syntax-cơ-bản-for-select---heap)
3. [struct, Mutex, defer, map](#3-struct-mutex-defer-map)
4. [Context, signal, graceful shutdown](#4-context-signal-graceful-shutdown)
5. [Redis Pub/Sub — Publish & Subscribe](#5-redis-pubsub--publish--subscribe)
6. [HTTP Handler với Gin — Login API](#6-http-handler-với-gin--login-api)
7. [NewAPI — Router, Middleware, Route Groups](#7-newapi--router-middleware-route-groups)
8. [Kafka Reader — []string, Consumer Group](#8-kafka-reader--string-consumer-group)
9. [Dependency Injection — DI Container](#9-dependency-injection--di-container)
10. [gRPC — Quy trình đầy đủ 5 bước](#10-grpc--quy-trình-đầy-đủ-5-bước)

---

## 1. select + channel — Delayed Queue Loop

### Code gốc
```go
// trading-bot-api/queue/queue.go
for {
    select {
    case <-q.stopCh:
        return
    case <-nextReadyAt:
        // handle timer
    case waitObj := <-q.waitItemCh:
        if waitObj.readyAt.After(time.Now()) {
            heap.Push(waitingForQueue, waitObj)
        } else {
            q.Add(waitObj.value)
        }
    }
}
```

### Giải thích

Đây là **time-delayed work queue dispatcher** — pattern tương tự Kubernetes workqueue. Vòng lặp `for` chạy mãi mãi, `select` canh 3 channel cùng lúc:

| Case | Điều kiện | Hành động |
|---|---|---|
| `<-q.stopCh` | Nhận tín hiệu dừng | `return` — thoát goroutine |
| `<-nextReadyAt` | Timer hết hạn | Pop item từ heap, xử lý ngay |
| `<-q.waitItemCh` | Item mới đến | Nếu chưa đến giờ → push vào heap; nếu rồi → xử lý ngay |

**Ứng dụng:** Retry với exponential backoff — item thất bại được re-add vào queue với `readyAt = now + backoff_duration`.

---

## 2. Syntax cơ bản: for, select, <-, heap

### `for { }` — Vòng lặp vô tận
```go
for {
    // chạy mãi mãi
}
```
> Tương đương `while True:` trong Python. Chỉ dừng khi có `return` hoặc `break`.

### `select { case }` — Canh nhiều channel
```go
select {
case <-channelA:
    // làm gì đó
case <-channelB:
    // làm gì đó khác
}
```
Block cho đến khi **một trong các channel có dữ liệu**. Nếu nhiều channel cùng sẵn → Go chọn ngẫu nhiên.

### `<-channel` — Nhận dữ liệu từ channel
```go
<-ch          // nhận, bỏ giá trị (chỉ cần signal)
x := <-ch     // nhận và lưu vào biến
```

### Bảng tóm tắt
| Syntax | Ý nghĩa | Tương đương |
|---|---|---|
| `for { }` | Lặp vô tận | `while True:` (Python) |
| `select { case }` | Canh nhiều channel | — |
| `<-ch` | Nhận từ channel | `ch.get()` |
| `ch <- x` | Gửi vào channel | `ch.put(x)` |
| `x := <-ch` | Nhận + lưu biến | `x = ch.get()` |

---

## 3. struct, Mutex, defer, map

### Code gốc
```go
type queue struct {
    failuresLock sync.Mutex
    failures     map[interface{}]int
    cond         *sync.Cond
    closing      bool
}

func (q *queue) When(item interface{}) time.Duration {
    q.failuresLock.Lock()
    defer q.failuresLock.Unlock()

    exp := q.failures[item]
    q.failures[item] = q.failures[item] + 1
}
```

### `struct` — Hộp chứa nhiều dữ liệu
Tương đương `class` trong Python/Java nhưng không có kế thừa, chỉ có data.

```go
type queue struct {
    failuresLock sync.Mutex          // Ổ khóa bảo vệ failures
    failures     map[interface{}]int // Bảng đếm: item → số lần thất bại
    cond         *sync.Cond          // Chuông báo goroutine đang chờ
    closing      bool                // Queue đang đóng không?
}
```

### `map[interface{}]int` — Dictionary
```go
// Tương đương Python dict
failures = {
    "order_123": 3,   // fail 3 lần
    "order_456": 1,   // fail 1 lần
}
```

### `sync.Mutex` — Ổ khóa
Khi nhiều goroutine cùng đọc/ghi một biến → **race condition**. Mutex ngăn chặn:
```
🚪 [LOCKED]
├── Goroutine A: Lock() → vào được, làm việc... Unlock()
└── Goroutine B: Lock() → CHỜ đến khi A Unlock xong
```

### `defer` — Hứa làm sau
```go
q.failuresLock.Lock()           // Chạy ngay
defer q.failuresLock.Unlock()   // Chạy khi hàm return
```
> Lý do dùng `defer`: dù hàm có nhiều `return`, `Unlock()` luôn được gọi, không bao giờ quên.

### Bảng tóm tắt
| Syntax | Ý nghĩa |
|---|---|
| `type X struct { }` | Tạo kiểu dữ liệu mới |
| `map[K]V` | Dictionary: key K, value V |
| `interface{}` | Bất kỳ kiểu nào (như `Any`) |
| `func (x *T) Method()` | Gắn method vào struct T |
| `*T` | Con trỏ tới T |
| `mutex.Lock()` | Khóa — 1 goroutine vào |
| `defer f()` | Gọi f() khi hàm return |
| `m[key]` | Lấy value từ map |

---

## 4. Context, signal, graceful shutdown

### Code gốc
```go
// trading-bot-api/app/server.go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

go background.RunBackgroundTasks(ctx, svc)

server := &http.Server{...}
go func() {
    server.ListenAndServe()
}()

<-ctx.Done()

ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
defer cancel()
server.Shutdown(ctx)
```

### Timeline khi chạy và tắt

```
T=0s   main() khởi động
       ├── signal.NotifyContext() → lắng nghe Ctrl+C / kill
       ├── go RunBackgroundTasks(ctx) → chạy song song
       ├── go server.ListenAndServe() → HTTP port 8080
       └── <-ctx.Done() ← ĐỨNG CHỜ TẠI ĐÂY

... server đang chạy bình thường ...

T=?s   Ctrl+C hoặc Docker stop
       → ctx bị cancel
       → <-ctx.Done() unblock → chạy tiếp

T=?+0s  context.WithTimeout(3 giây)
        server.Shutdown(ctx) → ngừng nhận request, chờ xong
T=?+3s  Nếu còn request → cắt luôn → kết thúc
```

### Bảng tóm tắt
| Syntax | Ý nghĩa |
|---|---|
| `context.Background()` | Context gốc rễ, trống rỗng |
| `signal.NotifyContext(ctx, SIG...)` | Ctx tự cancel khi nhận OS signal |
| `syscall.SIGINT` | Tín hiệu Ctrl+C |
| `syscall.SIGTERM` | Tín hiệu `kill` / Docker stop |
| `go func() { }()` | Goroutine ẩn danh, chạy ngay |
| `<-ctx.Done()` | Block đến khi ctx bị cancel |
| `context.WithTimeout(ctx, duration)` | Ctx có deadline |
| `3*time.Second` | 3 giây |

---

## 5. Redis Pub/Sub — Publish & Subscribe

### Code gốc
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
```

### Pub/Sub là gì?
```
Publisher (người phát)    →    Channel (kênh)    →    Subscriber (người nghe)
   Trading Bot gửi order  →  "order_executed"   →  Finance Service xử lý
```

### `context.WithCancel` vs `context.WithTimeout`
```
context.WithTimeout  → tự hủy sau N giây
context.WithCancel   → chỉ hủy khi gọi cancel() thủ công
```

### Method chaining
```go
r.Client.Publish(ctx, channel, payload).Err()
// = cmd := r.Client.Publish(...)
//   return cmd.Err()
```

### So sánh 2 hàm
| | `Publish` | `Subscribe` |
|---|---|---|
| Vai trò | Gửi message | Nhận message |
| Trả về | `error` | `*redis.PubSub` |
| Blocking? | Không | Có (caller chờ message) |

---

## 6. HTTP Handler với Gin — Login API

### Code gốc
```go
// market-maker-api/api/authentication.go

type AuthenticationHandler struct {
    AuthenticationSvc authentication.Service
}

func (a *AuthenticationHandler) Login(ctx *gin.Context) {
    args := &requests.LoginRequest{}
    if err := ctx.ShouldBindJSON(args); err != nil {
        ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    if err := args.Validate(); err != nil {
        ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    result, err := a.AuthenticationSvc.Login(args)
    if err != nil {
        ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    ctx.JSON(http.StatusOK, result)
}
```

### Flow xử lý Login
```
POST /login { "email": "...", "password": "..." }
        │
        ▼ ShouldBindJSON → parse JSON vào struct
        │ Lỗi? → 400
        ▼ args.Validate() → kiểm tra business rules
        │ Lỗi? → 400
        ▼ AuthenticationSvc.Login() → tìm DB, check bcrypt, tạo JWT
        │ Lỗi? → 400
        ▼
200 OK { "token": "eyJhbG..." }
```

### Bảng tóm tắt
| Syntax | Ý nghĩa |
|---|---|
| `ctx.ShouldBindJSON(&obj)` | Parse JSON body vào struct |
| `gin.H{"key": value}` | Shorthand tạo JSON response |
| `http.StatusBadRequest` | Hằng số = 400 |
| `http.StatusOK` | Hằng số = 200 |
| `ctx.JSON(status, data)` | Trả HTTP response JSON |
| `err.Error()` | Lấy nội dung lỗi dạng string |

---

## 7. NewAPI — Router, Middleware, Route Groups

### Code gốc
```go
// market-maker-api/api/rest.go
func NewAPI(svc *services.Services, cfg *configuration.Config, redisDB0 redis.Service) *gin.Engine {
    router := gin.Default()

    switch cfg.Web.Mode {
    case "dev":
        router.Use(middlewares.SecureNoRestrict(cfg))
    default:
        router.Use(middlewares.Secure(cfg))
    }

    serverRouter := router.Group("/api")
    publicRouter := serverRouter.Group("/v1")
    apiRouter    := publicRouter.Group("/fin20")
    apiRouter.Use(middlewares.JwtAuthMiddleware(cfg, redisDB0))

    adminRouter := serverRouter.Group("/v1/admin/fin20")
    adminRouter.Use(middlewares.Admin(cfg, redisDB0))

    return router
}
```

### Cấu trúc URL
```
/api/v1/login                   ← Public  (không cần token)
/api/v1/fin20/orders            ← User    (cần JWT token)
/api/v1/admin/fin20/users       ← Admin   (cần JWT + role admin)
```

### Quy trình xây dựng Backend API
```
Bước 1: Models     → Định nghĩa struct / bảng DB
Bước 2: Repository → Đọc/ghi Database
Bước 3: Service    → Business logic
Bước 4: Handler    → Nhận HTTP request, trả response
Bước 5: Router     → Nối URL + Middleware + Handler
```

---

## 8. Kafka Reader — []string, Consumer Group

### Code gốc
```go
// trading-bot-api/kafka/reader.go
func GetReader(kafkaURL string, topics []string) *kafka.Reader {
    return kafka.NewReader(kafka.ReaderConfig{
        Brokers:     []string{kafkaURL},
        GroupID:     "gateway-api-consumer",
        GroupTopics: topics,
        MaxBytes:    10e6,
    })
}

reader := kafka.GetReader(kafkaURL, []string{"bot.started", "bot.stopped"})
for {
    msg, err := reader.ReadMessage(ctx)
    if err != nil { break }
    processMessage(msg.Value)
}
```

### `[]string` — Slice of string
```go
topics := []string{"bot.started", "bot.stopped"}
topics[0]          // "bot.started"
len(topics)        // 2
append(topics, "bot.error")  // thêm phần tử
```
> Tương đương `list[str]` trong Python.

### GroupID — Đảm bảo mỗi message xử lý 1 lần
```
Topic "bot.started": msg1, msg2, msg3

Cùng GroupID "gateway-api-consumer":
├── Consumer A → nhận msg1
├── Consumer B → nhận msg2
└── Consumer C → nhận msg3  ← mỗi msg chỉ xử lý 1 lần

Khác GroupID → mỗi group nhận đầy đủ tất cả messages
```

### Dữ liệu thực tế qua Kafka
```go
// Producer gửi:
msg.Topic = "bot.started"
msg.Key   = []byte("bot_001")
msg.Value = []byte(`{"bot_id":"bot_001","user_id":42,"exchange":"binance"}`)

// Consumer nhận và parse:
var event BotEvent
json.Unmarshal(msg.Value, &event)
// event.BotID = "bot_001", event.UserID = 42
```

### Bảng tóm tắt
| Syntax | Ý nghĩa |
|---|---|
| `[]string` | Slice of string |
| `[]string{"a", "b"}` | Khai báo với giá trị |
| `10e6` | 10,000,000 (ký hiệu khoa học) |
| `msg.Value` | `[]byte` — nội dung message Kafka |

---

## 9. Dependency Injection — DI Container

### Code gốc
```go
// market-maker-api/services/services.go
type Services struct {
    AuthenticationSvc authentication.Service
    StrategiesSvc     strategies.Service
    UserSvc           user.Service
}

func NewServices(
    repo     *repository.Gorm,
    cfg      *configuration.Config,
    redisDB6 redis.Service,
    redisDB7 redis.Service,
) *Services {
    userSvc := user.NewService(repo, redisDB7, cfg)
    authSvc := authentication.NewService(repo, cfg, redisDB6, userSvc)
    return &Services{AuthenticationSvc: authSvc, UserSvc: userSvc}
}
```

### DI giải quyết vấn đề gì?

| | Không DI ❌ | Có DI ✅ |
|---|---|---|
| Tạo DB | Mỗi request tạo 1 connection | Tạo 1 lần, dùng chung pool |
| Test | Cần DB thật, Redis thật | Inject mock — không cần infrastructure |
| Thay DB | Sửa nhiều nơi | Chỉ sửa main.go |

### Chuỗi khởi tạo trong main.go
```
1. Đọc config         cfg := configuration.Load()
2. Infrastructure     db := NewGorm(cfg), redis6 := NewRedis(cfg, 6)
3. Services           svc := NewServices(db, cfg, redis6, redis7)
4. Handlers           authHandler := &AuthenticationHandler{AuthenticationSvc: svc.AuthenticationSvc}
5. Router             router := NewAPI(svc, cfg, redis6)
                      router.Run(":8080")
```

### Interface cho phép inject mock khi test
```go
// Production
svc := &realAuthService{repo: db, redis: redis}

// Test
svc := &mockAuthService{}  // không cần DB, Redis thật

handler := &AuthenticationHandler{AuthenticationSvc: svc}
```

---

## 10. gRPC — Quy trình đầy đủ 5 bước

### Tại sao dùng gRPC thay vì REST (internal)?
| | REST (JSON) | gRPC (Protobuf) |
|---|---|---|
| Format | Text JSON | Binary |
| Kích thước | Lớn hơn | ~3-10x nhỏ hơn |
| Tốc độ | Chậm hơn | Nhanh hơn |
| Type Safety | Không | Có (generated code) |
| Dùng khi | Frontend ↔ Backend | Service ↔ Service nội bộ |

---

### Bước 1 — Viết file `.proto`
```protobuf
// trading-gateway.proto
syntax = "proto3";
package protobuf;
option go_package = "./protobuf/trading-gateway";

service Job {
    rpc CreateJob (CreateJobRequest) returns (CreateJobResponse) {}
    rpc GetJob    (GetJobRequest)    returns (GetJobResponse)    {}
}

message CreateJobRequest {
    int64  bot_id       = 1;
    string body         = 2;
    string from_service = 3;
}

message CreateJobResponse {
    string message = 1;
}
```

### Bước 2 — Compile proto → Go code
```bash
protoc --go_out=. --go-grpc_out=. trading-gateway.proto
# Sinh ra:
#   trading-gateway.pb.go       → Structs
#   trading-gateway_grpc.pb.go  → Interfaces (JobServer, JobClient)
```

### Bước 3 — Implement gRPC Server
```go
// Embed Unimplemented để không lỗi nếu chưa implement hết
type Server struct {
    proto.UnimplementedJobServer
    services *Services
}

// Implement method
func (s *Server) CreateJob(ctx context.Context, req *proto.CreateJobRequest) (*proto.CreateJobResponse, error) {
    if req == nil {
        return nil, fmt.Errorf("request is nil")
    }
    if err := s.services.JobSvc.CreateJob(req.BotId, req.Body); err != nil {
        return nil, err
    }
    return &proto.CreateJobResponse{Message: "Success"}, nil
}

// Start server
func (s *Server) ListenAndServe(addr string, stopCh <-chan struct{}) error {
    listen, _ := net.Listen("tcp", addr)    // ":50001"
    grpcServer := grpc.NewServer()
    proto.RegisterJobServer(grpcServer, s)
    go func() { <-stopCh; grpcServer.GracefulStop() }()
    return grpcServer.Serve(listen)
}
```

### Bước 4 — Tạo gRPC Client
```go
// trading-bot-api/app/client.go
func NewJobClient(addr string) (proto.JobClient, error) {
    conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }
    return proto.NewJobClient(conn), nil
}

// Dùng — giống gọi hàm bình thường
result, err := jobClient.CreateJob(ctx, &proto.CreateJobRequest{
    BotId:       123,
    Body:        `{"exchange":"binance"}`,
    FromService: "trading-bot-api",
})
fmt.Println(result.Message)  // "Success"
```

### Bước 5 — Test với grpcurl
```bash
grpcurl -plaintext \
  -d '{"bot_id": 123, "body": "{}", "from_service": "trading-bot-api"}' \
  localhost:50001 \
  protobuf.Job/CreateJob
# { "message": "Success" }
```

### Cấu trúc file gRPC project
```
project/
├── proto/
│   ├── trading-gateway.proto          ← bạn viết
│   ├── trading-gateway.pb.go          ← tự động sinh
│   └── trading-gateway_grpc.pb.go     ← tự động sinh
├── server/
│   ├── server.go                      ← setup TCP + gRPC
│   └── job.go                         ← implement từng method
└── main.go
```

---

## Kiến trúc phân lớp tổng thể

```
┌─────────────────────────────────┐
│  Transport Layer (HTTP/gRPC)    │  Handler structs, api/rest/, api/rpc/
├─────────────────────────────────┤
│  Service Layer (Business Logic) │  services/*/ — Service interfaces
├─────────────────────────────────┤
│  Repository Layer (Data Access) │  repository/ — SQL queries, caching
├─────────────────────────────────┤
│  Infrastructure Layer           │  redis/, kafka/, models/
└─────────────────────────────────┘
```

```
main.go tạo và lắp ráp tất cả:
  cfg → db, redis → services → handlers → router → server.Run()
```

---

*Session học Golang từ dự án DataOps_Backend_Financial_Trading*
