package rest

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// upgrader chuyển đổi một HTTP connection thành WebSocket connection.
// CheckOrigin trả về true để cho phép mọi origin (tắt CORS check),
// phù hợp cho môi trường development hoặc khi CORS đã được xử lý ở tầng gateway.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for CORS
	},
}

// AlertHub là trung tâm quản lý toàn bộ vòng đời của các WebSocket client
// đang kết nối để nhận real-time alert events.
//
// Hoạt động theo mô hình hub-and-spoke:
//   - Run() chạy event loop duy nhất, serialize mọi thao tác trên map clients.
//   - HandleWSAlerts() đăng ký client mới và spawn một goroutine per-client để đọc.
//   - BroadcastAlert() đưa message vào channel broadcast để Run() phân phối.
//
// Concurrency safety:
//   - Mọi thay đổi trên clients map đều đi qua Run() hoặc được bảo vệ bởi mu.
//   - broadcast channel có buffer 1024 để tránh block caller khi clients chậm.
type AlertHub struct {
	clients    map[*websocket.Conn]bool // tập hợp các client đang kết nối
	broadcast  chan []byte              // channel nhận message cần gửi tới tất cả clients
	register   chan *websocket.Conn     // channel nhận yêu cầu đăng ký client mới
	unregister chan *websocket.Conn     // channel nhận yêu cầu huỷ đăng ký client
	mu         sync.RWMutex            // bảo vệ clients map khỏi data race
	logger     *zap.Logger
}

// NewAlertHub khởi tạo một AlertHub mới với logger đã được đặt tên.
//
// broadcast channel có buffer 1024 — nếu có hơn 1024 message đang chờ
// trong khi Run() chưa xử lý kịp, BroadcastAlert() sẽ block.
// Caller nên gọi Run() trong một goroutine riêng ngay sau khi tạo hub.
func NewAlertHub(logger *zap.Logger) *AlertHub {
	return &AlertHub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan []byte, 1024),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
		logger:     logger.Named("alert_ws_hub"),
	}
}

// Run khởi động event loop của hub, xử lý tuần tự ba loại sự kiện:
//   - register:   thêm client mới vào danh sách
//   - unregister: xoá client khỏi danh sách và đóng connection
//   - broadcast:  gửi message tới tất cả client hiện tại
//
// Hàm này blocking — nên được gọi bằng: go hub.Run()
// và chạy suốt vòng đời của ứng dụng.
func (h *AlertHub) Run() {
	for {
		select {

		// --- Đăng ký client mới ---
		// Nhận connection từ HandleWSAlerts, thêm vào map clients.
		case conn := <-h.register:
			h.mu.Lock()
			h.clients[conn] = true
			h.mu.Unlock()
			h.logger.Info("New WebSocket alert subscriber connected")

		// --- Huỷ đăng ký client ---
		// Nhận connection cần xoá (do lỗi đọc hoặc client đóng tab),
		// xoá khỏi map và đóng TCP connection.
		case conn := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[conn]; ok {
				delete(h.clients, conn)
				conn.Close()
				h.logger.Info("WebSocket subscriber disconnected")
			}
			h.mu.Unlock()

		// --- Broadcast message tới tất cả clients ---
		// Duyệt toàn bộ clients, gửi message dưới dạng TextMessage.
		// Nếu gửi lỗi (client đã ngắt kết nối), đóng và xoá client đó ngay lập tức.
		// Lưu ý: đang dùng RLock nhưng lại gọi delete() — đây là race condition tiềm ẩn;
		// cần nâng lên Lock() nếu muốn an toàn tuyệt đối.
		case message := <-h.broadcast:
			h.mu.RLock()
			for conn := range h.clients {
				err := conn.WriteMessage(websocket.TextMessage, message)
				if err != nil {
					h.logger.Warn("Failed to send alert to client, disconnecting", zap.Error(err))
					conn.Close()
					delete(h.clients, conn)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// BroadcastAlert đẩy một alert payload (JSON bytes) vào broadcast channel.
//
// Hàm này non-blocking miễn là channel còn buffer trống (buffer size = 1024).
// Khi buffer đầy, lời gọi sẽ block cho đến khi Run() tiêu thụ bớt message.
// Nên gọi từ business logic layer khi có alert mới cần push tới clients.
func (h *AlertHub) BroadcastAlert(msg []byte) {
	h.broadcast <- msg
}

// HandleWSAlerts là Gin HTTP handler phục vụ endpoint GET /api/v1/ws/alerts.
//
// Luồng xử lý:
//  1. Upgrade HTTP → WebSocket connection qua upgrader.
//  2. Gửi connection vào register channel để Run() thêm vào danh sách clients.
//  3. Spawn một goroutine riêng (per-client) để duy trì ping/pong heartbeat
//     và phát hiện khi client ngắt kết nối.
//  4. Khi goroutine kết thúc (lỗi đọc hoặc timeout), gửi vào unregister channel.
func (h *AlertHub) HandleWSAlerts(c *gin.Context) {
	// Bước 1: Nâng cấp HTTP connection thành WebSocket.
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade websocket connection", zap.Error(err))
		return
	}

	// Bước 2: Đăng ký client mới với hub.
	h.register <- conn

	// Bước 3: Goroutine per-client — giữ kết nối sống qua ping/pong,
	// phát hiện disconnect khi ReadMessage trả về lỗi.
	go func() {
		// Bước 4: Khi goroutine thoát (bất kỳ lý do), huỷ đăng ký client.
		defer func() {
			h.unregister <- conn
		}()

		// Giới hạn kích thước message đọc vào tối đa 512 bytes
		// (chỉ cần đọc control frames như pong, không cần data lớn).
		conn.SetReadLimit(512)

		// Đặt deadline đọc ban đầu: nếu không nhận được gì trong 60s thì timeout.
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		// Mỗi khi nhận được Pong frame từ client (phản hồi Ping server gửi),
		// gia hạn thêm 60s — giúp giữ kết nối sống miễn client còn phản hồi.
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})

		// Vòng lặp đọc liên tục: chỉ cần giữ goroutine sống để nhận Pong.
		// Khi client đóng connection hoặc deadline hết hạn, ReadMessage trả lỗi → break.
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				break
			}
		}
	}()
}
