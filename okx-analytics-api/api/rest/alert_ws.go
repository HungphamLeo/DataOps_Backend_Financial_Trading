package rest

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for CORS
	},
}

// AlertHub quản lý danh sách các WebSocket client đang kết nối để nhận real-time alerts
type AlertHub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.RWMutex
	logger     *zap.Logger
}

func NewAlertHub(logger *zap.Logger) *AlertHub {
	return &AlertHub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan []byte, 1024),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
		logger:     logger.Named("alert_ws_hub"),
	}
}

func (h *AlertHub) Run() {
	for {
		select {
		case conn := <-h.register:
			h.mu.Lock()
			h.clients[conn] = true
			h.mu.Unlock()
			h.logger.Info("New WebSocket alert subscriber connected")

		case conn := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[conn]; ok {
				delete(h.clients, conn)
				conn.Close()
				h.logger.Info("WebSocket subscriber disconnected")
			}
			h.mu.Unlock()

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

// BroadcastAlert đẩy một alert event tới tất cả clients
func (h *AlertHub) BroadcastAlert(msg []byte) {
	h.broadcast <- msg
}

// HandleWSAlerts Gin Handler phục vụ endpoint: /api/v1/ws/alerts
func (h *AlertHub) HandleWSAlerts(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade websocket connection", zap.Error(err))
		return
	}

	h.register <- conn

	// Đọc ping/pong giữ kết nối sống
	go func() {
		defer func() {
			h.unregister <- conn
		}()
		conn.SetReadLimit(512)
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				break
			}
		}
	}()
}
