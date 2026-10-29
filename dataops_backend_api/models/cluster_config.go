// Package models — GORM model cho Fencing Token mechanism.
//
// # Fencing Token — ngăn chặn Zombie Leader ghi đè dữ liệu
//
// ## Vấn đề: Zombie Leader sau GC Pause
//
// Kịch bản production thực tế:
//  1. kafka1 là Active Controller (Raft Leader).
//  2. kafka1 bị Stop-The-World GC Pause 15 giây.
//  3. kafka2 + kafka3 hết nhận heartbeat → bầu Leader mới (kafka2).
//  4. kafka2 trở thành Controller, increment epoch lên N+1.
//  5. kafka1 tỉnh dậy, không biết mình đã bị loại — vẫn nghĩ mình là Leader.
//  6. kafka1 (Zombie) nhận Write request từ Producer → ghi đè dữ liệu đã được
//     kafka2 commit → DATA CORRUPTION.
//
// ## Giải pháp: Monotonic Fencing Token
//
// Mỗi Leader epoch tương ứng một token value (tăng đơn điệu).
// Mọi write request phải kèm token hiện tại.
// Storage layer từ chối request với token < current_token.
//
// Flow:
//   Producer gửi write(token=5) → Storage kiểm tra DB: current_token=6
//   → 5 < 6 → REJECT → Zombie Leader không thể corrupt data.
//
// ## Tại sao dùng DB thay vì in-memory?
//
// In-memory token bị mất khi process restart.
// DB (MySQL) là shared, durable storage — tất cả service nodes thấy cùng giá trị.
// GORM AutoMigrate tự tạo table nếu chưa tồn tại.
//
// Tham chiếu:
//   - Martin Kleppmann, DDIA Chapter 8: "Fencing Tokens"
//   - CLUSTER_ID trong docker-compose.yml phải khớp với cluster_id trong row này.
package models

import (
	"time"

	"gorm.io/gorm"
)

// ClusterConfig lưu trạng thái cluster-level metadata, bao gồm Fencing Token.
//
// Thiết kế singleton: chỉ có đúng 1 row (id=1) trong bảng này.
// Mọi node đọc/ghi đều thao tác trên row duy nhất này.
//
// Tại sao không dùng gorm.Model (ID, CreatedAt, UpdatedAt, DeletedAt)?
//   Không cần soft delete cho config row.
//   Dùng primary key cố định id=1 thay vì auto-increment để tránh nhầm lẫn.
type ClusterConfig struct {
	// ID luôn = 1 (singleton row).
	// GORM primary key, không auto-increment (set explicitly khi seed).
	ID uint `gorm:"primaryKey;autoIncrement:false" json:"id"`

	// ClusterID phải khớp với CLUSTER_ID trong docker-compose.yml (KRaft metadata).
	// Format: Base64url-encoded 16 random bytes, ví dụ "MkU3OEVBNTcwNTJENDM2Qk".
	// Nếu cluster restart với CLUSTER_ID khác → row này phải được update thủ công.
	ClusterID string `gorm:"size:64;not null" json:"cluster_id"`

	// CurrentToken là Fencing Token value hiện tại.
	// Tăng đơn điệu (monotonic) mỗi khi Leader election xảy ra.
	// Write request mang token < CurrentToken sẽ bị reject.
	// Giá trị khởi đầu: 1 (seed trong repository.go).
	CurrentToken int64 `gorm:"not null;default:1" json:"current_token"`

	// LastLeaderID là node ID của Leader hiện tại (kafka1=1, kafka2=2, kafka3=3).
	// Dùng để debug/audit — không dùng để quyết định accept/reject write.
	LastLeaderID int `gorm:"default:0" json:"last_leader_id"`

	// UpdatedAt là thời điểm token được cập nhật lần cuối.
	// Dùng để phát hiện "token không đổi quá lâu" → cluster có thể stuck.
	UpdatedAt time.Time `json:"updated_at"`
}

// BeforeUpdate hook — GORM tự gọi trước UPDATE, tự set UpdatedAt.
// Không cần gọi thủ công trong business logic.
func (c *ClusterConfig) BeforeUpdate(tx *gorm.DB) error {
	c.UpdatedAt = time.Now()
	return nil
}
