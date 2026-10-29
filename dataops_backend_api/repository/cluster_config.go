// Package repository — ClusterConfig repository: Fencing Token validation và increment.
//
// # Tại sao Fencing Token cần được implement ở tầng Storage?
//
// Không thể validate token ở tầng Producer hay Kafka Broker — các tầng đó không có
// global state shared. Storage layer (DB) là điểm duy nhất có thể enforce ordering.
//
// ## Hai operation chính:
//
//  1. ValidateFencingToken(token int64) — kiểm tra token >= current_token.
//     Trả về error nếu token < current (request từ Zombie Leader).
//
//  2. IncrementToken(leaderID int) — tăng current_token + ghi leaderID mới.
//     Gọi khi phát hiện Leader election event.
//
// ## Tại sao dùng SELECT FOR UPDATE?
//
// Không dùng optimistic lock (version check) vì:
//   - Leader election có thể xảy ra đồng thời từ nhiều service nodes.
//   - Race condition: 2 nodes đọc token=5 cùng lúc → cả 2 ghi token=6 → mất increment.
//   - SELECT FOR UPDATE: MySQL lock row → chỉ 1 node được ghi tại một thời điểm.
//
// Trade-off:
//   + Strong consistency, zero race condition.
//   - Latency cao hơn so với optimistic lock (cần giữ lock trong transaction).
//   → Chấp nhận được vì Fencing Token operation ít xảy ra (chỉ khi election).
//
// Tham chiếu:
//   - models.ClusterConfig (GORM struct, singleton row id=1)
//   - repository.go: AutoMigrate + seed initial row
package repository

import (
	"errors"
	"fmt"
	"time"

	"dataops-backend-api/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ClusterConfigRepository là interface cho Fencing Token operations.
// Tách interface ra để dễ mock trong unit test (không cần real DB connection).
type ClusterConfigRepository interface {
	ValidateFencingToken(token int64) error
	IncrementToken(newLeaderID int) (int64, error)
	GetCurrentToken() (int64, error)
}

// clusterConfigRepo là implementation dùng GORM + MySQL.
type clusterConfigRepo struct {
	db *gorm.DB
}

// NewClusterConfigRepo khởi tạo repository với DB connection.
// db phải là *gorm.DB đã được InitializeDatabase() tạo (có sẵn connection pool).
func NewClusterConfigRepo(db *gorm.DB) ClusterConfigRepository {
	return &clusterConfigRepo{db: db}
}

// ValidateFencingToken kiểm tra xem token từ request có hợp lệ không.
//
// Logic: nếu request_token < current_token → request đến từ Zombie Leader.
//
// Caller (write handler) nên wrap lỗi này và trả HTTP 409 Conflict cho client.
// Không retry — Zombie Leader phải dừng lại, không phải thử lại.
//
// Ví dụ lỗi production:
//   kafka1 (GC pause 15s) tỉnh dậy với token=4, nhưng DB đã có current_token=5
//   (kafka2 đã elected và incremented).
//   ValidateFencingToken(4) → error "fencing token rejected: 4 < 5 (current)"
//   → Handler trả 409 → kafka1 biết mình là zombie, tự shutdown.
func (r *clusterConfigRepo) ValidateFencingToken(token int64) error {
	var cfg models.ClusterConfig
	if err := r.db.First(&cfg, 1).Error; err != nil {
		return fmt.Errorf("cluster config not found: %w", err)
	}
	if token < cfg.CurrentToken {
		return fmt.Errorf(
			"fencing token rejected: %d < %d (current) — possible zombie leader",
			token, cfg.CurrentToken,
		)
	}
	return nil
}

// IncrementToken tăng CurrentToken và ghi nhận LeaderID mới.
//
// Dùng SELECT FOR UPDATE trong transaction để tránh race condition khi 2 nodes
// đồng thời phát hiện election và gọi IncrementToken.
//
// Returns: new token value (để caller log/trace), error nếu DB fail.
//
// Khi nào gọi:
//   - Nhận Kafka controller election event (KRaft epoch change).
//   - Manual failover (ops team trigger qua admin API).
//
// Không gọi trong hot path (mỗi write request) — chỉ khi election xảy ra.
func (r *clusterConfigRepo) IncrementToken(newLeaderID int) (int64, error) {
	var newToken int64

	err := r.db.Transaction(func(tx *gorm.DB) error {
		// SELECT FOR UPDATE: lock row id=1 trong transaction.
		// Các transaction khác phải chờ lock được release.
		var cfg models.ClusterConfig
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&cfg, 1).Error; err != nil {
			return fmt.Errorf("lock cluster config failed: %w", err)
		}

		cfg.CurrentToken++
		cfg.LastLeaderID = newLeaderID
		cfg.UpdatedAt = time.Now()

		if err := tx.Save(&cfg).Error; err != nil {
			return fmt.Errorf("save fencing token failed: %w", err)
		}

		newToken = cfg.CurrentToken
		return nil
	})

	if err != nil {
		return 0, err
	}
	return newToken, nil
}

// GetCurrentToken trả về CurrentToken hiện tại (read-only, không lock).
//
// Dùng cho health check, metrics export, và debug endpoint.
// Không dùng để validate write requests — dùng ValidateFencingToken() cho mục đó.
func (r *clusterConfigRepo) GetCurrentToken() (int64, error) {
	var cfg models.ClusterConfig
	if err := r.db.First(&cfg, 1).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, fmt.Errorf("cluster config not initialized — run InitializeDatabase()")
		}
		return 0, fmt.Errorf("get current token: %w", err)
	}
	return cfg.CurrentToken, nil
}
