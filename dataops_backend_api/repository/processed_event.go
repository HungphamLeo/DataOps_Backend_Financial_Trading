// Package repository — ST13 processed-event idempotency repository.
package repository

import (
	"fmt"

	"dataops-backend-api/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ProcessedEventRepository defines the ST13 deduplication contract.
//
// Implementations must make TryMarkProcessed atomic: two callers that race
// with the same idempotency key must return exactly one owner.
type ProcessedEventRepository interface {
	TryMarkProcessed(event *models.ProcessedEvent) (bool, error)
	GetProcessedEvent(idempotencyKey string) (*models.ProcessedEvent, error)
}

type processedEventRepo struct {
	db *gorm.DB
}

// NewProcessedEventRepo creates an ST13 repository backed by MySQL/GORM.
//
// db must be the *gorm.DB returned by InitializeDatabase. The repository does
// not own the connection lifecycle.
func NewProcessedEventRepo(db *gorm.DB) ProcessedEventRepository {
	return &processedEventRepo{db: db}
}

// TryMarkProcessed atomically claims an event identity.
//
// MySQL ON DUPLICATE behavior is disabled via DoNothing, so a replay returns
// duplicate=false and must not overwrite the original event audit row.
//
// Args:
//
//	event: immutable ST13 identity and source metadata.
//
// Returns:
//
//	inserted=true when this caller owns the event for the first time.
//	inserted=false when the idempotency key was already recorded.
//
// Errors:
//
//	Database/validation failures are returned to the caller; in an at-least-
//	once pipeline the caller should fail the batch rather than publish data
//	when it cannot prove the event was deduplicated.
func (r *processedEventRepo) TryMarkProcessed(event *models.ProcessedEvent) (bool, error) {
	if event == nil {
		return false, fmt.Errorf("ST13 processed event is nil")
	}
	if len(event.IdempotencyKey) != 64 {
		return false, fmt.Errorf(
			"ST13 invalid idempotency key: got length %d, want 64",
			len(event.IdempotencyKey),
		)
	}
	if event.Source == "" || event.Symbol == "" {
		return false, fmt.Errorf(
			"ST13 processed event requires source and symbol",
		)
	}

	result := r.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "idempotency_key"}},
		DoNothing: true,
	}).Create(event)
	if result.Error != nil {
		return false, fmt.Errorf("ST13 mark processed failed: %w", result.Error)
	}

	return result.RowsAffected == 1, nil
}

// GetProcessedEvent reads the original ST13 audit row by idempotency key.
//
// It is useful for replay diagnostics and does not mutate the dedup state.
func (r *processedEventRepo) GetProcessedEvent(idempotencyKey string) (*models.ProcessedEvent, error) {
	var event models.ProcessedEvent
	if err := r.db.First(&event, "idempotency_key = ?", idempotencyKey).Error; err != nil {
		return nil, fmt.Errorf("ST13 get processed event failed: %w", err)
	}
	return &event, nil
}
