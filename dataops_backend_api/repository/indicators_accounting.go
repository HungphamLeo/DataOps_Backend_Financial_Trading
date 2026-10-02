// REUSE: ~85% từ trading-bot-api/repository/indicator_group.go + indicator.go + indicator_parameter.go
// LEARN: Repository layer — chỉ tương tác với database, không có business logic
//
// GORM patterns quan trọng trong file này:
//   1. Preload: load related records (JOIN nhưng dùng N+1 query thông minh)
//   2. Transaction: đảm bảo atomicity khi update có cascade delete
//   3. Unscoped: thao tác với soft-deleted records
//   4. Model().Where().Updates(): partial update, không overwrite toàn bộ record

package repository

import (
	"fmt"
	"dataops-backend-api/models"

	"gorm.io/gorm"
)

// ─── IndicatorGroup CRUD ──────────────────────────────────────────────────────

// GetListIndicatorGroups lấy tất cả indicator groups của 1 user
// LEARN: Preload("Indicators").Preload("Parameters") = eager loading
//        Không dùng Joins() để tránh cartesian product khi có nhiều associations
func (g *Gorm) GetListIndicatorGroups(userID uint) ([]*models.IndicatorGroup, error) {
	var groups []*models.IndicatorGroup
	err := g.Database.
		Preload("Parameters").
		Preload("Indicators").
		Where("created_by = ?", userID).
		Find(&groups).Error
	return groups, err
}

// GetIndicatorGroupDetail lấy 1 group với đầy đủ relationships
func (g *Gorm) GetIndicatorGroupDetail(id, userID uint) (*models.IndicatorGroup, error) {
	group := &models.IndicatorGroup{}
	err := g.Database.
		Preload("Parameters").
		Preload("Indicators").
		Where("id = ? AND created_by = ?", id, userID).
		First(group).Error
	return group, err
}

// CreateIndicatorGroup tạo group cùng với indicators và parameters trong 1 transaction
// REUSE: Từ trading-bot-api/repository/indicator_group.go CreateIndicatorGroup
// LEARN: g.Database.Create(&group) với has-many relationship
//        GORM tự động INSERT nested records nếu chúng có foreignKey set
func (g *Gorm) CreateIndicatorGroup(group *models.IndicatorGroup) error {
	return g.Database.Create(group).Error
}

// UpdateIndicatorGroup cập nhật group + cascade delete orphans
// LEARN: Database Transaction trong GORM:
//        g.Database.Transaction(func(tx *gorm.DB) error { ... })
//        Nếu callback return error → rollback tự động
//        Nếu return nil → commit tự động
func (g *Gorm) UpdateIndicatorGroup(group *models.IndicatorGroup, validIndicatorIDs, validParamIDs []uint) error {
	return g.Database.Transaction(func(tx *gorm.DB) error {
		// Bước 1: Xóa indicators không còn trong update request
		// LEARN: Unscoped().Delete() = hard delete (không soft delete)
		//        Dùng Unscoped khi muốn xóa thực sự khỏi DB
		if len(validIndicatorIDs) > 0 {
			if err := tx.Unscoped().Where("group_id = ? AND id NOT IN ?", group.ID, validIndicatorIDs).
				Delete(&models.Indicator{}).Error; err != nil {
				return fmt.Errorf("delete orphan indicators: %w", err)
			}
		} else {
			// Nếu không có valid IDs → xóa hết indicators cũ
			if err := tx.Unscoped().Where("group_id = ?", group.ID).
				Delete(&models.Indicator{}).Error; err != nil {
				return fmt.Errorf("delete all indicators: %w", err)
			}
		}

		// Bước 2: Xóa parameters không còn trong update request
		if len(validParamIDs) > 0 {
			if err := tx.Unscoped().Where("group_id = ? AND id NOT IN ?", group.ID, validParamIDs).
				Delete(&models.IndicatorParameter{}).Error; err != nil {
				return fmt.Errorf("delete orphan parameters: %w", err)
			}
		} else {
			if err := tx.Unscoped().Where("group_id = ?", group.ID).
				Delete(&models.IndicatorParameter{}).Error; err != nil {
				return fmt.Errorf("delete all parameters: %w", err)
			}
		}

		// Bước 3: Save group với nested records (upsert)
		// LEARN: Session(&gorm.Session{FullSaveAssociations: true}) = save all nested
		if err := tx.Session(&gorm.Session{FullSaveAssociations: true}).Save(group).Error; err != nil {
			return fmt.Errorf("save indicator group: %w", err)
		}

		return nil // commit
	})
}

// DeleteIndicatorGroup xóa group + tất cả children trong 1 transaction
func (g *Gorm) DeleteIndicatorGroup(id, userID uint) error {
	return g.Database.Transaction(func(tx *gorm.DB) error {
		group := &models.IndicatorGroup{}
		if err := tx.Where("id = ? AND created_by = ?", id, userID).First(group).Error; err != nil {
			return err
		}

		// Cascade: xóa indicators → parameters → group
		if err := tx.Where("group_id = ?", id).Delete(&models.Indicator{}).Error; err != nil {
			return err
		}
		if err := tx.Where("group_id = ?", id).Delete(&models.IndicatorParameter{}).Error; err != nil {
			return err
		}
		return tx.Delete(group).Error
	})
}

// ─── Indicator CRUD ────────────────────────────────────────────────────────────

func (g *Gorm) CreateIndicator(ind *models.Indicator) error {
	return g.Database.Create(ind).Error
}

func (g *Gorm) GetIndicator(id uint) (*models.Indicator, error) {
	ind := &models.Indicator{}
	err := g.Database.First(ind, id).Error
	return ind, err
}

func (g *Gorm) UpdateIndicator(ind *models.Indicator) error {
	return g.Database.Save(ind).Error
}

// ─── IndicatorParameter CRUD ──────────────────────────────────────────────────

func (g *Gorm) CreateIndicatorParameter(param *models.IndicatorParameter) error {
	return g.Database.Create(param).Error
}

func (g *Gorm) GetParametersByGroup(groupID uint) ([]*models.IndicatorParameter, error) {
	var params []*models.IndicatorParameter
	err := g.Database.Where("group_id = ?", groupID).Find(&params).Error
	return params, err
}

func (g *Gorm) UpdateIndicatorParameter(param *models.IndicatorParameter) error {
	// LEARN: Updates(map) chỉ update fields được chỉ định, không ảnh hưởng fields khác
	//        Khác với Save() = update TẤT CẢ fields kể cả zero values
	return g.Database.Model(&models.IndicatorParameter{}).Where("id = ?", param.ID).
		Updates(map[string]interface{}{
			"current_value": param.CurrentValue,
			"name":          param.Name,
		}).Error
}

// ─── Accounting CRUD ──────────────────────────────────────────────────────────

// RecordTransaction ghi lại transaction tài chính — append-only, không sửa
func (g *Gorm) RecordTransaction(tx *models.AccountTransaction) error {
	return g.Database.Create(tx).Error
}

// GetTransactionsByUser lấy lịch sử giao dịch của user với pagination
// REUSE: Pagination pattern từ trading-bot-api/repository/history.go
// LEARN: Offset pagination = (page-1) * pageSize
//        Cursor pagination = WHERE id > lastID (tốt hơn cho lớn, không dùng ở đây)
func (g *Gorm) GetTransactionsByUser(userID uint, page, pageSize int) ([]models.AccountTransaction, int64, error) {
	var txs []models.AccountTransaction
	var total int64

	offset := (page - 1) * pageSize

	// Count total trước (cho pagination UI)
	g.Database.Model(&models.AccountTransaction{}).Where("user_id = ?", userID).Count(&total)

	err := g.Database.
		Where("user_id = ?", userID).
		Order("executed_at DESC").
		Limit(pageSize).
		Offset(offset).
		Find(&txs).Error

	return txs, total, err
}

// GetDailyPnL lấy P&L tổng kết theo ngày
func (g *Gorm) GetDailyPnL(userID uint, exchange string, days int) ([]models.DailyPnL, error) {
	var pnl []models.DailyPnL
	err := g.Database.
		Where("user_id = ? AND exchange = ?", userID, exchange).
		Order("date DESC").
		Limit(days).
		Find(&pnl).Error
	return pnl, err
}

// UpsertBalance update số dư, tạo mới nếu chưa có
// LEARN: Save với OnConflict = UPSERT (Insert or Update)
func (g *Gorm) UpsertBalance(balance *models.AccountBalance) error {
	return g.Database.Save(balance).Error
}

func (g *Gorm) GetBalance(userID uint, exchange, currency string) (*models.AccountBalance, error) {
	bal := &models.AccountBalance{}
	err := g.Database.Where("user_id = ? AND exchange = ? AND currency = ?", userID, exchange, currency).First(bal).Error
	return bal, err
}

// LogExecution ghi execution log — append-only
func (g *Gorm) LogExecution(log *models.ExecutionLog) error {
	return g.Database.Create(log).Error
}
