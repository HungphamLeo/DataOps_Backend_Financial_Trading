// REUSE 100%: market-maker-api/models/auth.go
// GORM models — struct tags là "hợp đồng" với cả DB lẫn JSON API
package models

import (
	"dataops-backend-api/constants"

	"gorm.io/gorm"
)

// User — lưu tài khoản người dùng
// gorm.Model tự thêm: ID, CreatedAt, UpdatedAt, DeletedAt (soft delete)
type User struct {
	gorm.Model
	Username string              `gorm:"size:255;not null;unique" json:"username"`
	Password string              `gorm:"size:255;not null" json:"password"`
	Email    string              `gorm:"not null;unique" json:"email"`
	RoleId   constants.AUTH_ROLE `gorm:"default:1" json:"role_id"`
}

// UserSession — lưu session đang active (cũng cache trên Redis)
type UserSession struct {
	gorm.Model
	Session  string              `json:"session"`
	UserID   string              `json:"user_id"`
	UserId   uint                `json:"id"`
	Username string              `json:"username"`
	RoleID   uint                `json:"role_id"`
	RoleId   constants.AUTH_ROLE `json:"auth_role"`
	Status   string              `json:"status"`
	Active   uint8               `gorm:"default:1" json:"active"`
}
