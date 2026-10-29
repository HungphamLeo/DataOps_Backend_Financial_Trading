// REUSE 85%: market-maker-api/repository/repository.go
// GORM database initialization với AutoMigrate
package repository

import (
	"fmt"
	"dataops-backend-api/configuration"
	"dataops-backend-api/models"
	"dataops-backend-api/utils"
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var DB *gorm.DB

// Gorm wraps gorm.DB và được inject vào repositories
type Gorm struct {
	Database *gorm.DB
}

// InitializeDatabase kết nối MySQL và auto-migrate schemas
// REUSE: market-maker-api/repository/repository.go
func InitializeDatabase(cfg *configuration.Config) (*Gorm, error) {
	fmt.Printf("%s [Database] Connecting to MySQL...\n", utils.GetTimeNow())

	logMode := logger.Error
	if cfg.App.Mode == "dev" {
		logMode = logger.Info
	}

	var err error
	const maxRetries = 10
	const retryInterval = 2 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		DB, err = gorm.Open(mysql.Open(cfg.MySQL.DNS), &gorm.Config{
			Logger: logger.Default.LogMode(logMode),
		})
		if err == nil {
			break
		}
		log.Printf("[Database] connect attempt %d/%d failed: %v. Retrying in %v...", attempt, maxRetries, err, retryInterval)
		time.Sleep(retryInterval)
	}

	if err != nil {
		log.Printf("[Database] connect error after %d attempts: %v", maxRetries, err)
		return nil, err
	}

	// AutoMigrate: tự tạo/update tables từ structs.
	// ClusterConfig phải migrate trước User/UserSession để seed row có thể chạy ngay sau.
	// Fencing Token table cần tồn tại trước khi bất kỳ write handler nào chạy.
	err = DB.AutoMigrate(
		&models.ClusterConfig{}, // ST01: Fencing Token — singleton row id=1
		&models.User{},
		&models.UserSession{},
	)
	if err != nil {
		return nil, fmt.Errorf("[Database] migrate error: %w", err)
	}

	// Seed ClusterConfig row (singleton id=1) nếu chưa tồn tại.
	// FirstOrCreate: idempotent — safe để gọi lại nhiều lần (restart, redeploy).
	// CLUSTER_ID "MkU3OEVBNTcwNTJENDM2Qk" phải khớp với CLUSTER_ID trong docker-compose.yml.
	// CurrentToken khởi đầu = 1 (0 reserve cho "uninitialized" sentinel value).
	var clusterCfg models.ClusterConfig
	result := DB.FirstOrCreate(&clusterCfg, models.ClusterConfig{
		ID:           1,
		ClusterID:    "MkU3OEVBNTcwNTJENDM2Qk",
		CurrentToken: 1,
		LastLeaderID: 0,
	})
	if result.Error != nil {
		return nil, fmt.Errorf("[Database] seed cluster_config error: %w", result.Error)
	}
	if result.RowsAffected > 0 {
		log.Printf("[Database] cluster_config seeded: token=%d cluster=%s", clusterCfg.CurrentToken, clusterCfg.ClusterID)
	}

	fmt.Printf("%s [Database] Connected and migrated\n", utils.GetTimeNow())
	return &Gorm{Database: DB}, nil
}
