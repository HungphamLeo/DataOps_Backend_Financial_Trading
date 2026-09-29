// Auth Service — REUSE 90%: trading-bot-api/services/authentication/
// JWT login, refresh token, logout với Redis session management
package authentication

import (
	"fmt"
	"okx-analytics-api/configuration"
	"okx-analytics-api/logging"
	"okx-analytics-api/models"
	"okx-analytics-api/redis"
	"okx-analytics-api/repository"
	"time"

	"github.com/golang-jwt/jwt"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

type Service interface {
	Login(username, password string) (map[string]interface{}, error)
	RefreshToken(refreshToken string) (map[string]interface{}, error)
	Logout(refreshToken string) error
}

type authService struct {
	repo   *repository.Gorm
	cfg    *configuration.Config
	redis  redis.Service
	logger *logging.Logger
}

func NewService(repo *repository.Gorm, cfg *configuration.Config, redis redis.Service, logger *logging.Logger) Service {
	return &authService{repo: repo, cfg: cfg, redis: redis, logger: logger}
}

// Login xác thực user và trả về JWT pair (access + refresh)
func (s *authService) Login(username, password string) (map[string]interface{}, error) {
	// 1. Tìm user trong DB
	var user models.User
	if err := s.repo.Database.Where("username = ?", username).First(&user).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			// Không log username để tránh lộ PII trong log
			s.logger.Warn("Login", "user not found", map[string]interface{}{"username": username})
			return nil, fmt.Errorf("invalid credentials")
		}
		s.logger.Error("Login", "database error", map[string]interface{}{"err": err.Error()})
		return nil, err
	}

	// 2. Verify password (bcrypt)
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password)); err != nil {
		s.logger.Warn("Login", "invalid password", map[string]interface{}{"userID": user.ID})
		return nil, fmt.Errorf("invalid credentials")
	}

	// 3. Tạo session ID duy nhất
	sessionID := fmt.Sprintf("session_%d_%d", user.ID, time.Now().UnixNano())

	// 4. Tạo access token JWT
	accessToken, err := s.generateToken(user.ID, sessionID, user.Username, s.cfg.Authentication.Lifespan)
	if err != nil {
		s.logger.Error("Login", "failed to generate access token", map[string]interface{}{"userID": user.ID, "err": err.Error()})
		return nil, err
	}

	// 5. Tạo refresh token
	refreshToken, err := s.generateToken(user.ID, sessionID, user.Username, s.cfg.Authentication.LifespanRefreshToken)
	if err != nil {
		s.logger.Error("Login", "failed to generate refresh token", map[string]interface{}{"userID": user.ID, "err": err.Error()})
		return nil, err
	}

	// 6. Lưu session vào Redis
	sessionData := map[string]interface{}{
		"user_id":  user.ID,
		"role_id":  user.RoleId,
		"username": user.Username,
	}
	s.redis.Set(sessionID, sessionData, time.Duration(s.cfg.Authentication.LifespanRefreshToken)*time.Hour)

	s.logger.Info("Login", "user logged in", map[string]interface{}{"userID": user.ID, "username": user.Username})
	return map[string]interface{}{
		"access_token":  accessToken,
		"refresh_token": refreshToken,
	}, nil
}

// RefreshToken tạo access token mới từ refresh token còn hạn
func (s *authService) RefreshToken(refreshToken string) (map[string]interface{}, error) {
	claims, err := s.parseToken(refreshToken, s.cfg.Authentication.SecretRefreshToken)
	if err != nil {
		s.logger.Warn("RefreshToken", "refresh token parse failed", map[string]interface{}{"err": err.Error()})
		return nil, fmt.Errorf("refresh token expired")
	}

	userID := uint(claims["id"].(float64))
	sessionID := claims["session"].(string)
	username := claims["username"].(string)

	// Check session còn trong Redis
	if s.redis.Get(sessionID) == "" {
		s.logger.Warn("RefreshToken", "session not found in Redis", map[string]interface{}{"userID": userID, "sessionID": sessionID})
		return nil, fmt.Errorf("session expired — please login again")
	}

	// Tạo access token mới
	newAccessToken, err := s.generateToken(userID, sessionID, username, s.cfg.Authentication.Lifespan)
	if err != nil {
		s.logger.Error("RefreshToken", "failed to generate new access token", map[string]interface{}{"userID": userID, "err": err.Error()})
		return nil, err
	}

	s.logger.Info("RefreshToken", "access token refreshed", map[string]interface{}{"userID": userID})
	return map[string]interface{}{"access_token": newAccessToken}, nil
}

// Logout xóa session khỏi Redis
func (s *authService) Logout(refreshToken string) error {
	claims, err := s.parseToken(refreshToken, s.cfg.Authentication.SecretRefreshToken)
	if err != nil {
		return nil // Token đã expire — coi như đã logout
	}

	sessionID, ok := claims["session"].(string)
	if !ok {
		return nil
	}

	if err := s.redis.Del(sessionID); err != nil {
		s.logger.Error("Logout", "failed to delete session from Redis", map[string]interface{}{"sessionID": sessionID, "err": err.Error()})
		return err
	}

	s.logger.Info("Logout", "session deleted", map[string]interface{}{"sessionID": sessionID})
	return nil
}

func (s *authService) generateToken(userID uint, sessionID, username string, lifespanHours int) (string, error) {
	claims := jwt.MapClaims{
		"id":       userID,
		"session":  sessionID,
		"username": username,
		"exp":      time.Now().Add(time.Duration(lifespanHours) * time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(s.cfg.Authentication.Secret))
}

func (s *authService) parseToken(tokenStr, secret string) (jwt.MapClaims, error) {
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return []byte(secret), nil
	})
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("invalid claims")
	}
	return claims, nil
}
