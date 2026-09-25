// Auth Service — REUSE 90%: trading-bot-api/services/authentication/
// JWT login, refresh token, logout với Redis session management
package authentication

import (
	"fmt"
	"okx-analytics-api/configuration"
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
	repo  *repository.Gorm
	cfg   *configuration.Config
	redis redis.Service
}

func NewService(repo *repository.Gorm, cfg *configuration.Config, redis redis.Service) Service {
	return &authService{repo: repo, cfg: cfg, redis: redis}
}

// Login xác thực user và trả về JWT pair (access + refresh)
func (s *authService) Login(username, password string) (map[string]interface{}, error) {
	// 1. Tìm user trong DB
	var user models.User
	if err := s.repo.Database.Where("username = ?", username).First(&user).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("invalid credentials")
		}
		return nil, err
	}

	// 2. Verify password (bcrypt)
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password)); err != nil {
		return nil, fmt.Errorf("invalid credentials")
	}

	// 3. Tạo session ID duy nhất
	sessionID := fmt.Sprintf("session_%d_%d", user.ID, time.Now().UnixNano())

	// 4. Tạo access token JWT
	accessToken, err := s.generateToken(user.ID, sessionID, user.Username, s.cfg.Authentication.Lifespan)
	if err != nil {
		return nil, err
	}

	// 5. Tạo refresh token
	refreshToken, err := s.generateToken(user.ID, sessionID, user.Username, s.cfg.Authentication.LifespanRefreshToken)
	if err != nil {
		return nil, err
	}

	// 6. Lưu session vào Redis
	sessionData := map[string]interface{}{
		"user_id":  user.ID,
		"role_id":  user.RoleId,
		"username": user.Username,
	}
	s.redis.Set(sessionID, sessionData, time.Duration(s.cfg.Authentication.LifespanRefreshToken)*time.Hour)

	return map[string]interface{}{
		"access_token":  accessToken,
		"refresh_token": refreshToken,
	}, nil
}

// RefreshToken tạo access token mới từ refresh token còn hạn
func (s *authService) RefreshToken(refreshToken string) (map[string]interface{}, error) {
	claims, err := s.parseToken(refreshToken, s.cfg.Authentication.SecretRefreshToken)
	if err != nil {
		return nil, fmt.Errorf("refresh token expired")
	}

	userID := uint(claims["id"].(float64))
	sessionID := claims["session"].(string)
	username := claims["username"].(string)

	// Check session còn trong Redis
	if s.redis.Get(sessionID) == "" {
		return nil, fmt.Errorf("session expired — please login again")
	}

	// Tạo access token mới
	newAccessToken, err := s.generateToken(userID, sessionID, username, s.cfg.Authentication.Lifespan)
	if err != nil {
		return nil, err
	}

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

	return s.redis.Del(sessionID)
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
