package authentication

import (
	"fmt"
	"time"

	"dataops-backend-api/constants"
	"dataops-backend-api/middlewares"
	"dataops-backend-api/models"
)

// Trong hàm Login:
func (s *authService) Login(username, password string) (map[string]interface{}, error) {
	// ... xác thực password ...

	userIDStr := fmt.Sprintf("%d", user.ID)
	sessionID := fmt.Sprintf("session_%d_%d", user.ID, time.Now().UnixNano())

	// Tạo access token theo chuẩn middlewares.JWTClaims
	accessToken, err := middlewares.GenerateToken(
		s.cfg.Authentication.Secret,
		userIDStr,
		uint(user.RoleId),
		user.Email,
		time.Duration(s.cfg.Authentication.Lifespan)*time.Hour,
	)
	if err != nil {
		return nil, err
	}

	refreshToken, err := middlewares.GenerateToken(
		s.cfg.Authentication.SecretRefreshToken,
		userIDStr,
		uint(user.RoleId),
		user.Email,
		time.Duration(s.cfg.Authentication.LifespanRefreshToken)*time.Hour,
	)
	if err != nil {
		return nil, err
	}

	// Lưu session theo đúng format JwtAuthMiddleware mong đợi
	sessionKey := "session:" + userIDStr
	sessionData := models.UserSession{
		Session:  sessionID,
		UserID:   userIDStr,
		UserId:   user.ID,
		Username: user.Username,
		RoleID:   uint(user.RoleId),
		RoleId:   user.RoleId,
		Status:   constants.USER_STATUS_ACTIVE,
		Active:   1,
	}
	ttl := time.Duration(s.cfg.Authentication.LifespanRefreshToken) * time.Hour
	_ = s.redis.Set(sessionKey, sessionData, ttl)

	return map[string]interface{}{
		"access_token":  accessToken,
		"refresh_token": refreshToken,
	}, nil
}
