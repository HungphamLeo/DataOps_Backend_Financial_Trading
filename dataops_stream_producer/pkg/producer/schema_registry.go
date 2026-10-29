// Package producer — Confluent Schema Registry HTTP Client.
//
// ═══════════════════════════════════════════════════════════════════════════════
// ST08 — DISTRIBUTED TRACING HOOKS & ST10 — SCHEMA GOVERNANCE (PLAN_REPO03)
// ═══════════════════════════════════════════════════════════════════════════════
//
// ST10 — Schema Registry là gì và tại sao cần:
//   Vấn đề: team Backend đổi kiểu field "price" từ float → string.
//   Không có Schema Registry: Producer gửi message mới, Consumer cũ crash khi deserialize.
//   Với Schema Registry + BACKWARD compatibility:
//     → CI gate chạy "buf breaking" hoặc schema compatibility check.
//     → Nếu breaking change → deploy bị block ngay tại PR stage.
//     → Downstream pipeline không bao giờ nhận data không đọc được.
//
//   Schema Evolution Rules (BACKWARD compatibility):
//     ✅ Thêm field mới với default value → backward compatible.
//     ✅ Xóa field có default value → backward compatible.
//     ❌ Đổi kiểu dữ liệu (float → string) → BREAKING, bị chặn.
//     ❌ Thêm field bắt buộc không có default → BREAKING, bị chặn.
//
//   Schema ID caching: đăng ký 1 lần lúc startup, cache schema_id.
//   → Serialize chỉ cần prepend 5-byte header: [0x00][4-byte schema_id].
//   → Không có HTTP call per message → zero overhead trong hot path.
//
// ST08 — Distributed Tracing Integration (TODO):
//   Khi implement OTel (OpenTelemetry) trong ST08, inject trace context tại đây:
//   RegisterSchema() và GetSchemaByID() nên tạo child span → đo thời gian HTTP call.
//   Thêm attribute: schema.subject, schema.id → visible trong Jaeger/Tempo.
//   Pattern: tracer.Start(ctx, "schema_registry.register") → deferred span.End().
//
// Schema Registry REST API:
//   POST /subjects/{subject}/versions    → register schema, trả về schema_id
//   GET  /schemas/ids/{id}               → lấy schema theo ID
//   GET  /subjects/{subject}/versions/latest → lấy schema mới nhất
//   Subject naming convention: {topic-name}-value (value), {topic-name}-key (key)

package producer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

// SchemaRegistryClient HTTP client cho Confluent Schema Registry
type SchemaRegistryClient struct {
	baseURL    string
	httpClient *http.Client
	cache      map[string]int // subject → schema_id cache (tránh re-register)
}

// NewSchemaRegistryClient tạo client với timeout và cache
func NewSchemaRegistryClient(baseURL string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		cache: make(map[string]int),
	}
}

// RegisterSchema đọc .avsc file và đăng ký với Schema Registry
// Trả về schema_id để encode vào Confluent Wire Format header
func (c *SchemaRegistryClient) RegisterSchema(subject, avscFilePath string) (int, error) {
	// Kiểm tra cache trước
	if id, ok := c.cache[subject]; ok {
		return id, nil
	}

	// Đọc Avro schema từ file
	// LEARN: os.ReadFile = đọc toàn bộ file vào []byte (Go 1.16+)
	schemaBytes, err := os.ReadFile(avscFilePath)
	if err != nil {
		return 0, fmt.Errorf("read avsc file %s: %w", avscFilePath, err)
	}

	// Schema Registry expects JSON string (escaped) inside a JSON wrapper
	// { "schema": "{avro schema as escaped string}" }
	schemaStr := string(schemaBytes)
	requestBody := map[string]string{
		"schema":     schemaStr,
		"schemaType": "AVRO",
	}
	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return 0, fmt.Errorf("marshal schema request: %w", err)
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", c.baseURL, subject)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return 0, fmt.Errorf("create register request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("register schema request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read register response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return 0, fmt.Errorf("schema registry returned %d: %s", resp.StatusCode, string(respBody))
	}

	// Response: { "id": 42 }
	var result struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return 0, fmt.Errorf("parse schema registry response: %w", err)
	}

	c.cache[subject] = result.ID
	log.Printf("[SchemaRegistry] Registered subject=%s id=%d", subject, result.ID)
	return result.ID, nil
}

// GetSchemaByID lấy schema definition theo schema_id (dùng khi deserialize)
func (c *SchemaRegistryClient) GetSchemaByID(id int) (string, error) {
	url := fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return "", fmt.Errorf("get schema by id: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("parse schema response: %w", err)
	}
	return result.Schema, nil
}
