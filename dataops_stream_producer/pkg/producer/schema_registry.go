// LEARN: Schema Registry Client — giao tiếp với Confluent Schema Registry REST API
//
// Confluent Schema Registry REST API:
//   POST /subjects/{subject}/versions    → register schema, trả về schema_id
//   GET  /schemas/ids/{id}               → lấy schema theo ID
//   GET  /subjects/{subject}/versions/latest → lấy schema mới nhất
//
// Subject naming convention: {topic-name}-value (cho value), {topic-name}-key (cho key)

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
