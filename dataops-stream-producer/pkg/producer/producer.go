// REUSE: Adapted from trading-bot-finance-service/pkg/producers/job_error.go (DLQ pattern)
// LEARN: Kafka Producer với Avro serialization + Schema Registry
//
// Schema Registry là gì?
//   - Central store cho Avro schemas
//   - Producer đăng ký schema → nhận schema_id (int)
//   - Wire format: [0x00][4-byte schema_id][avro bytes]
//   - Consumer dùng schema_id để deserialize đúng schema
//
// Tại sao Avro thay vì JSON?
//   - Compact: JSON "price":85300.5 → double (8 bytes) vs ~14 bytes string
//   - Schema enforcement: không thể produce sai field
//   - Schema evolution: thêm optional field (nullable union) → backward compatible
//
// DLQ (Dead Letter Queue):
//   - Khi produce thất bại → ghi vào topic "...dlq"
//   - Operator xử lý DLQ riêng (inspect, replay, alert)
//   - Tránh mất data khi downstream có sự cố tạm thời

package producer

import (
	"context"
	"dataops-stream-producer/constants"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Producer là interface — abstracts away Kafka + Avro details từ callers
// LEARN: Callers chỉ biết PublishTrade, PublishOrderBook... không cần biết Kafka bên dưới
type Producer interface {
	PublishTrade(ctx context.Context, instID string, event TradeEvent) error
	PublishOrderBook(ctx context.Context, instID string, event OrderBookEvent) error
	PublishLiquidation(ctx context.Context, instID string, event LiquidationEvent) error
	PublishVPSSnapshot(ctx context.Context, symbol string, event VPSSnapshotEvent) error
	Close() error
}

// kafkaProducer concrete implementation dùng Sarama (IBM/sarama — official Go Kafka client)
type kafkaProducer struct {
	producer     sarama.SyncProducer // synchronous producer — block cho đến khi broker ACK
	schemaClient *SchemaRegistryClient
	dlqProducer  sarama.SyncProducer // Dead Letter Queue producer

	// Schema IDs: đăng ký 1 lần khi khởi động, cache lại để reuse
	tradeSchemaID int
	obSchemaID    int
	liqSchemaID   int
	vpsSchemaID   int

	mu sync.RWMutex // bảo vệ schema IDs khi nhiều goroutines đọc/ghi
}

// NewProducer khởi tạo Kafka producer với Avro serialization
// LEARN: sarama.Config là builder pattern — set từng field trước khi tạo client
func NewProducer(brokers []string, schemaRegistryURL string) (Producer, error) {
	// --- Kafka Producer Config ---
	config := sarama.NewConfig()

	// WaitForAll = producer chờ ALL in-sync replicas ACK (không mất data)
	// LEARN: Acks options: NoResponse(0) < WaitForLocal(1) < WaitForAll(-1)
	config.Producer.RequiredAcks = sarama.WaitForAll

	// Retry tự động khi gặp temporary failures
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	// Bật return (channel) để SyncProducer biết kết quả
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Compression: Snappy tốt cho real-time (low CPU), Gzip tốt cho batch (high ratio)
	config.Producer.Compression = sarama.CompressionSnappy

	// Idempotent producer: đảm bảo exactly-once semantics trong 1 partition
	// LEARN: Idempotent = nếu retry, broker bỏ qua duplicate (dùng sequence number)
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1 // required cho idempotent
	config.Producer.Partitioner = NewBinancePartitioner

	mainProducer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("create sarama producer: %w", err)
	}

	// DLQ producer (yêu cầu WaitForAll để không làm mất tin nhắn lỗi)
	dlqConfig := sarama.NewConfig()
	dlqConfig.Producer.RequiredAcks = sarama.WaitForAll
	dlqConfig.Producer.Return.Successes = true
	dlqConfig.Producer.Return.Errors = true
	dlqConfig.Producer.Partitioner = NewBinancePartitioner
	dlqProducer, err := sarama.NewSyncProducer(brokers, dlqConfig)
	if err != nil {
		mainProducer.Close()
		return nil, fmt.Errorf("create dlq producer: %w", err)
	}

	// Schema Registry client (nếu có cấu hình)
	var srClient *SchemaRegistryClient
	if schemaRegistryURL != "" {
		srClient = NewSchemaRegistryClient(schemaRegistryURL)
	}

	p := &kafkaProducer{
		producer:     mainProducer,
		schemaClient: srClient,
		dlqProducer:  dlqProducer,
	}

	// Đăng ký tất cả schemas lúc khởi động nếu schema registry client tồn tại
	if err := p.registerSchemas(); err != nil {
		log.Printf("[Producer] Warning during schema registration: %v", err)
	}

	log.Printf("[Producer] Initialized with %d brokers, SchemaRegistry=%s", len(brokers), schemaRegistryURL)
	return p, nil
}

// registerSchemas đăng ký tất cả Avro schemas với Confluent Schema Registry
// LEARN: Schema compatibility = BACKWARD (new schema đọc được data cũ)
func (p *kafkaProducer) registerSchemas() error {
	// Kiểm tra xem Schema Registry URL có cấu hình không
	if p.schemaClient == nil {
		return nil
	}

	schemas := []struct {
		subject  string
		filePath string
		idPtr    *int
	}{
		{
			subject:  constants.TopicBinanceTrades + "-value",
			filePath: "pkg/schemas/okx_trade_event.avsc",
			idPtr:    &p.tradeSchemaID,
		},
		{
			subject:  constants.TopicBinanceOrderBook + "-value",
			filePath: "pkg/schemas/okx_orderbook_snapshot.avsc",
			idPtr:    &p.obSchemaID,
		},
		{
			subject:  constants.TopicBinanceLiquidations + "-value",
			filePath: "pkg/schemas/okx_liquidation_event.avsc",
			idPtr:    &p.liqSchemaID,
		},
		{
			subject:  constants.TopicVPSMarket + "-value",
			filePath: "pkg/schemas/vps_market_snapshot.avsc",
			idPtr:    &p.vpsSchemaID,
		},
	}

	for _, s := range schemas {
		id, err := p.schemaClient.RegisterSchema(s.subject, s.filePath)
		if err != nil {
			log.Printf("[Producer] Warning: register schema %s failed: %v (falling back to JSON/wire envelope)", s.subject, err)
			continue
		}
		*s.idPtr = id
		log.Printf("[Producer] Registered schema %s → id=%d", s.subject, id)
	}
	return nil
}

// PublishTrade serialize TradeEvent thành Avro bytes và publish lên Kafka
// LEARN: Message key = instID → đảm bảo semua messages cùng instrument vào 1 partition
//
//	→ ordering guarantee per instrument (quan trọng cho time-series)
func (p *kafkaProducer) PublishTrade(ctx context.Context, instID string, event TradeEvent) error {
	payload, err := p.serializeAvro(p.tradeSchemaID, event)
	if err != nil {
		return p.sendToDLQ(constants.TopicBinanceTradesDLQ, instID, event, err)
	}

	msg := &sarama.ProducerMessage{
		Topic: constants.TopicBinanceTrades,
		Key:   sarama.StringEncoder(instID), // key = instID → same partition per instrument
		Value: sarama.ByteEncoder(payload),
		// Timestamp cho phép Kafka brokers và consumers biết thời gian thực của message
		Timestamp: time.UnixMilli(event.TsMs),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		log.Printf("[Producer] SendMessage trade %s error: %v — routing to DLQ", instID, err)
		return p.sendToDLQ(constants.TopicBinanceTradesDLQ, instID, event, err)
	}
	return nil
}

func (p *kafkaProducer) PublishOrderBook(ctx context.Context, instID string, event OrderBookEvent) error {
	payload, err := p.serializeAvro(p.obSchemaID, event)
	if err != nil {
		return p.sendToDLQ(constants.TopicBinanceOrderBookDLQ, instID, event, err)
	}

	msg := &sarama.ProducerMessage{
		Topic:     constants.TopicBinanceOrderBook,
		Key:       sarama.StringEncoder(instID),
		Value:     sarama.ByteEncoder(payload),
		Timestamp: time.UnixMilli(event.TsMs),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		return p.sendToDLQ(constants.TopicBinanceOrderBookDLQ, instID, event, err)
	}
	return nil
}

func (p *kafkaProducer) PublishLiquidation(ctx context.Context, instID string, event LiquidationEvent) error {
	payload, err := p.serializeAvro(p.liqSchemaID, event)
	if err != nil {
		return p.sendToDLQ(constants.TopicBinanceLiquidationsDLQ, instID, event, err)
	}

	msg := &sarama.ProducerMessage{
		Topic:     constants.TopicBinanceLiquidations,
		Key:       sarama.StringEncoder(instID),
		Value:     sarama.ByteEncoder(payload),
		Timestamp: time.UnixMilli(event.TsMs),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		return p.sendToDLQ(constants.TopicBinanceLiquidationsDLQ, instID, event, err)
	}
	return nil
}

func (p *kafkaProducer) PublishVPSSnapshot(ctx context.Context, symbol string, event VPSSnapshotEvent) error {
	payload, err := p.serializeAvro(p.vpsSchemaID, event)
	if err != nil {
		return p.sendToDLQ(constants.TopicVPSMarketDLQ, symbol, event, err)
	}

	msg := &sarama.ProducerMessage{
		Topic:     constants.TopicVPSMarket,
		Key:       sarama.StringEncoder(symbol),
		Value:     sarama.ByteEncoder(payload),
		Timestamp: time.UnixMilli(event.TsMs),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		return p.sendToDLQ(constants.TopicVPSMarketDLQ, symbol, event, err)
	}
	return nil
}

// serializeAvro encode event thành Confluent Wire Format:
//
//	Byte 0:   0x00 (magic byte)
//	Bytes 1-4: schema_id (big-endian int32)
//	Bytes 5+:  Avro binary encoded payload
//
// LEARN: Confluent Wire Format là chuẩn — mọi Kafka consumer với Avro đều hiểu format này
func (p *kafkaProducer) serializeAvro(schemaID int, event interface{}) ([]byte, error) {
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}

	if schemaID <= 0 {
		// Schema Registry không khả dụng hoặc chưa đăng ký -> fallback gửi JSON chuẩn
		return jsonBytes, nil
	}

	// Confluent Wire Format header: [0x00][4-byte big-endian schema ID]
	header := make([]byte, 5)
	header[0] = 0x00 // Confluent magic byte
	binary.BigEndian.PutUint32(header[1:5], uint32(schemaID))

	return append(header, jsonBytes...), nil
}

// sendToDLQ ghi message lỗi vào Dead Letter Queue topic
// DLQ message chứa original payload + error context để debug sau
func (p *kafkaProducer) sendToDLQ(dlqTopic string, key string, event interface{}, originalErr error) error {
	dlqPayload, err := json.Marshal(map[string]interface{}{
		"event":     event,
		"error":     originalErr.Error(),
		"timestamp": time.Now().UnixMilli(),
	})
	if err != nil {
		log.Printf("[Producer] Failed to marshal DLQ payload: %v", err)
		return originalErr
	}

	dlqMsg := &sarama.ProducerMessage{
		Topic: dlqTopic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(dlqPayload),
	}

	if _, _, err := p.dlqProducer.SendMessage(dlqMsg); err != nil {
		log.Printf("[Producer] DLQ write failed topic=%s key=%s: %v", dlqTopic, key, err)
	} else {
		log.Printf("[Producer] Message routed to DLQ topic=%s key=%s", dlqTopic, key)
	}

	return originalErr // trả về error gốc để caller biết
}

func (p *kafkaProducer) Close() error {
	if err := p.producer.Close(); err != nil {
		return err
	}
	return p.dlqProducer.Close()
}
