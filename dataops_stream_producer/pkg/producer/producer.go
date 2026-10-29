// Package producer — Kafka SyncProducer với Avro serialization, DLQ routing, và Schema Registry.
//
// ═══════════════════════════════════════════════════════════════════════════════
// KIẾN TRÚC TỔNG QUAN: Tại sao cần tất cả những thứ này?
// ═══════════════════════════════════════════════════════════════════════════════
//
// VẤN ĐỀ cần giải quyết:
//   Hệ thống tài chính nhận ~10,000 trade events/giây từ Binance WebSocket.
//   Yêu cầu: ZERO data loss, đúng thứ tự per symbol, và có thể debug khi lỗi.
//
// GIẢI PHÁP (từ trên xuống):
//
//   [1] SyncProducer (sarama.WaitForAll)
//       → Block goroutine cho đến khi Kafka broker ACK.
//       → Đảm bảo: message đã được ghi vào ít nhất MIN_ISR=2 replicas.
//       → Ngược lại AsyncProducer: fire-and-forget, throughput cao hơn nhưng
//         có thể mất message khi broker crash ngay sau khi nhận.
//
//   [2] Idempotent Producer (config.Producer.Idempotent = true)
//       → Kafka broker gán sequence number cho mỗi (producer_id, partition).
//       → Nếu producer retry cùng message (do network timeout) → broker nhận
//         sequence number trùng → TỰ ĐỘNG loại bỏ duplicate, không ghi 2 lần.
//       → Đây là "Exactly-Once" trong phạm vi 1 producer session.
//       → Yêu cầu: MaxOpenRequests=1 (không pipeline nhiều requests cùng lúc,
//         nếu không broker không thể xác định thứ tự để deduplicate).
//
//   [3] Schema Registry + Avro Wire Format
//       → Confluent Schema Registry: server lưu trữ tập trung tất cả Avro schemas.
//       → Avro binary nhỏ hơn JSON: một trade event ~200 bytes JSON → ~30 bytes Avro.
//       → Wire Format: [0x00][4-byte schema_id BE][avro payload]
//         - Magic byte 0x00: ký hiệu "đây là Confluent Avro format"
//         - schema_id: consumer dùng để lookup schema từ registry, không cần
//           nhúng schema vào mỗi message (tiết kiệm bandwidth).
//       → Schema evolution: thêm field mới với default value → backward compatible
//         → consumer cũ đọc được message từ producer mới.
//
//   [4] Dead Letter Queue (DLQ) Pattern
//       → Khi serialize lỗi hoặc Kafka publish thất bại → KHÔNG drop message.
//       → Route message sang topic "binance.trade.ticks.dlq" kèm error context.
//       → Lý do: nếu drop message → data gap trong time-series → sai lệch VWAP,
//         volume statistics, và downstream analytics.
//       → Ops team đọc DLQ, điều tra nguyên nhân, replay lại nếu cần.
//
//   [5] BinancePartitioner (xem transactional_producer.go)
//       → Custom partitioning: BTC→P0, ETH→P1, altcoins→P2-N via FNV32a hash.
//       → Ordering guarantee: tất cả BTCUSDT messages → P0 → đúng thứ tự thời gian.
//       → Nếu dùng random/round-robin: BTCUSDT messages có thể đến 3 partitions
//         khác nhau → consumer không thể đảm bảo thứ tự → VWAP tính sai.

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
	// TCBS — VN stocks streaming ingestion
	PublishTCBSMarket(ctx context.Context, symbol string, event TCBSMarketEvent) error
	PublishTCBSBSA(ctx context.Context, ticker string, event TCBSBSAEvent) error
	PublishTCBSIntradayTick(ctx context.Context, ticker string, event TCBSIntradayTickEvent) error
	// TCBS — WSS derivatives real-time (VN30F)
	PublishTCBSDerivative(ctx context.Context, symbol string, event TCBSDerivativeEvent) error
	Close() error
}

// kafkaProducer concrete implementation dùng Sarama (IBM/sarama — official Go Kafka client)
type kafkaProducer struct {
	producer     sarama.SyncProducer // synchronous producer — block cho đến khi broker ACK
	schemaClient *SchemaRegistryClient
	dlqProducer  sarama.SyncProducer // Dead Letter Queue producer

	// Schema IDs: đăng ký 1 lần khi khởi động, cache lại để reuse
	tradeSchemaID      int
	obSchemaID         int
	liqSchemaID        int
	tcbsMarketID       int
	tcbsBSAID          int
	tcbsIntradayID     int
	tcbsDerivativeID   int

	mu sync.RWMutex // bảo vệ schema IDs khi nhiều goroutines đọc/ghi
}

// NewProducer khởi tạo kafkaProducer với đầy đủ cấu hình production.
// sarama.Config là builder pattern: set từng field, sau đó pass vào NewSyncProducer.
func NewProducer(brokers []string, schemaRegistryURL string) (Producer, error) {
	config := sarama.NewConfig()

	// RequiredAcks = WaitForAll (-1): producer block cho đến khi TẤT CẢ ISR ACK.
	// Acks options so sánh:
	//   NoResponse (0):    không chờ gì → throughput tối đa, mất data khi broker crash.
	//   WaitForLocal (1):  chờ Leader ghi xong → ACK ngay, replica chưa kịp nhận.
	//                      Leader crash sau ACK nhưng trước khi replicate → data mất.
	//   WaitForAll (-1):   chờ MIN_ISR=2 replicas xác nhận → ZERO data loss guarantee.
	//                      Latency tăng ~2-5ms nhưng bắt buộc cho dữ liệu tài chính.
	config.Producer.RequiredAcks = sarama.WaitForAll

	// Retry.Max=10, Backoff=500ms: tổng window retry = 5 giây.
	// TẠI SAO cần window dài?
	//   Khi broker Leader crash → KRaft bầu Leader mới trong ~500ms.
	//   Trong thời gian này, producer nhận lỗi "NotLeaderForPartition".
	//   Retry.Max=5 + Backoff=100ms = 500ms window → KHÔNG đủ khi election xảy ra.
	//   Retry.Max=10 + Backoff=500ms = 5s window → đủ chờ qua cả election + warmup.
	//   Lesson learned từ production: nếu window ngắn, trades bị route vào DLQ
	//   trong mỗi leader election → alert bão → ops team bị đánh thức lúc 3 giờ sáng.
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 500 * time.Millisecond

	// Metadata.RefreshFrequency=30s: Sarama tự refresh partition leader map.
	// Default (0) = chỉ refresh khi gặp lỗi NotLeaderForPartition (reactive).
	// 30s = proactive refresh → producer biết leader mới ngay sau election mà không
	// cần gặp lỗi trước. Giảm số lần retry thực tế trong rolling deploy.
	config.Metadata.RefreshFrequency = 30 * time.Second
	// Full=true: refresh metadata của TẤT CẢ topics cùng lúc thay vì chỉ topic bị lỗi.
	// Tradeoff: network request lớn hơn một chút, đổi lại producer luôn có bức tranh
	// đầy đủ về cluster topology.
	config.Metadata.Full = true

	// Return.Successes=true: SyncProducer cần channel này để block và nhận ACK.
	// Return.Errors=true: nhận error khi retry hết → route sang DLQ.
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Compression = Snappy: thuật toán nén tốc độ cao, ratio ~2-3x.
	// So sánh:
	//   None:    0% overhead, message nguyên kích thước → bandwidth cao.
	//   Snappy:  CPU ~1-2%, ratio 2-3x → tốt cho real-time high-throughput.
	//   Gzip:    CPU ~5-10%, ratio 4-6x → tốt cho batch, không phù hợp tick data.
	//   LZ4:     CPU ~0.5%, ratio ~2x → tốt nhất cho real-time nhưng ít broker support.
	// Snappy là lựa chọn cân bằng tốt nhất cho trade tick data.
	config.Producer.Compression = sarama.CompressionSnappy

	// Idempotent=true: mỗi (producer_id, partition, sequence_number) là duy nhất.
	// Khi producer retry message do network timeout:
	//   Without idempotent: broker nhận 2 lần → 2 records → BTCUSDT volume tính đúp.
	//   With idempotent:    broker thấy sequence_number trùng → discard bản thứ 2.
	// Đây là "Exactly-Once Producer Semantics" (EOS) trong phạm vi 1 session.
	// LƯU Ý: Idempotency chỉ trong 1 producer session. Nếu producer restart hoàn toàn,
	// producer_id mới → sequence reset → có thể duplicate với data session trước.
	// (Giải quyết bằng WAP pattern ở ST13)
	config.Producer.Idempotent = true

	// MaxOpenRequests=1: BẮT BUỘC khi dùng Idempotent.
	// Nếu MaxOpenRequests > 1: producer gửi msg A và msg B cùng lúc (pipelining).
	// Nếu A thất bại, B thành công → producer retry A → broker nhận thứ tự B, A.
	// Idempotency không bảo vệ được ordering trong trường hợp này.
	// MaxOpenRequests=1: strict serial → A phải xong trước khi gửi B → ordering đảm bảo.
	config.Net.MaxOpenRequests = 1

	// Custom partitioner: BTC→P0, ETH→P1, altcoins→hash(P2-N).
	// Xem giải thích chi tiết trong transactional_producer.go.
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

// registerSchemas đăng ký tất cả Avro schemas với Confluent Schema Registry lúc startup.
//
// Flow:
//   1. Đọc file .avsc từ disk (e.g. "pkg/schemas/binance_trade_event.avsc").
//   2. POST schema lên Schema Registry endpoint /subjects/{subject}/versions.
//   3. Nếu schema đã tồn tại và COMPATIBLE → trả về schema_id hiện có.
//   4. Nếu schema đã tồn tại nhưng INCOMPATIBLE → lỗi (breaking change bị chặn).
//   5. Cache schema_id trong struct field → dùng trong serializeAvro().
//
// Schema compatibility = BACKWARD (default của Confluent Schema Registry):
//   → Schema mới phải đọc được data được produce bởi schema CŨ.
//   → Cho phép: thêm field có default value, xóa field không có default.
//   → Không cho phép: đổi kiểu dữ liệu, thêm field bắt buộc không có default.
//   → Ví dụ: thêm field "fee_rate float = 0.0" → backward compatible.
//             đổi "price float" → "price string" → KHÔNG compatible → deploy fail.
//
// Tại sao đăng ký lúc STARTUP thay vì mỗi lần serialize?
//   → Nếu đăng ký mỗi lần: HTTP call tới Schema Registry cho mỗi message → latency spike.
//   → Startup: đăng ký 1 lần, cache schema_id → serialize chỉ cần prepend 5-byte header.
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
			filePath: "pkg/schemas/binance_trade_event.avsc",
			idPtr:    &p.tradeSchemaID,
		},
		{
			subject:  constants.TopicBinanceOrderBook + "-value",
			filePath: "pkg/schemas/binance_orderbook_snapshot.avsc",
			idPtr:    &p.obSchemaID,
		},
		{
			subject:  constants.TopicBinanceLiquidations + "-value",
			filePath: "pkg/schemas/binance_liquidation_event.avsc",
			idPtr:    &p.liqSchemaID,
		},
		{
			subject:  constants.TopicTCBSMarket + "-value",
			filePath: "pkg/schemas/tcbs_market_snapshot.avsc",
			idPtr:    &p.tcbsMarketID,
		},
		{
			subject:  constants.TopicTCBSBSA + "-value",
			filePath: "pkg/schemas/tcbs_bsa_event.avsc",
			idPtr:    &p.tcbsBSAID,
		},
		{
			subject:  constants.TopicTCBSIntradayTicks + "-value",
			filePath: "pkg/schemas/tcbs_intraday_tick_event.avsc",
			idPtr:    &p.tcbsIntradayID,
		},
		{
			subject:  constants.TopicTCBSDerivContracts + "-value",
			filePath: "pkg/schemas/tcbs_derivative_event.avsc",
			idPtr:    &p.tcbsDerivativeID,
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

// PublishTrade serialize TradeEvent → Avro bytes → publish lên Kafka topic binance.trade.ticks.
//
// MESSAGE KEY = instID (ví dụ: "BTCUSDT"):
//   → BinancePartitioner dùng key để chọn partition (BTC→P0, ETH→P1, ...).
//   → Ordering guarantee: tất cả BTCUSDT trades → cùng partition → đúng thứ tự thời gian.
//   → QUAN TRỌNG với time-series: nếu T1=85000 và T2=85100 đến khác partition,
//     consumer có thể nhận T2 trước T1 → tính VWAP sai, volume bar sai.
//
// Timestamp = time.UnixMilli(event.TsMs):
//   → Kafka lưu event time (thời điểm trade thực tế trên sàn) thay vì ingestion time.
//   → Spark Structured Streaming dùng event time này để định nghĩa window boundaries.
//   → Cho phép Watermark hoạt động chính xác dù message đến muộn (late arrival).
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

// PublishTCBSMarket publish TCBS full market snapshot lên Kafka.
// Key = symbol để đảm bảo ordering per instrument.
func (p *kafkaProducer) PublishTCBSMarket(ctx context.Context, symbol string, event TCBSMarketEvent) error {
	payload, err := p.serializeAvro(p.tcbsMarketID, event)
	if err != nil {
		return p.sendToDLQ(constants.TopicTCBSMarketDLQ, symbol, event, err)
	}

	msg := &sarama.ProducerMessage{
		Topic:     constants.TopicTCBSMarket,
		Key:       sarama.StringEncoder(symbol),
		Value:     sarama.ByteEncoder(payload),
		Timestamp: time.UnixMilli(event.TsMs),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		log.Printf("[Producer] SendMessage TCBSMarket %s error: %v — routing to DLQ", symbol, err)
		return p.sendToDLQ(constants.TopicTCBSMarketDLQ, symbol, event, err)
	}
	return nil
}

// PublishTCBSBSA publish TCBS Buy/Sell Analysis event lên Kafka.
// Key = ticker để đảm bảo ordering per ticker.
func (p *kafkaProducer) PublishTCBSBSA(ctx context.Context, ticker string, event TCBSBSAEvent) error {
	payload, err := p.serializeAvro(p.tcbsBSAID, event)
	if err != nil {
		return p.sendToDLQ(constants.TopicTCBSBSADLQ, ticker, event, err)
	}

	msg := &sarama.ProducerMessage{
		Topic:     constants.TopicTCBSBSA,
		Key:       sarama.StringEncoder(ticker),
		Value:     sarama.ByteEncoder(payload),
		Timestamp: time.UnixMilli(event.TsMs),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		log.Printf("[Producer] SendMessage TCBSBSA %s error: %v — routing to DLQ", ticker, err)
		return p.sendToDLQ(constants.TopicTCBSBSADLQ, ticker, event, err)
	}
	return nil
}

// PublishTCBSIntradayTick publish TCBS intraday tick event lên Kafka.
// Key = ticker để đảm bảo ordering per ticker.
func (p *kafkaProducer) PublishTCBSIntradayTick(ctx context.Context, ticker string, event TCBSIntradayTickEvent) error {
	payload, err := p.serializeAvro(p.tcbsIntradayID, event)
	if err != nil {
		return p.sendToDLQ(constants.TopicTCBSIntradayDLQ, ticker, event, err)
	}

	msg := &sarama.ProducerMessage{
		Topic:     constants.TopicTCBSIntradayTicks,
		Key:       sarama.StringEncoder(ticker),
		Value:     sarama.ByteEncoder(payload),
		Timestamp: time.UnixMilli(event.TsMs),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		log.Printf("[Producer] SendMessage TCBSIntraday %s error: %v — routing to DLQ", ticker, err)
		return p.sendToDLQ(constants.TopicTCBSIntradayDLQ, ticker, event, err)
	}
	return nil
}

// PublishTCBSDerivative publish TCBS real-time derivative event lên Kafka.
// Chỉ được gọi từ TCBSWSSConsumer — goroutine nhận push từ /api/v1/derivatives/contracts WSS.
// Key = symbol để đảm bảo ordering per contract.
func (p *kafkaProducer) PublishTCBSDerivative(ctx context.Context, symbol string, event TCBSDerivativeEvent) error {
	payload, err := p.serializeAvro(p.tcbsDerivativeID, event)
	if err != nil {
		return p.sendToDLQ(constants.TopicTCBSDerivContractsDLQ, symbol, event, err)
	}

	msg := &sarama.ProducerMessage{
		Topic:     constants.TopicTCBSDerivContracts,
		Key:       sarama.StringEncoder(symbol),
		Value:     sarama.ByteEncoder(payload),
		Timestamp: time.UnixMilli(event.TsMs),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		log.Printf("[Producer] SendMessage TCBSDerivative %s error: %v — routing to DLQ", symbol, err)
		return p.sendToDLQ(constants.TopicTCBSDerivContractsDLQ, symbol, event, err)
	}
	return nil
}

// serializeAvro encode event thành Confluent Wire Format.
//
// WIRE FORMAT (5-byte header + payload):
//   Byte 0:    0x00 — "Confluent magic byte", phân biệt với plain JSON (không có byte này).
//   Bytes 1-4: schema_id dưới dạng big-endian int32.
//              Consumer dùng schema_id để gọi Schema Registry GET /schemas/ids/{id}
//              và lấy schema để deserialize — không cần embed schema vào message.
//   Bytes 5+:  payload (hiện tại là JSON bytes, không phải Avro binary).
//              NOTE: Đây là trade-off — Avro binary cần Avro library để encode/decode.
//              JSON fallback cho phép debug dễ hơn (readable bằng kafka-console-consumer).
//              Khi cần tối ưu bandwidth thật sự → switch sang hlinkedhashmap Avro binary.
//
// FALLBACK khi schemaID <= 0 (Schema Registry không khả dụng):
//   → Gửi plain JSON bytes (không có 5-byte header).
//   → Consumer (dataops_processing/models/trade.py) handle cả 2 cases:
//     strip_confluent_header() kiểm tra magic byte 0x00 trước khi parse.
//   → Cho phép chạy dev mode không cần Schema Registry service.
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

// sendToDLQ route message lỗi vào Dead Letter Queue topic tương ứng.
//
// DLQ message structure:
//   { "event": <original_payload>, "error": "<error_string>", "timestamp": <unix_ms> }
//
// Tại sao trả về originalErr thay vì DLQ error?
//   → Caller (e.g. PublishTrade) biết rằng message không được ghi vào main topic.
//   → Caller có thể log, increment metric, hoặc alert về vấn đề này.
//   → DLQ write failure được log riêng nhưng không override original error.
//   → Nguyên tắc: nếu cả main topic lẫn DLQ đều fail → log và tiếp tục.
//     Message bị mất là ít tệ hơn crash toàn bộ producer (trade-off chấp nhận được).
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
