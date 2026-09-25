package fabric

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// DataStage định nghĩa các giai đoạn chuẩn trong Data Fabric Pipeline
type DataStage string

const (
	StageSourceIngestion DataStage = "STAGE_1_INGESTION"     // Nhận từ Sàn (OKX WS / VPS Poller)
	StageKafkaBuffer     DataStage = "STAGE_2_KAFKA_BUFFER"   // Đã ghi vào Kafka KRaft Topic
	StageStreamProcess   DataStage = "STAGE_3_STREAM_PROCESS" // PySpark / Stream computation (VWAP, Z-Score)
	StageLakehouseBronze DataStage = "STAGE_4_DELTA_BRONZE"   // Ghi Delta Lake Bronze
	StageLakehouseSilver DataStage = "STAGE_5_DELTA_SILVER"   // Ghi Delta Lake Silver
	StageServingAPI      DataStage = "STAGE_6_SERVING_API"    // Phục vụ Client qua REST / WebSocket
)

// LineageEvent lưu vết metadata cho từng record hoặc batch dữ liệu
type LineageEvent struct {
	TraceID     string            `json:"trace_id"`     // Duy nhất cho mỗi tick/batch
	EventTime   time.Time         `json:"event_time"`   // Thời gian thực tế của dữ liệu
	Stage       DataStage         `json:"stage"`        // Stage hiện tại
	ServiceName string            `json:"service_name"` // Service đang xử lý
	Source      string            `json:"source"`       // OKX / VPS / MinIO
	Target      string            `json:"target"`       // Kafka topic / Delta table / InfluxDB
	RecordCount int64             `json:"record_count"` // Số lượng bản ghi
	Status      string            `json:"status"`       // SUCCESS / FAILED / RETRY
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// LineageTracker Interface cho phép theo dõi và debug từng Stage
type LineageTracker interface {
	Track(ctx context.Context, event LineageEvent)
}

type zapLineageTracker struct {
	logger *zap.Logger
}

func NewZapLineageTracker(logger *zap.Logger) LineageTracker {
	return &zapLineageTracker{logger: logger.Named("data_fabric_lineage")}
}

func (t *zapLineageTracker) Track(ctx context.Context, event LineageEvent) {
	metaJSON, _ := json.Marshal(event.Metadata)
	t.logger.Info(fmt.Sprintf("[DATA-FABRIC-LINEAGE] [%s] [%s -> %s]", event.Stage, event.Source, event.Target),
		zap.String("trace_id", event.TraceID),
		zap.String("stage", string(event.Stage)),
		zap.String("service", event.ServiceName),
		zap.String("source", event.Source),
		zap.String("target", event.Target),
		zap.Int64("record_count", event.RecordCount),
		zap.String("status", event.Status),
		zap.String("metadata", string(metaJSON)),
		zap.Time("event_time", event.EventTime),
	)
}
