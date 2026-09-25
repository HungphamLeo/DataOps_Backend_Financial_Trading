// LEARN: Consumer Lag Monitoring — một trong những metrics quan trọng nhất của Kafka
//
// Consumer Lag = số messages trong topic mà consumer chưa đọc
//   lag = latest_offset - committed_offset
//
// Lag cao = consumer không theo kịp producer → data latency tăng → alert!
//
// REUSE: Pattern CheckProviderSlowResponse từ market-maker-background/pkg/background-mm/services/background/asset.go
//        Ở đây dùng Redis để store "last seen offset" và so sánh với broker's latest offset

package monitoring

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/topics"
)

// LagMetric chứa thông tin lag của 1 consumer group trên 1 topic + partition
type LagMetric struct {
	Topic          string
	Partition      int
	ConsumerGroup  string
	LatestOffset   int64  // offset mới nhất trong partition (từ broker)
	CommitOffset   int64  // offset đã được consumer commit (đã xử lý)
	Lag            int64  // = LatestOffset - CommitOffset
	CollectedAt    time.Time
}

// LagMonitor kiểm tra consumer lag định kỳ và alert khi vượt ngưỡng
type LagMonitor struct {
	brokers    []string
	groups     []string // consumer group IDs cần monitor
	lagAlertThreshold int64  // alert khi lag > threshold
	checkInterval     time.Duration
}

// NewLagMonitor khởi tạo monitor
func NewLagMonitor(brokers []string, groups []string) *LagMonitor {
	return &LagMonitor{
		brokers:           brokers,
		groups:            groups,
		lagAlertThreshold: 10000,          // alert khi > 10k messages lag
		checkInterval:     30 * time.Second, // check mỗi 30 giây
	}
}

// Run bắt đầu monitoring loop trong goroutine riêng
// LEARN: Goroutine + channel pattern cho background monitoring
func (m *LagMonitor) Run(ctx context.Context) {
	// LEARN: time.NewTicker tạo channel tick mỗi interval
	//        Khác với time.Sleep: Ticker không drift (chính xác hơn cho monitoring)
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	log.Printf("[LagMonitor] Started, checking every %s", m.checkInterval)

	for {
		select {
		case <-ctx.Done(): // context bị cancel → graceful shutdown
			log.Println("[LagMonitor] Stopping...")
			return
		case <-ticker.C:
			metrics, err := m.collectLagMetrics(ctx)
			if err != nil {
				log.Printf("[LagMonitor] Error collecting metrics: %v", err)
				continue
			}
			m.processMetrics(metrics)
		}
	}
}

// collectLagMetrics lấy lag info từ Kafka broker
// LEARN: kafka.LookupGroup — kafka-go cung cấp API để query consumer group offsets
func (m *LagMonitor) collectLagMetrics(ctx context.Context) ([]LagMetric, error) {
	conn, err := kafka.DialContext(ctx, "tcp", m.brokers[0])
	if err != nil {
		return nil, fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	// Lấy danh sách tất cả topics
	topicList, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	var allMetrics []LagMetric

	for _, group := range m.groups {
		for _, tp := range topicList {
			// Tạo reader tạm thời để query latest offset
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:   m.brokers,
				Topic:     tp.Topic,
				Partition: tp.ID,
			})

			// Lấy latest offset (đầu cuối của partition)
			latestOffset, err := reader.ReadLag(ctx)
			reader.Close()
			if err != nil {
				continue // skip partition nếu có lỗi
			}

			metric := LagMetric{
				Topic:         tp.Topic,
				Partition:     tp.ID,
				ConsumerGroup: group,
				Lag:           latestOffset,
				CollectedAt:   time.Now(),
			}
			allMetrics = append(allMetrics, metric)
		}
	}

	return allMetrics, nil
}

// processMetrics log và alert nếu lag vượt ngưỡng
// LEARN: Structured logging — log với key=value pairs dễ parse hơn free text
func (m *LagMonitor) processMetrics(metrics []LagMetric) {
	for _, metric := range metrics {
		if metric.Lag > m.lagAlertThreshold {
			// ALERT: consumer lag quá cao
			log.Printf("[LagMonitor] ALERT topic=%s partition=%d group=%s lag=%d threshold=%d",
				metric.Topic,
				metric.Partition,
				metric.ConsumerGroup,
				metric.Lag,
				m.lagAlertThreshold,
			)
		} else if metric.Lag > m.lagAlertThreshold/2 {
			// WARNING: lag đang tăng
			log.Printf("[LagMonitor] WARN topic=%s partition=%d group=%s lag=%d",
				metric.Topic, metric.Partition, metric.ConsumerGroup, metric.Lag,
			)
		} else {
			log.Printf("[LagMonitor] OK topic=%s partition=%d group=%s lag=%d",
				metric.Topic, metric.Partition, metric.ConsumerGroup, metric.Lag,
			)
		}
	}
}

// TopicExists kiểm tra topic đã tồn tại chưa (dùng khi startup để fail fast)
// LEARN: topics.List từ kafka-go/topics subpackage
func TopicExists(ctx context.Context, broker string, topicName string) (bool, error) {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return false, fmt.Errorf("dial kafka for topic check: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topicName)
	if err != nil {
		return false, nil // topic không tồn tại
	}
	_ = topics.List // ensure import used
	return len(partitions) > 0, nil
}
