# DATA SYSTEMS METADATA — DataOps Backend Financial Trading

> **Mục đích:** Tài liệu tham chiếu toàn diện mô tả tất cả hệ thống lưu trữ dữ liệu trong platform:
> schema từng bảng/table, kiểu dữ liệu, business glossary, data class phân loại, nguồn gốc và retention.
>
> **Dùng cho:** Onboarding thành viên mới, tái sử dụng trong project khác, thiết kế data contract,
> data lineage tracing, và audit compliance.

---

## Table of Contents

1. [Tổng quan hệ thống lưu trữ](#1-tổng-quan-hệ-thống-lưu-trữ)
2. [Business Glossary](#2-business-glossary)
3. [Data Classification (Data Classes)](#3-data-classification-data-classes)
4. [MySQL 8.0 — Relational Database](#4-mysql-80--relational-database)
5. [Redis 7 — In-Memory Cache](#5-redis-7--in-memory-cache)
6. [InfluxDB v2 — Time-Series Database](#6-influxdb-v2--time-series-database)
7. [Delta Lake — Lakehouse (MinIO S3A)](#7-delta-lake--lakehouse-minio-s3a)
8. [Kafka Topics — Message Bus](#8-kafka-topics--message-bus)
9. [Protobuf / gRPC Schema](#9-protobuf--grpc-schema)
10. [Pydantic Validation Models (Ingestion Contracts)](#10-pydantic-validation-models-ingestion-contracts)
11. [Data Lineage Map](#11-data-lineage-map)
12. [Canonical Enums Cross-Reference](#12-canonical-enums-cross-reference)

---

## 1. Tổng quan hệ thống lưu trữ

| Storage System | Technology | Port | Vai trò chính | Retention |
|---|---|---|---|---|
| **MySQL** | MySQL 8.0 + GORM | 3306 | User auth, accounting ledger, sessions, indicators config | Vô thời hạn (soft-delete) |
| **Redis** | Redis 7 | 6379 | Real-time ticker/orderbook cache, session cache | TTL 10s – 120s |
| **InfluxDB** | InfluxDB v2 | 8086 | Time-series metrics: VWAP, Order Flow, funding rates, liquidations | 90 ngày mặc định |
| **Delta Lake** | delta-spark 3.1 + MinIO | 9010 | Batch historical OHLCV, market EOD, BSA, Silver indicators, Gold market breadth | Vô thời hạn (ACID) |
| **Kafka** | KRaft 3.7 | 9092 | Event streaming bus: trade ticks, market snapshots, derivatives | 7 ngày (configurable) |

---

## 2. Business Glossary

Danh sách thuật ngữ kinh doanh và kỹ thuật xuất hiện trong hệ thống. Dùng khi viết code, đặt tên biến, field, hoặc column.

### 2.1 Thuật ngữ thị trường chứng khoán VN (TCBS)

| Thuật ngữ | Viết tắt | Định nghĩa | Nguồn |
|---|---|---|---|
| **Giá tham chiếu** | `ref_price` | Giá đóng cửa phiên hôm trước, dùng làm mức tham chiếu tính giá trần/sàn | TCBS iFlash |
| **Giá trần** | `ceil_price` | Giá cao nhất được phép giao dịch trong phiên = `ref_price × 1.07` (HOSE) | TCBS iFlash |
| **Giá sàn** | `floor_price` | Giá thấp nhất được phép = `ref_price × 0.93` (HOSE), `× 0.90` (HNX) | TCBS iFlash |
| **Giá khớp lệnh** | `match_price` | Giá giao dịch cuối cùng được khớp trong phiên | TCBS iFlash |
| **Khối lượng khớp** | `match_qty` | Số cổ phiếu đã khớp lệnh | TCBS iFlash |
| **Room nước ngoài** | `room` | % hoặc số lượng cổ phiếu còn lại nước ngoài được phép mua | TCBS iFlash |
| **Khối lượng nước ngoài mua** | `buy_foreign_qty` | Số cổ phiếu nước ngoài đã mua trong phiên | TCBS iFlash |
| **Khối lượng nước ngoài bán** | `sell_foreign_qty` | Số cổ phiếu nước ngoài đã bán trong phiên | TCBS iFlash |
| **Tổng khối lượng** | `total_vol` | Tổng số cổ phiếu giao dịch trong phiên | TCBS iFlash |
| **Tổng giá trị** | `total_val` | Tổng giá trị giao dịch (VND) | TCBS iFlash |
| **Lệnh ATO** | — | At The Open — lệnh khớp tại giá mở cửa | VN Exchange |
| **Lệnh ATC** | — | At The Close — lệnh khớp tại giá đóng cửa | VN Exchange |
| **Lệnh LO** | — | Limit Order | VN Exchange |
| **Lệnh MP** | — | Market Price order | VN Exchange |
| **NB / NS** | `exec_type` | Normal Buy / Normal Sell — hướng lệnh cổ phiếu cơ sở | TCBS |
| **Buy Up** | `action=Buy Up` | Lệnh mua chủ động (taker buy) — giá khớp tăng so với tham chiếu | TCBS Nyx |
| **Sell Down** | `action=Sell Down` | Lệnh bán chủ động (taker sell) — giá khớp giảm | TCBS Nyx |
| **BSA** | — | Buy-Sell Analysis — phân tích cán cân cung/cầu theo loại nhà đầu tư | TCBS |
| **BUP** | `bup` | Buy Up Percent — % giao dịch mua chủ động trong tổng giao dịch | TCBS BSA |
| **SDP** | `sdp` | Sell Down Percent — % giao dịch bán chủ động | TCBS BSA |
| **BSR** | `bsr` | Buy/Sell Ratio = BUP / SDP; >1 = áp lực mua > bán | TCBS BSA |
| **Sheep** | `investor_type=sheep` | Nhà đầu tư nhỏ lẻ cá nhân (<200M VND/lệnh) | TCBS |
| **Wolf** | `investor_type=wolf` | Nhà đầu tư tổ chức vừa (200M-1B VND/lệnh) | TCBS |
| **Shark** | `investor_type=shark` | Nhà đầu tư tổ chức lớn (>1B VND/lệnh) | TCBS |
| **VN30F** | — | Hợp đồng tương lai chỉ số VN30 (phái sinh) — ký hiệu: VN30F{YYMM} | TCBS Derivatives |
| **Open Interest** | `open_vol` | Số hợp đồng phái sinh đang mở (chưa tất toán) | TCBS Derivatives |

### 2.2 Thuật ngữ thị trường Crypto (Binance)

| Thuật ngữ | Viết tắt | Định nghĩa | Nguồn |
|---|---|---|---|
| **Trade Tick** | — | 1 giao dịch khớp lệnh thực tế trên sàn | Binance WS @trade |
| **Taker Buy** | `is_buyer_maker=false` | Người mua chủ động tấn công orderbook (giá tăng) | Binance |
| **Taker Sell** | `is_buyer_maker=true` | Người bán chủ động tấn công orderbook (giá giảm) | Binance |
| **Funding Rate** | `funding_rate` | Phí duy trì vị thế perpetual futures; >0 = Long trả Short | Binance fstream |
| **Liquidation** | — | Vị thế bị sàn bắt buộc đóng khi margin < maintenance level | Binance fstream |
| **OHLCV** | — | Open, High, Low, Close, Volume — nến giá chuẩn | Binance klines |
| **Quote Volume** | `vol_ccy`, `quote_volume` | Khối lượng tính theo quote asset (USDT) — `row[7]` từ Binance klines | Binance |
| **Base Volume** | `volume` | Khối lượng tính theo base asset (BTC, ETH) — `row[5]` | Binance |
| **Depth5** | `@depth5` | OrderBook top 5 bid/ask levels, cập nhật mỗi 1000ms | Binance WS |
| **VWAP** | — | Volume Weighted Average Price = Σ(price×vol)/Σ(vol) trên 1 window | Computed |
| **CVD** | — | Cumulative Volume Delta = Σ(buy_vol - sell_vol) lũy kế | Computed |
| **Perpetual / PERP** | — | Hợp đồng tương lai không có ngày đáo hạn | Binance |

### 2.3 Thuật ngữ kỹ thuật platform

| Thuật ngữ | Viết tắt | Định nghĩa |
|---|---|---|
| **CDM** | — | Canonical Data Model — schema chuẩn hóa dùng chung cho tất cả module |
| **Bronze Layer** | — | Delta Lake tầng thô — raw data từ nguồn, minimal transform |
| **Silver Layer** | — | Delta Lake tầng làm sạch — deduplicated, validated, CDM-conform |
| **Gold Layer** | — | Delta Lake tầng tổng hợp — aggregated metrics, feature store |
| **TTL** | — | Time To Live — thời gian tồn tại của key trong Redis |
| **Watermark** | — | Spark Streaming: ngưỡng thời gian tối đa chấp nhận late-arriving data |
| **foreachBatch** | — | Pattern ghi InfluxDB từ Spark: convert micro-batch sang pandas → write |
| **DLQ** | — | Dead Letter Queue — Kafka topic chứa messages không xử lý được |
| **gRPC** | — | Google Remote Procedure Call — giao thức IPC giữa backend_api và batch_api |
| **inst_type** | — | Instrument Type: SPOT / FUTURES / DERIVATIVE / PERPETUAL |
| **exchange** | — | Tên sàn giao dịch: binance / tcbs_hose / tcbs_hnx / tcbs_upcom / tcbs_deriv |
| **KRaft** | — | Kafka Raft — Kafka cluster không cần ZooKeeper (Kafka 2.8+) |
| **Flux** | — | Ngôn ngữ query của InfluxDB v2 (thay thế InfluxQL) |
| **SNR** | — | Rolling NAV — tỷ số lợi nhuận tích lũy danh mục |

---

## 3. Data Classification (Data Classes)

Phân loại dữ liệu theo tính chất và cách xử lý:

| Data Class | Mô tả | Ví dụ | Storage | Latency |
|---|---|---|---|---|
| **Real-time Snapshot** | Snapshot tức thời, overwrite liên tục | Binance ticker, TCBS market snapshot | Redis | < 5ms |
| **Event Stream** | Chuỗi sự kiện bất biến, sorted by time | Trade ticks, order book updates | Kafka | < 50ms |
| **Time-Series Metric** | Điểm đo lường theo thời gian, append-only | VWAP, Order Flow CVD | InfluxDB | ~20ms |
| **Historical OHLCV** | Nến giá lịch sử, idempotent batch | Binance candles, TCBS EOD | Delta Bronze | ~50ms |
| **Derived Analytics** | Tính toán từ raw data | Volume Bars, Indicator Matrix | Delta Silver | ~100ms |
| **Aggregate Report** | Tổng hợp theo ngày/nhóm | Market Breadth, P&L summary | Delta Gold / MySQL | ~200ms |
| **Transactional** | Account state, không qua Kafka/Delta | TCBS cash balance, orders | Redis + MySQL | ~200ms |
| **Configuration** | Metadata hệ thống, ít thay đổi | Indicator groups, user roles | MySQL | — |
| **Audit Log** | Immutable history, compliance | Execution log, account transactions | MySQL | — |

---

## 4. MySQL 8.0 — Relational Database

**Host**: `localhost:3306`  
**Default database**: `dataops`  
**ORM**: GORM v1.25 (Go)  
**Auth**: username/password from `DATABASE_DSN` env  

> **Lưu ý**: Tất cả bảng đều có `gorm.Model` embed → tự động có `id (uint PK)`, `created_at`, `updated_at`, `deleted_at` (soft-delete).

---

### 4.1 Bảng `users`

**Mục đích**: Lưu tài khoản người dùng hệ thống — authentication & authorization.  
**Data class**: Configuration / Security  
**Go model**: `models.User`

| Column | Type | Constraints | Mô tả |
|---|---|---|---|
| `id` | `BIGINT UNSIGNED` | PK, AUTO_INCREMENT | User ID duy nhất |
| `created_at` | `DATETIME(3)` | NOT NULL | Thời điểm tạo |
| `updated_at` | `DATETIME(3)` | NOT NULL | Thời điểm cập nhật gần nhất |
| `deleted_at` | `DATETIME(3)` | NULLABLE, INDEX | Soft delete timestamp |
| `username` | `VARCHAR(255)` | NOT NULL, UNIQUE | Tên đăng nhập |
| `password` | `VARCHAR(255)` | NOT NULL | Password hash (bcrypt) |
| `email` | `VARCHAR(255)` | NOT NULL, UNIQUE | Email người dùng |
| `role_id` | `TINYINT` | DEFAULT 1 | Quyền hạn — xem enum `AUTH_ROLE` |

**Enum `AUTH_ROLE`**: `1=USER`, `2=ADMIN`, `3=ANALYST`

---

### 4.2 Bảng `user_sessions`

**Mục đích**: Quản lý JWT session đang active (cũng được cache trên Redis).  
**Data class**: Transactional / Security  
**Go model**: `models.UserSession`

| Column | Type | Constraints | Mô tả |
|---|---|---|---|
| `id` | `BIGINT UNSIGNED` | PK | Session ID |
| `created_at` | `DATETIME(3)` | NOT NULL | Thời điểm login |
| `updated_at` | `DATETIME(3)` | NOT NULL | — |
| `deleted_at` | `DATETIME(3)` | NULLABLE | Thời điểm logout (soft delete) |
| `session` | `VARCHAR(500)` | — | JWT access token |
| `user_id` | `VARCHAR(100)` | — | User ID (string form) |
| `user_id` | `BIGINT UNSIGNED` | — | User ID (FK → users.id) |
| `username` | `VARCHAR(255)` | — | Username snapshot |
| `role_id` | `TINYINT` | — | Role snapshot tại thời điểm login |
| `auth_role` | `TINYINT` | — | Role enum value |
| `status` | `VARCHAR(20)` | — | `"active"` \| `"expired"` \| `"revoked"` |
| `active` | `TINYINT(1)` | DEFAULT 1 | 1=active, 0=inactive |

---

### 4.3 Bảng `account_transactions`

**Mục đích**: Ledger ghi lại toàn bộ giao dịch tài chính — audit trail bất biến.  
**Data class**: Audit Log / Financial  
**Go model**: `models.AccountTransaction`  
**Pattern**: Event Sourcing — không UPDATE, chỉ INSERT

| Column | Type | Constraints | Mô tả |
|---|---|---|---|
| `id` | `BIGINT UNSIGNED` | PK | Transaction ID |
| `created_at` | `DATETIME(3)` | NOT NULL | Thời điểm ghi nhận |
| `updated_at` | `DATETIME(3)` | NOT NULL | — |
| `deleted_at` | `DATETIME(3)` | NULLABLE | Soft delete |
| `user_id` | `BIGINT UNSIGNED` | NOT NULL, INDEX | FK → users.id |
| `type` | `VARCHAR(20)` | NOT NULL | Loại giao dịch — xem enum bên dưới |
| `symbol` | `VARCHAR(20)` | — | Mã instrument, e.g. `"BTCUSDT"`, `"VCB"` |
| `exchange` | `VARCHAR(20)` | — | Sàn: `"binance"` \| `"tcbs"` |
| `amount` | `DOUBLE` | NOT NULL | Số tiền: dương = credit, âm = debit |
| `balance_before` | `DOUBLE` | NOT NULL | Số dư trước giao dịch |
| `balance_after` | `DOUBLE` | NOT NULL | Số dư sau giao dịch |
| `currency` | `VARCHAR(10)` | DEFAULT `'USDT'` | Đơn vị tiền tệ |
| `ref_id` | `VARCHAR(100)` | INDEX | Binance trade ID hoặc TCBS order no |
| `group_job_uuid` | `VARCHAR(36)` | INDEX | UUID liên kết batch job run |
| `note` | `TEXT` | — | Ghi chú tự do |
| `executed_at` | `DATETIME(3)` | NOT NULL | Thời điểm thực tế thực hiện |

**Enum `TransactionType`**:

| Giá trị | Mô tả |
|---|---|
| `deposit` | Nạp tiền vào tài khoản |
| `withdraw` | Rút tiền |
| `trade_fee` | Phí giao dịch |
| `profit` | Lợi nhuận từ giao dịch |
| `loss` | Lỗ từ giao dịch |
| `funding_fee` | Funding fee perpetual futures |
| `adjust` | Điều chỉnh số dư (admin) |

---

### 4.4 Bảng `daily_pn_ls`

**Mục đích**: Pre-aggregated P&L theo ngày — tránh join nhiều bảng khi query dashboard.  
**Data class**: Aggregate Report  
**Go model**: `models.DailyPnL`  
**Pattern**: Materialized View

| Column | Type | Constraints | Mô tả |
|---|---|---|---|
| `id` | `BIGINT UNSIGNED` | PK | — |
| `user_id` | `BIGINT UNSIGNED` | UNIQUE INDEX (user_id, date, exchange) | FK → users.id |
| `date` | `DATE` | UNIQUE INDEX | Ngày giao dịch |
| `exchange` | `VARCHAR(20)` | UNIQUE INDEX | Sàn: `"binance"` \| `"tcbs"` |
| `total_profit` | `DOUBLE` | — | Tổng lợi nhuận ngày |
| `total_loss` | `DOUBLE` | — | Tổng lỗ ngày |
| `trade_fee` | `DOUBLE` | — | Tổng phí giao dịch |
| `funding_fee` | `DOUBLE` | — | Tổng funding fee |
| `net_pnl` | `DOUBLE` | — | Net P&L = profit + loss - fee - funding |
| `trade_count` | `INT` | — | Số giao dịch trong ngày |

---

### 4.5 Bảng `account_balances`

**Mục đích**: Snapshot số dư hiện tại theo exchange + currency — tránh replay toàn bộ transaction history.  
**Data class**: Transactional Snapshot  
**Go model**: `models.AccountBalance`

| Column | Type | Constraints | Mô tả |
|---|---|---|---|
| `id` | `BIGINT UNSIGNED` | PK | — |
| `user_id` | `BIGINT UNSIGNED` | UNIQUE INDEX (user_id, exchange, currency) | FK → users.id |
| `exchange` | `VARCHAR(20)` | UNIQUE INDEX | `"binance"` \| `"tcbs"` |
| `currency` | `VARCHAR(10)` | UNIQUE INDEX | `"USDT"` \| `"VND"` |
| `available` | `DOUBLE` | — | Số dư khả dụng (có thể dùng) |
| `locked` | `DOUBLE` | — | Số dư đang bị khóa (lệnh đang mở) |
| `total` | `DOUBLE` | — | Tổng = available + locked |
| `updated_at` | `DATETIME(3)` | — | Thời điểm cập nhật snapshot gần nhất |

---

### 4.6 Bảng `execution_logs`

**Mục đích**: Audit log mọi action hệ thống — debugging, compliance.  
**Data class**: Audit Log  
**Go model**: `models.ExecutionLog`

| Column | Type | Constraints | Mô tả |
|---|---|---|---|
| `id` | `BIGINT UNSIGNED` | PK | — |
| `service` | `VARCHAR(50)` | — | Tên service: `"dataops-stream-producer"`, `"dataops-backend-api"` |
| `level` | `VARCHAR(10)` | — | `"INFO"` \| `"WARN"` \| `"ERROR"` |
| `action` | `VARCHAR(100)` | — | Hành động: `"BINANCE_CONNECT"`, `"TCBS_SESSION_REFRESH"` |
| `message` | `TEXT` | — | Nội dung log |
| `user_id` | `BIGINT UNSIGNED` | NULLABLE, INDEX | Nullable — system actions không có user |
| `stack_trace` | `TEXT` | — | Stack trace khi có lỗi |

---

### 4.7 Bảng `indicator_groups`

**Mục đích**: Container cho chiến lược indicator — user tự định nghĩa bộ indicator.  
**Data class**: Configuration  
**Go model**: `models.IndicatorGroup`

| Column | Type | Constraints | Mô tả |
|---|---|---|---|
| `id` | `BIGINT UNSIGNED` | PK | — |
| `short_name` | `VARCHAR(50)` | UNIQUE INDEX | Key ngắn: `"vwap-rsi"`, `"tcbs-bsa"` |
| `name` | `VARCHAR(100)` | NOT NULL | Tên đầy đủ |
| `type` | `TINYINT` | DEFAULT 0 | `0=crypto`, `1=vnstock` |
| `created_by` | `BIGINT UNSIGNED` | INDEX | FK → users.id |

---

### 4.8 Bảng `indicators`

**Mục đích**: Technical indicator trong 1 group — MA, RSI, VWAP, ATR, BB.  
**Data class**: Configuration  
**Go model**: `models.Indicator`

| Column | Type | Constraints | Mô tả |
|---|---|---|---|
| `id` | `BIGINT UNSIGNED` | PK | — |
| `group_id` | `BIGINT UNSIGNED` | NOT NULL, INDEX | FK → indicator_groups.id |
| `short_name` | `VARCHAR(50)` | — | `"MA"`, `"RSI"`, `"VWAP"`, `"ATR"`, `"BB"` |
| `name` | `VARCHAR(100)` | — | Tên đầy đủ |
| `formula` | `TEXT` | — | Công thức hoặc mô tả toán học |
| `is_active` | `TINYINT(1)` | DEFAULT 1 | Bật/tắt |

---

### 4.9 Bảng `indicator_parameters`

**Mục đích**: Tham số của indicator — period, source, multiplier.  
**Data class**: Configuration  
**Go model**: `models.IndicatorParameter`

| Column | Type | Constraints | Mô tả |
|---|---|---|---|
| `id` | `BIGINT UNSIGNED` | PK | — |
| `group_id` | `BIGINT UNSIGNED` | NOT NULL, INDEX | FK → indicator_groups.id |
| `indicator_id` | `BIGINT UNSIGNED` | NULLABLE, INDEX | FK → indicators.id |
| `name` | `VARCHAR(100)` | — | `"period"`, `"source"`, `"multiplier"` |
| `default_value` | `VARCHAR(255)` | — | Giá trị mặc định (string) |
| `current_value` | `VARCHAR(255)` | — | Giá trị đang áp dụng |
| `data_type` | `VARCHAR(20)` | — | `"int"` \| `"float"` \| `"string"` \| `"bool"` |
| `min_value` | `DOUBLE` | DEFAULT 0 | Giá trị nhỏ nhất cho phép |
| `max_value` | `DOUBLE` | DEFAULT 0 | Giá trị lớn nhất (0 = unlimited) |
| `description` | `TEXT` | — | Mô tả tham số |

---

### 4.10 Bảng `indicator_signals`

**Mục đích**: Kết quả tính toán indicator tại 1 thời điểm.  
**Data class**: Derived Analytics  
**Go model**: `models.IndicatorSignal`

| Column | Type | Constraints | Mô tả |
|---|---|---|---|
| `id` | `BIGINT UNSIGNED` | PK | — |
| `group_id` | `BIGINT UNSIGNED` | INDEX | FK → indicator_groups.id |
| `symbol` | `VARCHAR(20)` | INDEX | Mã instrument |
| `exchange` | `VARCHAR(20)` | — | Sàn |
| `signal_type` | `VARCHAR(50)` | — | `"BUY"` \| `"SELL"` \| `"HOLD"` \| `"NEUTRAL"` |
| `confidence` | `DOUBLE` | — | Độ tin cậy tín hiệu (0.0 – 1.0) |
| `value` | `DOUBLE` | — | Giá trị indicator (e.g. RSI=72.5) |
| `timeframe` | `VARCHAR(10)` | — | `"1m"`, `"5m"`, `"1h"`, `"1d"` |

---

## 5. Redis 7 — In-Memory Cache

**Host**: `localhost:6379`  
**Client**: `go-redis/v8` (stream-producer), `go-redis/v9` (backend-api)  
**Không có schema cứng** — mỗi key là JSON string.

| Redis Key Pattern | TTL | Kiểu dữ liệu | Mô tả | Nguồn ghi |
|---|---|---|---|---|
| `binance_{symbol}_ticker` | 10s | JSON string | Trade tick snapshot: `{instId, price, qty, side, tradeId, ts}` | stream-producer |
| `binance_{symbol}_depth5` | 10s | JSON string | OrderBook top 5: `{instId, bids:[[p,q,_,n]×5], asks:..., ts}` | stream-producer |
| `tcbs_{symbol}_ticker` | 120s | JSON string | Market snapshot: `{symbol, match_price, bid1..3, ask1..3, total_vol, ref_price, ts}` | stream-producer |
| `tcbs_account_balance_{account_id}` | 30s | JSON string | Cash balance từ TCBS proxy | backend-api |
| `tcbs_account_assets_{account_id}` | 30s | JSON string | Stock holdings từ TCBS proxy | backend-api |
| `tcbs_deriv_positions_{account_id}` | 10s | JSON string | Open positions phái sinh | backend-api |
| `session_{token}` | 24h | JSON string | User session cache | backend-api |

### Redis JSON Schema: `binance_{symbol}_ticker`

```json
{
  "instId":   "BTCUSDT",
  "tradeId":  "123456789",
  "px":       "97500.50",
  "sz":       "0.001",
  "side":     "buy",
  "ts":       "1729123456789"
}
```

### Redis JSON Schema: `tcbs_{symbol}_ticker`

```json
{
  "symbol":       "VCB",
  "match_price":  89500,
  "bid_price_01": 89400,
  "bid_qty_01":   1200,
  "offer_price_01": 89600,
  "offer_qty_01":   800,
  "total_vol":    2450000,
  "ref_price":    89000,
  "change_pct":   0.56,
  "ts":           1729123456789
}
```

---

## 6. InfluxDB v2 — Time-Series Database

**Host**: `http://localhost:8086`  
**Org**: `binance-vps-platform`  
**Bucket**: `market-data`  
**Query language**: Flux  
**Client**: influxdb-client-go v2 (backend), influxdb-client v1.44 (processing)

Dữ liệu được ghi từ Spark Structured Streaming jobs qua `foreachBatch` pattern.

---

### 6.1 Measurement `vwap`

**Mục đích**: VWAP 5 phút từ Binance trade ticks — kết quả của Spark VWAP job.  
**Data class**: Time-Series Metric (Derived Analytics)  
**Nguồn**: `dataops_processing/jobs/vwap_job.py`  
**Retention**: 90 ngày (configurable)

| Field / Tag | Kiểu | Là Tag/Field | Mô tả |
|---|---|---|---|
| `symbol` | `string` | **TAG** | Mã instrument: `"BTCUSDT"`, `"ETHUSDT"` |
| `interval` | `string` | **TAG** | Timeframe: luôn `"5m"` |
| `vwap` | `float64` | field | Giá VWAP của window: Σ(price×vol)/Σ(vol) |
| `volume` | `float64` | field | Tổng khối lượng base asset trong window |
| `num_trades` | `int64` | field | Số lượng trade ticks trong window |
| `_time` | `timestamp` | — | `window.end` (UTC nanoseconds) |

**Flux query mẫu**:
```flux
from(bucket: "market-data")
  |> range(start: -6h)
  |> filter(fn: (r) => r._measurement == "vwap" and r.symbol == "BTCUSDT" and r.interval == "5m")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
```

---

### 6.2 Measurement `order_flow`

**Mục đích**: Buy/Sell volume phân tích theo window 1 phút — cơ sở tính CVD.  
**Data class**: Time-Series Metric  
**Nguồn**: `dataops_processing/jobs/order_flow_job.py`

| Field / Tag | Kiểu | Là Tag/Field | Mô tả |
|---|---|---|---|
| `symbol` | `string` | **TAG** | Mã instrument |
| `interval` | `string` | **TAG** | `"1m"` |
| `buy_volume` | `float64` | field | Tổng volume mua chủ động trong window |
| `sell_volume` | `float64` | field | Tổng volume bán chủ động trong window |
| `delta` | `float64` | field | buy_volume - sell_volume (window-level CVD component) |
| `imbalance` | `float64` | field | delta/(buy+sell), range [-1, 1] |
| `num_trades` | `int64` | field | Số trade ticks phân loại được trong window |
| `_time` | `timestamp` | — | `window.end` (UTC nanoseconds) |

**Flux query CVD tích lũy**:
```flux
from(bucket: "market-data")
  |> range(start: -4h)
  |> filter(fn: (r) => r._measurement == "order_flow" and r.symbol == "BTCUSDT" and r._field == "delta")
  |> cumulativeSum()
```

---

### 6.3 Measurement `ohlcv`

**Mục đích**: OHLCV intraday (1m, 5m, 15m, 1h) từ stream processing.  
**Data class**: Time-Series Metric

| Field / Tag | Kiểu | Là Tag/Field | Mô tả |
|---|---|---|---|
| `symbol` | `string` | TAG | Mã instrument |
| `interval` | `string` | TAG | `"1m"`, `"5m"`, `"1h"` |
| `open` | `float64` | field | Giá mở cửa |
| `high` | `float64` | field | Giá cao nhất |
| `low` | `float64` | field | Giá thấp nhất |
| `close` | `float64` | field | Giá đóng cửa |
| `volume` | `float64` | field | Khối lượng base asset |
| `_time` | `timestamp` | — | UTC nanoseconds |

---

### 6.4 Measurement `funding_rate`

**Mục đích**: Funding rate perpetual futures Binance.  
**Data class**: Event Stream (Market Data)

| Field / Tag | Kiểu | Là Tag/Field | Mô tả |
|---|---|---|---|
| `symbol` | `string` | TAG | Contract: `"BTCUSDT"`, `"ETHUSDT"` |
| `funding_rate` | `float64` | field | Tỷ lệ funding (thường ±0.01%) |
| `next_funding_rate` | `float64` | field | Dự kiến funding kỳ tiếp theo |
| `_time` | `timestamp` | — | Funding settlement time |

---

### 6.5 Measurement `liquidations`

**Mục đích**: Sự kiện thanh lý vị thế bắt buộc futures.  
**Data class**: Event Stream (Market Data)

| Field / Tag | Kiểu | Là Tag/Field | Mô tả |
|---|---|---|---|
| `symbol` | `string` | TAG | Contract symbol |
| `side` | `string` | TAG | `"buy"` (short bị liquidate) \| `"sell"` (long bị liquidate) |
| `price` | `float64` | field | Giá liquidation |
| `size` | `float64` | field | Khối lượng bị liquidate |
| `_time` | `timestamp` | — | UTC nanoseconds |

---

## 7. Delta Lake — Lakehouse (MinIO S3A)

**Object Store**: MinIO, bucket: `binance-datalake`  
**Base path**: `s3a://binance-datalake/`  
**Engine**: PySpark 3.5.1 + delta-spark 3.1.0  
**Format**: Delta (ACID, schema enforcement, time travel)  

Cấu trúc thư mục:
```
s3a://binance-datalake/
├── delta/
│   ├── bronze/          ← raw ingested data (batch_api)
│   │   ├── binance_candles_1d/
│   │   ├── binance_candles_4h/
│   │   ├── tcbs_market_snapshot/
│   │   └── tcbs_market_eod/
│   ├── silver/          ← cleaned, enriched (processing)
│   │   ├── vwap_1m/
│   │   ├── volume_bars/
│   │   └── indicator_matrix/
│   └── gold/            ← aggregated (processing)
│       └── market_breadth/
```

---

### 7.1 Delta Table: `bronze/binance_candles_1d` và `bronze/binance_candles_4h`

**Mục đích**: OHLCV lịch sử từ Binance REST `/api/v3/klines`.  
**Data class**: Historical OHLCV (Bronze)  
**Nguồn**: `dataops_batch_api/cli/jobs/binance_1d_job.py` và `binance_4h_job.py`  
**Partitioned by**: `symbol`, `inst_type`  
**Idempotency**: Ghi đè theo partition

| Column | Type | Mô tả |
|---|---|---|
| `symbol` | `string` | Mã instrument: `"BTCUSDT"`, `"ETHUSDT"` |
| `inst_type` | `string` | `"SPOT"` \| `"FUTURES"` |
| `interval` | `string` | `"1d"` hoặc `"4h"` |
| `ts_open_ms` | `long` | Unix milliseconds — thời điểm mở cửa nến |
| `ts_close_ms` | `long` | Unix milliseconds — thời điểm đóng cửa nến |
| `open` | `double` | Giá mở cửa |
| `high` | `double` | Giá cao nhất |
| `low` | `double` | Giá thấp nhất |
| `close` | `double` | Giá đóng cửa |
| `volume` | `double` | Khối lượng base asset (BTC, ETH, SOL) |
| `quote_volume` | `double` | Khối lượng quote asset (USDT) — Binance `row[7]` |
| `num_trades` | `integer` | Số giao dịch trong nến |
| `confirm` | `integer` | `1` = nến đã đóng (hardcoded cho Binance) |
| `ingested_at` | `string` | ISO 8601 UTC — thời điểm ghi vào Bronze |

**Validation rules** (Pydantic `BinanceCandleRecord`):
- `open`, `high`, `low`, `close` > 0
- `high >= low`
- `volume >= 0`
- `confirm ∈ {0, 1}`

---

### 7.2 Delta Table: `bronze/tcbs_market_snapshot`

**Mục đích**: Market snapshot toàn phiên từ TCBS iFlash API — intraday realtime poll.  
**Data class**: Historical OHLCV (Bronze)  
**Nguồn**: `dataops_batch_api/cli/jobs/tcbs_eod_job.py`  
**Partitioned by**: `symbol`, `exchange`

| Column | Type | Mô tả |
|---|---|---|
| `symbol` | `string` | Mã cổ phiếu: `"VCB"`, `"HPG"` |
| `exchange` | `string` | `"HOSE"` \| `"HNX"` \| `"UPCOM"` |
| `ref_price` | `double` | Giá tham chiếu |
| `ceil_price` | `double` | Giá trần |
| `floor_price` | `double` | Giá sàn |
| `match_price` | `double` | Giá khớp lệnh |
| `match_qty` | `double` | Khối lượng khớp |
| `bid_price_01..03` | `double` | Giá chào mua 3 cấp |
| `bid_qty_01..03` | `double` | Khối lượng chào mua 3 cấp |
| `offer_price_01..03` | `double` | Giá chào bán 3 cấp |
| `offer_qty_01..03` | `double` | Khối lượng chào bán 3 cấp |
| `open` | `double` | Giá mở cửa phiên (ATO) |
| `high` | `double` | Giá cao nhất phiên |
| `low` | `double` | Giá thấp nhất phiên |
| `avg` | `double` | Giá bình quân phiên |
| `total_vol` | `double` | Tổng khối lượng (cổ phiếu) |
| `total_val` | `double` | Tổng giá trị (VND) |
| `buy_foreign_qty` | `double` | KL nước ngoài mua |
| `sell_foreign_qty` | `double` | KL nước ngoài bán |
| `room` | `string` | Room nước ngoài còn lại |
| `change` | `double` | Thay đổi giá tuyệt đối |
| `change_pct` | `double` | Thay đổi giá % |
| `trading_date` | `string` | Ngày giao dịch `"dd/MM/yyyy"` hoặc `"YYYY-MM-DD"` |
| `ts_ms` | `long` | Unix milliseconds lúc snapshot |
| `ingested_at` | `string` | ISO 8601 UTC |

---

### 7.3 Delta Table: `bronze/tcbs_market_eod`

**Mục đích**: Daily EOD (End-of-Day) summary cổ phiếu VN — dữ liệu lịch sử ngắn gọn hơn snapshot.  
**Data class**: Historical OHLCV (Bronze)  
**Nguồn**: `dataops_batch_api/cli/jobs/tcbs_eod_job.py`  
**Partitioned by**: `symbol`

| Column | Type | Mô tả |
|---|---|---|
| `trading_date` | `string` | `"YYYY-MM-DD"` |
| `symbol` | `string` | Mã cổ phiếu |
| `exchange` | `string` | `"HOSE"` \| `"HNX"` \| `"UPCOM"` |
| `open` | `double` | Giá ATO |
| `high` | `double` | Giá cao nhất phiên |
| `low` | `double` | Giá thấp nhất phiên |
| `close` | `double` | Giá đóng cửa ATC |
| `total_vol` | `double` | Tổng khối lượng |
| `total_val` | `double` | Tổng giá trị (VND) |
| `buy_foreign_qty` | `double` | KL nước ngoài mua |
| `sell_foreign_qty` | `double` | KL nước ngoài bán |
| `net_foreign_qty` | `double` | buy_foreign - sell_foreign |
| `change_pct` | `double` | % thay đổi |
| `ref_price` | `double` | Giá tham chiếu |
| `ceil_price` | `double` | Giá trần |
| `floor_price` | `double` | Giá sàn |

---

### 7.4 Delta Table: `silver/vwap_1m`

**Mục đích**: VWAP 5-minute windows từ Spark Streaming — Silver layer.  
**Data class**: Derived Analytics (Silver)  
**Nguồn**: `dataops_processing/jobs/vwap_job.py`  
**Partitioned by**: `symbol`  
**Write mode**: append

| Column | Type | Mô tả |
|---|---|---|
| `symbol` | `string` | Mã instrument |
| `window_start` | `timestamp` | Thời điểm bắt đầu window |
| `window_end` | `timestamp` | Thời điểm kết thúc window |
| `interval` | `string` | `"5m"` |
| `vwap` | `double` | Giá VWAP |
| `total_volume` | `double` | Tổng volume trong window |
| `num_trades` | `long` | Số trade ticks |

---

### 7.5 Delta Table: `silver/volume_bars`

**Mục đích**: Volume Bars từ De Prado algorithm — thay thế time bars bằng volume-based bars.  
**Data class**: Derived Analytics (Silver)  
**Nguồn**: `dataops_processing/jobs/volume_bars_job.py`  
**Partitioned by**: `symbol`

| Column | Type | Mô tả |
|---|---|---|
| `symbol` | `string` | Mã instrument |
| `inst_type` | `string` | `"SPOT"` \| `"FUTURES"` |
| `bar_index` | `integer` | Số thứ tự bar (0-based, reset khi overwrite) |
| `open` | `double` | Giá mở cửa bar |
| `high` | `double` | Giá cao nhất |
| `low` | `double` | Giá thấp nhất |
| `close` | `double` | Giá đóng cửa |
| `total_volume` | `double` | Tổng volume trong bar |
| `num_candles` | `integer` | Số nến 1d đã tích lũy để đủ threshold |
| `bar_open_ts` | `long` | Unix ms thời điểm đầu bar |
| `bar_close_ts` | `long` | Unix ms thời điểm cuối bar |
| `ingested_at` | `string` | ISO 8601 UTC |

---

### 7.6 Delta Table: `silver/indicator_matrix`

**Mục đích**: Ma trận chỉ báo kỹ thuật — EMA, ATR, RSI, Bollinger Bands.  
**Data class**: Derived Analytics (Silver)  
**Nguồn**: `dataops_processing/jobs/indicator_matrix_job.py`  
**Partitioned by**: `symbol`, `interval`

| Column | Type | Mô tả |
|---|---|---|
| `symbol` | `string` | Mã instrument |
| `interval` | `string` | `"1d"` \| `"1h"` |
| `timestamp` | `long` | Unix milliseconds của nến |
| `open` | `double` | Giá mở cửa |
| `high` | `double` | Giá cao nhất |
| `low` | `double` | Giá thấp nhất |
| `close` | `double` | Giá đóng cửa |
| `volume` | `double` | Khối lượng |
| `ema_20` | `double` | EMA 20 kỳ: `k=2/21` |
| `ema_50` | `double` | EMA 50 kỳ: `k=2/51` |
| `ema_200` | `double` | EMA 200 kỳ: `k=2/201` |
| `rsi_14` | `double` | RSI 14 kỳ (Wilder), range [0, 100] |
| `atr_14` | `double` | ATR 14 kỳ (Wilder smoothing), đơn vị giá |
| `bb_upper_20` | `double` | Bollinger Band trên: SMA20 + 2σ |
| `bb_middle_20` | `double` | Bollinger Band giữa: SMA20 |
| `bb_lower_20` | `double` | Bollinger Band dưới: SMA20 - 2σ |
| `bb_bandwidth` | `double` | (Upper - Lower) / Middle |
| `bb_pct_b` | `double` | (Close - Lower) / (Upper - Lower) |
| `computed_at` | `string` | ISO 8601 UTC — thời điểm tính toán |

---

### 7.7 Delta Table: `gold/market_breadth`

**Mục đích**: Market Breadth và foreign flow hàng ngày cho HOSE/HNX/UPCOM.  
**Data class**: Aggregate Report (Gold)  
**Nguồn**: `dataops_processing/jobs/market_breadth_job.py`  
**Idempotency**: `replaceWhere trading_date` — ghi đè theo ngày

| Column | Type | Mô tả |
|---|---|---|
| `trading_date` | `string` | `"YYYY-MM-DD"` |
| `group` | `string` | `"ALL"` \| `"HOSE"` \| `"HNX"` \| `"UPCOM"` \| `"VN30"` |
| `advancers` | `integer` | Số cổ phiếu tăng giá |
| `decliners` | `integer` | Số cổ phiếu giảm giá |
| `unchanged` | `integer` | Số cổ phiếu đứng giá |
| `advance_decline_ratio` | `double` | advancers / max(decliners, 1) |
| `pct_advancing` | `double` | % cổ phiếu tăng trên tổng |
| `ceiling_count` | `integer` | Số cổ phiếu chạm trần |
| `floor_count` | `integer` | Số cổ phiếu chạm sàn |
| `total_volume` | `double` | Tổng khối lượng toàn thị trường |
| `total_value` | `double` | Tổng giá trị (VND) |
| `foreign_buy_qty` | `double` | Tổng KL nước ngoài mua |
| `foreign_sell_qty` | `double` | Tổng KL nước ngoài bán |
| `foreign_net_qty` | `double` | Net flow nước ngoài |
| `foreign_net_value_est` | `double` | Ước tính giá trị net: Σ(net_qty × close) |
| `computed_at` | `string` | ISO 8601 UTC |

---

## 8. Kafka Topics — Message Bus

**Cluster**: Apache Kafka 3.7 KRaft (3 brokers, không ZooKeeper)  
**Bootstrap servers**: `kafka1:9092,kafka2:9092,kafka3:9092`  
**Default replication**: 3  
**Default min ISR**: 2  
**Message format**: Avro (Schema Registry) hoặc JSON fallback  

---

### 8.1 Topic: `binance.trade.ticks`

**Mục đích**: Real-time trade ticks từ Binance WebSocket `@trade`.  
**Partitioner**: BinancePartitioner (P0=BTC*, P1=ETH*, P2..N-1=FNV32a hash)  
**Message format**: Avro schema `binance_trade_event.avsc`

| Field | Type (Avro) | Mô tả |
|---|---|---|
| `symbol` | `string` | Mã instrument: `"BTCUSDT"` |
| `trade_id` | `long` | Binance trade ID |
| `price` | `double` | Giá giao dịch |
| `size` | `double` | Khối lượng giao dịch (base asset) |
| `side` | `string` | `"buy"` \| `"sell"` (derived từ `is_buyer_maker`) |
| `ts_ms` | `long` | Unix milliseconds |
| `exchange` | `string` | `"binance"` |
| `inst_type` | `string` | `"SPOT"` \| `"FUTURES"` |

---

### 8.2 Topic: `tcbs.market.snapshot`

**Mục đích**: Market snapshot từ TCBS iFlash REST poll.  
**Message format**: Avro schema `tcbs_market_snapshot.avsc`

| Field | Type (Avro) | Mô tả |
|---|---|---|
| `symbol` | `string` | Mã cổ phiếu |
| `exchange` | `string` | `"HOSE"` \| `"HNX"` \| `"UPCOM"` |
| `match_price` | `double` | Giá khớp |
| `bid_price_01` | `double` | Giá chào mua tốt nhất |
| `offer_price_01` | `double` | Giá chào bán tốt nhất |
| `total_vol` | `double` | Tổng khối lượng |
| `ref_price` | `double` | Giá tham chiếu |
| `change_pct` | `double` | % thay đổi |
| `ts_ms` | `long` | Unix milliseconds |

---

### 8.3 Topic: `binance.orderbook.snapshot`

**Mục đích**: OrderBook depth-5 snapshot từ Binance `@depth5`.  
**Message format**: Avro schema `binance_orderbook_snapshot.avsc`

| Field | Type (Avro) | Mô tả |
|---|---|---|
| `symbol` | `string` | Mã instrument |
| `bids` | `array<record{price,qty}>` | Top 5 bid levels |
| `asks` | `array<record{price,qty}>` | Top 5 ask levels |
| `ts_ms` | `long` | Unix milliseconds |

---

### 8.4 Topic: `tcbs.derivatives.ticks`

**Mục đích**: VN30F phái sinh ticks từ TCBS WebSocket.  
**Message format**: Avro schema `tcbs_derivative_event.avsc`

| Field | Type (Avro) | Mô tả |
|---|---|---|
| `symbol` | `string` | Contract: `"VN30F2411"` |
| `match_price` | `double` | Giá khớp |
| `match_qty` | `double` | KL khớp |
| `open_vol` | `double` | Open interest |
| `change_pct` | `double` | % thay đổi |
| `ts_ms` | `long` | Unix milliseconds |

---

### 8.5 DLQ Topics (Dead Letter Queue)

| Topic | Xử lý bởi | Mô tả |
|---|---|---|
| `binance.trade.ticks.dlq` | Manual / alert | Trade tick xử lý thất bại |
| `binance.orderbook.snapshot.dlq` | Manual | OrderBook snapshot lỗi |
| `tcbs.market.snapshot.dlq` | Manual | TCBS snapshot lỗi |
| `platform.dlq` | Manual | Generic platform DLQ |

---

## 9. Protobuf / gRPC Schema

### 9.1 `HistoricalDataService` — `batch_data.proto`

**Server**: `dataops_batch_api` (Python, port 50051)  
**Client**: `dataops_backend_api` (Go, repo: `repository/batch/client.go`)

| RPC Method | Request | Response | Mô tả |
|---|---|---|---|
| `GetBinanceOHLCV` | `BinanceOHLCVRequest` | `BinanceOHLCVResponse` | Lấy nến 4h/1d từ Delta Bronze |
| `GetTCBSMarketEOD` | `TCBSMarketEODRequest` | `TCBSMarketEODResponse` | Lấy EOD cổ phiếu VN từ Delta Bronze |
| `GetTCBSBSAHistory` | `TCBSBSARequest` | `TCBSBSAResponse` | Lấy BSA daily summary |
| `GetDataRange` | `DataRangeRequest` | `DataRangeResponse` | Metadata: oldest/newest date, total count |

### 9.2 `MarketDataStream` — `market_data.proto`

| RPC Method | Type | Mô tả |
|---|---|---|
| `GetTicker` | Unary | Lấy ticker tại thời điểm |
| `GetOrderBook` | Unary | Lấy orderbook snapshot |
| `GetKline` | Unary | Lấy nến giá |
| `StreamTicker` | Server-stream | Stream ticker real-time |
| `StreamOrderBook` | Server-stream | Stream orderbook real-time |
| `StreamTrades` | Server-stream | Stream trade ticks real-time |

---

## 10. Pydantic Validation Models (Ingestion Contracts)

Lớp validation ở tầng ingestion trước khi ghi Delta Lake. Tất cả trong `dataops_batch_api/cli/ingestion/schema/models.py`.

| Model | Dùng cho | Validation rules chính |
|---|---|---|
| `BinanceCandleRecord` | Binance klines → Bronze | `high >= low`, `open/high/low/close > 0`, `confirm ∈ {0,1}` |
| `TCBSMarketRecord` | TCBS tickerCommons → Bronze | `match_price` không vượt trần/sàn (1% tolerance) |
| `TCBSTickerSnapRecord` | TCBS tickerSnaps → Bronze | Giá/khối lượng >= 0 |
| `TCBSBSARecord` | TCBS BSA → Bronze | `bup/sdp ∈ [0,100]`, `investor_type ∈ {sheep, wolf, shark, all}` |
| `TCBSIntradayTickRecord` | TCBS intraday ticks → Bronze | `action ∈ {"Buy Up", "Sell Down"}` |
| `OHLCVRecord` | Generic OHLCV | `high >= low`, tất cả giá > 0 |
| `TCBSDerivativeRecord` | VN30F info → Bronze | Giá/volume >= 0 |
| `TCBSAccountCashRecord` | TCBS cash proxy | Số dư >= 0 |
| `TCBSStockAssetRecord` | TCBS holdings proxy | `quantity >= 0` |
| `TCBSOrderRecord` | TCBS order log | `exec_type ∈ {"NB", "NS"}` |
| `TCBSOpenPositionRecord` | TCBS position proxy | `side ∈ {"L", "S"}` |

---

## 11. Data Lineage Map

```
Binance WebSocket (@trade)
  └─→ stream-producer [Avro encode]
        ├─→ Kafka: binance.trade.ticks
        │     └─→ Spark VWAP job
        │           ├─→ InfluxDB: measurement=vwap
        │           └─→ Delta Silver: vwap_1m
        │     └─→ Spark Order Flow job
        │           └─→ InfluxDB: measurement=order_flow
        └─→ Redis: binance_{sym}_ticker (TTL 10s)
              └─→ backend_api: GET /crypto/ticker [< 5ms]

Binance WebSocket (@depth5)
  └─→ stream-producer [Avro encode]
        ├─→ Kafka: binance.orderbook.snapshot
        └─→ Redis: binance_{sym}_depth5 (TTL 10s)

Binance REST /api/v3/klines
  └─→ batch_api [Pydantic validate]
        └─→ Delta Bronze: binance_candles_{1d,4h}
              ├─→ gRPC → backend_api: GET /crypto/ohlcv
              ├─→ Spark Volume Bars job
              │     └─→ Delta Silver: volume_bars
              └─→ Spark Indicator Matrix job
                    └─→ Delta Silver: indicator_matrix

TCBS iFlash REST (market snapshots)
  └─→ stream-producer [REST poll, cron]
        ├─→ Kafka: tcbs.market.snapshot
        └─→ Redis: tcbs_{sym}_ticker (TTL 120s)
              └─→ backend_api: GET /tcbs/ticker [< 5ms]

TCBS iFlash REST (EOD/BSA/Ticks)
  └─→ batch_api [Pydantic validate]
        ├─→ Delta Bronze: tcbs_market_eod
        │     ├─→ gRPC → backend_api: GET /tcbs/ohlcv
        │     └─→ Spark Market Breadth job
        │           └─→ Delta Gold: market_breadth
        └─→ Delta Bronze: tcbs_bsa_daily
              └─→ gRPC → backend_api: GET /tcbs/bsa

TCBS OpenAPI (account data — transactional)
  └─→ backend_api [TCBS proxy, no Kafka]
        └─→ Redis: tcbs_account_{balance,assets,positions}_{account_id}

MySQL (users, accounting, indicators)
  └─→ backend_api [GORM]
        ├─→ Authentication: POST /login → JWT
        ├─→ Accounting: GET /portfolio/nav
        └─→ Indicators: GET /crypto/signal → IndicatorSignal
```

---

## 12. Canonical Enums Cross-Reference

Tất cả enum được định nghĩa trong `dataops_protobuf/market-data/market_data.proto` và được mirror tại Go/Python.

### `Interval` — Timeframe

| Enum Value | Proto int | String | Binance | TCBS | InfluxDB | Delta partition |
|---|---|---|---|---|---|---|
| `INTERVAL_1M` | 2 | `"1m"` | `"1m"` | aggregate | `"1m"` | `"1m"` |
| `INTERVAL_5M` | 4 | `"5m"` | `"5m"` | — | `"5m"` | `"5m"` |
| `INTERVAL_1H` | 7 | `"1h"` | `"1h"` | — | `"1h"` | `"1h"` |
| `INTERVAL_4H` | 9 | `"4h"` | `"4h"` | — | — | `"4h"` |
| `INTERVAL_1D` | 13 | `"1d"` | `"1d"` | EOD | — | `"1d"` |

### `Side` — Hướng giao dịch

| Enum Value | Proto int | Binance | TCBS Stock | TCBS Deriv | TCBS Position |
|---|---|---|---|---|---|
| `SIDE_BUY` | 1 | `m=false` | `exec_type=NB` | `side=B` | — |
| `SIDE_SELL` | 2 | `m=true` | `exec_type=NS` | `side=S` | — |
| `SIDE_LONG` | 3 | — | — | — | `side=L` |
| `SIDE_SHORT` | 4 | — | — | — | `side=S` |

### `OrderStatus` — Trạng thái lệnh

| Enum Value | Binance | TCBS Stock `orStatus` | TCBS Deriv `condition` |
|---|---|---|---|
| `PENDING` | `"NEW"` | `"8"` | `"0"` |
| `FILLED` | `"FILLED"` | `"4"`, `"12"`, `"S"` | — |
| `CANCELLED` | `"CANCELED"` | `"3"` | `"2"` |
| `REJECTED` | `"REJECTED"` | `"0"` | `"3"` |
| `PARTIALLY_FILLED` | `"PARTIALLY_FILLED"` | — | — |
| `ACTIVATED` | — | — | `"1"` |

### `Exchange` — Sàn giao dịch

| Enum Value | Proto int | String | Mô tả |
|---|---|---|---|
| `EXCHANGE_BINANCE` | 1 | `"binance"` | Binance Spot |
| `EXCHANGE_BINANCE_FUTURES` | 2 | `"binance_futures"` | Binance USD-M Futures |
| `EXCHANGE_TCBS_HOSE` | 3 | `"tcbs_hose"` | TCBS — Sàn HOSE |
| `EXCHANGE_TCBS_HNX` | 4 | `"tcbs_hnx"` | TCBS — Sàn HNX |
| `EXCHANGE_TCBS_UPCOM` | 5 | `"tcbs_upcom"` | TCBS — UPCoM |
| `EXCHANGE_TCBS_DERIV` | 6 | `"tcbs_deriv"` | TCBS — Phái sinh VN30F |

### `InstrumentType` — Loại sản phẩm

| Enum Value | Proto int | Binance | TCBS | Delta `inst_type` |
|---|---|---|---|---|
| `INSTRUMENT_TYPE_SPOT` | 1 | Spot API | Cổ phiếu cơ sở | `"SPOT"` |
| `INSTRUMENT_TYPE_FUTURES` | 2 | fapi | — | `"FUTURES"` |
| `INSTRUMENT_TYPE_DERIVATIVE` | 4 | — | VN30F | `"DERIVATIVE"` |

---

*Tài liệu này được tạo từ review toàn bộ source code của DataOps Backend Financial Trading.*  
*Cập nhật lần cuối: 2026-10-16. Nguồn tham chiếu: `dataops_protobuf/`, `dataops_backend_api/models/`, `dataops_batch_api/cli/ingestion/schema/`, `dataops_processing/config/processing_config.yaml`.*
