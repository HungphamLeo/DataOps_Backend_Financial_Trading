# SESSION EXPORT — Kafka Cluster Fix & Infrastructure Debug
**Date:** 2026-10-05  
**Branch:** 03102026_debug_infra  
**Scope:** `infra_dataops_finance_trading/docker-compose.yml`  
**References:**
- `export_task/SESSION_2026_10_05_INFRA_HARDENING_AND_KAFKA_TOPICS.md`
- `github/copilotinstructions/SKILL.md`
- `github/agent/devops-infra-engineer.agent.md`

---

## 1. MỤC TIÊU SESSION

1. Debug lỗi `dataops-kafka2` crash loop khi `docker compose up -d`.
2. Debug lỗi `dataops-spark-master` unhealthy.
3. Fix `kafka-init` không được trigger do dependency chain broken.
4. Verify Kafka cluster healthy qua Kafka UI.

---

## 2. FIXES ĐÃ THỰC HIỆN

### FIX-1 — `kafka2`: EXTERNAL listener port conflict

**Lỗi:**
```
IllegalArgumentException: Each listener must have a different port
listeners: PLAINTEXT://kafka2:29092,CONTROLLER://kafka2:9093,EXTERNAL://0.0.0.0:9093
port: 9093
```

**Root cause:** `CONTROLLER` và `EXTERNAL` listener của kafka2 cùng dùng port `9093`. Kafka KRaft yêu cầu mỗi listener phải có port riêng biệt.

**So sánh 3 brokers:**

| Broker | CONTROLLER port | EXTERNAL port (trước) | EXTERNAL port (sau) |
|---|---|---|---|
| kafka1 | 9093 | 9092 ✅ | 9092 (không đổi) |
| kafka2 | 9093 | **9093 ❌** | **9095 ✅** |
| kafka3 | 9093 | 9094 ✅ | 9094 (không đổi) |

**Fix trong `docker-compose.yml`:**
```yaml
# kafka2 — TRƯỚC (sai)
KAFKA_LISTENERS: PLAINTEXT://kafka2:29092,CONTROLLER://kafka2:9093,EXTERNAL://0.0.0.0:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka2:29092,EXTERNAL://kafka2:9093
ports:
  - "9093:9093"

# kafka2 — SAU (đúng)
KAFKA_LISTENERS: PLAINTEXT://kafka2:29092,CONTROLLER://kafka2:9093,EXTERNAL://0.0.0.0:9095
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka2:29092,EXTERNAL://kafka2:9095
ports:
  - "9095:9095"
```

**Quan trọng:** `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093` — port `9093` ở đây là CONTROLLER port, **không phải EXTERNAL** → giữ nguyên, không đổi.

---

### FIX-2 — `spark-master`: healthcheck dùng `curl` không tồn tại trong image

**Root cause:** Image `apache/spark:3.5.0` không có `curl` trong PATH.

**Fix:**
```yaml
# TRƯỚC (sai)
test: ["CMD", "curl", "-sf", "http://localhost:8080"]

# SAU (đúng)
test: ["CMD-SHELL", "wget -qO- http://localhost:8080 > /dev/null 2>&1 || exit 1"]
```

---

### FIX-3 — `kafka-init` không được trigger

**Root cause:** Lần `docker compose up` đầu tiên, kafka2 crash → dependency `kafka2: service_healthy` không fulfilled → `kafka-init` không được schedule. Sau khi fix kafka2 và `--force-recreate`, cần trigger thủ công:

```bash
docker compose --env-file ../.env up -d --force-recreate kafka2
docker compose --env-file ../.env up -d kafka-init
```

**Kết quả:** kafka-init tạo thành công 15 topics theo spec.

---

## 3. LESSON LEARNED

### [L1] Kafka KRaft — Port conflict rule

Mỗi listener name trong `KAFKA_LISTENERS` phải bind port **duy nhất** trong cùng 1 broker:
- `CONTROLLER://kafka2:9093` — port 9093 cho quorum voting (inter-broker)
- `PLAINTEXT://kafka2:29092` — port 29092 cho internal broker communication
- `EXTERNAL://0.0.0.0:9095` — port khác hoàn toàn cho external access

Rule phân port EXTERNAL cho 3 brokers:
```
kafka1 → EXTERNAL port 9092
kafka2 → EXTERNAL port 9095
kafka3 → EXTERNAL port 9094
```

### [L2] `apache/spark` image không có `curl`

Image `apache/spark:3.5.0` chỉ có `wget`. Healthcheck phải dùng `wget` hoặc `CMD-SHELL` với fallback.

```yaml
# Pattern chuẩn cho apache/spark healthcheck
test: ["CMD-SHELL", "wget -qO- http://localhost:8080 > /dev/null 2>&1 || exit 1"]
```

### [L3] `--force-recreate` bắt buộc khi thay đổi env vars

`docker compose up -d` chỉ recreate container khi **image** thay đổi, không phải khi env var thay đổi trong file. Khi sửa `KAFKA_LISTENERS` phải dùng `--force-recreate` để container nhận config mới.

```bash
docker compose --env-file ../.env up -d --force-recreate kafka2
```

### [L4] kafka-init dependency chain broken sau partial failure

Khi một broker trong cluster fail lúc startup → `kafka-init` (depends on tất cả brokers healthy) bị skip hoàn toàn. Sau khi fix broker, cần start lại `kafka-init` thủ công:
```bash
docker compose --env-file ../.env up -d kafka-init
docker logs dataops-kafka-init -f
```

### [L5] `cub kafka-ready` JVM cold-start timeout — chưa fix

`cub kafka-ready` (Confluent Utility Belt) cần khởi động JVM ~25s → timeout 30s không đủ trong healthcheck. **Chưa fix trong session này** — cần tăng `timeout: 45s` hoặc đổi sang `kafka-broker-api-versions.sh`.

```yaml
# TODO: đổi healthcheck kafka brokers sang
test: ["CMD-SHELL", "kafka-broker-api-versions.sh --bootstrap-server localhost:PORT > /dev/null 2>&1"]
timeout: 45s
```

### [L6] Leader Skew sau broker rejoin

Khi broker rejoin cluster sau downtime, leader skew sẽ cao (kafka2: -93.5% sau sự cố). Tự rebalance sau vài phút, hoặc trigger thủ công:
```bash
docker exec dataops-kafka1 kafka-leader-election \
  --bootstrap-server kafka1:29092 \
  --election-type PREFERRED \
  --all-topic-partitions
```

### [L7] Redis UI — cách monitor stream-producer cache

Keys Redis được tạo bởi stream-producer theo pattern:
- `binance_{symbol}_trade` — TTL 60s — BinanceProducer sau mỗi trade tick
- `binance_{symbol}_depth` — TTL 60s — BinanceProducer sau mỗi orderbook snapshot
- `vps_{symbol}_ticker` — TTL 120s — VPSPoller sau mỗi lần poll

Nếu keys này không tồn tại hoặc expired → producer đang có vấn đề.

```bash
# Monitor nhanh
docker exec dataops-redis redis-cli -a ${REDIS_PASSWORD} KEYS "binance_*"
docker exec dataops-redis redis-cli -a ${REDIS_PASSWORD} KEYS "vps_*"
```

Để có GUI: thêm `redislabs/redisinsight:latest` port `5540` vào `docker-compose.yml`.

---

## 4. TRẠNG THÁI CUỐI SESSION

### Kafka UI Verification (`localhost:8082`)

| Metric | Giá trị | Trạng thái |
|---|---|---|
| Broker Count | 3/3 | ✅ |
| Online Partitions | 93/93 | ✅ |
| URP (Under-Replicated) | 0 | ✅ |
| In Sync Replicas | 278/278 | ✅ |
| Out Of Sync Replicas | 0 | ✅ |
| Topics tạo bởi kafka-init | 15 topics | ✅ |
| Schema Registry | Up, no schemas yet | ✅ |
| Leader Skew kafka2 | -93.5% | ⚠️ Đang rebalance tự động |

### Services

| Service | Trạng thái | Ghi chú |
|---|---|---|
| dataops-kafka1 | ✅ healthy | Active Controller = broker 2 |
| dataops-kafka2 | ✅ healthy | Sau fix port 9095 + force-recreate |
| dataops-kafka3 | ✅ healthy | |
| dataops-kafka-init | ✅ exit 0 | 15 topics created |
| dataops-spark-master | ✅ healthy | Sau fix wget healthcheck |
| dataops-spark-worker | ✅ running | |
| dataops-mysql | ✅ healthy | |
| dataops-redis | ✅ healthy | |
| dataops-influxdb | ✅ healthy | |
| dataops-minio | ✅ healthy | |
| dataops-grafana | ✅ healthy | |
| dataops-schema-registry | ✅ healthy | |
| dataops-kafka-ui | ✅ up | `localhost:8082` |
| dataops-stream-producer | ⚠️ chưa start | Cần rebuild image |
| dataops-backend-api | ⚠️ chưa start | Cần rebuild image |
| dataops-batch-api | ⚠️ chưa start | Cần rebuild image |

---

## 5. VIỆC CẦN LÀM TIẾP THEO

### [HIGH] Fix healthcheck kafka brokers — cub timeout

Tăng `timeout` hoặc đổi command cho cả 3 brokers trong `docker-compose.yml`:
```yaml
# kafka1
test: ["CMD-SHELL", "kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
timeout: 45s

# kafka2
test: ["CMD-SHELL", "kafka-broker-api-versions.sh --bootstrap-server localhost:9095 > /dev/null 2>&1"]
timeout: 45s

# kafka3
test: ["CMD-SHELL", "kafka-broker-api-versions.sh --bootstrap-server localhost:9094 > /dev/null 2>&1"]
timeout: 45s
```

### [HIGH] Start app services và E2E test

```bash
cd ~/DataOps_Backend_Financial_Trading/infra_dataops_finance_trading

# Rebuild images
docker compose --env-file ../.env build --no-cache \
  dataops-stream-producer \
  dataops-backend-api \
  dataops-batch-api

# Start
docker compose --env-file ../.env up -d \
  dataops-stream-producer \
  dataops-backend-api \
  dataops-batch-api

# Verify stream-producer kết nối Binance WS
docker logs dataops-stream-producer -f

# Verify topics có messages
docker exec dataops-kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:29092 \
  --topic binance.trade.ticks \
  --from-beginning --max-messages 5 --timeout-ms 10000
```

### [MEDIUM] Thêm RedisInsight vào docker-compose.yml

```yaml
redis-insight:
  image: redislabs/redisinsight:latest
  container_name: dataops-redis-insight
  restart: unless-stopped
  networks:
    - dataops-internal-net
  ports:
    - "5540:5540"
  depends_on:
    redis:
      condition: service_healthy
  deploy:
    resources:
      limits:
        memory: "128M"
```

Truy cập: `http://localhost:5540`  
Kết nối: host=`dataops-redis`, port=`6379`, password=`${REDIS_PASSWORD}`

### [LOW] Trigger kafka-leader-election sau mỗi lần restart cluster

```bash
docker exec dataops-kafka1 kafka-leader-election \
  --bootstrap-server kafka1:29092 \
  --election-type PREFERRED \
  --all-topic-partitions
```
