"""
dataops_processing — Spark Streaming / Batch Processing Layer.

Tầng xử lý dữ liệu trung gian giữa Kafka (raw events) và storage (InfluxDB + Delta Silver).
Trách nhiệm duy nhất: consume từ Kafka → aggregate / transform → ghi vào InfluxDB + Delta Lake.

Ranh giới:
  - INPUT:  Kafka topics (binance.trade.ticks, binance.funding.rates, ...)
  - OUTPUT: InfluxDB (real-time metrics) + Delta Silver (s3://binance-datalake/delta/silver/)
  - KHÔNG đọc từ REST API — đó là việc của dataops_batch_api
  - KHÔNG serve HTTP — đó là việc của dataops_backend_api
"""
