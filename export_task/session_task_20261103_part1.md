Read , lines 150 to 280
Completed with input: {
  "path": "/home/hungpham/DataOps_Backend_Financial_Trading/dataops_processing/jobs/vwap_job.py",
  "view_range": [
    150,
    280
  ]
}

Read , lines 180 to 336
Completed with input: {
  "path": "/home/hungpham/DataOps_Backend_Financial_Trading/dataops_processing/jobs/order_flow_job.py",
  "view_range": [
    180,
    336
  ]
}

Read , lines 1 to 220
Completed with input: {
  "path": "/home/hungpham/.vscode-server/data/agentSessionData/b0516016-b3d1-4074-980c-9adb1326b624/attachments/4f432345-5768-46d9-9f21-823409f51a9c/file_order_flow_job.py",
  "view_range": [
    1,
    220
  ]
}

Được. Mình sẽ đưa đúng kiểu “chỉ code phần bổ sung cần chèn”, không rewrite whole file. Màu sắc của ST05 ở repo này đã gần đúng rồi; phần còn thiếu nhất là:

- validate `event_time` rõ ràng
- late-data policy (drop / DLQ)
- checkpoint health + bounded-state behavior
- chốt stream query lifecycle đúng

1) File cần bổ sung chính: `dataops_processing/jobs/vwap_job.py`

Chèn ngay sau phần:
- `logger.info("vwap_job.start correlation_id=%s", correlation_id)`
- trước đoạn `if spark is None:`

Code thêm:

```python
        # ── ST05: bounded state + late-data policy ─────────────────────────────
        # Dùng event_time thực sự của message (ts_ms), không dùng ingestion time.
        # Nếu event_time quá cũ hoặc không hợp lệ thì không để state tăng vô hạn.
        # Dữ liệu trễ quá watermark nên bị drop hoặc viết sang DLQ nếu cần.
        # Đây là "bounded state" policy của production stream.
        late_data_cutoff = F.expr("INTERVAL 30 MINUTES")
```

Sau đó, chèn vào đoạn `parsed = (` ở đúng vị trí trước `.withWatermark(...)` hoặc ngay sau `.filter(F.col("price") > 0)` để đảm bảo `event_time` chắc chắn hợp lệ:

```python
        parsed = (
            raw_stream
            .select(parse_trade_udf(F.col("value")).alias("trade"))
            .filter(F.col("trade").isNotNull())
            .select(
                F.col("trade.symbol").alias("symbol"),
                F.col("trade.price").alias("price"),
                F.col("trade.size").alias("size"),
                F.col("trade.side").alias("side"),
                F.col("trade.ts_ms").cast("long").alias("ts_ms"),
            )
            .filter(F.col("price") > 0)
            .filter(F.col("size") > 0)
            .withColumn("event_time", (F.col("ts_ms") / 1000).cast(TimestampType()))
            .filter(F.col("event_time").isNotNull())
            # ST05: ngăn state bị tăng vô hạn với data quá trễ
            .filter(F.col("event_time") >= F.current_timestamp() - late_data_cutoff)
        )
```

Và chèn ngay trước `# ── 4. Dual sink ──────────────────────────────────────────────────────`:

```python
        # ST05: query lifecycle / checkpoint health
        # queryName giúp dễ trace trên Spark UI; checkpointLocation là durable recovery.
        # Nếu stream bị stall, checkpoint stale/idle sẽ dễ phát hiện.
```

Cuối cùng, thay phần sink kết thúc ở cuối function bằng đoạn này (nếu file của bạn chưa có `awaitTermination` gọi đúng cách):

```python
        influx_query = (
            windowed
            .writeStream
            .outputMode(vwap_cfg.output_mode)
            .trigger(processingTime=vwap_cfg.trigger_interval)
            .foreachBatch(_write_to_influxdb)
            .option("checkpointLocation", f"{checkpoint_base}/influxdb")
            .queryName("vwap_to_influxdb")
            .start()
        )

        delta_query = (
            windowed
            .writeStream
            .format("delta")
            .outputMode("append")
            .trigger(processingTime=vwap_cfg.trigger_interval)
            .option("checkpointLocation", f"{checkpoint_base}/delta")
            .queryName("vwap_to_delta")
            .start(delta_path)
        )

        logger.info(
            "VWAP ST05 stream started — checkpoint_base=%s delta=%s",
            checkpoint_base,
            delta_path,
        )
        spark.streams.awaitAnyTermination()
```

2) File cần bổ sung tiếp: `dataops_processing/jobs/order_flow_job.py`

Chèn ngay sau:
- `logger.info("order_flow_job.start correlation_id=%s", correlation_id)`
- trước `if spark is None:`

Code:

```python
        # ── ST05: late-data hardening ─────────────────────────────────────────
        # Hạn chế state tăng vô hạn: data quá cũ so với watermark => drop.
        late_data_cutoff = F.expr("INTERVAL 30 MINUTES")
```

Sau đó chèn vào đoạn `parsed = (` trước `.withWatermark`:

```python
        parsed = (
            raw_stream
            .select(parse_trade_udf(F.col("value")).alias("trade"))
            .filter(F.col("trade").isNotNull())
            .filter(is_buy | is_sell)
            .select(
                F.col("trade.symbol").alias("symbol"),
                F.col("trade.size").alias("size"),
                is_buy.alias("is_buy"),
                F.col("trade.ts_ms").cast("long").alias("ts_ms"),
            )
            .filter(F.col("size") > 0)
            .withColumn("event_time", (F.col("ts_ms") / 1000).cast(TimestampType()))
            .filter(F.col("event_time").isNotNull())
            # ST05: ngăn late rows bám quá lâu trong state
            .filter(F.col("event_time") >= F.current_timestamp() - late_data_cutoff)
        )
```

Và cuối `writeStream` chain trước `.start()` thêm:

```python
            .queryName("order_flow_to_influxdb")
            .option("checkpointLocation", f"{checkpoint}/influxdb")
            .trigger(processingTime=cfg.trigger_interval)
            .outputMode(cfg.output_mode)
```

Nói ngắn gọn, đoạn cuối nên dạng:

```python
        (
            order_flow
            .writeStream
            .outputMode(cfg.output_mode)
            .trigger(processingTime=cfg.trigger_interval)
            .foreachBatch(_foreach_batch)
            .option("checkpointLocation", f"{checkpoint}/influxdb")
            .queryName("order_flow_to_influxdb")
            .start()
        )

        logger.info(
            "Order flow ST05 stream started — checkpoint=%s watermark=%s window=%s",
            checkpoint, cfg.watermark, cfg.window_duration
        )
        spark.streams.awaitAnyTermination()
```

3) File nào không cần sửa thêm

Với repo hiện tại, các file dưới đây đã cơ bản đúng rồi nên không cần patch thêm:

- `dataops_processing/config/processing_config.yaml`
  - `watermark: "30 seconds"`
  - `trigger_interval: "30 seconds"`
  - `group_instance_id_enabled: true`
  - `session_timeout_ms: 45000`
  - `heartbeat_interval_ms: 15000`
  - `max_poll_interval_ms: 300000`

- `dataops_processing/config/spark_config.py`
  - already has Kafka static membership + timeout properly

- `dataops_processing/config/paths.py`
  - checkpoint dir generation already aligned with ST05

4) Chốt lại ST05 “full production” cho repo này

Điểm quan trọng nhất mà bạn cần nhớ là:

- `event_time` phải là thời điểm sự kiện, lấy từ `trade.ts_ms`
- watermark phải bounded: `30 seconds` hoặc `1 minute`
- state phải được “drop đi” khi window quá cũ
- checkpoint phải ở `checkpointLocation` riêng cho từng sink
- stream phải có `.trigger(...)`, `.outputMode("append")`, `.queryName(...)`, `.awaitAnyTermination()`
- late rows quá cũ thì drop hoặc đưa vào DLQ, không cho state chạy vô hạn

Nếu bạn muốn, tôi có thể viết tiếp cho bạn một bản “patch hoàn chỉnh theo đúng syntax repo”, dạng code có thể paste trực tiếp vào cả 2 file `dataops_processing/jobs/vwap_job.py` và `dataops_processing/jobs/order_flow_job.py`, không cần suy nghĩ thêm.