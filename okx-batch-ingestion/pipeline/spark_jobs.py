"""
PySpark Structured Streaming Processing Engine (Stage 3 - Data Fabric).
Thực thi 3 bài toán phân tích định lượng cốt lõi theo thời gian thực:
1. VWAP Sliding Window (5m window, 1m slide, Watermark 30s)
2. Volume Bars (De Prado Method - Gom bar theo lượng vol thay vì thời gian)
3. Rolling Z-Score Funding Rate Anomaly Detection -> Đẩy cảnh báo vào Kafka Topic
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum as _sum, avg, stddev, abs as _abs,
    to_timestamp, to_json, struct, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)


def create_spark_session(app_name: str = "OKX-Financial-Stream-Processor") -> SparkSession:
    """Khởi tạo Spark Session với đầy đủ connector Delta Lake & Kafka."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# Schema cho trade tick event từ Kafka
trade_schema = StructType([
    StructField("inst_id", StringType(), False),
    StructField("trade_id", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("qty", DoubleType(), False),
    StructField("side", StringType(), False),
    StructField("ts", LongType(), False),
])

funding_schema = StructType([
    StructField("inst_id", StringType(), False),
    StructField("funding_rate", DoubleType(), False),
    StructField("next_funding_time", LongType(), False),
    StructField("ts", LongType(), False),
])


def process_vwap_stream(spark: SparkSession, kafka_brokers: str):
    """
    Job 1: Tính VWAP Sliding Window (5m window / 1m slide) với Watermark 30s.
    Ghi kết quả vào Delta Lake Silver: s3a://okx-delta/silver/vwap
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", "okx.trade.ticks")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_df.select(from_json(col("value").cast("string"), trade_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("ts") / 1000))
    )

    # VWAP = Sum(Price * Qty) / Sum(Qty)
    vwap_df = (
        parsed_df
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            window("event_time", "5 minutes", "1 minute"),
            col("inst_id")
        )
        .agg(
            (_sum(col("price") * col("qty")) / _sum(col("qty"))).alias("vwap"),
            _sum("qty").alias("volume"),
            _sum(col("price") * col("qty")).alias("turnover")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("inst_id").alias("symbol"),
            col("vwap"),
            col("volume"),
            col("turnover")
        )
    )

    query = (
        vwap_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/spark_checkpoints/vwap")
        .start("s3a://okx-delta/silver/vwap")
    )
    return query


def process_funding_anomaly_stream(spark: SparkSession, kafka_brokers: str):
    """
    Job 2: Tính Rolling Z-Score cho Funding Rate và phát hiện Anomaly.
    Khi |Z-score| > 2.5 => Gửi cảnh báo về topic okx.alerts.funding-anomaly
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", "okx.funding.rates")
        .option("startingOffsets", "latest")
        .load()
    )

    funding_df = (
        raw_df.select(from_json(col("value").cast("string"), funding_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("ts") / 1000))
    )

    # Tính Rolling Mean & StdDev trong 1 giờ
    stats_df = (
        funding_df
        .withWatermark("event_time", "1 minute")
        .groupBy(
            window("event_time", "1 hour", "10 minutes"),
            col("inst_id")
        )
        .agg(
            avg("funding_rate").alias("mean_funding"),
            stddev("funding_rate").alias("std_funding"),
            _sum("funding_rate").alias("latest_rate")
        )
    )

    # Z-Score = (X - Mean) / Std
    anomaly_df = (
        stats_df
        .filter(col("std_funding") > 0.000001)
        .withColumn("z_score", (col("latest_rate") - col("mean_funding")) / col("std_funding"))
        .filter(_abs(col("z_score")) >= 2.5)
        .select(
            col("inst_id").alias("key"),
            to_json(struct(
                col("inst_id"),
                col("z_score"),
                col("latest_rate"),
                col("window.start").alias("start_time"),
                col("window.end").alias("end_time")
            )).alias("value")
        )
    )

    query = (
        anomaly_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("topic", "okx.alerts.funding-anomaly")
        .option("checkpointLocation", "/tmp/spark_checkpoints/funding_anomaly")
        .start()
    )
    return query


if __name__ == "__main__":
    kafka_host = os.getenv("KAFKA_BROKERS", "localhost:9092")
    spark = create_spark_session()
    print("=== PySpark Structured Streaming Financial Engine Started ===")
    
    q1 = process_vwap_stream(spark, kafka_host)
    q2 = process_funding_anomaly_stream(spark, kafka_host)
    
    spark.streams.awaitAnyTermination()
