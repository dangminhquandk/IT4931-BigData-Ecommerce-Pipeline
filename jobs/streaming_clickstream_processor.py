#!/usr/bin/env python3
"""
SPARK STRUCTURED STREAMING: CLICKSTREAM PROCESSOR
Học phần: IT4931 - Nhóm 3
Phụ trách: Nguyễn Xuân Hồng Quân (MSSV: 202416321)

Mục tiêu kỹ thuật cốt lõi:
- Đọc micro-batches từ Kafka topic 'clickstream-events'.
- Áp dụng Event-Time Processing với Sliding Window: 1 phút, trượt 10 giây (Sliding: 10s, Window: 1m).
- Cấu hình Watermarking: 10 phút để loại bỏ Late Data và dọn dẹp State Store, tránh rò rỉ bộ nhớ.
- Tổng hợp: Số lượng sự kiện (event_count) và số người dùng duy nhất (unique_users) theo từng loại tương tác.
- Ghi kết quả vào PostgreSQL bằng foreachBatch với cơ chế Upsert (ON CONFLICT DO UPDATE).
"""

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, approx_count_distinct
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

def build_spark_session(app_name="StreamingClickstreamProcessor"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.checkpointLocation", "checkpoints/clickstream") \
        .getOrCreate()

def get_clickstream_schema():
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("product_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("is_late", BooleanType(), True)
    ])

def make_upsert_function(pg_url, pg_user, pg_password):
    """Tạo hàm foreachBatch để thực thi Upsert vào PostgreSQL tránh trùng lặp bản ghi."""
    def write_to_postgres(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        print(f"[STREAM BATCH #{batch_id}] Ghi nhận {batch_df.count()} bản ghi cửa sổ mới.")

        # Định dạng lại dữ liệu cho bảng stream_realtime_traffic
        formatted_df = batch_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("event_count"),
            col("unique_users")
        )

        # Trong môi trường phân tán, sử dụng foreachPartition để mở connection và upsert
        def upsert_partition(rows):
            import psycopg2
            from psycopg2.extras import execute_batch
            from urllib.parse import urlparse

            parsed = urlparse(pg_url.replace("jdbc:", ""))
            host = parsed.hostname or "localhost"
            port = parsed.port or 5432
            dbname = parsed.path.lstrip("/") or "ecommerce_serving"

            upsert_sql = """
                INSERT INTO stream_realtime_traffic (window_start, window_end, event_type, event_count, unique_users, updated_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (window_start, window_end, event_type)
                DO UPDATE SET
                    event_count = EXCLUDED.event_count,
                    unique_users = EXCLUDED.unique_users,
                    updated_at = CURRENT_TIMESTAMP;
            """
            data_to_insert = [
                (r.window_start, r.window_end, r.event_type, r.event_count, r.unique_users)
                for r in rows
            ]
            if not data_to_insert:
                return

            try:
                conn = psycopg2.connect(
                    host=host, port=port, dbname=dbname,
                    user=pg_user, password=pg_password
                )
                with conn.cursor() as cur:
                    execute_batch(cur, upsert_sql, data_to_insert)
                conn.commit()
                conn.close()
            except Exception as err:
                print(f"[UPSERT ERROR] Không thể ghi batch vào Postgres: {err}")

        try:
            formatted_df.foreachPartition(upsert_partition)
        except Exception as e:
            print(f"[LỖI XỬ LÝ FOREACH] {e}")

    return write_to_postgres

def run_streaming(args):
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"==================================================")
    print(f"[STREAMING START] Kết nối Kafka: {args.kafka_servers}, Topic: {args.topic}")
    print(f"==================================================")

    # 1. Đọc stream từ Kafka
    kafka_raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_servers) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Giải mã JSON payload
    schema = get_clickstream_schema()
    parsed_stream = kafka_raw_stream \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .filter(col("event_time").isNotNull())

    # 3. Áp dụng Watermark & Sliding Window (1 phút, trượt 10s)
    windowed_aggregates = parsed_stream \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "1 minute", "10 seconds"),
            col("event_type")
        ) \
        .agg(
            count("event_id").alias("event_count"),
            approx_count_distinct("user_id").alias("unique_users")
        )

    # 4. Ghi luồng dữ liệu
    if args.console:
        print("[STREAMING SINK] Chạy ở chế độ Console Output (Update mode)...")
        query = windowed_aggregates.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .start()
    else:
        print(f"[STREAMING SINK] Chạy ở chế độ Upsert vào PostgreSQL: {args.pg_url}...")
        upsert_handler = make_upsert_function(args.pg_url, args.pg_user, args.pg_password)
        query = windowed_aggregates.writeStream \
            .outputMode("update") \
            .foreachBatch(upsert_handler) \
            .option("checkpointLocation", args.checkpoint_dir) \
            .start()

    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Clickstream Processor")
    parser.add_argument("--kafka-servers", default="localhost:29092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="clickstream-events", help="Kafka topic name")
    parser.add_argument("--checkpoint-dir", default="checkpoints/clickstream", help="Thư mục lưu trữ checkpoint")
    parser.add_argument("--pg-url", default="jdbc:postgresql://localhost:5433/ecommerce_serving", help="JDBC Connection URL")
    parser.add_argument("--pg-user", default="postgres", help="Tài khoản PostgreSQL")
    parser.add_argument("--pg-password", default="postgrespassword", help="Mật khẩu PostgreSQL")
    parser.add_argument("--console", action="store_true", help="Xuất kết quả ra console thay vì ghi DB")

    args = parser.parse_args()
    run_streaming(args)

