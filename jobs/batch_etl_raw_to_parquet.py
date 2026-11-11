#!/usr/bin/env python3
"""
BATCH ETL: CSV RAW -> REFINED PARQUET + SNAPPY + PARTITIONING
Học phần: IT4931 - Nhóm 3
Phụ trách: Vũ Việt Tiến (MSSV: 202416362)

Mục tiêu:
- Đọc dữ liệu đơn hàng dạng CSV thô (>1M bản ghi từ Kaggle hoặc sample).
- Chuẩn hóa kiểu dữ liệu (Schema Enforcement), lọc bản ghi bất thường.
- Trích xuất 2 trường phân vùng (Partition columns): year và month.
- Ghi dữ liệu ra định dạng Parquet với chuẩn nén Snappy và phân vùng theo year=/month=/.
"""

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, regexp_replace, coalesce, when, upper, lit, concat
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def build_spark_session(app_name="BatchETL_CSV_to_Parquet"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def run_etl(input_path, output_path, spark=None):
    if spark is None:
        spark = build_spark_session()

    print(f"==================================================")
    print(f"[ETL START] Đọc dữ liệu từ: {input_path}")
    print(f"==================================================")

    # 1. Tự động nhận diện cấu trúc Schema (Auto-detect Kaggle vs Standard format)
    header_peek = spark.read.option("header", "true").csv(input_path).limit(1)
    col_names = [c.lower() for c in header_peek.columns]

    if "event_time" in col_names and "category_code" in col_names:
        print("[SCHEMA INFO] Nhận diện định dạng gốc Kaggle eCommerce Behavior Dataset")
        df = spark.read.option("header", "true").csv(input_path)
        initial_count = df.count()
        print(f"[INFO] Tổng số bản ghi đọc được từ Kaggle CSV: {initial_count}")

        transformed_df = df \
            .filter(col("price").isNotNull() & col("product_id").isNotNull()) \
            .withColumn("clean_time", regexp_replace(col("event_time"), " UTC", "")) \
            .withColumn("order_time", to_timestamp(col("clean_time"), "yyyy-MM-dd HH:mm:ss")) \
            .filter(col("order_time").isNotNull()) \
            .withColumn("year", year(col("order_time"))) \
            .withColumn("month", month(col("order_time"))) \
            .withColumn("customer_id", col("user_id")) \
            .withColumn("order_id", coalesce(col("user_session"), concat(col("user_id"), lit("_"), col("product_id")))) \
            .withColumn("category", coalesce(col("category_code"), lit("other"))) \
            .withColumn("order_status", when(col("event_type") == "purchase", "COMPLETED")
                                       .when(col("event_type") == "cart", "PROCESSING")
                                       .otherwise(upper(col("event_type")))) \
            .withColumn("price", col("price").cast(DoubleType())) \
            .filter(col("price") >= 0) \
            .select("order_id", "customer_id", "product_id", "category", "price", "order_status", "order_time", "year", "month")
    else:
        print("[SCHEMA INFO] Nhận diện định dạng đơn hàng tiêu chuẩn (Standard Orders)")
        raw_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), False),
            StructField("price", DoubleType(), True),
            StructField("category", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])

        df = spark.read \
            .option("header", "true") \
            .schema(raw_schema) \
            .csv(input_path)

        initial_count = df.count()
        print(f"[INFO] Tổng số bản ghi đọc được: {initial_count}")

        transformed_df = df \
            .filter(col("order_id").isNotNull() & col("price").isNotNull() & (col("price") >= 0)) \
            .withColumn("order_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
            .filter(col("order_time").isNotNull()) \
            .withColumn("year", year(col("order_time"))) \
            .withColumn("month", month(col("order_time"))) \
            .drop("timestamp")

    # Ghi dữ liệu ra Parquet có phân vùng và nén Snappy
    print(f"[ETL WRITE] Đang ghi Parquet (Snappy) phân vùng theo [year, month] vào: {output_path}")
    transformed_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)

    print(f"[ETL SUCCESS] Hoàn tất ghi dữ liệu Parquet phân vùng thành công.")
    print(f"==================================================")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Batch ETL: Raw CSV to Partitioned Parquet")
    parser.add_argument("--input", default="data/sample/orders_sample.csv", help="Đường dẫn file CSV đầu vào (HDFS hoặc Local)")
    parser.add_argument("--output", default="data/refined/orders.parquet", help="Đường dẫn thư mục Parquet đầu ra (HDFS hoặc Local)")

    args = parser.parse_args()
    run_etl(args.input, args.output)

