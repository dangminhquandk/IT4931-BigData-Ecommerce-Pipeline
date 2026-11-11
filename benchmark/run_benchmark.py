#!/usr/bin/env python3
"""
BENCHMARK SUITE: SO SÁNH HIỆU NĂNG TRUY VẤN GIỮA CSV vs PARQUET vs PARTITIONED PARQUET
Học phần: IT4931 - Nhóm 3
Phụ trách: Đặng Minh Quân (MSSV: 202416323)

Mục tiêu thực nghiệm:
1. Chứng minh cơ chế Predicate Pushdown (đẩy điều kiện lọc xuống tầng lưu trữ cột Parquet).
2. Chứng minh cơ chế Partition Pruning (loại bỏ các phân vùng dữ liệu không liên quan).
3. Đo lường 3 chỉ số cốt lõi:
   - Thời gian thực thi (Execution Time - ms)
   - Lượng dữ liệu đọc (Scan Volume / Bytes Read)
   - Mức tiêu hao tài nguyên (RAM MB & CPU %)
"""

import argparse
import os
import time
import json

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count

def build_spark_session():
    return SparkSession.builder \
        .appName("SQL_Storage_Benchmark") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

def get_process_metrics():
    if not PSUTIL_AVAILABLE:
        return {"ram_mb": 0.0, "cpu_percent": 0.0}
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        return {
            "ram_mb": round(mem_info.rss / (1024 * 1024), 2),
            "cpu_percent": process.cpu_percent(interval=0.1)
        }
    except Exception:
        return {"ram_mb": 0.0, "cpu_percent": 0.0}

def run_query_benchmark(spark, name, query_func):
    """Đo thời gian thực thi và tài nguyên của một truy vấn."""
    start_metrics = get_process_metrics()
    start_time = time.time()

    # Thực thi action Spark
    result_df = query_func()
    row_count = result_df.count()

    duration_ms = round((time.time() - start_time) * 1000, 2)
    end_metrics = get_process_metrics()

    return {
        "duration_ms": duration_ms,
        "row_count": row_count,
        "ram_mb": end_metrics["ram_mb"],
        "cpu_percent": end_metrics["cpu_percent"]
    }

def execute_benchmarks(spark, csv_path, parquet_path, part_parquet_path):
    print("=" * 80)
    print("BẮT ĐẦU BỘ THỰC NGHIỆM BENCHMARK SQL STORAGE")
    print("=" * 80)

    # Đọc dữ liệu từ 3 định dạng lưu trữ
    df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    df_parquet = spark.read.parquet(parquet_path)
    df_partitioned = spark.read.parquet(part_parquet_path)

    time_col = "event_time" if "event_time" in df_csv.columns else "timestamp"
    cat_col = "category_code" if "category_code" in df_csv.columns else "category"

    # Xác định phân vùng kiểm thử từ dữ liệu thực tế
    sample_part = df_partitioned.select("year", "month").distinct().first()
    target_year = sample_part["year"] if sample_part else 2019
    target_month = sample_part["month"] if sample_part else 10
    target_time_str = f"{target_year}-{target_month:02d}"

    results = []

    # -------------------------------------------------------------
    # Test Case 1: Lọc theo khoảng thời gian hẹp (Kiểm chứng Partition Pruning)
    # -------------------------------------------------------------
    tc1_name = f"TC1: Partition Pruning (Lọc theo year={target_year} & month={target_month})"
    print(f"\n[CHẠY] {tc1_name}...")

    # CSV: Phải quét toàn bộ file văn bản
    m_csv = run_query_benchmark(spark, tc1_name, lambda: df_csv.filter(
        col(time_col).startswith(target_time_str)
    ))
    # Parquet unpartitioned: Quét toàn bộ cột timestamp
    m_p = run_query_benchmark(spark, tc1_name, lambda: df_parquet.filter(
        col(time_col).startswith(target_time_str)
    ))
    # Partitioned Parquet: Chỉ đọc thư mục phân vùng
    m_part = run_query_benchmark(spark, tc1_name, lambda: df_partitioned.filter(
        (col("year") == target_year) & (col("month") == target_month)
    ))

    results.append({
        "test_case": tc1_name,
        "csv": m_csv,
        "parquet": m_p,
        "partitioned": m_part
    })

    # -------------------------------------------------------------
    # Test Case 2: Lọc theo điều kiện cột và giá (Kiểm chứng Predicate Pushdown)
    # -------------------------------------------------------------
    tc2_name = "TC2: Predicate Pushdown (Category LIKE 'electronics%' & Price > 500)"
    print(f"\n[CHẠY] {tc2_name}...")

    m_csv = run_query_benchmark(spark, tc2_name, lambda: df_csv.filter(
        col(cat_col).startswith("electronics") & (col("price") > 500.0)
    ))
    m_p = run_query_benchmark(spark, tc2_name, lambda: df_parquet.filter(
        col(cat_col).startswith("electronics") & (col("price") > 500.0)
    ))
    m_part = run_query_benchmark(spark, tc2_name, lambda: df_partitioned.filter(
        col("category").startswith("electronics") & (col("price") > 500.0)
    ))

    results.append({
        "test_case": tc2_name,
        "csv": m_csv,
        "parquet": m_p,
        "partitioned": m_part
    })

    # -------------------------------------------------------------
    # Test Case 3: Tổng hợp phức tạp (Group By Category, Sum & Avg Price, Order By)
    # -------------------------------------------------------------
    tc3_name = "TC3: Heavy Aggregation (Group By Category, Sum/Avg, Sort)"
    print(f"\n[CHẠY] {tc3_name}...")

    def agg_query(df, c_name):
        return df.groupBy(col(c_name).alias("category")).agg(
            _sum("price").alias("total_rev"),
            _avg("price").alias("avg_rev"),
            _count(col("product_id")).alias("total_orders")
        ).orderBy(col("total_rev").desc())

    m_csv = run_query_benchmark(spark, tc3_name, lambda: agg_query(df_csv, cat_col))
    m_p = run_query_benchmark(spark, tc3_name, lambda: agg_query(df_parquet, cat_col))
    m_part = run_query_benchmark(spark, tc3_name, lambda: agg_query(df_partitioned, "category"))

    results.append({
        "test_case": tc3_name,
        "csv": m_csv,
        "parquet": m_p,
        "partitioned": m_part
    })

    # In kết quả dạng bảng Trace
    print("\n" + "=" * 95)
    print(f"{'KỊCH BẢN KIỂM THỬ':<40} | {'ĐỊNH DẠNG':<22} | {'THỜI GIAN (ms)':<15} | {'BỘ NHỚ (MB)':<12}")
    print("=" * 95)
    for r in results:
        tc = r["test_case"]
        print(f"{tc:<40} | {'1. CSV Raw':<22} | {r['csv']['duration_ms']:<15} | {r['csv']['ram_mb']:<12}")
        print(f"{'':<40} | {'2. Parquet Standalone':<22} | {r['parquet']['duration_ms']:<15} | {r['parquet']['ram_mb']:<12}")
        print(f"{'':<40} | {'3. Partitioned Parquet':<22} | {r['partitioned']['duration_ms']:<15} | {r['partitioned']['ram_mb']:<12}")
        print("-" * 95)

    # Lưu kết quả ra file JSON phục vụ Streamlit Dashboard
    script_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(script_dir, "output")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "benchmark_results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n[KẾT QUẢ] Đã xuất kết quả thực nghiệm ra: {out_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chạy Benchmark so sánh CSV vs Parquet")
    parser.add_argument("--csv", default="data/sample/orders_sample.csv")
    parser.add_argument("--parquet", default="data/refined/orders_unpartitioned.parquet")
    parser.add_argument("--part-parquet", default="data/refined/orders.parquet")

    args = parser.parse_args()
    spark_session = build_spark_session()
    spark_session.sparkContext.setLogLevel("ERROR")

    # Đảm bảo có dữ liệu parquet unpartitioned nếu chưa tồn tại
    if not os.path.exists(args.parquet) and os.path.exists(args.csv):
        print(f"[PREPARE] Đang chuẩn bị file Parquet không phân vùng tại: {args.parquet}...")
        tmp_df = spark_session.read.option("header", "true").csv(args.csv)
        tmp_df.write.mode("overwrite").parquet(args.parquet)

    execute_benchmarks(spark_session, args.csv, args.parquet, args.part_parquet)

