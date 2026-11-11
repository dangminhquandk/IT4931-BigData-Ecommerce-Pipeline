#!/usr/bin/env python3
"""
Unit Test: Kiểm thử tính toàn vẹn và cú pháp của scripts/init_serving_db.sql
"""
import unittest
import os
import re
import sqlite3

class TestSqlDDL(unittest.TestCase):

    def setUp(self):
        self.sql_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts", "init_serving_db.sql"))
        self.assertTrue(os.path.exists(self.sql_path), f"Không tìm thấy file DDL tại {self.sql_path}")
        with open(self.sql_path, "r", encoding="utf-8") as f:
            self.sql_content = f.read()

    def test_required_tables_exist(self):
        """Kiểm tra sự tồn tại của 5 bảng cốt lõi trong Serving Layer."""
        expected_tables = [
            "batch_revenue_monthly",
            "batch_top_products",
            "batch_order_funnel",
            "stream_realtime_traffic",
            "benchmark_results"
        ]
        for tbl in expected_tables:
            pattern = rf"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?{tbl}\b"
            match = re.search(pattern, self.sql_content, re.IGNORECASE)
            self.assertIsNotNone(match, f"Bảng '{tbl}' chưa được định nghĩa trong init_serving_db.sql")

    def test_primary_keys_and_indexes(self):
        """Kiểm tra các khóa chính và Composite Index bắt buộc."""
        # Khóa chính composite trên bảng batch_revenue_monthly (year, month, category)
        self.assertRegex(self.sql_content, r"PRIMARY\s+KEY\s*\(\s*year\s*,\s*month\s*,\s*category\s*\)", "Thiếu composite PK trên batch_revenue_monthly")

        # Khóa chính composite trên stream_realtime_traffic (window_start, window_end, event_type)
        self.assertRegex(self.sql_content, r"PRIMARY\s+KEY\s*\(\s*window_start\s*,\s*window_end\s*,\s*event_type\s*\)", "Thiếu composite PK trên stream_realtime_traffic")

        # Kiểm tra Index
        expected_indexes = [
            "idx_batch_revenue_time",
            "idx_batch_top_products_rank",
            "idx_stream_traffic_latest",
            "idx_benchmark_query"
        ]
        for idx in expected_indexes:
            self.assertIn(idx, self.sql_content, f"Thiếu index '{idx}' trong init_serving_db.sql")

    def test_sqlite_ddl_compatibility(self):
        """Chuyển đổi nhẹ cú pháp PostgreSQL sang SQLite in-memory để kiểm thử cấu trúc cột."""
        # Thay thế các kiểu dữ liệu đặc thù Postgres để kiểm tra cú pháp trên SQLite
        compat_sql = self.sql_content
        compat_sql = re.sub(r"SERIAL\s+PRIMARY\s+KEY", "INTEGER PRIMARY KEY AUTOINCREMENT", compat_sql, flags=re.I)
        compat_sql = re.sub(r"TIMESTAMP\s+WITH\s+TIME\s+ZONE", "DATETIME", compat_sql, flags=re.I)
        compat_sql = re.sub(r"NUMERIC\(\d+,\s*\d+\)", "REAL", compat_sql, flags=re.I)

        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        try:
            cursor.executescript(compat_sql)
            # Kiểm tra xem các bảng đã tạo thành công trong SQLite chưa
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            self.assertIn("batch_revenue_monthly", tables)
            self.assertIn("batch_top_products", tables)
            self.assertIn("stream_realtime_traffic", tables)
        except Exception as e:
            self.fail(f"Lỗi cú pháp DDL khi kiểm tra cấu trúc: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    unittest.main()

