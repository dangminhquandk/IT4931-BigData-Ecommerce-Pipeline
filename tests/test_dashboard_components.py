#!/usr/bin/env python3
"""
Unit Test: Kiểm thử tính toàn vẹn của Dashboard components & Mock Callbacks
"""
import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

class TestDashboardComponents(unittest.TestCase):

    def test_mock_streaming_schema(self):
        """Kiểm tra schema và dữ liệu mock streaming cho Tab 1."""
        from dashboard.app import mock_streaming_data
        df = mock_streaming_data()
        expected_cols = ["window_start", "event_type", "event_count", "unique_users"]
        for col in expected_cols:
            self.assertIn(col, df.columns, f"Thiếu cột '{col}' trong mock streaming data")
        self.assertFalse(df.empty, "Mock streaming dataframe không được rỗng")
        self.assertTrue((df["event_count"] >= 0).all(), "event_count phải là số không âm")

    def test_mock_revenue_schema(self):
        """Kiểm tra schema và dữ liệu mock doanh thu cho Tab 2."""
        from dashboard.app import mock_revenue_data
        df = mock_revenue_data()
        expected_cols = ["year", "month", "category", "total_revenue", "total_orders", "avg_order_value"]
        for col in expected_cols:
            self.assertIn(col, df.columns, f"Thiếu cột '{col}' trong mock revenue data")
        self.assertFalse(df.empty, "Mock revenue dataframe không được rỗng")
        self.assertTrue((df["total_revenue"] >= 0).all(), "total_revenue phải là số không âm")

    def test_mock_top_products_schema(self):
        """Kiểm tra schema và danh sách Top 10 sản phẩm cho Tab 2."""
        from dashboard.app import mock_top_products_data
        df = mock_top_products_data()
        expected_cols = ["rank_position", "product_id", "category", "units_sold", "total_revenue"]
        for col in expected_cols:
            self.assertIn(col, df.columns, f"Thiếu cột '{col}' trong mock top products data")
        self.assertEqual(len(df), 10, "Danh sách top products phải có đúng 10 sản phẩm")
        # Kiểm tra thứ tự rank_position từ 1 đến 10
        self.assertListEqual(list(df["rank_position"]), list(range(1, 11)))

    def test_mock_funnel_schema(self):
        """Kiểm tra schema phễu đơn hàng cho Tab 2."""
        from dashboard.app import mock_funnel_data
        df = mock_funnel_data()
        expected_cols = ["status", "total_count", "conversion_rate"]
        for col in expected_cols:
            self.assertIn(col, df.columns, f"Thiếu cột '{col}' trong mock funnel data")
        self.assertAlmostEqual(df["conversion_rate"].sum(), 100.0, delta=0.5)

    def test_mock_rfm_schema(self):
        """Kiểm tra schema và dữ liệu phân cụm RFM cho Tab 3 (Spark MLlib K-Means)."""
        from dashboard.app import mock_rfm_data
        df = mock_rfm_data()
        expected_cols = ["segment_id", "segment_name", "customer_count", "avg_recency_days", "avg_frequency_orders", "avg_monetary_spend", "total_segment_revenue", "strategy_recommendation"]
        for col in expected_cols:
            self.assertIn(col, df.columns, f"Thiếu cột '{col}' trong mock RFM data")
        self.assertEqual(len(df), 4, "Mô hình K-Means phải có đúng 4 cụm phân khúc khách hàng")
        self.assertTrue((df["customer_count"] > 0).all(), "Số lượng khách hàng mỗi cụm phải lớn hơn 0")
        self.assertTrue((df["total_segment_revenue"] > 0).all(), "Doanh thu mỗi cụm phải lớn hơn 0")

if __name__ == "__main__":
    unittest.main()

