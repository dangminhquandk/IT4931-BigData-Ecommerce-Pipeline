#!/usr/bin/env python3
"""
Unit Test: Kiểm thử logic nghiệp vụ & toán học của các phép tổng hợp Batch Analytics
"""
import unittest
import csv
import os
from collections import defaultdict

class TestBatchAnalyticsMath(unittest.TestCase):

    def setUp(self):
        self.sample_csv = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "sample", "orders_sample.csv"))
        self.assertTrue(os.path.exists(self.sample_csv), f"Không tìm thấy file mẫu tại {self.sample_csv}")

    def test_sample_csv_data_integrity(self):
        """Kiểm tra tính hợp lệ của dữ liệu trong file sample orders."""
        with open(self.sample_csv, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.assertGreaterEqual(len(rows), 1000, "File sample orders phải có ít nhất 1,000 bản ghi")

        total_revenue = 0.0
        funnel_counts = defaultdict(int)
        product_sales = defaultdict(lambda: {"units": 0, "rev": 0.0, "category": ""})

        for r in rows:
            # Kiểm tra các trường bắt buộc không được rỗng
            self.assertTrue(r["order_id"].startswith("ORD-"), f"Mã order_id không hợp lệ: {r['order_id']}")
            self.assertTrue(r["product_id"].startswith("PROD-"), f"Mã product_id không hợp lệ: {r['product_id']}")

            price = float(r["price"])
            self.assertGreater(price, 0, f"Giá tiền phải lớn hơn 0, thực tế: {price}")
            total_revenue += price

            # Kiểm tra trạng thái đơn hàng
            status = r["order_status"]
            self.assertIn(status, ["COMPLETED", "PROCESSING", "CANCELLED", "RETURNED"])
            funnel_counts[status] += 1

            if status == "COMPLETED":
                pid = r["product_id"]
                product_sales[pid]["units"] += 1
                product_sales[pid]["rev"] += price
                product_sales[pid]["category"] = r["category"]

        # Kiểm tra tính toán phễu: tổng các phần trăm phải bằng 100%
        total_orders = len(rows)
        percentage_sum = sum(round((cnt / total_orders) * 100, 2) for cnt in funnel_counts.values())
        self.assertAlmostEqual(percentage_sum, 100.0, delta=0.5, msg="Tổng tỷ lệ phần trăm phễu đơn hàng phải tiệm cận 100%")

        # Kiểm tra Top 10 sản phẩm
        sorted_products = sorted(product_sales.items(), key=lambda x: (x[1]["units"], x[1]["rev"]), reverse=True)
        top_10 = sorted_products[:10]
        self.assertEqual(len(top_10), 10, "Phải xác định được đúng 10 sản phẩm bán chạy nhất")
        self.assertGreaterEqual(top_10[0][1]["units"], top_10[9][1]["units"], "Thứ hạng Top 1 phải có số lượng bán >= Top 10")

if __name__ == "__main__":
    unittest.main()

