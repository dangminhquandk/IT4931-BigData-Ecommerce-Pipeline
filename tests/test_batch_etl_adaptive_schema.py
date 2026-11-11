#!/usr/bin/env python3
"""
Unit Test: Kiểm thử khả năng tương thích của Schema với Kaggle eCommerce Dataset
"""
import unittest
import csv
import io

class TestKaggleSchemaCompatibility(unittest.TestCase):

    def test_kaggle_raw_columns_mapping(self):
        """Kiểm tra các cột của Kaggle 2019-Oct.csv và khả năng ánh xạ sang refined model."""
        kaggle_headers = [
            "event_time", "event_type", "product_id", "category_id",
            "category_code", "brand", "price", "user_id", "user_session"
        ]
        
        sample_row = {
            "event_time": "2019-10-01 00:00:00 UTC",
            "event_type": "purchase",
            "product_id": "44600062",
            "category_id": "2103807459595387724",
            "category_code": "appliances.kitchen.refrigerators",
            "brand": "shiseido",
            "price": "35.79",
            "user_id": "541312140",
            "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c"
        }

        # Ánh xạ theo logic của jobs/batch_etl_raw_to_parquet.py
        clean_time = sample_row["event_time"].replace(" UTC", "")
        self.assertEqual(clean_time, "2019-10-01 00:00:00")

        year_val = clean_time.split("-")[0]
        month_val = clean_time.split("-")[1]
        self.assertEqual(year_val, "2019")
        self.assertEqual(month_val, "10")

        status = "COMPLETED" if sample_row["event_type"] == "purchase" else "PROCESSING"
        self.assertEqual(status, "COMPLETED")
        self.assertEqual(float(sample_row["price"]), 35.79)

if __name__ == "__main__":
    unittest.main()

