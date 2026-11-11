#!/usr/bin/env python3
"""
Unit Test: Kiểm thử chức năng Clickstream Event Generator
"""
import unittest
import sys
import os
from datetime import datetime, timedelta

# Thêm đường dẫn root vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from producer.event_generator import create_event, EVENT_TYPES

class TestEventGenerator(unittest.TestCase):

    def test_event_schema_and_types(self):
        """Kiểm tra schema và các trường bắt buộc của sự kiện."""
        event = create_event(late_data=False)
        required_fields = ["event_id", "user_id", "session_id", "product_id", "event_type", "timestamp", "is_late"]
        
        for field in required_fields:
            self.assertIn(field, event, f"Thiếu trường bắt buộc '{field}' trong JSON event")

        self.assertIn(event["event_type"], EVENT_TYPES, f"event_type không hợp lệ: {event['event_type']}")
        self.assertFalse(event["is_late"], "is_late phải là False đối với sự kiện thông thường")

    def test_timestamp_format(self):
        """Kiểm tra định dạng timestamp theo chuẩn YYYY-MM-DD HH:MM:SS."""
        event = create_event(late_data=False)
        try:
            parsed_time = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
            self.assertIsInstance(parsed_time, datetime)
        except ValueError:
            self.fail(f"Timestamp không đúng định dạng YYYY-MM-DD HH:MM:SS: {event['timestamp']}")

    def test_late_data_generation(self):
        """Kiểm tra dữ liệu trễ: timestamp phải lùi về quá khứ ít nhất 10 phút."""
        event = create_event(late_data=True)
        self.assertTrue(event["is_late"], "is_late phải là True đối với sự kiện trễ")

        event_time = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
        now = datetime.now()
        diff_minutes = (now - event_time).total_seconds() / 60.0

        self.assertGreaterEqual(diff_minutes, 9.5, f"Dữ liệu trễ phải cách hiện tại ít nhất 10 phút, thực tế: {diff_minutes:.2f} phút")

if __name__ == "__main__":
    unittest.main()

