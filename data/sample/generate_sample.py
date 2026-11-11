#!/usr/bin/env python3
"""
Tạo tập dữ liệu mẫu đơn hàng TMĐT phục vụ kiểm thử cục bộ (5,000 bản ghi)
Tương thích với schema Kaggle eCommerce được đề xuất trong đồ án.
"""
import csv
import random
from datetime import datetime, timedelta

CATEGORIES = [
    "electronics.smartphone", "electronics.laptop", "appliances.kitchen.refrigerators",
    "appliances.personal.massager", "computers.peripherals.mouse", "apparel.shoes",
    "electronics.audio.headphone", "furniture.living_room.sofa"
]
STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "PROCESSING", "CANCELLED", "RETURNED"]

def generate_sample_csv(filename="data/sample/orders_sample.csv", num_records=5000):
    start_date = datetime(2025, 1, 1, 0, 0, 0)
    
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id", "price", "category", "order_status", "timestamp"])
        
        for i in range(1, num_records + 1):
            order_id = f"ORD-{i:07d}"
            customer_id = f"CUST-{random.randint(1000, 9999)}"
            product_id = f"PROD-{random.randint(100, 300)}"
            price = round(random.uniform(15.0, 1500.0), 2)
            category = random.choice(CATEGORIES)
            order_status = random.choice(STATUSES)
            
            # Phân bổ đều qua các tháng trong năm 2025 để kiểm thử partitioning
            random_seconds = random.randint(0, 365 * 24 * 3600)
            order_time = (start_date + timedelta(seconds=random_seconds)).strftime("%Y-%m-%d %H:%M:%S")
            
            writer.writerow([order_id, customer_id, product_id, price, category, order_status, order_time])
            
    print(f"Đã tạo thành công {num_records} bản ghi mẫu tại: {filename}")

if __name__ == "__main__":
    generate_sample_csv()

