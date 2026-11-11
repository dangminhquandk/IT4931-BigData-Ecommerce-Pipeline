#!/usr/bin/env python3
"""
CLICKSTREAM EVENT GENERATOR (MÔ PHỎNG LUỒNG DỮ LIỆU TƯƠNG TÁC TMĐT)
Học phần: IT4931 - Nhóm 3
Phụ trách: Phạm Văn Quyết (MSSV: 202416331)

Chức năng:
- Sinh luồng sự kiện clickstream JSON đẩy vào Kafka topic 'clickstream-events'.
- Hỗ trợ mô phỏng dữ liệu trễ (Late Data) để kiểm thử cơ chế Watermarking trên Spark Streaming.
- Hỗ trợ mô phỏng lưu lượng đột biến (Traffic Spike) để kiểm thử năng lực chịu tải của cụm.
- Hỗ trợ chế độ dry-run (in ra màn hình mà không cần kết nối Kafka thật).
"""

import argparse
import json
import random
import time
from datetime import datetime, timedelta

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

EVENT_TYPES = ["view", "cart", "purchase", "remove_from_cart"]
EVENT_WEIGHTS = [0.70, 0.18, 0.08, 0.04]  # Tỷ lệ hành vi thực tế trên sàn TMĐT

def create_event(late_data=False):
    """Tạo một bản ghi sự kiện clickstream JSON."""
    now = datetime.now()
    if late_data:
        # Giả lập dữ liệu đến muộn từ 12 đến 25 phút (vượt quá hoặc tiệm cận watermark 10 phút)
        delay_minutes = random.randint(12, 25)
        event_time = now - timedelta(minutes=delay_minutes)
    else:
        event_time = now

    user_id = f"user_{random.randint(1, 1000)}"
    product_id = f"PROD-{random.randint(100, 300)}"
    session_id = f"sess_{random.randint(1, 500)}_{now.strftime('%Y%m%d')}"
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]

    return {
        "event_id": f"evt_{int(time.time() * 1000)}_{random.randint(100, 999)}",
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product_id,
        "event_type": event_type,
        "timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "is_late": late_data
    }

def run_generator(args):
    producer = None
    if not args.dry_run:
        if not KAFKA_AVAILABLE:
            print("[LỖI] Thư viện 'kafka-python' chưa được cài đặt. Chạy 'pip install kafka-python' hoặc dùng flag --dry-run")
            return
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print(f"[KAFKA] Kết nối thành công tới {args.bootstrap_servers}, chuẩn bị đẩy vào topic '{args.topic}'")
        except Exception as e:
            print(f"[CẢNH BÁO] Không thể kết nối Kafka ({e}). Tự động chuyển sang chế độ dry-run.")
            args.dry_run = True

    print(f"[BẮT ĐẦU] Sinh sự kiện: Rate={args.rate} msg/s, LateRatio={args.late_ratio * 100}%, SpikeInterval={args.spike_interval}s")

    count = 0
    start_time = time.time()

    try:
        while True:
            if args.max_events > 0 and count >= args.max_events:
                print(f"[HOÀN THÀNH] Đã đạt giới hạn max_events={args.max_events}")
                break
            # Kiểm tra thời điểm tạo Traffic Spike
            elapsed = time.time() - start_time
            in_spike = (args.spike_interval > 0) and (int(elapsed) % args.spike_interval < 5)
            current_rate = args.rate * 5 if in_spike else args.rate

            # Xác định xem sự kiện này có phải là Late Data hay không
            is_late = random.random() < args.late_ratio
            event = create_event(late_data=is_late)

            if args.dry_run:
                spike_tag = " [SPIKE!]" if in_spike else ""
                late_tag = " [LATE DATA]" if is_late else ""
                print(f"[EMIT{spike_tag}{late_tag}] {json.dumps(event)}")
            else:
                key = event["user_id"].encode("utf-8") if args.key_by_user else None
                producer.send(args.topic, key=key, value=event)

            count += 1
            if count % 100 == 0 and not args.dry_run:
                producer.flush()
                print(f"[PROGRESS] Đã gửi {count} messages vào Kafka topic '{args.topic}'")

            # Điều tiết tốc độ
            time.sleep(1.0 / max(current_rate, 1))

    except KeyboardInterrupt:
        print(f"\n[DỪNG] Đã phát sinh tổng cộng {count} sự kiện.")
        if producer:
            producer.flush()
            producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sinh sự kiện clickstream TMĐT vào Kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:29092", help="Địa chỉ Kafka bootstrap servers (mặc định: localhost:29092)")
    parser.add_argument("--topic", default="clickstream-events", help="Tên Kafka topic (mặc định: clickstream-events)")
    parser.add_argument("--rate", type=float, default=5.0, help="Số lượng sự kiện mỗi giây (mặc định: 5.0)")
    parser.add_argument("--late-ratio", type=float, default=0.05, help="Tỷ lệ sinh dữ liệu trễ để test Watermark (0.0 đến 1.0, mặc định: 0.05)")
    parser.add_argument("--spike-interval", type=int, default=30, help="Chu kỳ xuất hiện spike (giây, 0 = không có spike)")
    parser.add_argument("--key-by-user", action="store_true", default=True, help="Dùng user_id làm Partition Key khi gửi vào Kafka (mặc định: True)")
    parser.add_argument("--max-events", type=int, default=0, help="Số lượng sự kiện tối đa cần sinh (0 = chạy vô tận)")
    parser.add_argument("--dry-run", action="store_true", help="Chạy thử nghiệm in ra terminal không gửi qua mạng")

    args = parser.parse_args()
    run_generator(args)
