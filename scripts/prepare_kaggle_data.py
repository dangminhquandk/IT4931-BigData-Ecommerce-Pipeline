#!/usr/bin/env python3
"""
TIỆN ÍCH CHUẨN BỊ VÀ TRÍCH XUẤT 1M BẢN GHI TỪ KAGGLE ECOMMERCE DATASET
Học phần: IT4931 - Nhóm 3
Phụ trách: Vũ Việt Tiến (MSSV: 202416362) & Đặng Minh Quân

Đường dẫn dataset:
https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
"""

import os
import sys
import glob
import subprocess

def find_kaggle_file():
    """Tìm kiếm file Kaggle 2019-Oct.csv hoặc 2019-Nov.csv trong máy."""
    search_dirs = [
        "data/raw",
        os.path.expanduser("~/Downloads"),
        os.path.expanduser("~/Downloads/ecommerce*"),
        os.path.expanduser("~")
    ]
    patterns = ["*2019-Oct.csv", "*2019-Nov.csv", "*2019-Oct.csv.zip", "*ecommerce*.zip"]
    
    for s_dir in search_dirs:
        for p in patterns:
            found = glob.glob(os.path.join(s_dir, p))
            if found:
                return found[0]
    return None

def extract_1m_records(source_file, target_file="data/raw/orders_1m.csv", num_records=1000000):
    print(f"[NGUỒN] Tìm thấy file gốc: {source_file}")
    os.makedirs(os.path.dirname(target_file), exist_ok=True)

    # Nếu là file zip, giải nén trực tiếp vào data/raw
    if source_file.endswith(".zip"):
        print(f"[ZIP] Đang giải nén tập tin zip...")
        subprocess.run(["unzip", "-q", "-o", source_file, "-d", "data/raw/"])
        # Sau khi giải nén, tìm file csv
        unzipped_files = glob.glob("data/raw/*2019-*.csv")
        if unzipped_files:
            source_file = unzipped_files[0]
            print(f"[UNZIPPED] File CSV sau giải nén: {source_file}")
        else:
            print("[LỖI] Không tìm thấy file 2019-*.csv trong tệp zip vừa giải nén.")
            return False

    print(f"[TRÍCH XUẤT] Đang trích xuất đúng {num_records:,} bản ghi đầu tiên vào {target_file}...")
    # head -n (num_records + 1) để lấy kèm dòng header
    lines_to_take = num_records + 1
    cmd = f"head -n {lines_to_take} '{source_file}' > '{target_file}'"
    subprocess.run(cmd, shell=True, check=True)

    file_size_mb = os.path.getsize(target_file) / (1024 * 1024)
    print(f"[THÀNH CÔNG] Đã tạo file: {target_file}")
    print(f"             Dung lượng: {file_size_mb:.2f} MB (~1 HDFS Block 128MB chuẩn đồ án)")
    return True

def main():
    print("=" * 80)
    print(" CÔNG CỤ CHUẨN BỊ TẬP DỮ LIỆU KAGGLE 1M BẢN GHI (NHÓM 3 - IT4931)")
    print("=" * 80)

    target = "data/raw/orders_1m.csv"
    if os.path.exists(target):
        size_mb = os.path.getsize(target) / (1024 * 1024)
        print(f"[THÔNG BÁO] File {target} đã tồn tại sẵn (Dung lượng: {size_mb:.2f} MB).")
        overwrite = input("Bạn có muốn ghi đè lại từ file gốc không? [y/N]: ").strip().lower()
        if overwrite != "y":
            print("Giữ nguyên file hiện tại. Hoàn tất.")
            return

    found = find_kaggle_file()
    if found:
        extract_1m_records(found, target)
    else:
        print("[HƯỚNG DẪN TẢI DATASET TỪ KAGGLE]")
        print("1. Truy cập liên kết: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store")
        print("2. Nhấn nút 'Download' (hoặc tải file 2019-Oct.csv dung lượng ~5GB).")
        print("3. Di chuyển file tải về vào thư mục 'data/raw/' hoặc để trong 'Downloads'.")
        print("4. Chạy lại script này: python3 scripts/prepare_kaggle_data.py")
        print("-" * 80)
        print("LƯU Ý: Trong khi chưa tải file 5GB từ Kaggle, hệ thống đã nạp sẵn 5,000 bản ghi mẫu")
        print("       tại 'data/sample/orders_sample.csv' để kiểm thử trơn tru toàn bộ pipeline.")
        print("=" * 80)

if __name__ == "__main__":
    main()

