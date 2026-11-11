#!/usr/bin/env python3
"""
Test Runner: Tự động chạy toàn bộ bộ kiểm thử của dự án Nhóm 3 (IT4931)
"""
import unittest
import sys
import os

def run_suite():
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(tests_dir)
    sys.path.insert(0, project_root)

    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=tests_dir, pattern="test_*.py")

    print("=" * 80)
    print(" BẮT ĐẦU KIỂM THỬ TỰ ĐỘNG TOÀN DIỆN DỰ ÁN NHÓM 3 (IT4931)")
    print("=" * 80)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 80)
    print(f"KẾT QUẢ KIỂM THỬ: {result.testsRun} tests đã chạy.")
    if result.wasSuccessful():
        print("TẤT CẢ CÁC BÀI TEST ĐỀU THÀNH CÔNG (PASSED 100%)")
        print("=" * 80)
        return 0
    else:
        print(f"THẤT BẠI: {len(result.failures)} lỗi, {len(result.errors)} exceptions.")
        print("=" * 80)
        return 1

if __name__ == "__main__":
    sys.exit(run_suite())

