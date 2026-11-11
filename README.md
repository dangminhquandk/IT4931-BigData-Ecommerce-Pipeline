# Hệ Thống Đường Ống Dữ Liệu Thương Mại Điện Tử Thời Gian Thực
> **Học phần:** IT4931 - Lưu Trữ và Xử Lý Dữ Liệu Lớn  
> **Đơn vị thực hiện:** Nhóm 3 (Lớp 172912 - KHMT K68, ĐHBK Hà Nội)  
> **Giảng viên hướng dẫn:** TS. Vũ Tuyết Trinh  

---

## 👥 Danh Sách Thành Viên & Phân Công Nhiệm Vụ

| Họ và tên | MSSV | Module phụ trách | Deliverables bàn giao |
| :--- | :---: | :--- | :--- |
| **Phạm Văn Quyết** | 202416331 | **Hạ tầng & Ingestion** | `docker-compose.yml`, `producer/event_generator.py`, Topic `clickstream-events` |
| **Vũ Việt Tiến** | 202416362 | **HDFS & Xử lý Batch** | `jobs/batch_etl_raw_to_parquet.py`, `jobs/batch_analytics_orders.py`, Dữ liệu Parquet HDFS |
| **Nguyễn Xuân Hồng Quân** | 202416321 | **Xử lý Luồng (Streaming)** | `jobs/streaming_clickstream_processor.py`, HDFS Checkpoint directory |
| **Đặng Minh Quân** | 202416323 | **Serving, Dashboard & Benchmark** | `scripts/init_serving_db.sql`, `dashboard/app.py`, `benchmark/run_benchmark.py`, Báo cáo & Slide |

---

## 🏗️ Kiến Trúc Hệ Thống (Lambda Architecture)

Hệ thống kết hợp 2 nhánh xử lý:
1. **Batch Layer (Nhánh Khối)**:
   - Dữ liệu lịch sử Kaggle (>1M bản ghi giao dịch) lưu tại HDFS `/data/raw/orders_1m.csv`.
   - PySpark ETL chuyển đổi sang Parquet nén Snappy, phân vùng theo `year=/month=/` tại `/data/refined/orders.parquet`.
   - PySpark Analytics tính doanh thu, Top 10 sản phẩm, phễu đơn hàng và phân cụm khách hàng RFM bằng Spark MLlib K-Means (k=4) ghi vào PostgreSQL Serving DB.
2. **Speed Layer (Nhánh Tốc Độ)**:
   - Python Event Generator giả lập luồng clickstream đẩy vào Kafka topic `clickstream-events`.
   - Spark Structured Streaming xử lý Event-Time với **Watermarking 10 phút** (lọc Late Data) và **Sliding Window 1 phút (trượt 10s)**.
   - Upsert kết quả real-time vào bảng `stream_realtime_traffic` trên PostgreSQL.
3. **Serving & Visualization Layer**:
   - PostgreSQL phục vụ lưu trữ kết quả (6 bảng Serving có B-Tree Indexes).
   - Streamlit Dashboard (`dashboard/app.py`) trực quan hóa 4 tabs: Giám sát luồng Kafka, Báo cáo kinh doanh 1M dòng, Phân cụm khách hàng RFM K-Means, và Benchmark lưu trữ SQL.
   - Bộ thực nghiệm Benchmark (`benchmark/run_benchmark.py`) chứng minh hiệu quả của **Predicate Pushdown** và **Partition Pruning** (tăng tốc gấp **5.3 lần**).

---

## 📚 Tài Liệu & Báo Cáo Học Thuật

Toàn bộ hệ thống tài liệu đồ án được chuẩn hóa theo khung đề cương môn học IT4931 và bám sát các slide bài giảng của **TS. Vũ Tuyết Trinh**:

- 📄 **[Báo Cáo Chi Tiết Đồ Án (Master Report)](docs/BAO_CAO_CHI_TIET_DU_AN_IT4931.md)**: Bản thuyết minh hoàn chỉnh 7 chương (~47 trang chuẩn mực), bao gồm mục tiêu, phân công 4 thành viên, cơ sở lý thuyết (HDFS, Kafka, Spark, Lambda), thiết kế chi tiết 4 tầng xử lý, phân cụm khách hàng Spark MLlib K-Means và kết quả thực nghiệm.
- 📐 **[Cơ Sở Lý Thuyết Chuyên Sâu (Architecture & Theory)](docs/ARCHITECTURE_AND_THEORY.md)**: Phân tích cơ chế bản chất: Căn chỉnh HDFS Block & Parquet Row Group 128MB, Engine Spark Catalyst/Tungsten, Kafka Log Compaction/ISR, Streaming Watermark & State Store.
- 📊 **[Báo Cáo Thực Nghiệm Hiệu Năng (Experimental Report)](docs/EXPERIMENTAL_REPORT.md)**: Đo lường định lượng trên 1.000.000 bản ghi Kaggle thực tế: Tiết kiệm 78.4% dung lượng đĩa, tăng tốc truy vấn 5.3 lần nhờ Columnar Storage + Predicate Pushdown + Partition Pruning.
- 📓 **[Jupyter Notebook Thuyết Minh Thực Nghiệm](notebooks/IT4931_Ecommerce_Analysis_and_Benchmark.ipynb)**: Mã nguồn phân tích EDA, trích xuất đặc trưng RFM, huấn luyện K-Means và biểu đồ so sánh latency.

---

## 🚀 Hướng Dẫn Khởi Chạy Nhanh (Quick Start)

### 1. Khởi động hạ tầng Docker
```bash
# Khởi chạy cụm Zookeeper, Kafka, HDFS, Spark, PostgreSQL
make up

# Kiểm tra trạng thái các container
make ps
```

### 2. Khởi tạo dữ liệu trên HDFS
```bash
make init-hdfs
```

### 3. Thực thi nhánh Batch (ETL & Analytics)
```bash
# 1. Chạy ETL chuyển đổi CSV sang Parquet phân vùng
make run-batch-etl

# 2. Chạy phân tích doanh thu & top sản phẩm ghi vào PostgreSQL
make run-batch-analytics
```

### 4. Thực thi nhánh Streaming (Clickstream Processing)
```bash
# Terminal 1: Chạy Spark Structured Streaming
make run-streaming

# Terminal 2: Chạy Generator sinh luồng sự kiện clickstream
make run-producer
```

### 5. Khởi chạy Dashboard & Benchmark
```bash
# Chạy bộ thực nghiệm Benchmark SQL
make run-benchmark

# Khởi chạy Streamlit Dashboard
make run-dashboard
```

### 6. Chạy bộ kiểm thử tự động (Unit Tests)
```bash
make test
```

---

## 📂 Cấu Trúc Thư Mục Dự Án

```text
.
├── docker-compose.yml                  # Khởi dựng Zookeeper, Kafka, HDFS, Spark, Postgres
├── Makefile                            # Bộ lệnh điều khiển tự động hóa cho nhóm
├── docs/                               # Bộ tài liệu học thuật & báo cáo đồ án IT4931
│   ├── BAO_CAO_CHI_TIET_DU_AN_IT4931.md # Báo cáo chi tiết đồ án (~47 trang A4 chuẩn BKHN)
│   ├── ARCHITECTURE_AND_THEORY.md      # Cơ sở lý thuyết chuyên sâu (HDFS, Kafka, Spark)
│   └── EXPERIMENTAL_REPORT.md          # Báo cáo thực nghiệm 1M dòng & Benchmark định lượng
├── notebooks/
│   └── IT4931_Ecommerce_Analysis_and_Benchmark.ipynb # Notebook phân tích EDA & RFM MLlib
├── src/                                # Source code module hóa (Python package)
│   ├── batch/                          # PySpark Batch ETL & MLlib RFM Customer Segmentation
│   ├── streaming/                      # Spark Structured Streaming with Watermarking
│   ├── serving/                        # Client kết nối PostgreSQL Serving Database
│   ├── ingestion/                      # Clickstream Event Generator (hỗ trợ spike/late data)
│   ├── storage/                        # Tiện ích quản lý HDFS Client
│   ├── dashboard/                      # Trực quan hóa Streamlit components
│   └── benchmark/                      # Đo lường hiệu năng truy vấn & I/O
├── config/
│   └── hadoop/hadoop.env               # Cấu hình HDFS block size 128MB, replication 1
├── data/
│   ├── raw/                            # Chứa orders_1m.csv từ Kaggle (gitignore)
│   └── sample/                         # Dữ liệu mẫu 5,000 bản ghi để test nhanh
├── producer/
│   ├── requirements.txt
│   └── event_generator.py              # CLI Generator sinh clickstream
├── jobs/
│   ├── batch_etl_raw_to_parquet.py     # PySpark ETL: CSV -> Parquet Snappy Partitioned
│   ├── batch_analytics_orders.py       # PySpark Batch: Doanh thu, Phễu & RFM K-Means
│   └── streaming_clickstream_processor.py # Spark Streaming: Sliding Window + Watermark
├── scripts/
│   ├── init_serving_db.sql             # DDL 6 bảng Serving & B-Tree Indexes
│   └── prepare_kaggle_data.py          # Script trích xuất 1M bản ghi chuẩn từ Kaggle
├── dashboard/
│   ├── requirements.txt
│   └── app.py                          # Streamlit Dashboard 4 tabs tương tác
├── benchmark/
│   ├── run_benchmark.py                # Script benchmark đo thời gian, RAM, CPU
│   └── output/                         # Chứa file kết quả JSON benchmark
├── tests/
│   ├── run_all_tests.py                # Runner tự động chạy toàn bộ 13 test suites
│   ├── test_event_generator.py         # Kiểm thử schema clickstream, late data
│   ├── test_sql_ddl.py                 # Kiểm thử schema PostgreSQL DDL, PK & Index
│   ├── test_batch_analytics_math.py    # Kiểm thử logic tính doanh thu & sample data
│   ├── test_dashboard_components.py    # Kiểm thử schema mock data cho UI Streamlit & RFM
│   └── test_batch_etl_adaptive_schema.py # Kiểm thử auto-detect schema Kaggle vs Standard
├── project/                            # Báo cáo đề xuất Word và Bảng phân công nhiệm vụ
└── slide/                              # Slide bài giảng và sơ đồ kiến trúc diagram.md
```

