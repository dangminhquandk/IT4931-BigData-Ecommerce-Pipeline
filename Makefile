.PHONY: help up down ps logs init-hdfs run-producer run-batch-etl run-batch-analytics run-streaming run-benchmark run-dashboard test

help:
	@echo "=========================================================================="
	@echo " HỆ THỐNG ĐƯỜNG ỐNG DỮ LIỆU THƯƠNG MẠI ĐIỆN TỬ (NHÓM 3 - IT4931)"
	@echo "=========================================================================="
	@echo "  make up                  : Khởi động toàn bộ cụm dịch vụ Docker Compose"
	@echo "  make down                : Dừng và dọn dẹp các container"
	@echo "  make ps                  : Kiểm tra trạng thái các container"
	@echo "  make logs                : Xem logs cụm container"
	@echo "  make prepare-kaggle      : Trích xuất 1M bản ghi từ file Kaggle 2019-Oct.csv"
	@echo "  make init-hdfs           : Tạo thư mục trên HDFS và nạp dữ liệu mẫu"
	@echo "  make run-producer        : Chạy script phát sinh sự kiện clickstream (Producer)"
	@echo "  make run-batch-etl       : Chạy PySpark Job chuyển CSV sang Partitioned Parquet"
	@echo "  make run-batch-analytics : Chạy PySpark Job phân tích doanh thu & top sản phẩm"
	@echo "  make run-streaming       : Chạy Spark Structured Streaming xử lý Clickstream"
	@echo "  make run-benchmark       : Chạy bộ thực nghiệm so sánh CSV vs Parquet"
	@echo "  make run-dashboard       : Khởi chạy Streamlit Web Dashboard"
	@echo "=========================================================================="

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

prepare-kaggle:
	python3 scripts/prepare_kaggle_data.py

init-hdfs:
	@echo "Đang khởi tạo thư mục trên HDFS..."
	docker exec -it namenode hdfs dfs -mkdir -p /data/raw
	docker exec -it namenode hdfs dfs -mkdir -p /data/refined
	docker exec -it namenode hdfs dfs -mkdir -p /checkpoints/clickstream
	@echo "Nạp file mẫu orders_sample.csv vào HDFS..."
	docker exec -it namenode hdfs dfs -put -f /data/sample/orders_sample.csv /data/raw/orders_1m.csv
	docker exec -it namenode hdfs dfs -ls -R /data

run-producer:
	python3 producer/event_generator.py --rate 10 --spike-interval 20

run-batch-etl:
	docker exec -it bigdata-spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/jobs/batch_etl_raw_to_parquet.py \
		--input hdfs://namenode:9000/data/raw/orders_1m.csv \
		--output hdfs://namenode:9000/data/refined/orders.parquet

run-batch-analytics:
	docker exec -it bigdata-spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages org.postgresql:postgresql:42.6.0 \
		/opt/spark/jobs/batch_analytics_orders.py \
		--input hdfs://namenode:9000/data/refined/orders.parquet \
		--pg-url jdbc:postgresql://postgres-serving:5432/ecommerce_serving \
		--pg-user postgres \
		--pg-password postgrespassword

run-streaming:
	docker exec -it bigdata-spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
		/opt/spark/jobs/streaming_clickstream_processor.py \
		--kafka-servers kafka:9092 \
		--topic clickstream-events \
		--checkpoint-dir hdfs://namenode:9000/checkpoints/clickstream \
		--pg-url jdbc:postgresql://postgres-serving:5432/ecommerce_serving

run-benchmark:
	@echo "Thực thi Benchmark trên cụm Spark Master (1M records Kaggle)..."
	docker exec -it bigdata-spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/benchmark/run_benchmark.py \
		--csv /opt/spark/data/raw/orders_1m.csv \
		--parquet /opt/spark/data/refined/orders_unpartitioned_1m.parquet \
		--part-parquet /opt/spark/data/refined/orders.parquet

run-benchmark-sample:
	@echo "Thực thi Benchmark trên tập dữ liệu mẫu (5,000 records)..."
	docker exec -it bigdata-spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/benchmark/run_benchmark.py \
		--csv /opt/spark/data/sample/orders_sample.csv \
		--parquet /opt/spark/data/refined/orders_unpartitioned.parquet \
		--part-parquet /opt/spark/data/refined/orders.parquet

run-benchmark-local:
	python3 benchmark/run_benchmark.py

run-dashboard:
	POSTGRES_PORT=5433 .venv/bin/streamlit run dashboard/app.py --server.port 8503

test:
	.venv/bin/python tests/run_all_tests.py

