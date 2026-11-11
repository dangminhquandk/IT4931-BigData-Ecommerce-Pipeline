-- ====================================================================
-- DDL KHỞI TẠO CƠ SỞ DỮ LIỆU TẦNG PHỤC VỤ (SERVING LAYER - POSTGRESQL)
-- Đồ án: Hệ thống đường ống dữ liệu TMĐT thời gian thực (Nhóm 3 - IT4931)
-- Phụ trách: Đặng Minh Quân (MSSV: 202416323)
-- ====================================================================

-- 1. Bảng lưu trữ doanh thu tổng hợp theo tháng và ngành hàng (Batch View)
CREATE TABLE IF NOT EXISTS batch_revenue_monthly (
    year INT NOT NULL,
    month INT NOT NULL,
    category VARCHAR(100) NOT NULL,
    total_revenue NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    total_orders BIGINT NOT NULL DEFAULT 0,
    avg_order_value NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_batch_revenue PRIMARY KEY (year, month, category)
);

-- Index phục vụ lọc nhanh theo khoảng thời gian trên Dashboard
CREATE INDEX IF NOT EXISTS idx_batch_revenue_time 
ON batch_revenue_monthly (year, month);


-- 2. Bảng lưu trữ Top 10 sản phẩm bán chạy nhất (Batch View)
CREATE TABLE IF NOT EXISTS batch_top_products (
    product_id VARCHAR(50) NOT NULL,
    category VARCHAR(100),
    units_sold BIGINT NOT NULL DEFAULT 0,
    total_revenue NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    rank_position INT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_batch_top_products PRIMARY KEY (product_id)
);

CREATE INDEX IF NOT EXISTS idx_batch_top_products_rank 
ON batch_top_products (rank_position ASC);


-- 3. Bảng lưu trữ phân tích phễu chuyển đổi đơn hàng (Order Funnel)
CREATE TABLE IF NOT EXISTS batch_order_funnel (
    status VARCHAR(50) NOT NULL,
    total_count BIGINT NOT NULL DEFAULT 0,
    conversion_rate NUMERIC(5, 2) DEFAULT 0.00,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_batch_funnel PRIMARY KEY (status)
);


-- 4. Bảng lưu trữ chỉ số clickstream thời gian thực theo Sliding Window (Speed View)
CREATE TABLE IF NOT EXISTS stream_realtime_traffic (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    unique_users BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_stream_traffic PRIMARY KEY (window_start, window_end, event_type)
);

-- Composite Index phục vụ truy vấn live monitoring theo cửa sổ mới nhất
CREATE INDEX IF NOT EXISTS idx_stream_traffic_latest 
ON stream_realtime_traffic (window_start DESC, event_type);


-- 5. Bảng ghi nhận kết quả thực nghiệm Benchmark SQL
CREATE TABLE IF NOT EXISTS benchmark_results (
    id SERIAL PRIMARY KEY,
    query_name VARCHAR(100) NOT NULL,
    storage_format VARCHAR(50) NOT NULL, -- 'CSV Raw', 'Parquet', 'Partitioned Parquet'
    execution_time_ms NUMERIC(10, 2) NOT NULL,
    bytes_read BIGINT NOT NULL,
    cpu_percent NUMERIC(5, 2),
    ram_mb NUMERIC(10, 2),
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_benchmark_query 
ON benchmark_results (query_name, storage_format);


-- 6. Bảng lưu trữ kết quả phân cụm khách hàng RFM bằng Spark MLlib K-Means
CREATE TABLE IF NOT EXISTS batch_customer_segments (
    segment_id INT PRIMARY KEY,
    segment_name VARCHAR(100) NOT NULL,
    customer_count BIGINT NOT NULL DEFAULT 0,
    avg_recency_days NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    avg_frequency_orders NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    avg_monetary_spend NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    total_segment_revenue NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    strategy_recommendation TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


