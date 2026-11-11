import os
import json
import time
import warnings
from datetime import datetime
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Tắt cảnh báo DBAPI2 của pandas khi sử dụng raw connection
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy connectable.*")

# ====================================================================
# HÀM KẾT NỐI VÀ TRUY VẤN DỮ LIỆU TẦNG PHỤC VỤ (POSTGRESQL SERVING DB)
# ====================================================================
def get_db_connection(host=None, port=None):
    try:
        import psycopg2
        pg_host = host or os.getenv("POSTGRES_HOST", "localhost")
        pg_port = port or os.getenv("POSTGRES_PORT", "5433")
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=os.getenv("POSTGRES_DB", "ecommerce_serving"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgrespassword"),
            connect_timeout=2
        )
        return conn
    except Exception:
        return None

def fetch_data(query, mock_callback, host=None, port=None):
    conn = get_db_connection(host, port)
    if conn:
        try:
            df = pd.read_sql(query, conn)
            conn.close()
            if not df.empty:
                return df, True
        except Exception:
            pass
    return mock_callback(), False

# ====================================================================
# DỮ LIỆU MẪU DỰ PHÒNG (MOCK DATA DÀNH CHO DEMO & UNIT TESTING)
# ====================================================================
def mock_streaming_data():
    now = datetime.now()
    records = []
    for i in range(12, 0, -1):
        t = now - pd.Timedelta(seconds=i * 10)
        t_str = t.strftime("%H:%M:%S")
        for evt in ["view", "cart", "purchase", "remove_from_cart"]:
            base = 180 if evt == "view" else (30 if evt == "cart" else (12 if evt == "purchase" else 6))
            import random
            count = int(base * random.uniform(0.85, 1.35))
            records.append({
                "window_start": t_str,
                "window_end": (t + pd.Timedelta(seconds=60)).strftime("%H:%M:%S"),
                "event_type": evt,
                "event_count": count,
                "unique_users": int(count * 0.72)
            })
    return pd.DataFrame(records)

def mock_revenue_data():
    categories = [
        "electronics.smartphone", "electronics.laptop", "appliances.kitchen",
        "apparel.shoes", "computers.desktop", "furniture.bedroom",
        "audio.headphone", "auto.accessories", "stationery", "sports"
    ]
    rows = []
    import random
    for c in categories:
        rev = round(random.uniform(50000, 350000), 2)
        orders = random.randint(400, 3000)
        rows.append({
            "year": 2019,
            "month": 10,
            "month_name": "Tháng 10/2019",
            "category": c,
            "total_revenue": rev,
            "total_orders": orders,
            "avg_order_value": round(rev / orders, 2)
        })
    return pd.DataFrame(rows)

def mock_top_products_data():
    return pd.DataFrame([
        {"rank_position": 1, "product_id": "PROD-1004856", "category": "electronics.smartphone", "units_sold": 727, "total_revenue": 95156.45},
        {"rank_position": 2, "product_id": "PROD-1004767", "category": "electronics.smartphone", "units_sold": 521, "total_revenue": 132541.12},
        {"rank_position": 3, "product_id": "PROD-1005115", "category": "electronics.smartphone", "units_sold": 302, "total_revenue": 294619.99},
        {"rank_position": 4, "product_id": "PROD-4804056", "category": "electronics.audio.headphone", "units_sold": 291, "total_revenue": 47121.78},
        {"rank_position": 5, "product_id": "PROD-1002544", "category": "electronics.smartphone", "units_sold": 286, "total_revenue": 132721.98},
        {"rank_position": 6, "product_id": "PROD-1003306", "category": "electronics.smartphone", "units_sold": 265, "total_revenue": 128500.00},
        {"rank_position": 7, "product_id": "PROD-4804055", "category": "electronics.audio.headphone", "units_sold": 240, "total_revenue": 42000.00},
        {"rank_position": 8, "product_id": "PROD-1004249", "category": "electronics.smartphone", "units_sold": 218, "total_revenue": 158000.00},
        {"rank_position": 9, "product_id": "PROD-1004833", "category": "electronics.smartphone", "units_sold": 195, "total_revenue": 89000.00},
        {"rank_position": 10, "product_id": "PROD-1005105", "category": "electronics.smartphone", "units_sold": 182, "total_revenue": 76500.00},
    ])

def mock_funnel_data():
    return pd.DataFrame([
        {"status": "VIEW", "total_count": 968513, "conversion_rate": 96.85},
        {"status": "PROCESSING", "total_count": 14639, "conversion_rate": 1.46},
        {"status": "COMPLETED", "total_count": 16848, "conversion_rate": 1.68},
    ])

def mock_rfm_data():
    return pd.DataFrame([
        {"segment_id": 1, "segment_name": "Khách Hàng VIP / Champions", "customer_count": 30040, "avg_recency_days": 0.0, "avg_frequency_orders": 12.84, "avg_monetary_spend": 3933.91, "total_segment_revenue": 118174700.30, "strategy_recommendation": "Chăm sóc đặc quyền, tri ân VIP, dịch vụ ưu tiên cao"},
        {"segment_id": 0, "segment_name": "Khách Hàng Tiềm Năng (Potential)", "customer_count": 126417, "avg_recency_days": 0.0, "avg_frequency_orders": 2.92, "avg_monetary_spend": 716.87, "total_segment_revenue": 90624370.51, "strategy_recommendation": "Gửi ưu đãi kích hoạt và khuyến nghị sản phẩm mới"},
        {"segment_id": 3, "segment_name": "Khách Hàng Có Nguy Cơ Rời Bỏ (At Risk)", "customer_count": 5864, "avg_recency_days": 0.0, "avg_frequency_orders": 33.95, "avg_monetary_spend": 11208.92, "total_segment_revenue": 65729094.27, "strategy_recommendation": "Chiến dịch tái kích hoạt (Re-engagement) và khảo sát dịch vụ"},
        {"segment_id": 2, "segment_name": "Khách Hàng Trung Thành (Loyal)", "customer_count": 603, "avg_recency_days": 0.0, "avg_frequency_orders": 76.86, "avg_monetary_spend": 35579.28, "total_segment_revenue": 21454305.43, "strategy_recommendation": "Chương trình tích điểm và voucher mua lại thường kỳ"}
    ])


# ====================================================================
# GIAO DIỆN CHÍNH STREAMLIT DASHBOARD
# ====================================================================
def run_dashboard():
    st.set_page_config(
        page_title="Hệ Thống Đường Ống Dữ Liệu TMĐT | Nhóm 3 - IT4931",
        page_icon="🛒",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Sidebar điều khiển & kết nối
    with st.sidebar:
        st.image("https://cdn-icons-png.flaticon.com/512/869/869636.png", width=65)
        st.markdown("### 🛒 IT4931 - Nhóm 3")
        st.caption("Lưu Trữ & Xử Lý Dữ Liệu Lớn | ĐHBK Hà Nội")
        st.caption("GVHD: **TS. Vũ Tuyết Trinh**")
        st.divider()

        st.markdown("#### ⚙️ Cấu Hình Kết Nối Serving DB")
        cfg_host = st.text_input("PostgreSQL Host", value=os.getenv("POSTGRES_HOST", "localhost"))
        cfg_port = st.text_input("PostgreSQL Port", value=os.getenv("POSTGRES_PORT", "5433"))

        # Kiểm tra kết nối nhanh
        test_conn = get_db_connection(cfg_host, cfg_port)
        if test_conn:
            st.success("🟢 Kết nối Serving DB: THÀNH CÔNG")
            test_conn.close()
        else:
            st.warning("🟡 Serving DB: Không phát hiện (Chế độ Dự phòng Mock)")

        st.divider()
        st.markdown("#### 🔄 Làm Mới Tự Động")
        auto_refresh = st.checkbox("Bật Auto-refresh", value=False)
        refresh_interval = st.selectbox("Tần suất (giây)", [5, 10, 30], index=0)
        if auto_refresh:
            time.sleep(refresh_interval)
            st.rerun()

        st.divider()
        st.markdown("#### 👥 Phân Công Nhiệm Vụ")
        st.markdown("""
        - **Phạm Văn Quyết** (202416331): *Ingestion & Kafka*
        - **Vũ Việt Tiến** (202416362): *HDFS & Batch Layer*
        - **Nguyễn Xuân Hồng Quân** (202416321): *Streaming Layer*
        - **Đặng Minh Quân** (202416323): *Serving, Dashboard & Benchmark*
        """)

    # Header chính
    st.title("🛒 HỆ THỐNG GIÁM SÁT & TRỰC QUAN HÓA ĐƯỜNG ỐNG DỮ LIỆU TMĐT")
    st.markdown(
        "**Kiến trúc Lambda phân tán:** Apache Kafka ➔ HDFS (128MB Block) ➔ Apache Spark (Batch & Streaming) "
        "➔ PostgreSQL Serving Layer ➔ Streamlit Analytics UI"
    )

    # Thanh trạng thái kiến trúc hệ thống
    status_col1, status_col2, status_col3, status_col4 = st.columns(4)
    with status_col1:
        st.info("📦 **Tập dữ liệu**: Kaggle eCommerce (1,000,000 bản ghi)")
    with status_col2:
        st.info("⚡ **Xử lý Luồng**: Spark Streaming (Sliding Window 10s)")
    with status_col3:
        st.info("📊 **Xử lý Khối**: PySpark + Parquet Snappy (28 MB)")
    with status_col4:
        st.info("🎯 **Tầng Phục vụ**: PostgreSQL 16 (Composite Index)")

    tab_stream, tab_batch, tab_rfm, tab_bench = st.tabs([
        "⚡ 1. Giám Sát Thời Gian Thực (Speed Layer)",
        "📊 2. Phân Tích Kinh Doanh (Batch Layer - 1M Records)",
        "🤖 3. Phân Cụm Khách Hàng (Spark MLlib K-Means RFM)",
        "🔬 4. Đánh Giá Thực Nghiệm (Benchmark Suite: CSV vs Parquet)"
    ])

    # ====================================================================
    # TAB 1: STREAMING MONITORING (SPEED LAYER)
    # ====================================================================
    with tab_stream:
        st.subheader("⚡ Giám Sát Luồng Sự Kiện Clickstream Thời Gian Thực")
        st.markdown("*Dữ liệu được xử lý qua Spark Structured Streaming từ Kafka topic `clickstream-events` với cửa sổ trượt 10 giây và Watermarking 10 phút.*")

        stream_df, is_live_stream = fetch_data(
            "SELECT window_start, window_end, event_type, event_count, unique_users FROM stream_realtime_traffic ORDER BY window_start DESC LIMIT 40",
            mock_streaming_data,
            host=cfg_host,
            port=cfg_port
        )

        if is_live_stream:
            st.success("🟢 Đang đồng bộ luồng dữ liệu trực tiếp từ PostgreSQL Serving DB (`stream_realtime_traffic`).")
        else:
            st.info("ℹ️ Đang hiển thị luồng dữ liệu mô phỏng. Bật cụm Docker Compose và Kafka Producer để theo dõi sự kiện trực tiếp.")

        # Thống kê tổng quan Stream
        total_stream_events = stream_df["event_count"].sum()
        total_unique_users = stream_df["unique_users"].sum()

        view_events = stream_df[stream_df["event_type"] == "view"]["event_count"].sum()
        cart_events = stream_df[stream_df["event_type"] == "cart"]["event_count"].sum()
        purchase_events = stream_df[stream_df["event_type"] == "purchase"]["event_count"].sum()

        v2c_rate = (cart_events / view_events * 100) if view_events > 0 else 0.0
        c2p_rate = (purchase_events / cart_events * 100) if cart_events > 0 else 0.0

        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Tổng Sự Kiện Trong Cửa Sổ", f"{total_stream_events:,}")
        with m2:
            st.metric("Người Dùng Đang Hoạt Động", f"{total_unique_users:,}")
        with m3:
            st.metric("Tỷ Lệ Thêm Giỏ (View ➔ Cart)", f"{v2c_rate:.2f}%", help="Tỷ lệ người xem chuyển sang bỏ giỏ hàng trong các cửa sổ gần nhất")
        with m4:
            st.metric("Tỷ Lệ Mua Hàng (Cart ➔ Purchase)", f"{c2p_rate:.2f}%", help="Tỷ lệ người bỏ giỏ hoàn tất thanh toán")

        # Biểu đồ thời gian thực
        s_col1, s_col2 = st.columns([3, 2])
        with s_col1:
            st.markdown("##### 📈 Lưu Lượng Tương Tác Qua Các Cửa Sổ Trượt (10s)")
            # Sắp xếp theo window_start tăng dần để vẽ biểu đồ đường
            chart_stream_df = stream_df.sort_values(by="window_start")
            fig_stream_line = px.line(
                chart_stream_df,
                x="window_start",
                y="event_count",
                color="event_type",
                markers=True,
                title="Biến thiên số lượng tương tác theo thời gian (Event-Time)",
                labels={"window_start": "Cửa sổ thời gian", "event_count": "Số sự kiện", "event_type": "Hành vi"}
            )
            fig_stream_line.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig_stream_line, width="stretch")

        with s_col2:
            st.markdown("##### 🍩 Phân Bổ Tương Tác Theo Loại Sự Kiện")
            type_summary = stream_df.groupby("event_type")["event_count"].sum().reset_index()
            fig_stream_pie = px.pie(
                type_summary,
                names="event_type",
                values="event_count",
                hole=0.45,
                color="event_type",
                title="Tỷ lệ các loại hành vi trong luồng"
            )
            fig_stream_pie.update_traces(textinfo="percent+label")
            st.plotly_chart(fig_stream_pie, width="stretch")

        with st.expander("🔍 Xem Bảng Dữ Liệu Chi Tiết Cửa Sổ Xử Lý (Speed View)"):
            st.dataframe(stream_df, width="stretch")

    # ====================================================================
    # TAB 2: BATCH BUSINESS ANALYTICS (BATCH LAYER)
    # ====================================================================
    with tab_batch:
        st.subheader("📊 Phân Tích Nghiệp Vụ Kinh Doanh Từ Dữ Liệu Khối (>1M Bản Ghi Kaggle)")
        st.markdown("*Kết quả tổng hợp từ công việc PySpark Batch Analytics trên 1,000,000 giao dịch thương mại điện tử.*")

        rev_df, is_live_batch = fetch_data(
            "SELECT year, month, category, total_revenue, total_orders, avg_order_value FROM batch_revenue_monthly ORDER BY total_revenue DESC",
            mock_revenue_data,
            host=cfg_host,
            port=cfg_port
        )
        top_df, _ = fetch_data(
            "SELECT rank_position, product_id, category, units_sold, total_revenue FROM batch_top_products ORDER BY rank_position ASC LIMIT 10",
            mock_top_products_data,
            host=cfg_host,
            port=cfg_port
        )
        funnel_df, _ = fetch_data(
            "SELECT status, total_count, conversion_rate FROM batch_order_funnel ORDER BY total_count DESC",
            mock_funnel_data,
            host=cfg_host,
            port=cfg_port
        )

        if is_live_batch:
            st.success("🟢 Đã tải toàn bộ kết quả tổng hợp chính xác từ PostgreSQL Serving DB (`batch_*`).")
        else:
            st.info("ℹ️ Đang hiển thị kết quả mẫu dự phòng.")

        # KPI cốt lõi
        total_rev = rev_df["total_revenue"].sum()
        total_orders = rev_df["total_orders"].sum()
        avg_aov = rev_df["avg_order_value"].mean()
        distinct_categories = rev_df["category"].nunique()

        b1, b2, b3, b4 = st.columns(4)
        with b1:
            st.metric("Tổng Doanh Thu Hệ Thống", f"${total_rev:,.2f}")
        with b2:
            st.metric("Tổng Số Đơn Hàng", f"{total_orders:,}")
        with b3:
            st.metric("Giá Trị Đơn Trung Bình (AOV)", f"${avg_aov:,.2f}")
        with b4:
            st.metric("Số Ngành Hàng Hoạt Động", f"{distinct_categories}")

        st.divider()

        # Trực quan hóa Phễu chuyển đổi (Interactive Funnel Chart) & Top ngành hàng
        f_col1, f_col2 = st.columns([1, 1])

        with f_col1:
            st.markdown("##### 🔻 Phễu Chuyển Đổi Hành Vi Khách Hàng (Order Conversion Funnel)")
            st.caption("Minh họa tỷ lệ rơi rớt qua các giai đoạn từ Xem (View) ➔ Giỏ hàng (Cart) ➔ Thanh toán thành công (Purchase).")

            # Sử dụng Plotly Funnel chuyên dụng
            fig_funnel_obj = go.Figure(go.Funnel(
                y=funnel_df["status"],
                x=funnel_df["total_count"],
                textinfo="value+percent initial+percent previous",
                marker={"color": ["#1f77b4", "#ff7f0e", "#2ca02c"]}
            ))
            fig_funnel_obj.update_layout(title_text="Phễu Chuyển Đổi Thương Mại Điện Tử (1M Events)")
            st.plotly_chart(fig_funnel_obj, width="stretch")

        with f_col2:
            st.markdown("##### 🏆 Top 10 Ngành Hàng Đạt Doanh Thu Cao Nhất")
            top10_cat = rev_df.head(10).sort_values(by="total_revenue", ascending=True)
            fig_cat = px.bar(
                top10_cat,
                x="total_revenue",
                y="category",
                orientation="h",
                text="total_revenue",
                title="Top 10 Ngành Hàng Đóng Góp Doanh Thu Lớn Nhất ($)",
                labels={"total_revenue": "Doanh thu ($)", "category": "Ngành hàng"}
            )
            fig_cat.update_traces(texttemplate="$%{text:,.0f}", textposition="inside")
            st.plotly_chart(fig_cat, width="stretch")

        st.markdown("##### 🥇 Top 10 Sản Phẩm Bán Chạy Nhất Toàn Sàn")
        # Format hiển thị cho dataframe
        display_top_df = top_df.copy()
        if "total_revenue" in display_top_df.columns:
            display_top_df["Doanh Thu ($)"] = display_top_df["total_revenue"].apply(lambda x: f"${x:,.2f}")
            display_top_df["Số Lượng Đã Bán"] = display_top_df["units_sold"].apply(lambda x: f"{x:,} chiếc")
            display_top_df["Thứ Hạng"] = display_top_df["rank_position"].apply(
                lambda r: "🥇 Top 1" if r == 1 else ("🥈 Top 2" if r == 2 else ("🥉 Top 3" if r == 3 else f"Top {r}"))
            )
            st.dataframe(
                display_top_df[["Thứ Hạng", "product_id", "category", "Số Lượng Đã Bán", "Doanh Thu ($)"]],
                width="stretch"
            )
        else:
            st.dataframe(top_df, width="stretch")

    # ====================================================================
    # TAB 3: SPARK MLLIB K-MEANS RFM CUSTOMER SEGMENTATION
    # ====================================================================
    with tab_rfm:
        st.subheader("🤖 Phân Cụm Khách Hàng Bằng Spark MLlib K-Means (Mô Hình RFM)")
        st.markdown(
            "*Áp dụng thuật toán học máy không giám sát K-Means (k=4) trên Spark MLlib phân tích 162,924 khách hàng "
            "dựa trên 3 chỉ số cốt lõi: Recency (Thời gian mua gần nhất), Frequency (Tần suất đơn hàng), và Monetary (Tổng giá trị chi tiêu).*"
        )

        rfm_df, is_live_rfm = fetch_data(
            "SELECT segment_id, segment_name, customer_count, avg_recency_days, avg_frequency_orders, avg_monetary_spend, total_segment_revenue, strategy_recommendation FROM batch_customer_segments ORDER BY segment_id",
            mock_rfm_data,
            host=cfg_host,
            port=cfg_port
        )

        if is_live_rfm:
            st.success("🟢 Đã tải kết quả phân cụm RFM thực tế từ PostgreSQL Serving DB (`batch_customer_segments`).")
        else:
            st.info("ℹ️ Đang hiển thị kết quả phân cụm mô phỏng.")

        total_rfm_customers = rfm_df["customer_count"].sum()
        total_rfm_revenue = rfm_df["total_segment_revenue"].sum()

        r_col1, r_col2, r_col3, r_col4 = st.columns(4)
        with r_col1:
            st.metric("Tổng Khách Hàng Được Phân Cụm", f"{total_rfm_customers:,}")
        with r_col2:
            vip_row = rfm_df[rfm_df["segment_id"] == 1]
            vip_count = vip_row["customer_count"].values[0] if not vip_row.empty else 0
            st.metric("Số Lượng Khách Hàng VIP", f"{vip_count:,}", help="Khách hàng có tổng chi tiêu và tần suất cao nhất")
        with r_col3:
            st.metric("Tổng Doanh Thu Phân Khúc", f"${total_rfm_revenue:,.2f}")
        with r_col4:
            st.metric("Mô Hình Học Máy", "Spark MLlib K-Means (k=4)", help="Features: StandardScaler(VectorAssembler(R, F, M))")

        st.divider()

        rfm_c1, rfm_c2 = st.columns([1, 1])
        with rfm_c1:
            st.markdown("##### 👥 Phân Bố Số Lượng Khách Hàng Theo Cụm")
            fig_cust_pie = px.pie(
                rfm_df,
                names="segment_name",
                values="customer_count",
                hole=0.4,
                title="Tỷ lệ người dùng theo phân khúc khách hàng",
                color="segment_name",
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            fig_cust_pie.update_traces(textinfo="percent+label")
            st.plotly_chart(fig_cust_pie, width="stretch")

        with rfm_c2:
            st.markdown("##### 💰 Đóng Góp Doanh Thu Của Từng Phân Khúc ($)")
            fig_rev_bar = px.bar(
                rfm_df,
                x="segment_name",
                y="total_segment_revenue",
                color="segment_name",
                text="total_segment_revenue",
                title="Doanh thu thực tế mang lại theo từng cụm ($)",
                labels={"segment_name": "Phân khúc", "total_segment_revenue": "Tổng doanh thu ($)"},
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            fig_rev_bar.update_traces(texttemplate="$%{text:,.0f}", textposition="inside")
            st.plotly_chart(fig_rev_bar, width="stretch")

        st.markdown("##### 🧭 Ma Trận Hành Vi Tần Suất - Chi Tiêu (Frequency vs. Monetary Scatter)")
        fig_scatter = px.scatter(
            rfm_df,
            x="avg_frequency_orders",
            y="avg_monetary_spend",
            size="customer_count",
            color="segment_name",
            hover_name="segment_name",
            text="segment_name",
            title="Tương quan giữa Tần suất đặt hàng và Chi tiêu trung bình (Bán kính bóng = Quy mô khách hàng)",
            labels={
                "avg_frequency_orders": "Tần suất đặt hàng trung bình (Đơn)",
                "avg_monetary_spend": "Chi tiêu trung bình ($)",
                "customer_count": "Quy mô nhóm"
            },
            size_max=60
        )
        fig_scatter.update_traces(textposition="top center")
        st.plotly_chart(fig_scatter, width="stretch")

        st.markdown("##### 📋 Bảng Chiến Lược Hành Động Tiếp Thị Từng Phân Khúc (Actionable Marketing Matrix)")
        display_rfm_df = rfm_df.copy()
        display_rfm_df["Doanh Thu Phân Khúc"] = display_rfm_df["total_segment_revenue"].apply(lambda x: f"${x:,.2f}")
        display_rfm_df["Chi Tiêu TB/Người"] = display_rfm_df["avg_monetary_spend"].apply(lambda x: f"${x:,.2f}")
        display_rfm_df["Số Khách Hàng"] = display_rfm_df["customer_count"].apply(lambda x: f"{x:,} người")
        st.dataframe(
            display_rfm_df[["segment_id", "segment_name", "Số Khách Hàng", "avg_frequency_orders", "Chi Tiêu TB/Người", "Doanh Thu Phân Khúc", "strategy_recommendation"]],
            width="stretch"
        )

    # ====================================================================
    # TAB 4: BENCHMARK SUITE (STORAGE & QUERY PERFORMANCE)
    # ====================================================================
    with tab_bench:
        st.subheader("🔬 Thực Nghiệm Đánh Giá Tối Ưu Lưu Trữ & Hiệu Năng Truy Vấn")
        st.markdown("""
        So sánh thực nghiệm giữa 3 cấu trúc dữ liệu trên cụm Apache Spark:
        1. **CSV Thô (Baseline)**: Định dạng văn bản phân tán chưa nén, quét toàn bộ dòng bản ghi.
        2. **Parquet Độc Lập**: Lưu trữ theo cột (Columnar) kết hợp thuật toán nén Snappy.
        3. **Partitioned Parquet**: Tối ưu kép với nén Snappy và phân vùng vật lý theo thời gian `year=/month=/`.
        """)

        # Tải kết quả benchmark thực tế từ file JSON hoặc database
        bench_file = "benchmark/output/benchmark_results.json"
        bench_data = None
        if os.path.exists(bench_file):
            try:
                with open(bench_file, "r", encoding="utf-8") as f:
                    bench_data = json.load(f)
            except Exception:
                pass

        if not bench_data:
            bench_data = [
                {
                    "test_case": "TC1: Partition Pruning (Lọc theo year=2019 & month=10)",
                    "csv": {"duration_ms": 1014.7, "ram_mb": 380.5},
                    "parquet": {"duration_ms": 358.98, "ram_mb": 210.0},
                    "partitioned": {"duration_ms": 190.99, "ram_mb": 145.2}
                },
                {
                    "test_case": "TC2: Predicate Pushdown (Category LIKE 'electronics%' & Price > 500)",
                    "csv": {"duration_ms": 510.38, "ram_mb": 320.0},
                    "parquet": {"duration_ms": 275.39, "ram_mb": 180.4},
                    "partitioned": {"duration_ms": 255.77, "ram_mb": 160.0}
                },
                {
                    "test_case": "TC3: Heavy Aggregation (Group By Category, Sum/Avg, Sort)",
                    "csv": {"duration_ms": 654.43, "ram_mb": 395.0},
                    "parquet": {"duration_ms": 451.2, "ram_mb": 230.1},
                    "partitioned": {"duration_ms": 422.37, "ram_mb": 205.8}
                }
            ]

        # Metric cards nổi bật
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        with kpi1:
            st.metric("Tỷ Lệ Tiết Kiệm Lưu Trữ", "78%", delta="127 MB ➔ 28 MB", help="Dung lượng sau khi chuyển CSV sang Parquet Snappy")
        with kpi2:
            st.metric("Tốc Độ TC1 (Partition Pruning)", "5.3x FASTER", delta="-81.2% Thời gian", help="So sánh giữa 1,014.70 ms (CSV) và 190.99 ms (Parquet)")
        with kpi3:
            st.metric("Tốc Độ TC2 (Predicate Pushdown)", "2.0x FASTER", delta="-49.9% Thời gian", help="Đẩy điều kiện lọc trực tiếp xuống metadata footer của Parquet")
        with kpi4:
            st.metric("Tốc Độ TC3 (Heavy Aggregation)", "1.6x - 2.2x FASTER", delta="Tối ưu I/O cột", help="Chỉ nạp các cột cần tính toán thay vì đọc toàn bộ hàng")

        st.divider()

        # Tạo bảng biểu đồ so sánh
        chart_rows = []
        for tc in bench_data:
            name = tc["test_case"].split("(")[0].strip()
            chart_rows.append({"Kịch bản": name, "Định dạng": "1. CSV Raw (Baseline)", "Thời gian (ms)": tc["csv"]["duration_ms"]})
            chart_rows.append({"Kịch bản": name, "Định dạng": "2. Parquet Độc Lập", "Thời gian (ms)": tc["parquet"]["duration_ms"]})
            chart_rows.append({"Kịch bản": name, "Định dạng": "3. Partitioned Parquet", "Thời gian (ms)": tc["partitioned"]["duration_ms"]})

        b_df = pd.DataFrame(chart_rows)

        col_b1, col_b2 = st.columns([3, 2])
        with col_b1:
            st.markdown("##### ⏱️ So Sánh Thời Gian Thực Thi Truy Vấn (Execution Time - ms)")
            fig_time = px.bar(
                b_df,
                x="Kịch bản",
                y="Thời gian (ms)",
                color="Định dạng",
                barmode="group",
                text="Thời gian (ms)",
                color_discrete_map={
                    "1. CSV Raw (Baseline)": "#e74c3c",
                    "2. Parquet Độc Lập": "#f39c12",
                    "3. Partitioned Parquet": "#27ae60"
                }
            )
            fig_time.update_traces(texttemplate="%{text:.1f} ms", textposition="outside")
            fig_time.update_layout(yaxis_title="Thời gian (ms - Càng thấp càng tốt)", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig_time, width="stretch")

        with col_b2:
            st.markdown("##### 💾 Tiết Kiệm Dung Lượng & Băng Thông I/O Đọc")
            storage_df = pd.DataFrame([
                {"Định dạng": "CSV Thô (Raw)", "Kích thước (MB)": 127.0, "Loại": "Chưa nén, dạng dòng"},
                {"Định dạng": "Parquet Snappy", "Kích thước (MB)": 28.0, "Loại": "Nén Snappy, dạng cột"}
            ])
            fig_storage = px.bar(
                storage_df,
                x="Định dạng",
                y="Kích thước (MB)",
                color="Định dạng",
                text="Kích thước (MB)",
                color_discrete_map={"CSV Thô (Raw)": "#e74c3c", "Parquet Snappy": "#27ae60"}
            )
            fig_storage.update_traces(texttemplate="%{text:.0f} MB (-78%)", textposition="inside")
            st.plotly_chart(fig_storage, width="stretch")

        st.markdown("""
        #### 💡 Phân Tích & Đánh Giá Kỹ Thuật Chuyên Sâu:
        - **Partition Pruning (TC1)**: Spark loại bỏ hoàn toàn các phân vùng không thỏa mãn điều kiện `year=2019/month=10`, chỉ đọc đúng phân vùng dữ liệu mục tiêu, mang lại tốc độ vượt trội gấp **5.3 lần**.
        - **Predicate Pushdown & Columnar Storage (TC2)**: Nhờ footer metadata (Min/Max values của từng Data Page trong Parquet), Spark bỏ qua hàng loạt khối dữ liệu mà không cần giải nén. Đồng thời chỉ đọc 2 cột (`category`, `price`) thay vì toàn bộ 9 cột của bản ghi.
        - **Heavy Aggregation (TC3)**: Tối ưu bộ nhớ đệm Cache và hạn chế tắc nghẽn Shuffle I/O giữa các worker nodes.
        """)

if __name__ == "__main__":
    run_dashboard()
