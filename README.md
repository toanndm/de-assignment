# DE Assessment — Hướng dẫn cài đặt và chạy

## Cấu trúc project

```text
assessment/
├── dags/
│   └── pipeline.py          # Part B — Airflow DAG
├── streaming/
│   ├── producer.py          # Part C — Kafka producer
│   └── consumer.py          # Part C — Kafka consumer
├── sql/
│   ├── schema.sql           # Part A — DDL (bảng + indexes)
│   ├── load.sql             # Part A — Load CSV + transform
│   └── queries.sql          # Part A — 2 analytical queries
├── design.md                # Part D — Câu hỏi thiết kế
├── monitor.py               # Health check sau khi chạy pipeline
├── docker-compose.yml       # Môi trường local
├── de_assessment_data.csv   # Dataset (không nộp kèm)
├── requirements.txt
└── README.md
```

---

## Kiến trúc tổng quan

```text
┌─────────────────────────────────────────────────────┐
│                  DOCKER (services)                   │
│                                                      │
│   PostgreSQL :5432   Airflow :8080   Kafka :9092     │
│                          ↑                           │
│                   dags/pipeline.py                   │
│                 (chạy bên trong Airflow)             │
└─────────────────────────────────────────────────────┘
                           ↑ kết nối qua localhost
┌─────────────────────────────────────────────────────┐
│                   MÁY HOST                           │
│                                                      │
│   streaming/producer.py  →  Kafka :9092              │
│   streaming/consumer.py  →  Kafka :9092 + PG :5432   │
│   monitor.py             →  PostgreSQL :5432         │
└─────────────────────────────────────────────────────┘
```

- **Batch pipeline (Part A + B):** Airflow DAG chạy bên trong Docker, bạn chỉ trigger qua UI
- **Streaming (Part C):** Producer và consumer chạy trên máy host, kết nối vào services qua localhost
- **Docker:** Chỉ chứa services (PostgreSQL, Airflow, Kafka) — không chứa code pipeline

---

## Data Model

### Sơ đồ quan hệ (Star Schema)

```text
                    ┌──────────────────┐
                    │  dim_event_type  │
                    │  ────────────── │
                    │  id (PK)        │
                    │  name           │
                    └────────┬─────────┘
                             │
┌──────────────┐    ┌────────▼─────────────────────────────┐    ┌───────────────────┐
│  dim_zone    │    │              fact_events              │    │ dim_payment_method│
│  ──────────  │    │  ───────────────────────────────────  │    │ ─────────────────│
│  zone_id(PK) ├────│  event_id (PK, UUID)                 │    │  id (PK)          │
│  zone_name   │    │  event_timestamp                      ├────│  name             │
└──────────────┘    │  entity_id                            │    └───────────────────┘
                    │  zone_id (FK → dim_zone)              │
                    │  destination_id                       │
                    │  vendor_id (FK → dim_vendor)          │
                    │  event_type_id (FK → dim_event_type)  │
                    │  rate_type                            │
                    │  duration_seconds                     │
                    │  passenger_count                      │
                    │  value / sub_value / total_value      │
                    │  payment_method_id (FK → dim_pm)      │
                    │  is_anomaly                           │
                    │  ingested_at                          │
                    └────────────────┬──────────────────────┘
                                     │
                    ┌────────────────▼──────────────────────┐
                    │          dim_vendor  (SCD Type 1)      │
                    │  ─────────────────────────────────────│
                    │  id (PK)                              │
                    │  name  (placeholder, not in CSV)      │
                    └───────────────────────────────────────┘
```

---

### Chi tiết từng bảng

#### `fact_events` — Bảng sự kiện chính

| Cột | Kiểu | Mô tả |
| --- | --- | --- |
| `event_id` | UUID PK | Định danh duy nhất của sự kiện |
| `event_timestamp` | TIMESTAMPTZ | Thời điểm xảy ra (UTC) |
| `entity_id` | INTEGER | ID thực thể (xe/tài xế) |
| `zone_id` | INTEGER FK | Khu vực xuất phát |
| `destination_id` | INTEGER | Khu vực đến |
| `vendor_id` | SMALLINT FK | Trỏ vào `dim_vendor.id` |
| `event_type_id` | SMALLINT FK | Loại sự kiện |
| `rate_type` | SMALLINT | Loại giá (1–6) |
| `duration_seconds` | INTEGER | Thời lượng (giây) |
| `passenger_count` | SMALLINT | Số hành khách |
| `value` | NUMERIC(10,2) | Giá trị chính |
| `sub_value` | NUMERIC(10,2) | Phụ phí |
| `total_value` | NUMERIC(10,2) | Tổng giá trị |
| `payment_method_id` | SMALLINT FK | Phương thức thanh toán |
| `is_anomaly` | BOOLEAN | TRUE nếu `total_value` < 0 |
| `ingested_at` | TIMESTAMPTZ | Thời điểm load vào DB |

---

#### `dim_vendor` — SCD Type 1

| Cột | Kiểu | Mô tả |
| --- | --- | --- |
| `id` | SMALLINT PK | Natural key từ source system (vendor_id trong CSV) |
| `name` | TEXT | Tên vendor — placeholder, không có trong CSV |

Vendor được seed sẵn 2 record. Vendor mới xuất hiện trong data sẽ tự động được đăng ký với tên `'Vendor <id>'` qua bước auto-register trong pipeline.

---

#### `dim_event_type` — SCD Type 1

| Cột | Kiểu | Giá trị |
| --- | --- | --- |
| `id` | SMALLINT PK | 1–5 |
| `name` | TEXT | `standard`, `express`, `premium`, `bulk`, `scheduled` |

---

#### `dim_payment_method` — SCD Type 1

| Cột | Kiểu | Giá trị |
| --- | --- | --- |
| `id` | SMALLINT PK | 1–4 |
| `name` | TEXT | `card`, `cash`, `account`, `voucher` |

---

#### `dim_zone` — SCD Type 1

| Cột | Kiểu | Mô tả |
| --- | --- | --- |
| `zone_id` | INTEGER PK | ID khu vực (tự động populate từ data) |
| `zone_name` | TEXT | Tên zone (có thể enrich sau) |

---

#### `raw_events` — Staging table

Bảng trung gian, tất cả cột đều là TEXT — mirror 1:1 với CSV. Dùng để load dữ liệu thô trước khi transform vào `fact_events`. Bị TRUNCATE mỗi lần pipeline chạy.

---

#### `streaming_events` — Kafka consumer target

Cấu trúc tương tự `fact_events` nhưng lưu trực tiếp `event_type` và `payment_method` dưới dạng TEXT (không normalize FK) để đơn giản hóa luồng streaming. Thêm cột `received_at` là thời điểm consumer nhận được message.

---

### Indexes

| Index | Bảng | Cột | Mục đích |
| --- | --- | --- | --- |
| `idx_fact_events_timestamp` | `fact_events` | `event_timestamp` | Time-series queries và monthly rollup |
| `idx_fact_events_entity` | `fact_events` | `entity_id` | Lọc theo entity |
| `idx_fact_events_zone` | `fact_events` | `zone_id` | Lọc theo khu vực |
| `idx_fact_events_event_type` | `fact_events` | `event_type_id` | Lọc theo loại sự kiện |
| `idx_fact_events_anomaly` | `fact_events` | `is_anomaly` (partial) | Tìm anomaly nhanh |

---

## Điều kiện tiên quyết

| Yêu cầu | Phiên bản |
| --- | --- |
| Docker Desktop | 4.x+ (đang chạy) |
| Docker Compose | v2.x (đi kèm Docker Desktop) |
| Python | 3.10+ |
| RAM trống | 4 GB tối thiểu |

> **Dataset:** File `de_assessment_data.csv` không được nộp kèm theo yêu cầu của đề bài.
> Đặt file vào cùng thư mục với `docker-compose.yml` trước khi chạy `docker compose up -d`.

---

## BƯỚC 1 — Cài đặt Python environment (máy host)

```bash
# Di chuyển vào thư mục project (thư mục chứa docker-compose.yml)
cd <đường dẫn tới thư mục project>

python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt
```

> `requirements.txt` chỉ chứa `kafka-python` và `psycopg2-binary` — Airflow chạy trong Docker, không cần cài trên host.

---

## BƯỚC 2 — Khởi động Docker

```bash
docker compose up -d
```

Chờ khoảng **2–3 phút** để tất cả services khởi động. Theo dõi `airflow-init` hoàn thành:

```bash
docker compose logs -f airflow-init
# Chờ đến khi thấy: exited with code 0
```

Kiểm tra trạng thái:

```bash
docker compose ps
```

Kết quả mong đợi:

```text
NAME                STATUS
postgres            healthy
postgres-airflow    healthy
airflow-webserver   healthy
airflow-scheduler   running
zookeeper           running
kafka               healthy
airflow-init        exited (0)   ← bình thường, chạy 1 lần rồi tắt
```

Thông tin kết nối:

| Service | Địa chỉ | Tài khoản |
| --- | --- | --- |
| Airflow UI | <http://localhost:8080> | airflow / airflow |
| PostgreSQL | localhost:5432 | db=assessment, user=de, pass=de |
| Kafka | localhost:9092 | — |

---

## BƯỚC 3 — Tạo Airflow Connection

Airflow cần biết cách kết nối tới PostgreSQL assessment.

1. Mở <http://localhost:8080> → đăng nhập `airflow / airflow`
2. Vào **Admin → Connections → dấu `+`**
3. Điền thông tin:

| Field | Giá trị |
| --- | --- |
| Connection Id | `postgres_assessment` |
| Connection Type | `Postgres` |
| Host | `postgres` |
| Schema | `assessment` |
| Login | `de` |
| Password | `de` |
| Port | `5432` |

Nhấn **Save**

---

## BƯỚC 4 — Chạy Airflow DAG (Part A + B)

DAG tự động xuất hiện trong Airflow vì folder `dags/` đã được mount vào container.

1. Mở <http://localhost:8080> → **DAGs**
2. Tìm `de_assessment_pipeline` → toggle **On**
3. Nhấn **▶ Trigger DAG** để chạy ngay

4 tasks chạy tuần tự (~1 phút tổng):

```text
create_schema  →  ingest_to_staging  →  transform_to_fact  →  validate_and_log
    (~5s)              (~30s)                (~20s)                (~5s)
```

Khi tất cả tasks chuyển sang màu xanh lá là thành công. Xem log chi tiết bằng cách click vào từng task → **Logs**.

> DAG idempotent — re-run bao nhiêu lần cũng không duplicate dữ liệu.

---

## BƯỚC 5 — Chạy Kafka Streaming (Part C)

Cần **2 terminal riêng biệt**, cả 2 đều activate venv trước.

**Terminal 1 — Chạy Consumer trước:**

```bash
.venv\Scripts\activate
python streaming/consumer.py
```

Chờ thấy thông báo:

```text
streaming_events table ready.
Consumer subscribed to topic 'events'. Waiting for messages...
```

**Terminal 2 — Chạy Producer:**

```bash
.venv\Scripts\activate
python streaming/producer.py
```

Producer sẽ stream từng dòng CSV lên Kafka. Consumer nhận, parse, flag anomaly và insert vào PostgreSQL theo batch.

Các tùy chọn có thể điều chỉnh:

```bash
# Producer
python streaming/producer.py --delay 0.01 --topic events --bootstrap localhost:9092

# Consumer
python streaming/consumer.py --batch-size 100 --group de_assessment_consumer
```

Dừng bằng `Ctrl+C` — consumer sẽ drain batch cuối rồi thoát sạch.

---

## BƯỚC 6 — Verify kết quả

```bash
python monitor.py
```

Kết quả mong đợi:

```text
=== DE Assessment Pipeline Monitor ===

1. Schema checks       ✓ tất cả bảng tồn tại
2. Row counts          ✓ fact_events: 287,924 rows
3. Data quality        ✓ 863 anomalies flagged, 0 duplicates
4. Date range          ✓ Oct → Dec 2024 (3 tháng)
5. Dimension tables    ✓ 5 event types, 4 payment methods, 2 vendors
6. Revenue sanity      ✓ tổng doanh thu hợp lệ

All checks passed ✓
```

Hoặc kiểm tra thủ công qua psql:

```bash
# Số dòng trong fact_events
docker exec -it %i psql -U de -d assessment -c "SELECT COUNT(*) FROM fact_events;"

# Số dòng streaming
docker exec -it %i psql -U de -d assessment -c "SELECT COUNT(*) FROM streaming_events;"

# Chạy analytical queries
docker exec -i %i psql -U de -d assessment < sql/queries.sql
```

---

## BƯỚC 7 — Dọn dẹp

```bash
# Dừng containers, giữ data
docker compose down

# Reset hoàn toàn (xóa cả volumes)
docker compose down -v
```

---

## Sơ đồ luồng tổng quát

```text
Bước 1: python -m venv + pip install
    ↓
Bước 2: docker compose up -d
    ↓
Bước 3: Airflow Connection (1 lần duy nhất)
    ↓
    ├── Bước 4: Airflow DAG ──── batch: CSV → PostgreSQL
    │              ↓                   (Part A + B)
    │         fact_events
    │
    └── Bước 5: Kafka Streaming ─ realtime: CSV → Kafka → PostgreSQL
                   ↓                          (Part C)
              streaming_events
    ↓
Bước 6: python monitor.py ── verify kết quả
```

---

## Ghi chú thiết kế

**Schema:**

- Star schema với dimension tables (`dim_event_type`, `dim_payment_method`, `dim_vendor`) để chuẩn hóa các giá trị phân loại
- Cờ `is_anomaly = TRUE` cho 863 dòng có `total_value` âm — giữ lại trong bảng thay vì xóa để dữ liệu có thể audit
- Index trên `event_timestamp`, `entity_id`, `zone_id`, `event_type_id` phục vụ các pattern truy vấn phổ biến nhất

**Idempotency:**

- Airflow DAG: staging dùng `TRUNCATE + COPY`, fact table dùng `INSERT ... ON CONFLICT DO NOTHING`
- Kafka consumer: cũng dùng `ON CONFLICT DO NOTHING` — replay topic từ offset 0 hoàn toàn an toàn

**Streaming vs Batch:**

- Consumer ghi vào `streaming_events` (bảng riêng) để tránh xung đột với DAG đang ghi vào `fact_events`
- Trong production, hai luồng này sẽ được hợp nhất qua Flink/Spark Streaming (xem `design.md`)

**Hạn chế hiện tại:**

- `dim_zone` chỉ có `zone_id` — cần dataset tham chiếu để enrich tên zone
- Consumer dùng at-least-once semantics; exactly-once cần Kafka transactions + DB transactional wired cùng nhau
- Chưa có dbt transformations cho các model downstream (tổng hợp hàng ngày, entity metrics)

---
