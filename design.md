# Phần D — Câu hỏi Thiết kế

**Tình huống:** Khối lượng dữ liệu tăng 100 lần (~28 triệu sự kiện/ngày). Các sự kiện phải có thể truy vấn được trong vòng 30 giây kể từ khi xảy ra.

---

## Kiến trúc hiện tại

```text
CSV → Airflow DAG (batch hàng ngày) → PostgreSQL (fact_events)
CSV → Kafka producer → Kafka topic → Python consumer → PostgreSQL (streaming_events)
```

Ở quy mô hiện tại (~288K sự kiện/ngày), kiến trúc này hoạt động tốt và đơn giản để vận hành. Tuy nhiên khi tăng 100 lần (~28 triệu sự kiện/ngày, tương đương ~320 sự kiện/giây liên tục), một số thành phần sẽ trở thành điểm nghẽn.

---

## Cải thiện

### 1. Chuyển từ batch sang streaming pipeline

Airflow DAG chạy hàng ngày có độ trễ lên đến 24 giờ — hoàn toàn không đáp ứng được yêu cầu truy vấn trong 30 giây. Cần chuyển toàn bộ luồng ingest sang streaming-first.

Thay thế Python consumer đơn giản bằng **Databricks Structured Streaming** (chạy trên Apache Spark). Databricks xử lý được hàng trăm nghìn sự kiện/giây, hỗ trợ stateful aggregation ngay trong luồng, ghi kết quả vào database trong vòng vài giây sau khi sự kiện xảy ra, và đồng thời cung cấp tầng analytics SQL trên cùng nền tảng — giảm số lượng hệ thống cần vận hành.

Airflow vẫn giữ lại nhưng chỉ dùng cho các batch job không nhạy cảm về độ trễ như: tổng hợp báo cáo hàng ngày, kiểm tra chất lượng dữ liệu, lưu trữ dữ liệu cũ.

### 2. Mở rộng Kafka theo chiều ngang

Kafka topic với một partition duy nhất sẽ trở thành điểm nghẽn khi throughput tăng cao. Cần:

- Tăng số partition của topic `events` lên **8–16 partitions**
- Triển khai **consumer group** với số consumer tương ứng số partition để xử lý song song
- Tăng **retention policy** lên ít nhất 7 ngày để có thể replay dữ liệu khi cần

### 3. Phân vùng bảng fact trong PostgreSQL

Bảng `fact_events` phẳng với 28 triệu dòng/ngày sẽ làm giảm hiệu suất truy vấn nhanh chóng. Giải pháp là **range partitioning theo tháng**:

```sql
CREATE TABLE fact_events (...)
PARTITION BY RANGE (event_timestamp);

CREATE TABLE fact_events_2024_10 PARTITION OF fact_events
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
```

Mỗi partition nhỏ hơn giúp query planner bỏ qua dữ liệu không liên quan, cho phép lưu trữ hoặc xóa các partition cũ dễ dàng mà không ảnh hưởng đến dữ liệu mới.

### 4. Bổ sung tầng OLAP cho analytical queries

PostgreSQL phù hợp cho workload giao dịch và phân tích vừa phải. Nhưng khi cần aggregation phức tạp trên hàng trăm triệu dòng, độ trễ truy vấn sẽ tăng đáng kể. Giải pháp là sử dụng **Databricks SQL** làm tầng analytics chuyên dụng:

- Databricks Structured Streaming đọc từ Kafka, xử lý và ghi vào **Delta Lake** (lưu trữ trên cloud storage như S3/ADLS)
- Databricks SQL cho phép query trực tiếp trên Delta Lake với hiệu suất cao nhờ caching và indexing tự động
- PostgreSQL vẫn là nguồn dữ liệu vận hành (source of truth) cho các query đơn giản và low-latency
- Lợi thế: Databricks gộp cả streaming pipeline và analytics platform vào một nền tảng — giảm số hệ thống cần vận hành so với việc dùng Flink + ClickHouse riêng biệt

---

## Những gì tôi sẽ giữ lại

- **Kafka** — đã được kiểm chứng, dễ mở rộng, tách biệt rõ ràng giữa producer và consumer
- **Star schema** — dimension tables nhỏ, fact table lớn — phù hợp cho analytical queries ở mọi quy mô
- **Idempotent writes** (`ON CONFLICT DO NOTHING`) — thiết yếu khi dùng at-least-once delivery
- **Anomaly flagging** (`is_anomaly`) — logic đơn giản, không cần thay đổi khi scale
- **Airflow** — giữ lại cho orchestration các job batch không nhạy cảm về thời gian

---

## Kiến trúc đề xuất (quy mô 100 lần)

```text
Nguồn sự kiện
      ↓
  Kafka (16 partitions)
      ↓
  Databricks Structured Streaming
      ↓               ↓
PostgreSQL          Delta Lake
(vận hành,          (lưu trữ phân tích,
 low-latency)        Databricks SQL)
      ↓
  Airflow
(báo cáo hàng ngày,
 data quality,
 lưu trữ dữ liệu cũ)
```

---

## Đánh đổi chính

| Quyết định | Lợi ích | Chi phí |
| --- | --- | --- |
| Databricks Streaming + SQL | Đáp ứng SLA 30 giây, gộp streaming + analytics vào 1 platform | Chi phí cloud cao, phụ thuộc vendor (Databricks/Azure/AWS) |
| Kafka 16 partitions | Tăng throughput tuyến tính | Chỉ đảm bảo thứ tự trong cùng một partition |
| PostgreSQL partitioning | Query nhanh hơn, dễ lưu trữ | Cần migration DDL trên dữ liệu hiện có |
| Thêm Databricks SQL + Delta Lake | Analytical queries nhanh, không cần quản lý thêm OLAP database riêng | Chi phí cao hơn PostgreSQL, cần cloud environment |
| At-least-once delivery | Đơn giản, không mất dữ liệu | Consumer phải idempotent (đã xử lý) |

Đánh đổi lớn nhất là **độ phức tạp vs độ trễ**. Kiến trúc hiện tại đơn giản, chi phí thấp nhưng không đáp ứng SLA 30 giây. Kiến trúc đề xuất đáp ứng được nhưng cần đội ngũ kỹ thuật để vận hành.

Với đội nhỏ hoặc ngân sách hạn chế, giải pháp trung gian thực tế là: giữ PostgreSQL nhưng thêm partitioning và tối ưu indexing — có thể xử lý tăng trưởng 5–10 lần trước khi cần đến Databricks hay Delta Lake.
