# Bonus Challenge — CDC từ Ride-Hailing Việt Nam → Lakehouse (Tuân thủ Nghị định 13)

> **Topic C** — Kiến trúc Lakehouse cho hệ thống gọi xe Việt Nam

---

## 1. Problem Statement

Một nền tảng gọi xe Việt Nam với **100 triệu chuyến/năm, 30K writes/giây peak giờ cao điểm**. Production database là Oracle, cần CDC (Change Data Capture) qua Debezium vào Lakehouse cho analytics team. PII của tài xế và hành khách (số điện thoại, CMND/CCCD, GPS coordinates) thuộc phạm vi điều chỉnh của **Nghị định 13/2023/NĐ-CP** về bảo vệ dữ liệu cá nhân.

**Constraints:**
- SLA dashboard: refresh trong **60 giây** kể từ source commit
- Ad-hoc query p95 < **1 giây**
- Sự kiện đến muộn (late-arriving) xảy ra thường xuyên do mất mạng ở tỉnh xa
- Mọi access vào PII phải được audit
- PII phải được tokenize/redact ngay tại Bronze layer
- Chi phí storage + compute ≤ $3K/tháng (startup stage)

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PRODUCTION (Oracle RAC)                             │
│  trips  │  drivers  │  riders  │  payments  │  driver_locations (GPS)  │   │
└─────────┴───────────┴──────────┴────────────┴───────────────────────────┘──┘
                                    │ Oracle LogMiner / XStream
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Debezium Connect Cluster                            │
│  Oracle CDC Connector → Kafka (Avro, 12 partitions/topic)                   │
│  topic: pg.*.trips, pg.*.drivers, pg.*.riders, pg.*.payments               │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ Kafka Streams (PII Tokenization)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (S3 /delta/bronze/, Parquet + Delta, Append-only)            │
│                                                                             │
│  trips_raw:  {trip_id, ts, source_ts, driver_info_tokenized,               │
│               rider_info_tokenized, pickup_gps, dropoff_gps,                │
│               status, fare}                                                 │
│                                                                             │
│  PII_TOKEN_MAP:  {token_id → encrypted_pii, created_at, accessed_by}       │
│  (∆ table riêng, SSE-KMS encrypted, audit-triggered)                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ Spark Structured Streaming
                                    │ (batch every 30s, MERGE for UPSERT)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (S3 /delta/silver/, Partitioned by date + hour)              │
│                                                                             │
│  trips: SCD Type 2 — {trip_id, valid_from, valid_to, is_current,           │
│                        status, fare, driver_id, rider_id, ...}             │
│  drivers: SCD Type 2 — {driver_id, phone_token, id_token, gps, ...}        │
│  riders: SCD Type 2 — similarly                                             │
│  late_data: {trip_id, original_date, landed_ts, ...}  (late-arriving)      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ dbt / SQL transformations (hourly)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (S3 /delta/gold/)                                              │
│                                                                             │
│  daily_metrics:  {date, city, total_trips, avg_fare, p95_wait_time,        │
│                   driver_utilization, ...}                                  │
│  realtime_dashboard:  {window_5min, active_drivers, trips_in_progress,     │
│                        surge_zones, ...}                                    │
│  pii_audit_log:  {who, what_token, when, purpose} (immutable)              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ Trino / DuckDB
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  QUERY & CONSUMPTION                                                       │
│  • Metabase (dashboard refresh 60s)                                        │
│  • Jupyter for ad-hoc analytics                                            │
│  • Audit team: pii_audit_log queries                                       │
│  • ML team: feature store từ Silver                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Key Decisions & Rejected Alternatives

### 3.1 Table Format: **Delta Lake** (chọn) vs Apache Iceberg vs Apache Hudi

| Format | Decision |
|---|---|
| **Delta Lake** ✅ | **Chọn.** Lý do: (1) `MERGE WHEN MATCHED AND src.ts > tgt.ts` native support cho late-arriving data, (2) Delta CDF (Change Data Feed) cho real-time CDC incrementals, (3) time travel built-in cho audit "table tại thời điểm X", (4) ecosystem match — team đã dùng delta-rs. |
| Apache Iceberg ❌ | Loại vì: `MERGE` với điều kiện thời gian phức tạp hơn (phải dùng Spark `mergeInto`), CDF mới hơn và ít production-proven hơn với CDC workloads. Tuy nhiên, nếu tương lai cần multi-engine (Trino, Snowflake) thì Iceberg có lợi thế hơn về interoperability. |
| Apache Hudi ❌ | Loại vì: Hiệu năng ghi tốt nhưng query pattern của team nghiêng về analytics (read-heavy), Hudi tối ưu cho write-heavy workloads. Cộng đồng Python ecosystem nhỏ hơn Delta. |

**Trade-off:** Chọn Delta đồng nghĩa với vendor affinity với Databricks ecosystem, nhưng với delta-rs và DuckDB có thể query độc lập.

### 3.2 Catalog: **Apache Polaris** (chọn) vs Unity Catalog vs Hive Metastore

| Catalog | Decision |
|---|---|
| **Apache Polaris** ✅ | **Chọn.** REST Catalog spec, vendor-neutral, có thể serve Spark, Trino, DuckDB cùng lúc. Hỗ trợ time travel semantics qua REST API. |
| Unity Catalog ❌ | Loại vì vendor lock-in. Team cần multi-engine access (DuckDB cho ad-hoc, Spark cho batch, Trino cho dashboard). |
| Hive Metastore ❌ | Loại vì không hỗ trợ Delta Lake table properties, schema evolution tracking kém. |

### 3.3 Partitioning Strategy: **Date + Hour** (chọn) vs Date + City vs Hash

| Strategy | Decision |
|---|---|
| **Date + Hour** ✅ | **Chọn.** Vì: (1) Dashboard queries luôn filter theo time window, (2) late-arriving events biết rõ landing hour, (3) partition pruning hiệu quả. |
| Date + City ❌ | Loại vì: Số city ~63 tỉnh → 63 partitions/ngày, quá nhiều small files. City filter không phải hot path. |
| Hash(trip_id) ❌ | Loại vì: Không hỗ trợ time-range pruning, full-scan mỗi query. |

### 3.4 Compression: **ZStandard (zstd)** (chọn) vs Snappy vs Gzip

| Codec | Decision |
|---|---|
| **ZStandard** ✅ | **Chọn.** Compression ratio ~3-4× (vs Snappy ~2×), decompress speed gần bằng Snappy. Gold layer lưu lâu ngày cần compression cao. |
| Snappy ❌ | Loại vì compression ratio thấp, tốn storage cost. |
| Gzip ❌ | Loại vì decompress chậm, ảnh hưởng query p95. |

**Detail:** Bronze dùng Snappy (cần tốc độ ghi). Silver dùng zstd (cân bằng). Gold dùng zstd (ưu tiên storage).

### 3.5 PII Handling: **Tokenization tại Bronze** (chọn) vs Encryption at rest vs Column-level masking

| Approach | Decision |
|---|---|
| **Tokenization (deterministic) tại Bronze** ✅ | **Chọn.** Khi CDC event đến, Kafka Streams tokenize PII fields ngay trước khi ghi Bronze. Token → PII map lưu trong Delta table riêng (SSE-KMS encrypted). Audit trigger trên mọi read. |
| Encryption at rest ❌ | Loại vì: Không đủ cho Decree 13 — phải kiểm soát ai *decrypt* được, không chỉ encrypt storage. |
| Column-level masking ❌ | Loại vì: Masking không reversible. Nếu cần PII gốc cho investigation, không có cách nào lấy lại. Tokenization cho phép reversible với audit trail. |

### 3.6 Late-Arriving Data: **MERGE với timestamp condition** (chọn) vs Append-only vs Overwrite partition

| Strategy | Decision |
|---|---|
| **MERGE WHEN MATCHED AND src.ts > tgt.ts** ✅ | **Chọn.** Native Delta support. Khi event đến muộn, nếu đã có row thì chỉ update nếu event timestamp mới hơn. Giữ được tính đúng đắn temporal. |
| Append-only ❌ | Loại vì: Sẽ có duplicate rows, dashboard sai số liệu. |
| Overwrite partition ❌ | Loại vì: Có thể mất dữ liệu hợp lệ đã ghi sau. |

---

## 4. Failure Modes

### 4.1 3 AM — Debezium connector bị lag, Oracle archive log đầy

**Phát hiện:** Prometheus alert trên `debezium_lag_seconds > 300`. Kafka consumer lag monitor.

**Rollback:**
1. Pause Spark streaming job → drain Kafka queue
2. Snapshot Oracle với `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`
3. Dùng **Delta time travel** để "quay đồng hồ" Silver/Gold về version trước khi lag bắt đầu
4. Re-run CDC từ SCN đã snapshot → batch catch-up
5. Resume streaming

**Liên hệ Day 18:** Time travel (`RESTORE TABLE TO VERSION`) dùng để rollback Silver/Gold về state consistent với Oracle snapshot.

### 4.2 3 AM — PII token mapping bị corrupt / accidentally deleted

**Phát hiện:** Queries trả về token không map được → error rate tăng trên dashboard. Data quality monitor (`COUNT(token) WHERE token NOT IN token_map`).

**Rollback:**
1. Dùng **Delta time travel** trên `pii_token_map` table: `RESTORE TABLE pii_token_map TO VERSION N-1`
2. Nếu token map bị delete rows (deletion vector), dùng `FSCK REPAIR TABLE` + `VACUUM` retention
3. Verify: so sánh số lượng token trước và sau restore
4. Re-tokenize batch các dòng Bronze nếu cần

**Liên hệ Day 18:** Schema evolution + Deletion Vectors — nếu ai đó drop column trên token map, Delta cho phép rollback version.

### 4.3 3 AM — Sự kiện đến muộn gây duplicate fare (trip đã closed, sau đó có update)

**Phát hiện:** Gold aggregate `avg_fare` tăng đột biến. Alert khi daily avg_fare deviation > 3σ.

**Rollback:**
1. Xác định batch trip_ids bị ảnh hưởng
2. Dùng **Delta CDF** (Change Data Feed) để tìm chính xác những row nào changed trong MERGE
3. MERGE lại với logic: chỉ update nếu `src.ts > tgt.ts AND tgt.status != 'closed'`
4. Re-materialize Gold layer cho affected partition

**Liên hệ Day 18:** Delta CDF (`table_changes`) cho phép trace row-level changes để detect và fix.

### 4.4 3 AM — Storage cost overrun (do data spike bất thường)

**Phát hiện:** AWS Budget alert > 80% monthly. Cost explorer show S3 Standard tăng đột biến.

**Rollback:**
1. Dùng Delta `OPTIMIZE` + `ZORDER` để compact small files → giảm storage cost
2. Lên lịch data lifecycle: Bronze (7 ngày → S3 Glacier), Silver (90 ngày → S3 Glacier Deep Archive)
3. Dùng `VACUUM` để xoá stale files
4. Nếu cần gấp: drop Bronze partitions cũ hơn 30 ngày (có thể rebuild từ Kafka log)

**Liên hệ Day 18:** OPTIMIZE/ZORDER không chỉ tăng tốc query mà còn giảm số lượng file → giảm cost.

---

## 5. Cost Back-of-Envelope

### Assumptions
- **100M trips/year** ≈ 274K trips/day ≈ 190 trips/min
- **30K writes/sec peak** (driver GPS + trip updates)
- Mỗi event ~1KB (Avro)
- Storage: 1 copy Bronze + 1 copy Silver + 0.3 copy Gold

### Storage Cost (S3, us-east-1)

| Layer | Daily data | Retention | Total TB | Tier | $/TB/mo | Cost/mo |
|-------|-----------|-----------|----------|------|---------|---------|
| Bronze (raw) | 30K × 1KB × 86400 ≈ 2.5 TB | 7 days | 17.5 TB | S3 Standard | $23 | ~$403 |
| Silver (cleaned) | ~1 TB | 90 days | 90 TB | S3 IA | $12.5 | ~$1,125 |
| Gold (aggregates) | ~10 GB | 365 days | 3.6 TB | Glacier | $1 | ~$4 |
| PII Token Map | ~50 MB | 365 days | ~18 GB | S3 Standard | $23 | ~$0.4 |
| **Total storage** | | | | | | **~$1,532/mo** |

### Compute Cost (Spot instances, us-east-1)

| Workload | Instance | Hours/mo | $/hr | Cost/mo |
|----------|----------|----------|------|---------|
| Spark Streaming (Bronze→Silver) | r6i.2xlarge (8 vCPU, 64GB) | 730 | ~$0.25 | ~$183 |
| dbt transformations (Silver→Gold) | r6i.xlarge (4 vCPU, 32GB) | 180 | ~$0.13 | ~$23 |
| Ad-hoc queries (Trino) | r6i.4xlarge (16 vCPU, 128GB) | 100 | ~$0.50 | ~$50 |
| **Total compute** | | | | **~$256/mo** |

### Total: ~$1,788/mo ✅ (Under $3K budget)

---

## 6. MVP — một tuần đầu tiên

### Slice nhỏ nhất shippable (Week 1)

```
Tuần 1: "CDC từ 1 bảng Oracle → Bronze table, dashboard 1 metric"
```

1. **Setup Debezium** cho 1 bảng `trips` (non-PII fields: trip_id, ts, status, fare)
2. **Delta Bronze table** với Append-only + Snappy compression
3. **PII tokenization** hard-code (fake token) — chưa nối KMS
4. **1 View trên Gold layer:** `SELECT COUNT(*) AS trips_today`
5. **Metabase dashboard** refresh mỗi 60s — chứng minh latency SLA
6. **Script MERGE late-arriving** với timestamp condition

**Kiểm chứng:** Từ Oracle commit → Metabase thấy số đúng trong < 60s.

---

## 7. PoC Notebook (optional)

Xem `submission/bonus/poc/cdc_late_data.py` — demo cơ chế MERGE với timestamp condition cho late-arriving events, sử dụng Delta Lake + delta-rs.
