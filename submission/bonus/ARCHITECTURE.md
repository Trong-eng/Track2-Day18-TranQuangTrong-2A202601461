# Architecture Brief — Tu chính CDC ride-hailing Việt Nam vào Lakehouse

**Topic:** CDC từ Oracle/Debezium vào Lakehouse, tuân thủ Nghị định 13/2023/NĐ-CP
**Author:** Tran Quang Trong — 2A202601461
**Status:** Design proposal for review

## 1. Problem statement

Hệ thống ride-hailing ghi khoảng 100 triệu chuyến mỗi năm và đạt đỉnh 30.000 thay đổi/giây từ Oracle. Dữ liệu gồm trạng thái chuyến, giá tiền, số điện thoại, mã định danh và GPS của tài xế/hành khách. Đội phân tích cần dashboard cập nhật trong 60 giây từ lúc source commit và truy vấn ad-hoc p95 dưới 1 giây. Sự kiện đến muộn thường xuyên vì kết nối ở tỉnh xa không ổn định. Dữ liệu PII thuộc phạm vi Nghị định 13/2023/NĐ-CP: con người không được đọc PII chưa xử lý, mọi lần truy cập phải có audit, và phải hỗ trợ xóa hoặc chứng minh lịch sử thay đổi.

Bài toán khó vì CDC phải có thứ tự và idempotency, nhưng dữ liệu đến muộn; schema Oracle thay đổi độc lập với consumer; dashboard cần dữ liệu nóng trong khi lịch sử phải rẻ; và rollback một bản sửa sai không được làm mất lineage hoặc làm lộ PII.

## 2. Proposed architecture

```text
Oracle OLTP
   │  redo log / transaction commit SCN
   ▼
Debezium CDC ──► Kafka (raw events, schema registry, 24h replay)
   │                         │
   │                         └──► dead-letter topic + alert
   ▼
Encrypted quarantine (break-glass only; KMS, 7 days)
   │ tokenise phone/ID/GPS before human-readable access
   ▼
Delta Bronze: append-only sanitized CDC + operation/SCN/source_ts
   │  checkpoint + CDF + quality gates
   ▼
Delta Silver: SCD2 trip history, late-data MERGE, typed/tokenized PII
   │                    │
   │                    └──► CDF consumers: fraud/features/audit
   ▼
Delta Gold: current-trip + daily/city/driver aggregates
   │
   ├──► dashboard service (last 7 days, Z-order by city_id/event_ts)
   └──► analyst SQL (row/column policy, audit every PII-sensitive read)

REST catalog + RBAC/policy engine + lineage store + audit table
```

The encrypted quarantine is not an analyst-facing Bronze table. It is a short-lived recovery buffer. The first human-readable layer is the sanitized Delta Bronze table, so accidental SQL access cannot reveal raw phone numbers, identity numbers or exact GPS.

## 3. Key decisions and rejected alternatives

### Decision 1 — Delta Lake as the table format

I choose **Delta Lake** because this workload needs high-rate append, Change Data Feed (CDF), idempotent `MERGE`, schema enforcement and time-travel rollback. CDF lets downstream consumers receive an update/delete event rather than repeatedly scanning the whole table. SCD2 corrections can be audited by version.

I reject **plain Parquet** because it has no atomic transaction log, reliable concurrent-writer protocol or native CDC contract. I reject **Iceberg as the primary format** because it is excellent for hidden partitioning and multi-engine reads, but the critical first requirement here is Delta CDF plus the existing Spark/SQL MERGE ecosystem; adding a separate CDC bridge would increase operational surface. Iceberg remains a valid future interchange format for curated exports.

### Decision 2 — Kafka + Debezium instead of polling Oracle

I choose **Debezium reading Oracle redo/LogMiner into Kafka**, carrying source commit SCN, operation type and schema version. Kafka gives replay, back-pressure and a durable hand-off between source capture and lakehouse writers.

I reject **timestamp polling** because updates with the same timestamp can be missed or duplicated, and deletes are difficult to reconstruct. I reject **direct Oracle-to-Parquet batch export** because a slow export would block the 60-second freshness SLA and offers no independent replay buffer when the lakehouse is unavailable.

### Decision 3 — Tokenisation before the readable Bronze layer

I choose **deterministic tokenisation** for phone and identity fields using an HSM/KMS-held key, and coarse geohash (for example, 6-character precision) for analyst-facing GPS. The same input maps to the same token, allowing joins and fraud investigation without exposing the original value. The token service records key version and purpose, never the plaintext in application logs.

I reject **hashing without a managed key** because low-entropy phone numbers are vulnerable to dictionary attacks. I reject **masking only in the query UI** because raw PII would still exist in files, caches and ad-hoc exports. Exact GPS and reversible mappings are restricted to a break-glass service with approval and a separate audit stream.

### Decision 4 — Partition by event date and city group; cluster the hot predicates

I choose **`event_date` plus a bounded `city_group` partition**, targeting 128–512 MB Parquet files, and Z-order/clustering on `city_id`, `event_ts` and `trip_id`. This keeps the number of partitions manageable while allowing last-7-day city dashboards to prune files. Writers buffer micro-batches until the target file size rather than committing every Kafka message.

I reject **one partition per trip or driver** because it creates a small-files and metadata outage. I reject **partitioning by exact event timestamp** because it creates too many tiny partitions. I also reject **partitioning only by city** because a city partition would grow indefinitely and make historical retention and parallel maintenance harder.

### Decision 5 — SCD Type 2 in Silver, current-state projection in Gold

I choose **SCD2** for trip status history with `valid_from`, `valid_to`, `is_current`, `source_scn` and `record_hash`. A late event is applied only when its source timestamp/SCN is newer than the current record; equal messages are idempotently ignored. Gold exposes a compact current-trip table and pre-aggregated city/day metrics for the dashboard.

I reject **overwrite-in-place** because it destroys the answer to “what did we know at 10:03?” and makes incident replay impossible. I reject **event-sourcing every query directly from Bronze** because analysts would repeatedly decode CDC envelopes and pay the latency cost. Silver is the governed history; Gold is the serving projection.

### Decision 6 — REST catalog and policy enforcement at the table boundary

I choose a **REST-compatible catalog backed by PostgreSQL** for table names, schema versions, ownership and environment-specific credentials. Row filters limit analysts to permitted cities; column policies hide token mappings and exact GPS. The audit table records principal, query ID, table version, columns touched and purpose code.

I reject **filesystem paths as the security boundary** because a path grants no consistent ownership, row policy or lineage. I reject **one vendor-only catalog API** because it would make future Trino, DuckDB or migration workflows expensive. The catalog is the control plane; Delta logs remain the table-level source of transaction truth.

### Decision 7 — Retention and deletion are explicit jobs

I choose 7 days of encrypted quarantine, 30 days of detailed sanitized Bronze, 365 days of Silver SCD2, and 2 years of Gold aggregates unless a legal hold exists. Delta VACUUM is run only after the retention guard; CDF consumers acknowledge delete events before old files are reclaimed. A deletion request creates an auditable tombstone and propagates through CDF to derived tables.

I reject “keep everything forever” because storage and PII exposure grow without bound. I reject immediate physical deletion because active readers and downstream consumers may still reference a version. Time travel and erasure are reconciled by a documented retention window, legal holds and a deletion verification report.

## 4. Failure modes and recovery

### Failure 1 — Kafka backlog exceeds the freshness budget

**Detection:** consumer lag, end-to-end commit-to-Bronze latency and DLQ rate are paged; the SLO is 60 seconds.
**Response:** pause non-critical CDF consumers, scale Debezium/Kafka partitions and prioritize trip-state topics.
**Rollback:** do not skip offsets. Replay from the last committed SCN; Bronze writes are idempotent on `(source_table, primary_key, source_scn, op)`.

### Failure 2 — Oracle schema change breaks the writer

**Detection:** schema registry compatibility check and a Bronze quality gate reject unknown required fields before commit.
**Response:** route incompatible events to the DLQ, alert the owning team and keep the last compatible writer serving.
**Rollback:** Delta schema evolution is opt-in. Restore the previous table version if a bad merge was committed, then replay the quarantined events after the contract is updated.

### Failure 3 — Late event rewrites a trip incorrectly

**Detection:** monitor event-time lateness, duplicate `(trip_id, source_scn)` keys and SCD2 overlap (`valid_from >= valid_to`).
**Response:** isolate the affected trip IDs and stop only the repair job, not the entire ingestion stream.
**Rollback:** use the Delta version before the repair, validate the source SCN ordering, then re-run the guarded MERGE. The old version remains available for incident comparison.

### Failure 4 — PII appears in a readable table or log

**Detection:** DLP scan on new Parquet files, tokenisation counters, and a canary query that must never return plaintext patterns.
**Response:** revoke the offending principal, quarantine the table and rotate the token key if needed.
**Rollback:** restore the last sanitized Delta version, delete contaminated derived files after downstream acknowledgements, and attach the incident ID to the audit/lineage record.

## 5. Back-of-the-envelope cost estimate

Assumptions for a first-year steady state:

- 100M trips/year × 5 KB CDC payload ≈ 500 GB raw/year.
- Sanitized Parquet plus Delta history, indexes and two replicas: 1.2 TB logical.
- 30 days of detailed Bronze and 365 days of Silver are retained; Gold aggregates are 20 GB.
- Object storage blended price: **$0.023/GB-month** for hot data, **$0.012/GB-month** for warm data.

Storage estimate:

```text
0.35 TB hot × $23/TB-month       = $8.05/month
0.85 TB warm × $12/TB-month      = $10.20/month
Kafka/DLQ temporary storage      ≈ $80/month
------------------------------------------------
Estimated storage subtotal        ≈ $98/month
```

Compute and operations dominate: two small Kafka/Debezium workers ($650/month), a streaming Delta job sized for the 30K writes/sec peak ($1,200/month), dashboard/query warehouse ($600/month), catalog/lineage/audit services ($300/month), and monitoring/DLP ($250/month). The first production estimate is therefore approximately **$2,450/month**, excluding Oracle licensing and people. The design keeps storage cheap by expiring detailed layers while preserving Gold aggregates; it keeps query cost predictable through compaction, projection and file pruning.

## 6. One-week MVP slice

The first week will not ingest all regions. It will prove the hardest contracts on one city and 1% of production traffic:

1. Capture Oracle CDC for the trip table with Debezium and preserve SCN/op/schema version in Kafka.
2. Implement deterministic phone/ID tokenisation and coarse GPS geohash; add a plaintext canary test.
3. Write append-only sanitized Bronze Delta with checkpoints and idempotent keys.
4. Build one SCD2 Silver MERGE path with a 15-minute lateness window and one Gold city/day dashboard.
5. Demonstrate a CDF delete, a time-travel rollback, a schema-drift rejection and a DLQ replay.
6. Measure commit-to-dashboard latency, duplicate rate, file size, query p95 and audit completeness.

The MVP is successful when the dashboard stays under 60 seconds for the sampled traffic, a late event produces one corrected SCD2 interval, no plaintext PII is returned, and the rollback drill completes within 30 minutes. Only then would we expand city coverage and raise the Kafka partitions.

## 7. Optional PoC

The executable companion is [topic_c_tokenization.ipynb](poc/topic_c_tokenization.ipynb). It demonstrates two non-trivial contracts from this design: deterministic PII tokenisation before readable Bronze, and SCN-guarded CDC application that ignores late or duplicate events. Its assertions provide a small, runnable proof that plaintext PII does not enter the resulting state and that a lower SCN cannot overwrite a newer trip status.

## Decision summary

This design treats the lakehouse as both a serving system and an audit system: Delta provides ACID/CDF/time travel, the catalog provides policy and ownership, tokenisation limits PII exposure, and explicit maintenance/retention jobs control cost. The rejected alternatives are not universally bad; they are rejected because their failure modes conflict with this workload's freshness, compliance and replay requirements.
