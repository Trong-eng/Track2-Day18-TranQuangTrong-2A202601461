# Bản mô tả kiến trúc — Đưa CDC ride-hailing Việt Nam vào Lakehouse

**Chủ đề:** CDC từ Oracle/Debezium vào Lakehouse, tuân thủ Nghị định 13/2023/NĐ-CP
**Tác giả:** Tran Quang Trong — 2A202601461
**Trạng thái:** Đề xuất thiết kế

## 1. Mô tả bài toán

Hệ thống ride-hailing ghi khoảng 100 triệu chuyến mỗi năm và đạt đỉnh 30.000 thay đổi/giây từ Oracle. Dữ liệu gồm trạng thái chuyến, giá tiền, số điện thoại, mã định danh và GPS của tài xế/hành khách. Đội phân tích cần dashboard cập nhật trong 60 giây từ lúc source commit và truy vấn ad-hoc p95 dưới 1 giây. Sự kiện đến muộn thường xuyên vì kết nối ở tỉnh xa không ổn định. Dữ liệu PII thuộc phạm vi Nghị định 13/2023/NĐ-CP: con người không được đọc PII chưa xử lý, mọi lần truy cập phải có audit, và phải hỗ trợ xóa hoặc chứng minh lịch sử thay đổi.

Bài toán khó vì CDC phải có thứ tự và idempotency, nhưng dữ liệu đến muộn; schema Oracle thay đổi độc lập với consumer; dashboard cần dữ liệu nóng trong khi lịch sử phải rẻ; và rollback một bản sửa sai không được làm mất lineage hoặc làm lộ PII.

## 2. Kiến trúc đề xuất

```text
Oracle OLTP
   │  redo log / transaction commit SCN
   ▼
Debezium CDC ──► Kafka (sự kiện thô, schema registry, replay 24 giờ)
   │                         │
   │                         └──► dead-letter topic + cảnh báo
   ▼
Vùng cách ly mã hóa (chỉ break-glass; KMS, 7 ngày)
   │ token hóa phone/ID/GPS trước khi con người được đọc
   ▼
Delta Bronze: CDC đã làm sạch, chỉ append + operation/SCN/source_ts
   │  checkpoint + CDF + quality gates
   ▼
Delta Silver: lịch sử chuyến SCD2, MERGE dữ liệu trễ, PII đã kiểu hóa/token hóa
   │                    │
   │                    └──► CDF consumers: gian lận/feature/audit
   ▼
Delta Gold: chuyến hiện tại + tổng hợp theo ngày/thành phố/tài xế
   │
   ├──► dịch vụ dashboard (7 ngày gần nhất, Z-order theo city_id/event_ts)
   └──► SQL phân tích (row/column policy, audit mọi lượt đọc nhạy cảm với PII)

REST catalog + RBAC/policy engine + kho lineage + bảng audit
```

Vùng cách ly mã hóa không phải là bảng Bronze dành cho analyst. Đây là bộ đệm khôi phục có thời gian sống ngắn. Lớp đầu tiên con người có thể đọc là Delta Bronze đã làm sạch, vì vậy một câu lệnh SQL vô tình cũng không làm lộ số điện thoại, mã định danh hoặc GPS chính xác.

## 3. Các quyết định chính và phương án bị loại

### Quyết định 1 — Chọn Delta Lake làm định dạng bảng

Mình chọn **Delta Lake** vì workload này cần append tốc độ cao, Change Data Feed (CDF), `MERGE` có tính idempotent, schema enforcement và rollback bằng time travel. CDF cho phép hệ thống hạ nguồn nhận một sự kiện update/delete thay vì quét lại toàn bộ bảng. Các chỉnh sửa SCD2 có thể được audit theo từng version.

Mình loại **Parquet thuần** vì nó không có transaction log nguyên tử, giao thức ghi đồng thời đáng tin cậy hay hợp đồng CDC gốc. Mình không chọn **Iceberg làm định dạng chính** vì dù Iceberg mạnh về hidden partitioning và đọc đa engine, yêu cầu cấp thiết đầu tiên ở đây là Delta CDF cùng hệ sinh thái Spark/SQL có `MERGE`; thêm một cầu nối CDC riêng sẽ làm tăng bề mặt vận hành. Iceberg vẫn là định dạng trao đổi hợp lệ cho các bản export đã curate trong tương lai.

### Quyết định 2 — Dùng Kafka + Debezium thay vì polling Oracle

Mình chọn **Debezium đọc redo/LogMiner của Oracle rồi đưa vào Kafka**, mang theo source commit SCN, loại thao tác và phiên bản schema. Kafka cung cấp replay, back-pressure và một điểm bàn giao bền vững giữa việc bắt sự kiện nguồn và writer của lakehouse.

Mình loại **polling theo timestamp** vì các update có cùng timestamp có thể bị bỏ sót hoặc lặp lại, còn delete rất khó tái dựng. Mình loại **batch export trực tiếp Oracle-to-Parquet** vì một lần export chậm sẽ vi phạm SLA freshness 60 giây và không có vùng replay độc lập khi lakehouse tạm thời không hoạt động.

### Quyết định 3 — Token hóa trước lớp Bronze có thể đọc

Mình chọn **token hóa xác định** cho phone và identity bằng khóa do HSM/KMS quản lý, đồng thời dùng geohash thô (ví dụ độ chính xác 6 ký tự) cho GPS dành cho analyst. Cùng một đầu vào luôn tạo cùng một token, cho phép join và điều tra gian lận mà không lộ giá trị gốc. Dịch vụ token ghi lại phiên bản khóa và mục đích sử dụng, tuyệt đối không ghi plaintext vào application log.

Mình loại **hash không có khóa được quản lý** vì số điện thoại có entropy thấp và dễ bị tấn công bằng từ điển. Mình loại **chỉ masking trên giao diện query** vì PII thô vẫn còn trong file, cache và các bản export ad-hoc. GPS chính xác và ánh xạ có thể đảo ngược chỉ được phép qua một dịch vụ break-glass, có phê duyệt và audit stream riêng.

### Quyết định 4 — Partition theo ngày sự kiện và nhóm thành phố; cluster theo predicate nóng

Mình chọn partition **`event_date` cộng với `city_group` có giới hạn**, nhắm file Parquet 128–512 MB, và Z-order/clustering theo `city_id`, `event_ts`, `trip_id`. Cách này giữ số partition ở mức quản lý được, đồng thời cho dashboard thành phố 7 ngày gần nhất khả năng prune file. Writer gom micro-batch tới kích thước file mục tiêu thay vì commit cho từng message Kafka.

Mình loại **mỗi trip hoặc driver một partition** vì sẽ tạo small-files và làm metadata quá tải. Mình loại **partition theo timestamp chính xác** vì tạo quá nhiều partition nhỏ. Mình cũng loại **chỉ partition theo city** vì partition của một thành phố sẽ phình vô hạn, khiến retention lịch sử và maintenance song song khó hơn.

### Quyết định 5 — SCD Type 2 ở Silver, projection trạng thái hiện tại ở Gold

Mình chọn **SCD2** cho lịch sử trạng thái chuyến với `valid_from`, `valid_to`, `is_current`, `source_scn` và `record_hash`. Sự kiện trễ chỉ được áp dụng khi timestamp/SCN nguồn mới hơn bản ghi hiện tại; message trùng có SCN bằng nhau bị bỏ qua một cách idempotent. Gold cung cấp bảng chuyến hiện tại gọn nhẹ và các metric thành phố/ngày đã tổng hợp cho dashboard.

Mình loại **overwrite tại chỗ** vì nó xóa câu trả lời cho “lúc 10:03 chúng ta đã biết gì?” và khiến replay sự cố bất khả thi. Mình loại **event-sourcing trực tiếp từ Bronze cho mọi query** vì analyst phải giải mã CDC envelope lặp đi lặp lại và chịu thêm độ trễ. Silver là lịch sử được quản trị; Gold là projection phục vụ truy vấn.

### Quyết định 6 — REST catalog và policy enforcement tại biên bảng

Mình chọn **catalog tương thích REST, dùng PostgreSQL làm backend** cho tên bảng, phiên bản schema, ownership và credential theo môi trường. Row filter giới hạn analyst theo thành phố được phép; column policy ẩn token mapping và GPS chính xác. Bảng audit ghi principal, query ID, version bảng, các cột đã chạm tới và mã mục đích.

Mình loại **filesystem path làm ranh giới bảo mật** vì path không cung cấp ownership, row policy hay lineage nhất quán. Mình loại **một API catalog độc quyền của một vendor** vì sẽ làm các workflow Trino, DuckDB hoặc migration sau này đắt đỏ. Catalog là control plane; Delta log vẫn là nguồn sự thật cấp giao dịch của bảng.

### Quyết định 7 — Retention và deletion là các job tường minh

Mình chọn vùng cách ly mã hóa 7 ngày, Bronze chi tiết đã làm sạch 30 ngày, Silver SCD2 365 ngày và Gold aggregate 2 năm, trừ khi có legal hold. Delta VACUUM chỉ chạy sau retention guard; consumer CDF phải xác nhận các sự kiện delete trước khi file cũ được thu hồi. Một yêu cầu xóa tạo tombstone có audit và lan truyền qua CDF tới các bảng dẫn xuất.

Mình loại **giữ mọi thứ vĩnh viễn** vì storage và mức phơi nhiễm PII sẽ tăng vô hạn. Mình loại **xóa vật lý ngay lập tức** vì reader đang hoạt động và consumer hạ nguồn có thể vẫn tham chiếu một version. Time travel và erasure được cân bằng bằng retention window có tài liệu, legal hold và báo cáo xác minh xóa.

## 4. Failure mode và khôi phục

### Failure 1 — Kafka backlog vượt ngân sách freshness

**Phát hiện:** consumer lag, độ trễ commit-to-Bronze end-to-end và tỷ lệ DLQ được đưa vào cảnh báo; SLO là 60 giây.
**Ứng phó:** tạm dừng các CDF consumer không quan trọng, scale Debezium/Kafka partition và ưu tiên topic trạng thái chuyến.
**Rollback:** không bỏ qua offset. Replay từ SCN cuối đã commit; Bronze ghi idempotent theo `(source_table, primary_key, source_scn, op)`.

### Failure 2 — Thay đổi schema Oracle làm writer hỏng

**Phát hiện:** schema registry compatibility check và Bronze quality gate từ chối field bắt buộc chưa biết trước khi commit.
**Ứng phó:** chuyển event không tương thích vào DLQ, cảnh báo team sở hữu và giữ writer tương thích gần nhất tiếp tục phục vụ.
**Rollback:** schema evolution của Delta là opt-in. Khôi phục version bảng trước đó nếu merge sai đã commit, sau đó replay event trong vùng cách ly khi contract đã được cập nhật.

### Failure 3 — Event trễ làm sửa sai một chuyến

**Phát hiện:** theo dõi event-time lateness, duplicate key `(trip_id, source_scn)` và overlap SCD2 (`valid_from >= valid_to`).
**Ứng phó:** cô lập các trip ID bị ảnh hưởng và chỉ dừng repair job, không dừng toàn bộ ingestion stream.
**Rollback:** dùng Delta version trước khi sửa, xác thực thứ tự SCN nguồn rồi chạy lại MERGE có guard. Version cũ vẫn còn để đối chiếu sự cố.

### Failure 4 — PII xuất hiện trong bảng hoặc log có thể đọc

**Phát hiện:** DLP scan trên Parquet mới, bộ đếm tokenization và canary query bắt buộc không bao giờ trả về pattern plaintext.
**Ứng phó:** thu hồi principal vi phạm, cách ly bảng và xoay token key nếu cần.
**Rollback:** khôi phục Delta version đã làm sạch gần nhất, xóa file dẫn xuất nhiễm bẩn sau khi downstream xác nhận và gắn incident ID vào bản ghi audit/lineage.

## 5. Ước tính chi phí sơ bộ

Giả định cho trạng thái ổn định năm đầu:

- 100M chuyến/năm × payload CDC 5 KB ≈ 500 GB raw/năm.
- Parquet đã làm sạch cộng Delta history, index và hai bản replica: 1,2 TB logical.
- Lưu Bronze chi tiết 30 ngày và Silver 365 ngày; Gold aggregate là 20 GB.
- Giá object storage gộp: **$0.023/GB-tháng** cho dữ liệu hot, **$0.012/GB-tháng** cho dữ liệu warm.

Ước tính storage:

```text
0,35 TB hot × $23/TB-tháng       = $8,05/tháng
0,85 TB warm × $12/TB-tháng      = $10,20/tháng
Kafka/DLQ temporary storage      ≈ $80/tháng
------------------------------------------------
Tổng storage ước tính             ≈ $98/tháng
```

Compute và vận hành chiếm phần lớn: hai worker Kafka/Debezium nhỏ ($650/tháng), một Delta streaming job đủ cho đỉnh 30K writes/giây ($1.200/tháng), dashboard/query warehouse ($600/tháng), dịch vụ catalog/lineage/audit ($300/tháng), monitoring/DLP ($250/tháng). Tổng ước tính production đầu tiên là khoảng **$2.450/tháng**, chưa tính license Oracle và nhân sự. Thiết kế giữ storage rẻ bằng cách hết hạn các lớp chi tiết nhưng bảo toàn Gold aggregate; chi phí query được kiểm soát nhờ compaction, projection và file pruning.

## 6. Phạm vi MVP trong một tuần

Tuần đầu không ingest toàn bộ khu vực. MVP sẽ chứng minh các contract khó nhất trên một thành phố và 1% traffic production:

1. Bắt CDC Oracle cho bảng trip bằng Debezium, giữ SCN/op/schema version trong Kafka.
2. Triển khai token hóa xác định cho phone/ID và geohash GPS thô; thêm plaintext canary test.
3. Ghi Bronze Delta append-only đã làm sạch, có checkpoint và khóa idempotent.
4. Xây một đường SCD2 Silver MERGE với cửa sổ trễ 15 phút và một dashboard Gold theo thành phố/ngày.
5. Chứng minh CDF delete, rollback bằng time travel, từ chối schema drift và replay DLQ.
6. Đo latency commit-to-dashboard, tỷ lệ duplicate, kích thước file, p95 query và độ đầy đủ của audit.

MVP được xem là đạt khi dashboard duy trì dưới 60 giây với traffic mẫu, một event trễ tạo đúng một khoảng SCD2 đã sửa, không trả về PII plaintext và bài diễn tập rollback hoàn tất trong 30 phút. Chỉ sau đó mới mở rộng số thành phố và tăng số Kafka partition.

## 7. PoC đi kèm

PoC có thể chạy được là [topic_c_tokenization.ipynb](poc/topic_c_tokenization.ipynb). PoC chứng minh hai contract không tầm thường trong thiết kế này: token hóa PII xác định trước Bronze có thể đọc, và áp dụng CDC có kiểm tra SCN để bỏ qua event trễ hoặc trùng. Các assertion cung cấp bằng chứng nhỏ, có thể chạy lại, rằng PII plaintext không đi vào state cuối và SCN thấp hơn không thể ghi đè trạng thái chuyến mới hơn.

## Tóm tắt quyết định

Thiết kế này xem lakehouse vừa là hệ thống phục vụ truy vấn vừa là hệ thống audit: Delta cung cấp ACID/CDF/time travel, catalog cung cấp policy và ownership, token hóa hạn chế phơi nhiễm PII, còn các job maintenance/retention tường minh kiểm soát chi phí. Các phương án bị loại không phải lúc nào cũng xấu; chúng bị loại vì failure mode của chúng xung đột với yêu cầu freshness, compliance và replay của workload này.
