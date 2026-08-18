# Reflection — Lakehouse Anti-Pattern

Anti-pattern team tôi dễ mắc phải nhất là **Small-Files Problem**. Hệ thống streaming thường ghi dữ liệu theo các micro-batch rất nhỏ. Mỗi lần ghi đều hợp lệ, nhưng sau một thời gian sẽ tạo ra rất nhiều file Parquet nhỏ. Điều này làm truy vấn chậm vì engine phải mở nhiều file và đọc nhiều metadata, đồng thời làm tăng chi phí object-storage request.

Giải pháp là theo dõi số file và kích thước file trung bình, điều chỉnh khoảng thời gian trigger của streaming writer, rồi chạy compaction định kỳ. Ngoài compaction, cần clustering hoặc Z-order trên các cột thường được lọc để file statistics có thể loại bỏ những file không liên quan. Team cũng phải lập lịch snapshot expiry và orphan removal, vì xóa snapshot không đồng nghĩa với việc file rác trên disk đã được xóa. Các job bảo trì nên có metric, cảnh báo và age guard để tránh xóa nhầm file của transaction đang chạy.
