
file_SKILL.md
ST05 theo đúng hướng repo và theo rule “không mở rộng quá mức” là:

Streaming Watermarking
Persistent Checkpoint
Bounded State / aging state
Recovery after restart
Alert khi stream stall hoặc checkpoint stale
Không nên làm kiểu “RocksDB state store toàn diện” hay “hot-key skew complex tuning” ngay lúc này. Với repo hiện tại, ST05 nên focus vào pipeline stream chạy ổn và có thể phục hồi sau lỗi, chứ không phải tối ưu sâu quá mức.

Scope đúng của ST05
Nói ngắn gọn:
Đối với Spark Structured Streaming, bạn phải có:
event-time column
withWatermark(...)
checkpointLocation
trigger rõ ràng
window/groupBy dựa trên event time
cleanup state theo watermark
Mục tiêu:
không giữ state vô hạn
nếu job restart thì vẫn resume từ checkpoint
nếu dữ liệu trễ quá mức, stream không “đơ” hoặc “tăng RAM vô tận”
Cấu trúc thực thi cần làm
Bạn nên làm theo 4 phần:
A. Xác định event time

ví dụ: timestamp của sự kiện, không phải ingestion time
kiểm tra schema, parse đúng timezone
B. Thêm watermark

ví dụ: withWatermark("event_ts", "10 minutes")
nghĩa là stream chấp nhận dữ liệu trễ tối đa 10 phút
C. Viết checkpoint persistent

checkpoint phải đi vào ổ đĩa durable, không phải memory
thường là path trên local volume hoặc object storage
mỗi query stream phải có checkpoint riêng
D. Giới hạn state

nếu dùng aggregation/window, watermark phải khiến state cũ được drop
không để state tăng không giới hạn khi có dữ liệu trễ hoặc delay lớn
Pattern đúng cho PySpark
Ví dụ đúng kiểu ST05:

Điểm quan trọng ở đây:

watermark có 10 minutes
window 5 phút
checkpointLocation bắt buộc
trigger rõ ràng
outputMode phù hợp với aggregate query
Rule thực tế cho repo
Theo hướng project, ST05 nên làm tối thiểu:
watermark trên event time
checkpoint persistent
alert when stream is stale / checkpoint too old
đảm bảo no unbounded state growth
Không cần:

full complex state backend
custom RocksDB tuning
deep sessionization / hot key skew handling ngay lúc này
Điều cần cảnh báo khi triển khai
Bạn cần chắc chắn:
event time phải thực sự là thời điểm phát sinh sự kiện, không phải thời điểm ingest
watermark không quá nhỏ so với latency thực tế
nếu dữ liệu trễ quá lâu, state sẽ bị drop theo watermark, nên cần monitor lag
checkpoint folder phải nằm trên volume bền vững, tránh mất khi pod restart
Alarm/monitor nên có
ST05 nên kèm theo 2 alert:
checkpoint stale > threshold (ví dụ 20 phút)
stream no data / watermark not advancing
Nói dễ hiểu:

nếu stream đang chạy mà checkpoint không cập nhật, có khả năng job bị treo hoặc restart vô hạn
nếu watermark dừng tiến, có thể dữ liệu quá trễ hoặc job bị block
Khuyến nghị thực chiến cho bạn
Nếu đang làm theo plan, ST05 nên được triển khai như dạng “safe baseline”:
watermark + checkpoint
1 job stream demo
1 monitoring rule cho stale checkpoint
không đi quá xa sang state backend và hot-key optimization
Nói cách khác: ST05 phải giải quyết câu hỏi:
“Job stream có đau đầu gì khi restart, dữ liệu trễ, hoặc state lớn không?”
Nếu làm đúng, bạn đã ăn được core issue của streaming reliability.

Nếu bạn muốn, tôi có thể viết tiếp cho bạn:

một phiên bản ST05 chuẩn “copy-paste” cho Spark job
hoặc một checklist triển khai cụ thể theo repo để bạn làm ngay.
10:41 PM
17s
•
Auto (ma