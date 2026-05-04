# Reflection — Day 18 Lakehouse Lab

## Anti-pattern dễ mắc phải nhất: "One-size-fits-all table design" (Thiết kế bảng một-cỡ-cho-tất-cả)

Trong slide §5, anti-pattern mà tôi nghĩ dữ liệu của team dễ mắc phải nhất là **áp dụng cùng một cấu trúc bảng, cùng một partitioning strategy cho mọi use case** — bất kể đó là transactional CDC, log event, hay aggregate metrics.

Lý do: Khi team còn non kinh nghiệm về Lakehouse, thường có xu hướng "đổ hết dữ liệu vào một bảng duy nhất" với hy vọng đơn giản hoá. Hậu quả là:
- Bảng Bronze chứa raw JSON (nặng, nhiều cột) không nên dùng partition hay Z-order giống bảng Silver đã được parse, clean
- Bảng Gold aggregate với ít dòng nhưng cần query nhanh theo model+date — Z-order theo `model` là hợp lý, nhưng nếu copy nguyên strategy từ Silver (partition by date) thì hiệu năng dashboard sẽ kém
- Small-file problem xuất hiện ở bảng Bronze khi ingest streaming nhưng lại không ảnh hưởng đến Gold

Bài học: Mỗi layer (Bronze/Silver/Gold) cần có chiến lược partitioning, compaction, Z-order riêng biệt dựa trên access pattern cụ thể — không thể "one-size-fits-all".
