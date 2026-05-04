Viewed BONUS-CHALLENGE-EN.md:1-173
Viewed BONUS-CHALLENGE.md:1-175
Viewed README.md:1-174
Viewed rubric.md:1-61

Chào bạn, dựa trên các tài liệu bạn cung cấp, để đạt được **100/100 điểm tuyệt đối** cho bài lab này, bạn cần hoàn thành các phần bắt buộc (Core) và làm theo đúng quy trình nộp bài. Phần Bonus là tự nguyện và không cộng vào điểm chính.

Dưới đây là danh sách chi tiết các công việc bạn cần làm:

### 1. Hoàn thiện 4 Notebooks (95 điểm)
Bạn cần chạy thành công 4 file notebook (sử dụng *Lightweight path* hoặc *Spark/Docker path* đều được). **Lưu ý quan trọng: Bạn phải giữ lại output (kết quả chạy) của tất cả các cell trong file notebook khi commit.**

*   **`01_delta_basics.ipynb` (20 điểm):**
    *   Tạo thành công Delta table và thư mục `_delta_log/` chứa các file JSON được sinh ra (10đ).
    *   Cơ chế Schema enforcement chặn thành công thao tác ghi dữ liệu lỗi (cột `age` bị truyền vào chuỗi string) (5đ).
    *   Sử dụng `schema_mode="merge"` (hoặc `mergeSchema=true`) để thêm thành công cột `tier` vào schema (5đ).
*   **`02_optimize_zorder.ipynb` (25 điểm):**
    *   Tái hiện được vấn đề small-file (có ≥ 100 files trước khi chạy OPTIMIZE) (5đ).
    *   Tốc độ sau khi chạy `OPTIMIZE+ZORDER` phải **nhanh hơn ≥ 3 lần** HOẶC tỷ lệ **files-pruned (loại bỏ file) ≥ 10 lần** (15đ).
    *   Số lượng file (`numFiles`) giảm đi rõ rệt sau khi chạy lệnh OPTIMIZE (5đ).
*   **`03_time_travel.ipynb` (25 điểm):**
    *   Khi gọi `history()`, hiển thị được ≥ 5 versions (bao gồm cả version sinh ra từ lệnh RESTORE) (5đ).
    *   Lệnh MERGE upsert 100,000 dòng chạy thành công trong thời gian < 60 giây (10đ).
    *   Lệnh RESTORE khôi phục thành công dữ liệu bị lỗi trong thời gian < 30 giây, và kiểm tra thấy số dòng `score < 0` bằng 0 (10đ).
*   **`04_medallion.ipynb` (25 điểm):**
    *   Cả 3 bảng Bronze, Silver, và Gold đều tồn tại trên storage (10đ).
    *   Quá trình khử trùng lặp (dedup) ở lớp Silver hoạt động, làm giảm số dòng rõ rệt (số dòng Silver < số dòng Bronze) (5đ).
    *   Lớp Gold tổng hợp chính xác dữ liệu (p50/p95 latency, cost_usd, error_rate) bao phủ ≥ 7 ngày và 3 models (10đ).

### 2. Đảm bảo tính tái lập - Reproducible (5 điểm)
Toàn bộ code của bạn trong các notebook phải chạy lại thành công từ đầu thông qua lệnh `make setup && make smoke` (đối với Lightweight) hoặc `make spark-up && make spark-smoke` (đối với Docker) trên một môi trường mới hoàn toàn.

### 3. Chuẩn bị tài liệu nộp bài (Bắt buộc)
Bạn cần tạo và commit 2 thành phần này vào trong repository của mình:
*   **Thư mục `submission/screenshots/`:** Chứa hình ảnh minh chứng.
    *   *Nếu dùng Lightweight:* Ảnh chụp terminal chạy lệnh `tree _lakehouse/` VÀ ảnh chụp nội dung của 1 file `*.json` bất kỳ trong thư mục `_delta_log/`.
    *   *Nếu dùng Spark/Docker:* Ảnh chụp console MinIO cho thấy thư mục `_delta_log/` và bucket layout.
*   **File `submission/REFLECTION.md`:** 
    *   Viết một đoạn văn bản ngắn (≤ 200 từ) trả lời câu hỏi: *"Anti-pattern nào trong slide §5 mà dữ liệu của team bạn dễ mắc phải nhất, và tại sao?"*

### 4. Quy trình nộp bài chuẩn chỉnh
1.  **Fork** repository về tài khoản GitHub của bạn (`<your-username>/Day18-Track2-Lakehouse-Lab`).
2.  **Commit & Push** các mục sau lên fork của bạn:
    *   4 files notebook đã được chạy xong (có đầy đủ kết quả/output).
    *   Thư mục `submission/screenshots/` chứa ảnh minh chứng.
    *   File `submission/REFLECTION.md`.
3.  Tạo **Pull Request (PR)** ngược lại về repository gốc (upstream) với cú pháp tiêu đề chuẩn: 
    *   `[NXX] Lab18 — <Họ Tên>` (Ví dụ: `[N01] Lab18 — Nguyễn Văn A`).

---
**💡 Ghi chú về phần Bonus Challenge:**
Phần thiết kế kiến trúc Lakehouse (`BONUS-CHALLENGE.md`) là **không bắt buộc và không tính vào tổng 100 điểm của bài này**. Tuy nhiên, nếu bạn muốn làm để lấy review đánh giá tư duy thiết kế hệ thống từ Instructor (dùng làm portfolio CV), bạn có thể nộp file thiết kế vào `submission/bonus/ARCHITECTURE.md` và đổi tên tiêu đề PR thành: `[NXX] Lab18 — <Họ Tên> [+bonus]`.

Bạn có cần hỗ trợ thiết lập môi trường hay bắt đầu xử lý notebook đầu tiên luôn không?