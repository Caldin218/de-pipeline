# Job Spec — Daily Metrics Pipeline

**Business goal**: Theo dõi tăng trưởng và hiệu quả marketing mỗi ngày:
- DAU, revenue, conversion và phân bổ theo kênh.

**Sources**
- `events_*.ndjson`: web/app events (view, add_to_cart, checkout_start)
- `orders_*.csv`: đơn hàng
- `users.csv`: thuộc tính người dùng (channel, signup_date)

**Acceptance Criteria**
- Bảng silver xuất ra các file CSV tương ứng.
- Chất lượng dữ liệu pass (không NULL order_id, amount > 0, user_id unique).
- Tải kết quả vào warehouse (sqlite) dưới tên bảng đã định.
- Runtime < 5 phút với dữ liệu ~ vài trăm nghìn dòng (tuỳ máy).
