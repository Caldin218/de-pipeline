# DE Job Sim — E‑commerce Daily Metrics (Local-First)

Mục tiêu: mô phỏng **một công việc Data Engineer** kiểu batch: ingest → transform → quality checks → load → (tuỳ chọn) Airflow.

## Nhanh gọn để chạy local

```bash
# 1) Tạo venv (Windows PowerShell đổi `source` thành `.\.venv\Scripts\activate`)
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2) Chạy pipeline local (đầu-cuối)
python pipelines/ingest.py
python pipelines/transform.py
python pipelines/quality_checks.py
python pipelines/load.py

# 3) Xem kết quả
ls data/silver
# -> daily_user_metrics.csv, daily_orders.csv, daily_revenue.csv
# Warehouse sqlite: data/warehouse.sqlite
```

> Lưu ý: Airflow/ Docker ở đây có **mã mẫu** để bạn đọc/tuỳ biến; không bắt buộc chạy ngay.

## Kiến trúc (bronze → silver → warehouse)

- **Raw**: `data/raw/*.csv|ndjson` (orders, events)
- **Bronze**: raw chuẩn hoá (delimiter, schema) → `data/bronze/`
- **Silver**: bảng chuẩn hoá + fact/agg đơn giản → `data/silver/`
- **Warehouse**: `sqlite` file để demo nếu không có BigQuery/Snowflake

## Deliverables giống “job thật”
- DAG Airflow mẫu (`airflow/dags/de_job_dag.py`)
- Chất lượng dữ liệu đơn giản (`pipelines/quality_checks.py` + `tests/`)
- Config YAML, logging, Makefile và CI skeleton
- Tài liệu: `docs/job_spec.md`, `docs/system_design.md` (mermaid) 

Bạn có thể thay `sqlite` bằng BigQuery/Snowflake/DuckDB tuỳ môi trường.
