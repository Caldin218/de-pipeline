.PHONY: setup run_local quality load clean

# Tạo virtualenv và cài dependencies
setup:
	python -m venv .venv && . .venv/Scripts/activate && pip install -r requirements.txt

# Chạy pipeline local end-to-end
run_local:
	python pipelines/ingest.py
	python pipelines/transform_spark.py
	python pipelines/quality_checks.py
	python pipelines/load_s3.py

# Chạy riêng quality check
quality:
	python pipelines/quality_checks.py

# Chạy riêng load
load:
	python pipelines/load_s3.py

# Xóa dữ liệu local
clean:
	rm -rf data/bronze/* data/silver/* data/warehouse.sqlite