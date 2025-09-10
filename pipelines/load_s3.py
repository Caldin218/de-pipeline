import boto3
import pathlib
import os
from dotenv import load_dotenv

# ===== [NEW] Logging JSON tối giản + timing =====
import logging, sys, time, json
def _logger():
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter('%(message)s'))  # in thẳng JSON line
    lg = logging.getLogger("load")
    lg.handlers = []
    lg.addHandler(h)
    lg.setLevel(logging.INFO)
    return lg
log = _logger()
_t0 = time.time()

# ===========================
# LOAD CONFIG + ENV
# ===========================
load_dotenv()

RUN_DATE = os.getenv("RUN_DATE")
print(f"[INFO] RUN_DATE = {RUN_DATE}")  # giữ nguyên

SILVER = pathlib.Path("data/silver")
BUCKET = os.getenv("S3_BUCKET", "de-pipeline-test-853409708")
REGION = os.getenv("AWS_REGION", "ap-southeast-1")

def upload_file(file_path, key):
    s3 = boto3.client("s3", region_name=REGION)
    s3.upload_file(str(file_path), BUCKET, key)
    print(f"[upload] {file_path} -> s3://{BUCKET}/{key}")  # giữ nguyên
    return True

# ===========================
# MAIN LOGIC
# ===========================
def main():
    uploaded = 0
    errors = 0

    for file in SILVER.glob("*.csv"):
        key = f"silver/{file.name}"
        try:
            ok = upload_file(file, key)
            if ok:
                uploaded += 1
        except Exception as e:
            errors += 1
            log.info(json.dumps({
                "stage": "load",
                "run_date": RUN_DATE,
                "file": str(file),
                "error": str(e),
                "event": "upload_failed"
            }, ensure_ascii=False))

    # ===== [NEW] Tổng kết JSON log chuẩn =====
    log.info(json.dumps({
        "stage": "load",
        "run_date": RUN_DATE,
        "files_total": len(list(SILVER.glob('*.csv'))),
        "files_uploaded": uploaded,
        "errors": errors,
        "duration_s": round(time.time() - _t0, 2)
    }, ensure_ascii=False))

    # Exit code chuẩn cho Airflow
    if errors > 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()

#...
