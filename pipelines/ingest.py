import os, glob, json, csv, pathlib, yaml
import boto3
from urllib.parse import urlparse

# ===== [NEW] Logging đơn giản dạng JSON (stdout) =====
import logging, sys, time
def _logger():
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter('%(message)s')  # in thẳng JSON line
    h.setFormatter(fmt)
    lg = logging.getLogger("ingest")
    lg.handlers = []
    lg.addHandler(h)
    lg.setLevel(logging.INFO)
    return lg
log = _logger()

# Load config
CFG = yaml.safe_load(open("/opt/airflow/configs/config.yaml", "r", encoding="utf-8"))
RAW = CFG["paths"]["raw"]
BRONZE = pathlib.Path(CFG["paths"]["bronze"])
BRONZE.mkdir(parents=True, exist_ok=True)

# 🔑 Fix: tạo session từ profile cụ thể (AWS_PROFILE hoặc hardcode)
REGION = os.getenv("AWS_REGION", "ap-southeast-1")

# Dùng AWS credentials từ biến môi trường (đã mount trong docker-compose)
session = boto3.Session(region_name=REGION)
S3_CLIENT = session.client("s3")


def download_from_s3(s3_path, local_path):
    """Download 1 file từ S3 về local"""
    parsed = urlparse(s3_path, allow_fragments=False)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    S3_CLIENT.download_file(bucket, key, str(local_path))


def ndjson_to_csv(in_path, out_path):
    with open(in_path, "r", encoding="utf-8") as f, open(out_path, "w", newline="", encoding="utf-8") as w:
        writer = None
        for line in f:
            row = json.loads(line)
            if writer is None:
                writer = csv.DictWriter(w, fieldnames=list(row.keys()))
                writer.writeheader()
            writer.writerow(row)


# ===== [NEW] Paginator S3 + match theo basename (ổn định hơn) =====
def _s3_iter_keys(bucket, prefix):
    """Yield tất cả key dưới prefix (đã phân trang)."""
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in (page.get("Contents") or []):
            yield obj["Key"]

def resolve_raw_files(pattern):
    """Trả về danh sách file path (có thể local hoặc S3)"""
    if RAW.startswith("s3://"):
        parsed = urlparse(RAW, allow_fragments=False)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        files = []
        for key in _s3_iter_keys(bucket, prefix):
            # so khớp theo basename để pattern 'orders_*.csv' khớp đúng
            base = os.path.basename(key)
            if pathlib.PurePath(base).match(pattern):
                files.append(f"s3://{bucket}/{key}")
        return files
    else:
        return glob.glob(str(pathlib.Path(RAW) / pattern))


def main():
    t0 = time.time()
    files_out_orders = 0
    files_out_events = 0

    RUN_DATE = os.getenv("RUN_DATE")
    print(f"[INFO] RUN_DATE = {RUN_DATE}")  # giữ nguyên hành vi cũ
    if RUN_DATE:
        order_pattern = f"orders_{RUN_DATE}.csv"
        event_pattern = f"events_{RUN_DATE}.ndjson"
    else:
        order_pattern = "orders_*.csv"
        event_pattern = "events_*.ndjson"

    # Copy/Download orders
    for fp in resolve_raw_files(order_pattern):
        try:
            name = os.path.basename(fp)
            out = BRONZE / f"orders_{name.split('_')[-1]}"
            if fp.startswith("s3://"):
                tmp = pathlib.Path("tmp") / name
                tmp.parent.mkdir(parents=True, exist_ok=True)
                download_from_s3(fp, tmp)
                with open(tmp, "r", encoding="utf-8") as f, open(out, "w", encoding="utf-8", newline="") as w:
                    for line in f:
                        w.write(line)
            else:
                with open(fp, "r", encoding="utf-8") as f, open(out, "w", encoding="utf-8", newline="") as w:
                    for line in f:
                        w.write(line)
            files_out_orders += 1
            print(f"[ingest] orders -> {out}")  # giữ nguyên
        except Exception as e:
            log.info(json.dumps({
                "stage": "ingest",
                "kind": "orders",
                "event": "error",
                "src": fp,
                "error": str(e)
            }, ensure_ascii=False))

    # Convert events
    for fp in resolve_raw_files(event_pattern):
        try:
            name = os.path.basename(fp).replace(".ndjson", ".csv")
            out = BRONZE / name
            if fp.startswith("s3://"):
                tmp = pathlib.Path("tmp") / os.path.basename(fp)
                tmp.parent.mkdir(parents=True, exist_ok=True)
                download_from_s3(fp, tmp)
                ndjson_to_csv(tmp, out)
            else:
                ndjson_to_csv(fp, out)
            files_out_events += 1
            print(f"[ingest] events -> {out}")  # giữ nguyên
        except Exception as e:
            log.info(json.dumps({
                "stage": "ingest",
                "kind": "events",
                "event": "error",
                "src": fp,
                "error": str(e)
            }, ensure_ascii=False))

    # ===== [NEW] Tổng kết JSON log chuẩn để đọc trên Airflow UI =====
    log.info(json.dumps({
        "stage": "ingest",
        "run_date": RUN_DATE,
        "orders_files_out": files_out_orders,
        "events_files_out": files_out_events,
        "duration_s": round(time.time() - t0, 2)
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
