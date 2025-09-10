import os, yaml
from dotenv import load_dotenv

load_dotenv()  # nạp biến môi trường từ file .env

def load_config():
    with open("configs/config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Override bằng biến môi trường nếu có
    cfg["gcp_project"] = os.getenv("GCP_PROJECT", cfg.get("gcp_project"))
    cfg["bq_dataset"] = os.getenv("BQ_DATASET", cfg.get("bq_dataset"))
    cfg["bq_location"] = os.getenv("BQ_LOCATION", cfg.get("bq_location"))
    return cfg


# Test khi chạy trực tiếp file này
if __name__ == "__main__":
    config = load_config()
    print(config)   # in ra config để kiểm tra
