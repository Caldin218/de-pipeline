import sys, pandas as pd, pathlib, yaml, os

# ===== [NEW] Logging JSON tối giản + timing =====
import logging, sys as _sys, time, json
def _logger():
    h = logging.StreamHandler(_sys.stdout)
    h.setFormatter(logging.Formatter('%(message)s'))  # in thẳng JSON line
    lg = logging.getLogger("quality")
    lg.handlers = []
    lg.addHandler(h)
    lg.setLevel(logging.INFO)
    return lg
log = _logger()
_t0 = time.time()

# ===========================
# LOAD CONFIG + ENV
# ===========================
RUN_DATE = os.getenv("RUN_DATE")
print(f"[INFO] RUN_DATE = {RUN_DATE}")  # giữ nguyên hành vi cũ

CFG = yaml.safe_load(open("/opt/airflow/configs/config.yaml", "r", encoding="utf-8"))
BRONZE = pathlib.Path(CFG["paths"]["bronze"])
REF = pathlib.Path(CFG["paths"]["reference"])

# ===========================
# RULE DEFINITIONS (giữ nguyên, thêm bad rows)
# ===========================
def check_not_null(df, col):
    bad = df[df[col].isna()]
    ok = bad.empty
    return ok, f"not_null({col})", bad

def check_gt_zero(df, col):
    bad = df[df[col] <= 0]
    ok = bad.empty
    return ok, f"gt_zero({col})", bad

def check_unique(df, col):
    dup = df[df.duplicated(col, keep=False)]
    ok = dup.empty
    return ok, f"unique({col})", dup

# ===========================
# MAIN LOGIC
# ===========================
def main():
    failures = []
    rules_run = 0
    rows_checked = 0

    # Determine which files to load (giữ nguyên)
    if RUN_DATE:
        order_files = list(BRONZE.glob(f"orders_{RUN_DATE}.csv"))
    else:
        order_files = list(BRONZE.glob("orders_*.csv"))

    if not order_files:
        print(f"[WARN] No order files found for RUN_DATE={RUN_DATE}")
        log.info(json.dumps({
            "stage": "quality",
            "run_date": RUN_DATE,
            "orders_files": 0,
            "rows_checked": 0,
            "rules_run": 0,
            "violations": 0,
            "duration_s": round(time.time() - _t0, 2)
        }, ensure_ascii=False))
        sys.exit(0)

    orders = pd.concat([pd.read_csv(fp) for fp in order_files], ignore_index=True)
    users = pd.read_csv(REF / "users.csv")

    rows_checked = len(orders)

    # ======================
    # RULES (giữ nguyên + đếm rules_run)
    # ======================
    ok, rule, bad = check_not_null(orders, "order_id"); rules_run += 1
    if not ok:
        failures.append(("orders", rule))
        for _, row in bad.iterrows():
            log.info(json.dumps({
                "stage": "quality", "run_date": RUN_DATE,
                "table": "orders", "violation": rule,
                "row": row.to_dict()
            }, ensure_ascii=False))

    ok, rule, bad = check_gt_zero(orders, "amount"); rules_run += 1
    if not ok:
        failures.append(("orders", rule))
        for _, row in bad.iterrows():
            log.info(json.dumps({
                "stage": "quality", "run_date": RUN_DATE,
                "table": "orders", "violation": rule,
                "row": row.to_dict()
            }, ensure_ascii=False))

    ok, rule, bad = check_unique(users, "user_id"); rules_run += 1
    if not ok:
        failures.append(("users", rule))
        for _, row in bad.iterrows():
            log.info(json.dumps({
                "stage": "quality", "run_date": RUN_DATE,
                "table": "users", "violation": rule,
                "row": row.to_dict()
            }, ensure_ascii=False))

    # ======================
    # REPORT RESULT
    # ======================
    if failures:
        for t, r in failures:
            print(f"[FAIL] {t} :: {r}")
            log.info(json.dumps({
                "stage": "quality",
                "run_date": RUN_DATE,
                "table": t,
                "violation": r,
                "event": "fail_rule"
            }, ensure_ascii=False))

        log.info(json.dumps({
            "stage": "quality",
            "run_date": RUN_DATE,
            "orders_files": len(order_files),
            "rows_checked": rows_checked,
            "rules_run": rules_run,
            "violations": len(failures),
            "duration_s": round(time.time() - _t0, 2)
        }, ensure_ascii=False))
        sys.exit(1)
    else:
        print("[quality] all checks passed ✅")
        log.info(json.dumps({
            "stage": "quality",
            "run_date": RUN_DATE,
            "orders_files": len(order_files),
            "rows_checked": rows_checked,
            "rules_run": rules_run,
            "violations": 0,
            "duration_s": round(time.time() - _t0, 2)
        }, ensure_ascii=False))
        sys.exit(0)

if __name__ == "__main__":
    main()