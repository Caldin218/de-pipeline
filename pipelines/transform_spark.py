from pathlib import Path
import pathlib
import yaml, os, glob
import pandas as pd

# placeholders để Airflow parser không đòi pyspark khi import file
SparkSession = None
F = None

# ===== Logging JSON tối giản + timing (không ảnh hưởng logic) =====
import logging, sys as _sys, time, json
def _logger(name="transform"):
    h = logging.StreamHandler(_sys.stdout)
    h.setFormatter(logging.Formatter('%(message)s'))  # in thẳng JSON
    lg = logging.getLogger(name); lg.handlers = []; lg.addHandler(h); lg.setLevel(logging.INFO)
    return lg
log = _logger()
_t0 = time.time()
COUNT_OUTPUT = os.getenv("METRICS_COUNTS", "0") == "1"  # đặt 1 để đếm rows (có thể chậm hơn)

# ==========================================================================
# INFRA / BOOTSTRAP
# ==========================================================================
def resolve_path(path_str, do_mkdir=False):
    """Trả về Path nếu local, string nếu s3a://"""
    if str(path_str).startswith("s3a://"):
        return str(path_str)
    p = pathlib.Path(path_str).absolute()
    if do_mkdir:
        p.mkdir(parents=True, exist_ok=True)
    return p

def bootstrap():
    BASE_DIR    = Path(__file__).resolve().parent
    CONFIG_FILE = Path(os.getenv("CONFIG_PATH", "/opt/airflow/configs/config.yaml"))

    print(f"[DEBUG] __file__     : {__file__}")
    print(f"[DEBUG] BASE_DIR     : {BASE_DIR}")
    print(f"[DEBUG] CONFIG_FILE  : {CONFIG_FILE}  (exists={CONFIG_FILE.exists()})")

    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"config.yaml not found at {CONFIG_FILE}")

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        CFG = yaml.safe_load(f)

    print("=== CONFIG.YAML raw dump ===")
    for k, v in CFG.get("spark_conf", {}).items():
        print(f"{k}: {v}")
    print("======================HEHEHEHEHE===========")

    # [NEW] lazy-import pyspark
    global SparkSession, F
    from pyspark.sql import SparkSession as _SparkSession
    from pyspark.sql import functions as _F
    SparkSession = _SparkSession
    F = _F

    BRONZE = resolve_path(CFG["paths"]["bronze"], do_mkdir=True)
    SILVER = resolve_path(CFG["paths"]["silver"], do_mkdir=True)
    REF    = resolve_path(CFG["paths"]["reference"], do_mkdir=True)

    spark_builder = (
        SparkSession.builder
        .appName("TransformPipeline")
        .config(
            "spark.jars",
            "/opt/bitnami/spark/extra-jars/hadoop-aws-3.3.4.jar,"
            "/opt/bitnami/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar"
        )
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "5000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.retry.limit", "3")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
        .config("spark.local.dir", "/tmp/spark")
        .config("spark.hadoop.hadoop.tmp.dir", "/tmp/spark")
        # ⭐ Bật event log để History Server đọc
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")
    )

    for k, v in CFG.get("spark_conf", {}).items():
        spark_builder = spark_builder.config(k, v)

    spark = spark_builder.getOrCreate()

    hc = spark.sparkContext._jsc.hadoopConfiguration()
    print("=== EFFECTIVE fs.s3a.* CONFIGS ===")
    itr = hc.iterator()
    while itr.hasNext():
        entry = itr.next()
        k = entry.getKey()
        v = entry.getValue()
        if k.startswith("fs.s3a."):
            print(f"{k} = {v}")
    print("=== END fs.s3a ===")

    RUN_DATE = os.getenv("RUN_DATE")
    if RUN_DATE:
        RUN_DATE = pd.to_datetime(RUN_DATE).date()
        print(f"[INFO] RUN_DATE (from env) = {os.getenv('RUN_DATE')}")

    return CFG, BRONZE, SILVER, REF, spark, RUN_DATE

# ==========================================================================
# MAIN LOGIC
# ==========================================================================
def main():
    CFG, BRONZE, SILVER, REF, spark, RUN_DATE = bootstrap()

    # [NEW] log START (JSON)
    log.info(json.dumps({"stage":"transform","event":"start","run_date":str(RUN_DATE) if RUN_DATE else None}, ensure_ascii=False))

    if RUN_DATE:
        order_path = f"{BRONZE}/orders_{RUN_DATE}.csv"
        event_path = f"{BRONZE}/events_{RUN_DATE}.csv"
    else:
        order_path = f"{BRONZE}/orders_*.csv"
        event_path = f"{BRONZE}/events_*.csv"
    
    print(f"[INFO] Reading orders from {order_path}")
    print(f"[INFO] Reading events from {event_path}")
   
    
    # Đọc trực tiếp từ S3 (Spark tự handle)
    orders = spark.read.csv(order_path, header=True, inferSchema=True)
    events = spark.read.csv(event_path, header=True, inferSchema=True)

    # reference vẫn local
    users = spark.read.csv((REF / "users.csv").as_posix(), header=True, inferSchema=True)


    orders = orders.withColumn("order_ts", F.to_timestamp("order_ts"))
    events = events.withColumn("ts", F.to_timestamp("ts"))

    if RUN_DATE:
        orders = orders.filter(F.to_date("order_ts") == str(RUN_DATE))
        events = events.filter(F.to_date("ts") == str(RUN_DATE))

    daily_orders = (
        orders.withColumn("order_date", F.to_date("order_ts"))
              .groupBy("order_date")
              .agg(
                  F.countDistinct("order_id").alias("orders"),
                  F.countDistinct("user_id").alias("buyers"),
                  F.sum("amount").alias("revenue")
              )
    )

    daily_revenue = (
        orders.withColumn("order_date", F.to_date("order_ts"))
              .groupBy("order_date", "user_id")
              .agg(F.sum("amount").alias("amount"))
    )

    dau = (
        events.withColumn("event_date", F.to_date("ts"))
              .groupBy("event_date")
              .agg(F.countDistinct("user_id").alias("dau"))
    )

    dau_ch = (
        events.withColumn("event_date", F.to_date("ts"))
              .select("event_date", "user_id").distinct()
              .join(users.select("user_id", "channel"), on="user_id", how="left")
              .groupBy("event_date", "channel")
              .agg(F.countDistinct("user_id").alias("dau"))
    )

    rev_ch = (
        orders.withColumn("order_date", F.to_date("order_ts"))
              .select("order_date", "user_id", "amount")
              .join(users.select("user_id", "channel"), on="user_id", how="left")
              .groupBy("order_date", "channel")
              .agg(F.sum("amount").alias("revenue"))
    )

    conv = (
        dau.join(
            daily_orders.select("order_date", "buyers"),
            dau.event_date == daily_orders.order_date,
            how="left"
        )
        .fillna({"buyers": 0})
        .withColumn("conv_rate", F.round(F.col("buyers") / F.col("dau"), 4))
        .select(F.col("event_date").alias("date"), "dau", "buyers", "conv_rate")
    )

    # Save locally (silver layer) — GIỮ NGUYÊN
    if not str(SILVER).startswith("s3a://"):
        daily_orders.toPandas().to_csv(Path(SILVER) / "daily_orders.csv", index=False, encoding="utf-8")
        dau.toPandas().to_csv(Path(SILVER) / "daily_active_users.csv", index=False, encoding="utf-8")
        dau_ch.toPandas().to_csv(Path(SILVER) / "daily_active_users_by_channel.csv", index=False, encoding="utf-8")
        rev_ch.toPandas().to_csv(Path(SILVER) / "daily_revenue_by_channel.csv", index=False, encoding="utf-8")
        conv.toPandas().to_csv(Path(SILVER) / "daily_conversion.csv", index=False, encoding="utf-8")

    # Save to S3 — GIỮ NGUYÊN
    tables = {
        "daily_orders": {
            "df": daily_orders,
            "partition_cols": ["order_date"],
            "s3_path": os.getenv("S3_DAILY_ORDERS_PATH", "s3a://de-pipeline-test-853409708/silver/daily_orders/")
        },
        "daily_revenue": {
            "df": daily_revenue,
            "partition_cols": ["order_date"],
            "s3_path": os.getenv("S3_DAILY_REVENUE_PATH", "s3a://de-pipeline-test-853409708/silver/daily_revenue/")
        },
        "daily_active_users": {
            "df": dau,
            "partition_cols": ["event_date"],
            "s3_path": os.getenv("S3_DAU_PATH", "s3a://de-pipeline-test-853409708/silver/daily_active_users/")
        },
        "daily_active_users_by_channel": {
            "df": dau_ch,
            "partition_cols": ["event_date"],
            "s3_path": os.getenv("S3_DAU_CHANNEL_PATH", "s3a://de-pipeline-test-853409708/silver/daily_active_users_by_channel/")
        },
        "daily_revenue_by_channel": {
            "df": rev_ch,
            "partition_cols": ["order_date"],
            "s3_path": os.getenv("S3_REVENUE_CHANNEL_PATH", "s3a://de-pipeline-test-853409708/silver/daily_revenue_by_channel/")
        },
        "daily_conversion": {
            "df": conv.withColumnRenamed("date", "partition_date"),
            "partition_cols": ["partition_date"],
            "s3_path": os.getenv("S3_CONVERSION_PATH", "s3a://de-pipeline-test-853409708/silver/daily_conversion/")
        },
    }

    written = []
    for name, props in tables.items():
        df = props["df"]
        s3_path = props["s3_path"]
        partition_cols = props["partition_cols"]

        rows_out = None
        if COUNT_OUTPUT:
            try:
                rows_out = df.count()
            except Exception:
                rows_out = -1

        for col in partition_cols:
            df = df.withColumn(col, F.col(col).cast("string"))

        df.write \
          .partitionBy(*partition_cols) \
          .mode("overwrite") \
          .option("header", True) \
          .csv(s3_path)

        print(f"[transform_spark] wrote {name} to S3 at {s3_path}, partitioned by {partition_cols}")
        log.info(json.dumps({
            "stage":"transform","event":"write_done","run_date":str(RUN_DATE) if RUN_DATE else None,
            "table":name,"partition_cols":partition_cols,"rows_out":rows_out,"path":s3_path
        }, ensure_ascii=False))
        written.append(name)

    print(f"[transform_spark] wrote silver CSVs for RUN_DATE={RUN_DATE or 'ALL'}")

    import pyfiglet
    banner = pyfiglet.figlet_format("FINISH")
    print(banner)

    log.info(json.dumps({
        "stage":"transform","event":"done","run_date":str(RUN_DATE) if RUN_DATE else None,
        "tables_written":written,"duration_s":round(time.time()-_t0,2)
    }, ensure_ascii=False))

# ==========================================================================
# ENTRY
# ==========================================================================
if __name__ == "__main__":
    main()