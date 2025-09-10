from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os, pathlib, yaml, glob
import pandas as pd

# =======================================================================
#                            INFRA / BOOTSTRAP
# =======================================================================

config_path = "configs/config.yaml"
print(f"[DEBUG] Loading config from: {config_path}")
CFG = yaml.safe_load(open(config_path, "r", encoding="utf-8"))

print("=== CONFIG.YAML raw dump ===")
for k, v in CFG.get("spark_conf", {}).items():
    print(f"{k}: {v}")
print("======================HEHEHEHEHE===========")

BRONZE = pathlib.Path(CFG["paths"]["bronze"]).absolute()
SILVER = pathlib.Path(CFG["paths"]["silver"]).absolute()
SILVER.mkdir(parents=True, exist_ok=True)
REF = pathlib.Path(CFG["paths"]["reference"]).absolute()

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

# =======================================================================
#                                MAIN
# =======================================================================

def main():
    order_files = glob.glob(str(BRONZE / "orders_*.csv"))
    event_files = glob.glob(str(BRONZE / "events_*.csv"))

    print("Order files:", order_files)
    print("Event files:", event_files)

    orders = spark.read.csv(order_files, header=True, inferSchema=True)
    events = spark.read.csv(event_files, header=True, inferSchema=True)
    users  = spark.read.csv(str(REF / "users.csv").replace("\\", "/"), header=True, inferSchema=True)

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

    daily_orders.toPandas().to_csv(SILVER / "daily_orders.csv", index=False, encoding="utf-8")
    dau.toPandas().to_csv(SILVER / "daily_active_users.csv", index=False, encoding="utf-8")
    dau_ch.toPandas().to_csv(SILVER / "daily_active_users_by_channel.csv", index=False, encoding="utf-8")
    rev_ch.toPandas().to_csv(SILVER / "daily_revenue_by_channel.csv", index=False, encoding="utf-8")
    conv.toPandas().to_csv(SILVER / "daily_conversion.csv", index=False, encoding="utf-8")

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

    for name, props in tables.items():
        df = props["df"]
        s3_path = props["s3_path"]
        partition_cols = props["partition_cols"]

        for col in partition_cols:
            df = df.withColumn(col, F.col(col).cast("string"))

        df.write \
          .partitionBy(*partition_cols) \
          .mode("overwrite") \
          .option("header", True) \
          .csv(s3_path)

        print(f"[transform_spark] wrote {name} to S3 at {s3_path}, partitioned by {partition_cols}")

    print(f"[transform_spark] wrote silver CSVs for RUN_DATE={RUN_DATE or 'ALL'}")

# =======================================================================
#                                 ENTRY
# =======================================================================

if __name__ == "__main__":
    main()
    import pyfiglet
    banner = pyfiglet.figlet_format("FINISH")
    print(banner)
