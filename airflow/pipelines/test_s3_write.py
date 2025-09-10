from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestS3Write")
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_KEY")
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-1.amazonaws.com")
    .getOrCreate()
)

df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
df.write.mode("overwrite").csv(
    "s3a://de-pipeline-test-853409708/silver/test_commit/",
    header=True
)

print("✅ Write success")
