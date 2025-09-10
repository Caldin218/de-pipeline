from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestS3").getOrCreate()

df = spark.range(5)

# đúng bucket ở AWS console: de-pipeline-test-853409708
df.write.mode("overwrite").parquet("s3a://de-pipeline-test-853409708/test_s3_write/")

spark.stop()
