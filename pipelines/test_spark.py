from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TestSparkConnection").getOrCreate()

    df = spark.range(0, 1000)
    count = df.count()

    print("==== Spark test job ran successfully, CALDINNNNNNNNNNNNNNN ====")
    print(f"Row count: {count}")

    spark.stop()
