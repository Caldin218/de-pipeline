from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Khởi tạo SparkSession
    spark = SparkSession.builder.appName("ArrowSparkTest").getOrCreate()

    # Tạo DataFrame mẫu
    data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    df = spark.createDataFrame(data, ["id", "name"])

    print("===== SHOW DATAFRAME =====")
    df.show()

    # Tắt SparkSession
    spark.stop()
