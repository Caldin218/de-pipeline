# Sử dụng image chính thức
FROM bitnami/spark:3.5.0

# Set working dir
WORKDIR /app

# Copy code vào container
COPY pipelines/ ./pipelines/
COPY configs/ ./configs/
COPY requirements.txt .

# Cài dependencies Python
RUN pip install -r requirements.txt

# Entrypoint mặc định (chạy transform_spark)
CMD ["python", "pipelines/transform_spark.py"]
