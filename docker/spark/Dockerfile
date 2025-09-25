FROM bitnami/spark:3.5.0

WORKDIR /app

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s $(which python3) /usr/bin/python3.9 && \
    rm -rf /var/lib/apt/lists/*

# ✅ Cài pyspark (kèm py4j) + boto3 để Spark nói chuyện với AWS
RUN pip3 install --no-cache-dir pyspark==3.5.0 boto3

USER 1001