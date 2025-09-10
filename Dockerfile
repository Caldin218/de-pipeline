FROM bitnami/spark:3.5.0

WORKDIR /app

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s $(which python3) /usr/bin/python3.9 && \
    rm -rf /var/lib/apt/lists/*

USER 1001

COPY pipelines/ ./pipelines/
COPY configs/ ./configs/
COPY requirements.txt .

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r requirements.txt

CMD ["python3", "pipelines/transform_spark.py"]
