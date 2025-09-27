FROM bitnami/spark:3.5.0

WORKDIR /app

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -sf $(which python3) /usr/bin/python3.9 && \
    rm -rf /var/lib/apt/lists/*

# Cài đầy đủ libs Python cho pipeline
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    boto3 \
    pandas \
    pyyaml \
    pyarrow \
    pyfiglet

USER 1001