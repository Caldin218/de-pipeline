from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, subprocess
from utils.alerts import slack_alert

# ===============================================================
# Default arguments
# =============================
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": slack_alert,
    "email": ["ngoctung21805@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

# ===============================
# Helpers (dùng context["ds"])
# =============================
def run_ingest(**context):
    env = os.environ.copy()
    env["RUN_DATE"] = context["ds"]   # lấy ngày từ DAG run
    res = subprocess.run(
        ["python", "/opt/airflow/dags/pipelines/ingest.py"],
        env=env,
        check=True   # BẮT BUỘC, propagate sys.exit
    )


def run_quality(**context):
    env = os.environ.copy()
    env["RUN_DATE"] = context["ds"]
    res = subprocess.run(
        ["python", "/opt/airflow/dags/pipelines/quality_checks.py"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    print(res.stdout)  # log ra hết output
    if res.returncode != 0:
        raise Exception(f"❌ Quality checks failed (RC={res.returncode})")
    else:
        print("✅ Quality checks passed")



def run_load(**context):
    env = os.environ.copy()
    env["RUN_DATE"] = context["ds"]
    res = subprocess.run(
        ["python", "/opt/airflow/dags/pipelines/load_s3.py"],
        env=env,
        check=True   # BẮT BUỘC
    )
    

# =============================
# DAG definition
# =============================
with DAG(
    dag_id="de_pipeline_dag",
    start_date=datetime(2025, 8, 20),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["de", "production"],
) as dag:

    # Task 0: Ingest raw data
    ingest_raw = PythonOperator(
        task_id="ingest_raw_files",
        python_callable=run_ingest,
        provide_context=True,
    )

    # Task 1: Run Spark transformation
    run_transform = SparkSubmitOperator(
        task_id="run_transform",
        application="/opt/airflow/dags/pipelines/transform_spark.py",
        conn_id="spark_default",
        verbose=True,
        jars="/opt/bitnami/spark/extra-jars/hadoop-aws-3.3.4.jar,"
             "/opt/bitnami/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/bitnami/spark/extra-jars/hadoop-cloud-storage-3.3.4.jar,"
             "/opt/bitnami/spark/extra-jars/hadoop-committer-3.3.4.jar",
        application_args=["--run_date", "{{ ds }}"],  # SparkSubmitOperator hỗ trợ templating
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client",

            # S3A configs
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
            "spark.hadoop.fs.s3a.endpoint": "s3.ap-southeast-1.amazonaws.com",
            "spark.hadoop.fs.s3a.region": "ap-southeast-1",

            # Magic Committer configs
            "spark.sql.parquet.output.committer.class": "org.apache.hadoop.fs.s3a.commit.S3ACommitter",
            "mapreduce.outputcommitter.factory.scheme.s3a": "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
            "spark.hadoop.fs.s3a.committer.name": "magic",
            "spark.hadoop.fs.s3a.committer.magic.enabled": "true",
            "spark.hadoop.fs.s3a.committer.staging.conflict-mode": "replace",
            "spark.hadoop.fs.s3a.committer.staging.abort.pending_uploads": "true",

            # AWS creds for driver & executor
            "spark.driverEnv.AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
            "spark.driverEnv.AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
            "spark.driverEnv.AWS_SESSION_TOKEN": "{{ var.value.AWS_SESSION_TOKEN | default('') }}",
            "spark.driverEnv.AWS_REGION": "ap-southeast-1",

            "spark.executorEnv.AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
            "spark.executorEnv.AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
            "spark.executorEnv.AWS_SESSION_TOKEN": "{{ var.value.AWS_SESSION_TOKEN | default('') }}",
            "spark.executorEnv.AWS_REGION": "ap-southeast-1",
        },
    )

    # Task 2: Quality checks
    quality_checks = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality,
        provide_context=True,
    )

    # Task 3: Load to S3
    load_to_s3 = PythonOperator(
        task_id="load_to_s3",
        python_callable=run_load,
        provide_context=True,
    )

    # [ADD] SLA 20 phút cho 4 task
    for t in [ingest_raw, run_transform, quality_checks, load_to_s3]:
        t.sla = timedelta(minutes=20)

    # Task dependencies
    ingest_raw >> run_transform >> quality_checks >> load_to_s3

    #............