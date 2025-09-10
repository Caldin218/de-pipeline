import requests
import os

def slack_alert(context):
    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_url:
        print("⚠️ No SLACK_WEBHOOK_URL found, skipping alert")
        return

    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    message = (
        f"❌ Airflow Task Failed!\n"
        f"DAG: {dag_id}\n"
        f"TASK: {task_id}\n"
        f"DATE: {execution_date}\n"
        f"Log: {log_url}"
    )

    requests.post(slack_url, json={"text": message})
