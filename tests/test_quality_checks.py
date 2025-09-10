import subprocess, os, sys

def test_quality_script_runs():
    # run quality script after ingest/transform to ensure data exists
    subprocess.check_call([sys.executable, "pipelines/ingest.py"])
    subprocess.check_call([sys.executable, "pipelines/transform.py"])
    subprocess.check_call([sys.executable, "pipelines/quality_checks.py"])
