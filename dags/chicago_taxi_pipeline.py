import os
import subprocess
from datetime import datetime,timedelta,timezone
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from ingestion.download_taxi_data import run as ingest
from monitoring.db import task_metric


def env_for_run():
    context=get_current_context()
    return {**os.environ,"PIPELINE_RUN_ID":context["run_id"]}


def extract():
    started=datetime.now(timezone.utc)
    old=os.environ.get("PIPELINE_RUN_ID")
    os.environ["PIPELINE_RUN_ID"]=get_current_context()["run_id"]
    try:
        manifest=ingest()
        task_metric("extract_bronze","SUCCESS",started,manifest["rows"],manifest["rows"],0)
    except Exception as e:
        task_metric("extract_bronze","FAILED",started,error=e);raise
    finally:
        if old is None:os.environ.pop("PIPELINE_RUN_ID",None)
        else:os.environ["PIPELINE_RUN_ID"]=old


def run_script(path,spark=False):
    env=env_for_run()
    env["PYTHONPATH"]="/opt/airflow/project"
    if spark:
        cmd=["spark-submit","--master","local[2]","--packages","org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.4","--conf","spark.sql.session.timeZone=America/Chicago",f"/opt/airflow/project/{path}"]
    else:cmd=["python",f"/opt/airflow/project/{path}"]
    subprocess.run(cmd,check=True,env=env)


with DAG(dag_id="test_technique_data_engineer_ma",start_date=datetime(2023,1,1),schedule_interval=None,catchup=False,
         max_active_runs=1,default_args={"owner":"data-engineering","retries":2,"retry_delay":timedelta(minutes=2)},
         tags=["bronze","great-expectations","postgres","gold"]) as dag:
    extract_bronze=PythonOperator(task_id="extract_to_bronze",python_callable=extract)
    parquet=PythonOperator(task_id="bronze_csv_to_parquet",python_callable=run_script,op_args=["bronze/csv_to_parquet.py",True])
    dq1=PythonOperator(task_id="dq1_bronze",python_callable=run_script,op_args=["data_quality/dq1_bronze.py",True])
    silver=PythonOperator(task_id="write_silver_minio",python_callable=run_script,op_args=["silver/write_silver.py",True])
    dq2=PythonOperator(task_id="dq2_silver",python_callable=run_script,op_args=["data_quality/dq2_silver.py",True])
    gold=PythonOperator(task_id="build_gold_minio",python_callable=run_script,op_args=["gold/build_gold_minio.py",True])
    dq3=PythonOperator(task_id="dq3_gold_minio",python_callable=run_script,op_args=["data_quality/dq3_gold_minio.py",True])
    serving_silver=PythonOperator(task_id="publish_silver_postgres",python_callable=run_script,op_args=["silver/load_silver.py",True])
    serving_star=PythonOperator(task_id="publish_star_schema",python_callable=run_script,op_args=["gold/build_gold.py"])
    serving_gold=PythonOperator(task_id="publish_gold_postgres",python_callable=run_script,op_args=["gold/publish_bi.py",True])
    serving_check=PythonOperator(task_id="validate_bi_postgres",python_callable=run_script,op_args=["data_quality/dq3_gold.py"])
    extract_bronze >> parquet >> dq1 >> silver >> dq2 >> gold >> dq3 >> serving_silver >> serving_star >> serving_gold >> serving_check
