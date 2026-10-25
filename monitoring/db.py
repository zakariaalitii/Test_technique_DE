import json
import os
from datetime import datetime, timezone
import psycopg2


def connection():
    return psycopg2.connect(host=os.getenv("WAREHOUSE_HOST", "warehouse"), port=int(os.getenv("WAREHOUSE_PORT", "5432")), dbname=os.getenv("WAREHOUSE_DB", "taxi"), user=os.getenv("WAREHOUSE_USER", "taxi"), password=os.environ["WAREHOUSE_PASSWORD"])


def run_id():
    return os.getenv("PIPELINE_RUN_ID", "manual")


def check(stage, name, success, value=None, details=None):
    with connection() as db:
        with db.cursor() as cur:
            cur.execute("INSERT INTO data_quality.check_results(run_id,stage,check_name,success,observed_value,details) VALUES (%s,%s,%s,%s,%s,%s::jsonb)", (run_id(), stage, name, bool(success), str(value) if value is not None else None, json.dumps(details or {})))
    if not success:
        raise AssertionError(f"DQ {stage}: {name} failed (observed={value})")


def task_metric(name, status, started, input_rows=None, output_rows=None, rejected_rows=None, error=None):
    now = datetime.now(timezone.utc)
    with connection() as db:
        with db.cursor() as cur:
            cur.execute("""INSERT INTO monitoring.pipeline_runs(run_id,task_name,status,input_rows,output_rows,rejected_rows,started_at,finished_at,duration_seconds,error_message)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id,task_name) DO UPDATE SET status=EXCLUDED.status,input_rows=EXCLUDED.input_rows,output_rows=EXCLUDED.output_rows,rejected_rows=EXCLUDED.rejected_rows,started_at=EXCLUDED.started_at,finished_at=EXCLUDED.finished_at,duration_seconds=EXCLUDED.duration_seconds,error_message=EXCLUDED.error_message""", (run_id(), name, status, input_rows, output_rows, rejected_rows, started, now, (now-started).total_seconds(), str(error)[:2000] if error else None))
