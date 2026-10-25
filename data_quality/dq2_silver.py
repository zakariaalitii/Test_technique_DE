"""Great Expectations checks on the authoritative Silver MinIO Parquet snapshot."""
import os
from datetime import datetime, timezone
from pyspark.sql import functions as F
from spark.utils.spark_session import make_spark, s3path
from spark.utils.quality import gx_expectations
from monitoring.db import check, task_metric

def main():
    started = datetime.now(timezone.utc)
    spark = make_spark("dq2-silver-minio")

    try:
        start, end = os.environ["TAXI_START"][:10], os.environ["TAXI_END"][:10]
        df = spark.read.parquet(s3path(f"silver/chicago_taxi/start={start}/end={end}"))
        n = df.count()
        check("DQ2", "silver_not_empty", n > 0, n)
        checks = [("expect_column_values_to_not_be_null", ["trip_id"]), ("expect_column_values_to_be_unique", ["trip_id"]), ("expect_column_values_to_be_between", ["trip_miles", 0, None]), ("expect_column_values_to_be_between", ["trip_total", 0, None]), ("expect_column_values_to_be_between", ["trip_seconds", 0, None])]
        for name, ok, details in gx_expectations(df, checks): check("DQ2", name, ok, details=details)
        invalid = df.filter(F.col("trip_end") < F.col("trip_start")).count()
        check("DQ2", "end_after_start", invalid == 0, invalid)
        task_metric("dq2_silver", "SUCCESS", started, n, n, 0)

    except Exception as exc:
        task_metric("dq2_silver", "FAILED", started, error=exc)
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__": main()
