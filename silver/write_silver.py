import os
from datetime import datetime, timezone
from pyspark.sql import functions as F
from spark.utils.spark_session import make_spark, s3path
from monitoring.db import task_metric

COLUMNS = ["trip_id", "trip_start", "trip_end", "trip_date", "trip_seconds", "trip_miles", "fare", "tips", "trip_total", "pickup_community_area", "dropoff_community_area", "payment_type", "company"]

def main():
    started = datetime.now(timezone.utc)
    spark = make_spark("silver-minio")
    try:
        start, end = os.environ["TAXI_START"][:10], os.environ["TAXI_END"][:10]
        source = s3path(f"validated/chicago_taxi/start={start}/end={end}")
        target = s3path(f"silver/chicago_taxi/start={start}/end={end}")
        df = spark.read.parquet(source).select(*COLUMNS)
        n = df.count()
        if n == 0: raise ValueError("Silver is empty")
        df.repartition(2).write.mode("overwrite").parquet(target)
        actual = spark.read.parquet(target).count()
        if actual != n: raise AssertionError(f"Silver write mismatch: {n} vs {actual}")
        task_metric("write_silver_minio", "SUCCESS", started, n, actual, 0)
    except Exception as exc:
        task_metric("write_silver_minio", "FAILED", started, error=exc)
        raise
    finally:
        spark.stop()

if __name__ == "__main__": main()
