"""Produce the business Gold aggregates using PySpark; persist every table to MinIO."""
import os
from datetime import datetime, timezone
from pyspark.sql import functions as F
from spark.utils.spark_session import make_spark, s3path
from spark.utils.transforms import gold_tables
from monitoring.db import task_metric

def gold_prefix():
    return f"gold/chicago_taxi/start={os.environ['TAXI_START'][:10]}/end={os.environ['TAXI_END'][:10]}"

def main():
    started = datetime.now(timezone.utc)
    spark = make_spark("gold-aggregations-minio")

    try:
        start, end = os.environ["TAXI_START"][:10], os.environ["TAXI_END"][:10]
        silver = spark.read.parquet(s3path(f"silver/chicago_taxi/start={start}/end={end}"))
        n = silver.count()
        if not n: raise ValueError("No Silver rows to aggregate")
        tables = gold_tables(silver)
        payment = silver.withColumn("payment_type", F.coalesce("payment_type", F.lit("UNKNOWN"))).groupBy("payment_type").agg(F.count("*").alias("trip_count"), F.round(F.sum("trip_total"), 2).alias("total_revenue"))
        tables["payment_methods"] = payment
        for name, df in tables.items():
            path = s3path(f"{gold_prefix()}/{name}")
            df.coalesce(1).write.mode("overwrite").parquet(path)
            if spark.read.parquet(path).count() == 0: raise AssertionError(f"Empty Gold dataset: {name}")
        task_metric("build_gold_minio", "SUCCESS", started, n, n, 0)

    except Exception as exc:
        task_metric("build_gold_minio", "FAILED", started, error=exc)
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__": main()
