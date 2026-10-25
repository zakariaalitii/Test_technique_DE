import os
from datetime import datetime, timezone
from pyspark.sql import functions as F
from spark.utils.spark_session import make_spark, s3path
from monitoring.db import check, task_metric

def main():
    started = datetime.now(timezone.utc)
    spark = make_spark("dq3-gold-minio")
    try:
        start, end = os.environ["TAXI_START"][:10], os.environ["TAXI_END"][:10]
        silver = spark.read.parquet(s3path(f"silver/chicago_taxi/start={start}/end={end}"))
        prefix = f"gold/chicago_taxi/start={start}/end={end}"
        daily = spark.read.parquet(s3path(f"{prefix}/daily_revenue"))
        stats = spark.read.parquet(s3path(f"{prefix}/trip_statistics"))
        zones = spark.read.parquet(s3path(f"{prefix}/pickup_zones"))
        payment = spark.read.parquet(s3path(f"{prefix}/payment_methods"))
        n = silver.count()
        silver_total = silver.agg(F.sum("trip_total")).first()[0] or 0
        for name, df in [("daily", daily), ("statistics", stats), ("payment", payment)]:
            aggregated = df.agg(F.sum("trip_count")).first()[0] or 0
            check("DQ3", f"{name}_trip_count_reconciles", aggregated == n, aggregated, details={"silver": n})
        non_null_pickups = silver.filter(F.col("pickup_community_area").isNotNull()).count()
        zone_count = zones.agg(F.sum("trip_count")).first()[0] or 0
        check("DQ3", "pickup_zone_count_reconciles", zone_count == non_null_pickups, zone_count)
        for name, df in [("daily", daily), ("payment", payment)]:
            revenue = df.agg(F.sum("total_revenue")).first()[0] or 0
            # Individual groups are rounded to cents, so tolerate cumulative rounding error.
            tolerance = max(0.01, df.count() * 0.005 + 0.01)
            check("DQ3", f"{name}_revenue_reconciles", abs(float(revenue)-float(silver_total)) <= tolerance, revenue, details={"silver": str(silver_total), "tolerance": tolerance})
        invalid = daily.filter((F.col("trip_count") < 0) | (F.col("total_revenue") < 0)).count()
        check("DQ3", "daily_nonnegative", invalid == 0, invalid)
        task_metric("dq3_gold_minio", "SUCCESS", started, n, n, 0)
    except Exception as exc:
        task_metric("dq3_gold_minio", "FAILED", started, error=exc)
        raise
    finally:
        spark.stop()

if __name__ == "__main__": main()
