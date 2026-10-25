import os
from pyspark.sql import functions as F
from spark.utils.spark_session import make_spark, s3path


def main():
    spark = make_spark("validate-gold")

    try:
        start, end = os.environ["TAXI_START"][:10], os.environ["TAXI_END"][:10]
        root = f"gold/chicago_taxi/start={start}/end={end}"
        daily = spark.read.parquet(s3path(f"{root}/daily_revenue"))
        stats = spark.read.parquet(s3path(f"{root}/trip_statistics"))
        zones = spark.read.parquet(s3path(f"{root}/pickup_zones"))
        silver = spark.read.parquet(s3path(f"silver/chicago_taxi/start={start}/end={end}"))
        silver_count = silver.count()
        assert daily.agg(F.sum("trip_count")).first()[0] == silver_count
        assert stats.agg(F.sum("trip_count")).first()[0] == silver_count
        assert daily.filter((F.col("total_revenue") < 0) | F.col("trip_date").isNull()).count() == 0
        assert zones.filter(F.col("trip_count") <= 0).count() == 0
        print(f"PASS: {silver_count} Silver trips reconcile with daily and stats Gold tables")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
