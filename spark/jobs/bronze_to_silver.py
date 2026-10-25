import os
from pyspark.sql import functions as F
from spark.utils.spark_session import make_spark, s3path
from spark.utils.transforms import clean_trips


def main():
    spark = make_spark("bronze-to-silver")

    try:
        start, end = os.environ["TAXI_START"][:10], os.environ["TAXI_END"][:10]
        bronze = s3path(f"bronze/chicago_taxi/start={start}/end={end}/part-*.csv")
        raw = spark.read.option("header", "true").option("mode", "PERMISSIVE").csv(bronze)
        cleaned = clean_trips(raw).cache()
        raw_count, clean_count = raw.count(), cleaned.count()
        if clean_count == 0:
            raise ValueError("Silver is empty after quality filtering")
        if clean_count > raw_count:
            raise AssertionError("Silver count exceeds Bronze")
        output = s3path(f"silver/chicago_taxi/start={start}/end={end}")
        cleaned.write.mode("overwrite").partitionBy("trip_date").parquet(output)
        print(f"BRONZE={raw_count}, SILVER={clean_count}, REMOVED={raw_count-clean_count}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
