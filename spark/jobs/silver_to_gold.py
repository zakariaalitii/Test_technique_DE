import os
from spark.utils.spark_session import make_spark, s3path
from spark.utils.transforms import gold_tables


def main():
    spark = make_spark("silver-to-gold")

    try:
        start, end = os.environ["TAXI_START"][:10], os.environ["TAXI_END"][:10]
        silver = spark.read.parquet(s3path(f"silver/chicago_taxi/start={start}/end={end}"))
        for name, table in gold_tables(silver).items():
            n = table.count()
            if n == 0:
                raise ValueError(f"Gold table {name} is empty")
            table.write.mode("overwrite").parquet(s3path(f"gold/chicago_taxi/start={start}/end={end}/{name}"))
            print(f"GOLD {name}: {n} rows")
            
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
