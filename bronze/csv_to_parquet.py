import os
from datetime import datetime, timezone
from spark.utils.spark_session import make_spark, s3path
from monitoring.db import task_metric


def main():
    started=datetime.now(timezone.utc)
    spark=make_spark("bronze-parquet")
    try:
        start,end=os.environ["TAXI_START"][:10],os.environ["TAXI_END"][:10]
        raw=spark.read.option("header","true").option("mode","PERMISSIVE").csv(s3path(f"bronze/chicago_taxi/start={start}/end={end}/part-*.csv"))
        n=raw.count()
        if n == 0: raise ValueError("Bronze is empty")
        raw.write.mode("overwrite").parquet(s3path(f"bronze_parquet/chicago_taxi/start={start}/end={end}"))
        task_metric("bronze_parquet","SUCCESS",started,n,n,0)
    except Exception as e:
        task_metric("bronze_parquet","FAILED",started,error=e);raise
    finally:spark.stop()

if __name__=="__main__":main()
