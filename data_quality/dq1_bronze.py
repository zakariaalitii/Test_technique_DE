import os
from datetime import datetime, timezone
from pyspark.sql import functions as F
from spark.utils.spark_session import make_spark, s3path
from spark.utils.quality import split_rows, gx_expectations
from monitoring.db import check, task_metric, run_id


def main():
    started=datetime.now(timezone.utc)
    spark=make_spark("dq1-bronze")

    try:
        start,end=os.environ["TAXI_START"][:10],os.environ["TAXI_END"][:10]
        raw=spark.read.parquet(s3path(f"bronze_parquet/chicago_taxi/start={start}/end={end}"))
        for name,ok,detail in gx_expectations(raw,[("expect_column_to_exist",[x]) for x in ["trip_id","trip_start_timestamp","trip_end_timestamp","trip_seconds","trip_miles","fare","trip_total"]]):
            check("DQ1",name,ok,details=detail)
        valid,bad=split_rows(raw)
        n,v,b=raw.count(),valid.count(),bad.count()
        check("DQ1","row_count_reconciles",n==v+b,n,details={"source":n,"valid":v,"rejected":b})
        check("DQ1","valid_rows_exist",v>0,v)
        valid.write.mode("overwrite").parquet(s3path(f"validated/chicago_taxi/start={start}/end={end}"))
        bad.withColumn("run_id",F.lit(run_id())).withColumn("rejected_at",F.current_timestamp()).write.mode("overwrite").parquet(s3path(f"rejects/chicago_taxi/start={start}/end={end}"))
        task_metric("dq1_bronze","SUCCESS",started,n,v,b)

    except Exception as e:
        task_metric("dq1_bronze","FAILED",started,error=e);raise
    
    finally:spark.stop()

if __name__=="__main__":main()
