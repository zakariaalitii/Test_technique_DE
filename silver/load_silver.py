import os
from datetime import datetime, timezone
from spark.utils.spark_session import make_spark,s3path
from monitoring.db import connection, task_metric, run_id
from pyspark.sql import functions as F

COLUMNS=["trip_id","trip_start","trip_end","trip_date","trip_seconds","trip_miles","fare","tips","trip_total","pickup_community_area","dropoff_community_area","payment_type","company","source_run_id"]

def main():
    started=datetime.now(timezone.utc)
    spark=make_spark("load-silver")
    try:
        start,end=os.environ["TAXI_START"][:10],os.environ["TAXI_END"][:10]
        valid=spark.read.parquet(s3path(f"silver/chicago_taxi/start={start}/end={end}"))
        n=valid.count()
        if not n: raise ValueError("No valid records")
        df=valid.withColumn("source_run_id",F.lit(run_id())).select(*COLUMNS)
        jdbc=f"jdbc:postgresql://{os.getenv('WAREHOUSE_HOST','warehouse')}:5432/{os.getenv('WAREHOUSE_DB','taxi')}"
        options={"url":jdbc,"user":os.getenv("WAREHOUSE_USER","taxi"),"password":os.environ["WAREHOUSE_PASSWORD"],"driver":"org.postgresql.Driver"}
        with connection() as db:
            with db.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS silver.trips_stage (LIKE silver.trips INCLUDING DEFAULTS)")
                cur.execute("TRUNCATE silver.trips_stage")
        df.write.format("jdbc").options(**options).option("dbtable","silver.trips_stage").mode("append").save()
        with connection() as db:
            with db.cursor() as cur:
                cur.execute("SELECT count(*) FROM silver.trips_stage")
                staged=cur.fetchone()[0]
                if staged!=n: raise AssertionError(f"JDBC staged {staged} but expected {n}")
                cur.execute("DELETE FROM silver.trips WHERE trip_start >= %s AND trip_start < %s",(os.environ["TAXI_START"],os.environ["TAXI_END"]))
                cur.execute("""INSERT INTO silver.trips ("""+",".join(COLUMNS)+""") SELECT """+",".join(COLUMNS)+""" FROM silver.trips_stage WHERE true
                    ON CONFLICT (trip_id) DO UPDATE SET """+",".join(f"{x}=EXCLUDED.{x}" for x in COLUMNS if x!="trip_id"))
        task_metric("load_silver","SUCCESS",started,n,n,0)
    except Exception as e:
        task_metric("load_silver","FAILED",started,error=e);raise
    finally:spark.stop()

if __name__=="__main__":main()
