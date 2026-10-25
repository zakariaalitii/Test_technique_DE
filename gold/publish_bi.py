import os
from datetime import datetime, timezone
from spark.utils.spark_session import make_spark, s3path
from monitoring.db import connection, task_metric

TABLES = {
 "daily_revenue": ("trip_date DATE, trip_count BIGINT, total_revenue NUMERIC, avg_ticket NUMERIC", ["trip_date", "trip_count", "total_revenue", "avg_ticket"]),
 "trip_statistics": ("trip_date DATE, trip_count BIGINT, avg_duration_minutes NUMERIC, avg_distance_miles NUMERIC, avg_fare NUMERIC", ["trip_date", "trip_count", "avg_duration_minutes", "avg_distance_miles", "avg_fare"]),
 "pickup_zones": ("pickup_community_area INTEGER, trip_count BIGINT, total_revenue NUMERIC", ["pickup_community_area", "trip_count", "total_revenue"]),
 "payment_methods": ("payment_type TEXT, trip_count BIGINT, total_revenue NUMERIC", ["payment_type", "trip_count", "total_revenue"]),
}

def main():
    started = datetime.now(timezone.utc)
    spark = make_spark("publish-gold-bi")
    try:
        start, end = os.environ["TAXI_START"][:10], os.environ["TAXI_END"][:10]
        prefix = f"gold/chicago_taxi/start={start}/end={end}"
        jdbc = f"jdbc:postgresql://{os.getenv('WAREHOUSE_HOST', 'warehouse')}:{os.getenv('WAREHOUSE_PORT', '5432')}/{os.getenv('WAREHOUSE_DB', 'taxi')}"
        options = {"url": jdbc, "user": os.getenv("WAREHOUSE_USER", "taxi"), "password": os.environ["WAREHOUSE_PASSWORD"], "driver": "org.postgresql.Driver"}

        for name, (definition, cols) in TABLES.items():
            df = spark.read.parquet(s3path(f"{prefix}/{name}")).select(*cols)
            with connection() as db:
                with db.cursor() as cur:
                    cur.execute(f"CREATE TABLE IF NOT EXISTS gold.kpi_{name} ({definition})")
                    cur.execute(f"CREATE TABLE IF NOT EXISTS gold.stage_{name} ({definition})")
                    cur.execute(f"TRUNCATE gold.stage_{name}")
            df.write.format("jdbc").options(**options).option("dbtable", f"gold.stage_{name}").mode("append").save()
            with connection() as db:
                with db.cursor() as cur:
                    cur.execute(f"SELECT count(*) FROM gold.stage_{name}")
                    if cur.fetchone()[0] != df.count(): raise AssertionError(f"BI staging mismatch: {name}")
        with connection() as db:
            with db.cursor() as cur:
                for name, (_, cols) in TABLES.items():
                    cur.execute(f"DELETE FROM gold.kpi_{name}")
                    cur.execute(f"INSERT INTO gold.kpi_{name} ({','.join(cols)}) SELECT {','.join(cols)} FROM gold.stage_{name}")

                cur.execute("CREATE OR REPLACE VIEW gold.v_daily_revenue AS SELECT * FROM gold.kpi_daily_revenue")
                cur.execute("CREATE OR REPLACE VIEW gold.v_trip_statistics AS SELECT * FROM gold.kpi_trip_statistics")
                cur.execute("CREATE OR REPLACE VIEW gold.v_pickup_zones AS SELECT * FROM gold.kpi_pickup_zones")
                cur.execute("CREATE OR REPLACE VIEW gold.v_payment_methods AS SELECT * FROM gold.kpi_payment_methods")
        task_metric("publish_gold_bi", "SUCCESS", started)
    except Exception as exc:
        task_metric("publish_gold_bi", "FAILED", started, error=exc)
        raise
    finally:
        spark.stop()

if __name__ == "__main__": main()
