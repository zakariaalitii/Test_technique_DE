"""Transactional dimensional build; Gold is committed only if internal reconciliations pass."""
import os
from datetime import datetime, timezone
from monitoring.db import connection,task_metric,check


def main():
    started=datetime.now(timezone.utc)
    start,end=os.environ["TAXI_START"],os.environ["TAXI_END"]

    try:
        with connection() as db:
            with db.cursor() as cur:
                cur.execute("SELECT count(*),coalesce(sum(trip_total),0) FROM silver.trips WHERE trip_start >= %s AND trip_start < %s",(start,end))
                n,revenue=cur.fetchone()
                if n==0:raise ValueError("Silver snapshot empty")
                cur.execute("""INSERT INTO gold.dim_date(date_key,full_date,year,month,day,day_of_week)
                  SELECT DISTINCT to_char(trip_date,'YYYYMMDD')::int,trip_date,extract(year from trip_date)::int,
                  extract(month from trip_date)::int,extract(day from trip_date)::int,extract(isodow from trip_date)::int
                  FROM silver.trips ON CONFLICT(date_key) DO NOTHING""")
                cur.execute("""INSERT INTO gold.dim_location(location_key,community_area)
                  SELECT DISTINCT community_area,community_area FROM (
                    SELECT pickup_community_area AS community_area FROM silver.trips UNION
                    SELECT dropoff_community_area FROM silver.trips) a WHERE community_area IS NOT NULL
                    ON CONFLICT(location_key) DO NOTHING""")
                cur.execute("""INSERT INTO gold.dim_payment_type(payment_type)
                  SELECT DISTINCT payment_type FROM silver.trips WHERE payment_type IS NOT NULL
                  ON CONFLICT(payment_type) DO NOTHING""")
                cur.execute("""INSERT INTO gold.dim_company(company)
                  SELECT DISTINCT company FROM silver.trips WHERE company IS NOT NULL
                  ON CONFLICT(company) DO NOTHING""")
                cur.execute("DELETE FROM gold.fact_trips WHERE date_key BETWEEN to_char(%s::timestamp,'YYYYMMDD')::int AND to_char((%s::timestamp - interval '1 microsecond'),'YYYYMMDD')::int",(start,end))
                cur.execute("""INSERT INTO gold.fact_trips
                  (trip_id,date_key,pickup_location_key,dropoff_location_key,payment_type_key,company_key,
                   trip_seconds,trip_miles,fare,tips,trip_total,source_run_id)
                  SELECT s.trip_id,to_char(s.trip_date,'YYYYMMDD')::int,s.pickup_community_area,s.dropoff_community_area,
                         p.payment_type_key,c.company_key,s.trip_seconds,s.trip_miles,s.fare,s.tips,s.trip_total,s.source_run_id
                  FROM silver.trips s
                  LEFT JOIN gold.dim_payment_type p ON p.payment_type=s.payment_type
                  LEFT JOIN gold.dim_company c ON c.company=s.company
                  WHERE s.trip_start >= %s AND s.trip_start < %s
                  ON CONFLICT(trip_id) DO UPDATE SET
                    date_key=EXCLUDED.date_key,pickup_location_key=EXCLUDED.pickup_location_key,
                    dropoff_location_key=EXCLUDED.dropoff_location_key,payment_type_key=EXCLUDED.payment_type_key,
                    company_key=EXCLUDED.company_key,trip_seconds=EXCLUDED.trip_seconds,
                    trip_miles=EXCLUDED.trip_miles,fare=EXCLUDED.fare,tips=EXCLUDED.tips,
                    trip_total=EXCLUDED.trip_total,source_run_id=EXCLUDED.source_run_id""",(start,end))
                cur.execute("""SELECT count(*),coalesce(sum(trip_total),0) FROM gold.fact_trips
                    WHERE date_key BETWEEN to_char(%s::timestamp,'YYYYMMDD')::int AND to_char((%s::timestamp - interval '1 microsecond'),'YYYYMMDD')::int""",(start,end))
                gold_n,gold_rev=cur.fetchone()
                if gold_n!=n or abs(float(gold_rev)-float(revenue))>0.01:
                    raise AssertionError(f"Gold validation failed silver=({n},{revenue}) gold=({gold_n},{gold_rev})")
        check("BUILD_GOLD","transactional_reconciliation",True,n)
        task_metric("build_gold","SUCCESS",started,n,n,0)
        
    except Exception as e:
        task_metric("build_gold","FAILED",started,error=e);raise

if __name__=="__main__":main()
