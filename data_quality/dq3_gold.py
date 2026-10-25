import os
from datetime import datetime,timezone
import pandas as pd
from great_expectations.dataset import PandasDataset
from monitoring.db import connection,check,task_metric


def main():
    started=datetime.now(timezone.utc)
    start,end=os.environ["TAXI_START"],os.environ["TAXI_END"]

    try:
        with connection() as db:
            with db.cursor() as cur:
                cur.execute("SELECT count(*),coalesce(sum(trip_total),0) FROM silver.trips WHERE trip_start >= %s AND trip_start < %s",(start,end))
                silver_count,silver_revenue=cur.fetchone()
                cur.execute("""SELECT count(*),coalesce(sum(f.trip_total),0) FROM gold.fact_trips f
                  JOIN gold.dim_date d USING(date_key) WHERE d.full_date >= %s::date AND d.full_date < %s::date""",(start,end))
                gold_count,gold_revenue=cur.fetchone()
                cur.execute("""SELECT count(*) FROM gold.fact_trips f
                  LEFT JOIN gold.dim_date d USING(date_key) WHERE d.date_key IS NULL""")
                orphan_dates=cur.fetchone()[0]
                cur.execute("""SELECT trip_date,trip_count,total_revenue FROM gold.v_daily_revenue
                    WHERE trip_date >= %s::date AND trip_date < %s::date ORDER BY trip_date""",(start,end))
                daily=cur.fetchall()
        check("DQ3","trip_count_reconciles",silver_count==gold_count,gold_count,details={"silver":silver_count})
        check("DQ3","revenue_reconciles",abs(float(silver_revenue)-float(gold_revenue))<=0.01,gold_revenue,details={"silver":str(silver_revenue)})
        check("DQ3","no_orphan_dates",orphan_dates==0,orphan_dates)
        check("DQ3","daily_trip_counts_reconcile",sum(x[1] for x in daily)==gold_count,sum(x[1] for x in daily))
        gx=PandasDataset(pd.DataFrame(daily,columns=["trip_date","trip_count","total_revenue"]))
        for column in ("trip_count","total_revenue"):
            result=gx.expect_column_values_to_be_between(column,min_value=0)
            check("DQ3",f"gx_{column}_nonnegative",result["success"],details=result.get("result",{}))
        task_metric("dq3_gold","SUCCESS",started,silver_count,gold_count,0)
        
    except Exception as e:
        task_metric("dq3_gold","FAILED",started,error=e);raise

if __name__=="__main__":main()
