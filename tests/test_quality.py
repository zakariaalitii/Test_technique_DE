from pyspark.sql import SparkSession
from spark.utils.quality import split_rows


def test_reject_and_deduplicate():
    spark=SparkSession.builder.master("local[1]").appName("quality-test").getOrCreate()
    try:
        cols=["trip_id","trip_start_timestamp","trip_end_timestamp","trip_seconds","trip_miles","fare","trip_total"]
        rows=[("a","2023-01-01T10:00:00","2023-01-01T10:10:00","600","2","8","10"),
              ("a","2023-01-01T10:00:00","2023-01-01T10:10:00","600","2","8","10"),
              ("bad","2023-01-01T10:00:00","2023-01-01T10:10:00","600","-1","8","10")]
        good,bad=split_rows(spark.createDataFrame(rows,cols))
        assert good.count()==1
        assert bad.count()==2
        assert set(x.reject_reason for x in bad.select("reject_reason").collect())=={"duplicate_trip_id","invalid_distance"}
    finally:spark.stop()
