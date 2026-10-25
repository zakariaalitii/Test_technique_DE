import pytest
from pyspark.sql import SparkSession
from spark.utils.transforms import clean_trips, gold_tables


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()
    yield s
    s.stop()


def test_clean_and_aggregate(spark):
    cols = ["trip_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds", "trip_miles", "fare", "trip_total", "pickup_community_area"]
    rows = [
        ("a", "2023-01-01T10:00:00.000", "2023-01-01T10:10:00.000", "600", "2", "8", "10", "12"),
        ("a", "2023-01-01T10:00:00.000", "2023-01-01T10:10:00.000", "600", "2", "8", "10", "12"),
        ("b", "2023-01-01T11:00:00.000", "2023-01-01T11:20:00.000", "1200", "4", "12", "20", "12"),
        ("bad", "2023-01-01T11:00:00.000", "2023-01-01T11:20:00.000", "1200", "-1", "12", "20", "12"),
    ]
    cleaned = clean_trips(spark.createDataFrame(rows, cols))
    assert cleaned.count() == 2
    tables = gold_tables(cleaned)
    daily = tables["daily_revenue"].first()
    assert daily["trip_count"] == 2
    assert daily["total_revenue"] == 30
    assert tables["pickup_zones"].first()["trip_count"] == 2
