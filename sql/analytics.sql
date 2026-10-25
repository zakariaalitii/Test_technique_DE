SELECT * FROM read_parquet('s3://taxi-lake/gold/chicago_taxi/start=2023-01-01/end=2023-02-01/daily_revenue/*.parquet') ORDER BY trip_date;
SELECT * FROM read_parquet('s3://taxi-lake/gold/chicago_taxi/start=2023-01-01/end=2023-02-01/trip_statistics/*.parquet') ORDER BY trip_date;
SELECT * FROM read_parquet('s3://taxi-lake/gold/chicago_taxi/start=2023-01-01/end=2023-02-01/pickup_zones/*.parquet') ORDER BY trip_count DESC LIMIT 10;
