from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_trips(raw):
    df = (raw
          .withColumn("trip_start", F.to_timestamp("trip_start_timestamp"))
          .withColumn("trip_end", F.to_timestamp("trip_end_timestamp"))
          .withColumn("trip_seconds", F.col("trip_seconds").cast("double"))
          .withColumn("trip_miles", F.col("trip_miles").cast("double"))
          .withColumn("fare", F.col("fare").cast("double"))
          .withColumn("trip_total", F.col("trip_total").cast("double"))
          .withColumn("pickup_community_area", F.col("pickup_community_area").cast("int"))
          .withColumn("trip_date", F.to_date("trip_start")))
    valid = (F.col("trip_id").isNotNull() & (F.length(F.trim("trip_id")) > 0)
             & F.col("trip_start").isNotNull() & F.col("trip_end").isNotNull()
             & (F.col("trip_end") >= F.col("trip_start"))
             & F.col("trip_seconds").isNotNull() & (F.col("trip_seconds") >= 0)
             & F.col("trip_miles").isNotNull() & (F.col("trip_miles") >= 0)
             & F.col("trip_total").isNotNull() & (F.col("trip_total") >= 0)
             & F.col("fare").isNotNull() & (F.col("fare") >= 0))
    window = Window.partitionBy("trip_id").orderBy(F.col("trip_start").desc(), F.col("trip_end").desc(), F.col("trip_total").desc())
    cleaned = (df.filter(valid).withColumn("rn", F.row_number().over(window))
               .filter(F.col("rn") == 1).drop("rn"))
    return cleaned.select("trip_id", "trip_start", "trip_end", "trip_date", "trip_seconds",
                          "trip_miles", "fare", "trip_total", "pickup_community_area")


def gold_tables(silver):
    daily = (silver.groupBy("trip_date")
             .agg(F.count("*").alias("trip_count"),
                  F.round(F.sum("trip_total"), 2).alias("total_revenue"),
                  F.round(F.avg("trip_total"), 2).alias("avg_ticket")))
    stats = (silver.groupBy("trip_date")
             .agg(F.count("*").alias("trip_count"),
                  F.round(F.avg("trip_seconds") / 60, 2).alias("avg_duration_minutes"),
                  F.round(F.avg("trip_miles"), 2).alias("avg_distance_miles"),
                  F.round(F.avg("fare"), 2).alias("avg_fare")))
    zones = (silver.filter(F.col("pickup_community_area").isNotNull())
             .groupBy("pickup_community_area")
             .agg(F.count("*").alias("trip_count"),
                  F.round(F.sum("trip_total"), 2).alias("total_revenue")))
    return {"daily_revenue": daily, "trip_statistics": stats, "pickup_zones": zones}
