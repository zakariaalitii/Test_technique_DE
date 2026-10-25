from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from great_expectations.dataset import SparkDFDataset

REQUIRED = ["trip_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds", "trip_miles", "fare", "trip_total"]


def split_rows(raw):
    missing = set(REQUIRED) - set(raw.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    df = (raw.withColumn("trip_start", F.to_timestamp("trip_start_timestamp"))
          .withColumn("trip_end", F.to_timestamp("trip_end_timestamp"))
          .withColumn("trip_seconds", F.col("trip_seconds").cast("double"))
          .withColumn("trip_miles", F.col("trip_miles").cast("double"))
          .withColumn("fare", F.col("fare").cast("double"))
          .withColumn("tips", F.col("tips").cast("double") if "tips" in raw.columns else F.lit(None).cast("double"))
          .withColumn("trip_total", F.col("trip_total").cast("double"))
          .withColumn("pickup_community_area", F.col("pickup_community_area").cast("int") if "pickup_community_area" in raw.columns else F.lit(None).cast("int"))
          .withColumn("dropoff_community_area", F.col("dropoff_community_area").cast("int") if "dropoff_community_area" in raw.columns else F.lit(None).cast("int"))
          .withColumn("payment_type", F.col("payment_type") if "payment_type" in raw.columns else F.lit(None).cast("string"))
          .withColumn("company", F.col("company") if "company" in raw.columns else F.lit(None).cast("string"))
          .withColumn("trip_date", F.to_date("trip_start")))
    rules = [
      ("missing_trip_id", F.col("trip_id").isNull() | (F.length(F.trim(F.col("trip_id"))) == 0)),
      ("invalid_timestamps", F.col("trip_start").isNull() | F.col("trip_end").isNull()),
      ("end_before_start", F.col("trip_end") < F.col("trip_start")),
      ("invalid_duration", F.col("trip_seconds").isNull() | F.isnan("trip_seconds") | (F.col("trip_seconds") < 0)),
      ("invalid_distance", F.col("trip_miles").isNull() | F.isnan("trip_miles") | (F.col("trip_miles") < 0)),
      ("invalid_fare", F.col("fare").isNull() | F.isnan("fare") | (F.col("fare") < 0)),
      ("invalid_total", F.col("trip_total").isNull() | F.isnan("trip_total") | (F.col("trip_total") < 0)),
    ]
    reasons = F.array(*[F.when(expr, F.lit(name)) for name, expr in rules])
    df = df.withColumn("reject_reason", F.concat_ws(",", F.array_compact(reasons)))
    bad = df.filter(F.length("reject_reason") > 0)
    valid = df.filter(F.length("reject_reason") == 0)
    w = Window.partitionBy("trip_id").orderBy(F.col("trip_start").desc(), F.col("trip_end").desc(), F.col("trip_total").desc(), F.col("trip_miles").desc(), F.col("fare").desc())
    numbered = valid.withColumn("_rn", F.row_number().over(w))
    dupes = numbered.filter(F.col("_rn") > 1).drop("_rn", "reject_reason").withColumn("reject_reason", F.lit("duplicate_trip_id"))
    clean = numbered.filter(F.col("_rn") == 1).drop("_rn", "reject_reason")
    return clean, bad.unionByName(dupes)


def gx_expectations(df, checks):
    dataset = SparkDFDataset(df)
    results = []
    for method, args in checks:
        result = getattr(dataset, method)(*args)
        results.append((method + ":" + ",".join(map(str,args)), bool(result["success"]), result.get("result", {})))
    return results
