import os
from pyspark.sql import SparkSession


def make_spark(app_name):
    return (SparkSession.builder.master("local[2]").appName(app_name)
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
            .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate())


def s3path(key):
    return f"s3a://{os.environ['S3_BUCKET']}/{key}"
