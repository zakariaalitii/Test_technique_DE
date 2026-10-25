import io
import json
import logging
import os
from datetime import datetime

import boto3
import requests
from botocore.config import Config
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOG = logging.getLogger(__name__)
SOURCE = "https://data.cityofchicago.org/resource/wrvz-psew.csv"


def client():
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                        config=Config(signature_version="s3v4", retries={"max_attempts": 5}))


def run():
    start, end = os.environ["TAXI_START"], os.environ["TAXI_END"]
    datetime.fromisoformat(start); datetime.fromisoformat(end)
    if start >= end:
        raise ValueError("TAXI_START must be before TAXI_END")
    max_rows, page_size = int(os.environ["TAXI_MAX_ROWS"]), int(os.environ["TAXI_PAGE_SIZE"])
    if max_rows <= 0 or not (1 <= page_size <= 50000):
        raise ValueError("Invalid TAXI_MAX_ROWS or TAXI_PAGE_SIZE")
    bucket = os.environ["S3_BUCKET"]
    prefix = f"bronze/chicago_taxi/start={start[:10]}/end={end[:10]}/"
    s3 = client()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        for i in range(0, len(objects), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects[i:i+1000]})
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    total, page_index = 0, 0
    while total < max_rows:
        limit = min(page_size, max_rows-total)
        params = {
            "$where": f"trip_start_timestamp >= '{start}' AND trip_start_timestamp < '{end}'",
            "$order": "trip_start_timestamp ASC, trip_id ASC",
            "$limit": limit,
            "$offset": total,
        }
        response = session.get(SOURCE, params=params, timeout=(15, 120))
        response.raise_for_status()
        import csv
        rows = list(csv.reader(io.StringIO(response.text)))
        count = max(0, len(rows)-1)
        if count == 0:
            break
        key = prefix + f"part-{page_index:05d}.csv"
        s3.put_object(Bucket=bucket, Key=key, Body=response.content, ContentType="text/csv")
        LOG.info("Uploaded %s: %s records", key, count)
        total += count
        page_index += 1
        if count < limit:
            break
    if total == 0:
        raise RuntimeError("SODA returned no records; choose a populated period")
    manifest = {"source": SOURCE, "start": start, "end": end, "rows": total,
                "pages": page_index, "page_size": page_size, "max_rows": max_rows}
    s3.put_object(Bucket=bucket, Key=prefix+"manifest.json", Body=json.dumps(manifest).encode())
    LOG.info("Bronze snapshot completed: %s", manifest)
    return manifest


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(run(), indent=2))
