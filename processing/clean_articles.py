import os
import json
import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
from dateutil import parser as dateparser

# LOAD ENV VARIABLES
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET")

if not S3_BUCKET:
    raise ValueError("S3_BUCKET missing from .env")

# S3 client
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# HELPER FUNCTIONS

def list_raw_files():
    """List ALL raw and bulk files in S3."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")

    for prefix in ["raw/", "bulk/"]:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for item in page.get("Contents", []):
                keys.append(item["Key"])

    print(f"Found {len(keys)} raw files in S3.")
    return keys


def load_json_from_s3(key):
    """Load a JSON file from S3 into Python dict."""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    data = obj["Body"].read().decode("utf-8")
    return json.loads(data)


def clean_article(article):
    """Normalize + flatten a single NYT article record."""

    def safe(x):
        return x if x is not None else None

    def parse_dt(x):
        try:
            return dateparser.parse(x)
        except:
            return None

    return {
        "uri": article.get("uri"),
        "url": article.get("url"),
        "section": article.get("section"),
        "subsection": article.get("subsection"),
        "title": article.get("title"),
        "abstract": article.get("abstract"),
        "byline": article.get("byline"),
        "published_date": parse_dt(article.get("published_date")),
        "updated_date": parse_dt(article.get("updated_date")),
        "multimedia_count": len(article.get("multimedia", [])) if isinstance(article.get("multimedia"), list) else 0,
        "geo_facet": ", ".join(article.get("geo_facet", [])) if isinstance(article.get("geo_facet"), list) else None,
        "des_facet": ", ".join(article.get("des_facet", [])) if isinstance(article.get("des_facet"), list) else None,
        "org_facet": ", ".join(article.get("org_facet", [])) if isinstance(article.get("org_facet"), list) else None,
        "per_facet": ", ".join(article.get("per_facet", [])) if isinstance(article.get("per_facet"), list) else None,
    }

# MAIN CLEANING PROCEDURE

def clean_all_articles():
    keys = list_raw_files()
    cleaned_rows = []

    for key in keys:
        print(f"Processing: {key}")
        data = load_json_from_s3(key)

        raw_articles = data.get("results", [])

        for article in raw_articles:
            cleaned = clean_article(article)
            cleaned_rows.append(cleaned)

    print(f"Total articles before dedupe: {len(cleaned_rows)}")

    df = pd.DataFrame(cleaned_rows)

    # drop rows with missing uri (invalid)
    df = df.dropna(subset=["uri"])

    # deduplicate
    df = df.drop_duplicates(subset=["uri"])
    print(f"Total articles AFTER dedupe: {len(df)}")

    return df
# SAVE CLEANED DATA → S3 (PARQUET)

def save_to_s3(df):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key="clean/clean_articles.parquet",
        Body=buffer,
        ContentType="application/octet-stream"
    )

    print("\nUploaded clean dataset to s3://{}/clean/clean_articles.parquet".format(S3_BUCKET))

# MAIN

def main():
    print("Cleaning all articles...")
    df = clean_all_articles()
    save_to_s3(df)
    print("Done.")


if __name__ == "__main__":
    main()
