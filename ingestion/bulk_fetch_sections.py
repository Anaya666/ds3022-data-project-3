import os
import json
import time
import boto3
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("NYT_API_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

if not API_KEY:
    raise ValueError("NYT_API_KEY missing")
if not S3_BUCKET:
    raise ValueError("S3_BUCKET missing")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# HIGH-VOLUME SECTIONS ONLY (best for 50K+ records)
SECTIONS = [
    "world", "us", "politics", "business", "technology", "science",
    "health", "sports", "arts", "movies", "books", "travel"
]

BASE_URL = "https://api.nytimes.com/svc/news/v3/content/nyt/{section}.json"


def upload_to_s3(data, section, offset):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    date_folder = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"bulk/{date_folder}/{section}_{offset}_{ts}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json"
    )

    print(f"Uploaded: s3://{S3_BUCKET}/{key}")


def safe_request(url, params):
    """Make a request with retry + backoff for 429 errors."""
    for attempt in range(6):  
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f"429 rate limit → waiting {wait} seconds…")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            print(f"Request failed (attempt {attempt+1}): {e}")
            time.sleep(3)

    return None


def fetch_bulk():
    total_batches = 0

    for section in SECTIONS:
        print(f"\nFetching: {section.upper()}")

        # Reduce offset max to avoid rate-limit hell
        for offset in range(0, 600, 20):  # first 30 pages (≈600 articles/section)
            url = BASE_URL.format(section=section)
            params = {"api-key": API_KEY, "offset": offset}

            data = safe_request(url, params)
            if not data or not data.get("results"):
                print(f"No more results for {section} at offset={offset}")
                break

            upload_to_s3(data, section, offset)
            total_batches += 1

            # Slow down to avoid bans
            time.sleep(1.2)

    print(f"\n Bulk ingestion complete. Total batches: {total_batches}")
    print("Each batch = ~20 articles. You likely collected 20 × batches articles.")
    print("For example: 20 × 300 batches ≈ 6000 articles.")
    print("Run multiple times to reach 50K+ today.")


if __name__ == "__main__":
    fetch_bulk()
