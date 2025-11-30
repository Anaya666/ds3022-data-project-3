import os
import json
import time
import random
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

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

SECTIONS = [
    "arts", "books", "business", "fashion", "food", "health", "home",
    "insider", "magazine", "movies", "nyregion", "obituaries", "opinion",
    "politics", "realestate", "science", "sports", "sundayreview",
    "technology", "theater", "travel", "upshot", "us", "world"
]

BASE_URL = "https://api.nytimes.com/svc/news/v3/content/nyt/{section}.json"
ALL_URL = "https://api.nytimes.com/svc/news/v3/content/all/all.json"


def upload_to_s3(data, prefix, offset):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    date_folder = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"bulk/{date_folder}/{prefix}_{offset}_{timestamp}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    print(f"Uploaded: {key}")


def safe_request(url, params, max_retries=10):
    """NYT API-safe request with exponential backoff."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 429:
                wait = (2 ** attempt) + random.uniform(0, 2)
                print(f"429 rate limit → waiting {wait:.1f}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Error: {e} → retrying...")
            time.sleep((2 ** attempt) + 1)
    return None


def fetch_all(prefix, url):
    total = 0
    for offset in range(0, 10000, 20):  # attempt up to 10K pages (200K articles)
        params = {"api-key": API_KEY, "offset": offset}
        data = safe_request(url, params)
        if not data or not data.get("results"):
            print(f"No more results at offset {offset}.")
            break
        upload_to_s3(data, prefix, offset)
        total += 1
        time.sleep(1.2 + random.uniform(0.1, 0.7))
    return total


def run_ingestion():
    total_calls = 0

    # 1. Section-by-section ingestion (slow + stable)
    for section in SECTIONS:
        print(f"\nFetching section: {section}")
        url = BASE_URL.format(section=section)
        total_calls += fetch_all(section, url)
        print("Cooling down 20 seconds before next section...")
        time.sleep(20)

    # 2. High-volume ALL feed
    print("\nFetching GLOBAL ALL feed (volume booster)")
    total_calls += fetch_all("all", ALL_URL)

    print("\nDone!")
    print(f"Total API calls: {total_calls}")
    print(f"Approx articles: {total_calls * 20:,}")


if __name__ == "__main__":
    run_ingestion()
