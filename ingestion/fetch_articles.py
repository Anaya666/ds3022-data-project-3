import os
import json
import boto3
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv


# STEP 1 — Load environment variables
load_dotenv()

API_KEY = os.getenv("NYT_API_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET")

if not API_KEY:
    raise ValueError("ERROR: NYT_API_KEY not found in .env")

if not S3_BUCKET:
    raise ValueError("ERROR: S3_BUCKET not found in .env")

# STEP 2 — Create S3 client using boto3

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# STEP 3 — API endpoint for Newswire
BASE_URL = "https://api.nytimes.com/svc/news/v3/content/all/all.json"


def fetch_articles():
    """Fetch the latest NYT Newswire articles."""
    params = {"api-key": API_KEY}

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f" API request failed: {e}")
        return None


def upload_to_s3(data):
    """Upload raw JSON dictionary directly to S3."""
    
    # Use timezone-aware timestamp for consistency
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")

    # Organize files by date in S3
    date_folder = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3_key = f"raw/{date_folder}/newswire_{timestamp}.json"

    # Convert dictionary to bytes
    json_bytes = json.dumps(data, indent=2).encode("utf-8")

    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_bytes,
            ContentType="application/json"
        )

        print(f"Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")
        return s3_key

    except Exception as e:
        print(f" Failed to upload to S3: {e}")
        return None


def preview_metadata(data):
    """Print metadata for sanity check."""
    if "results" not in data:
        print("⚠️ No 'results' in API response.")
        return

    articles = data["results"]
    print(f"Fetched {len(articles)} articles.")

    if articles:
        a = articles[0]
        print("\nSample Article:")
        print(f"  Title: {a.get('title')}")
        print(f"  Section: {a.get('section')}")
        print(f"  Published: {a.get('published_date')}")


def main():
    print("\n Fetching NYTimes Newswire data...")

    data = fetch_articles()
    if not data:
        print("No data fetched. Exiting.")
        return

    upload_to_s3(data)
    preview_metadata(data)

    print("\n Ingestion complete.\n")


if __name__ == "__main__":
    main()
