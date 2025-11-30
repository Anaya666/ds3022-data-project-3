import os
import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv

# LOAD ENVIRONMENT VARIABLES
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

KEY_CLEAN_PARQUET = "clean/clean_articles.parquet"


# CONNECT TO S3
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# LOAD CLEAN DATA FROM S3
def load_clean_data():
    print(" Downloading clean dataset from S3...")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=KEY_CLEAN_PARQUET)
    data = obj["Body"].read()

    print("Reading Parquet into DataFrame...")
    df = pd.read_parquet(BytesIO(data))

    print(f"Loaded {len(df)} cleaned articles.")
    print("\nColumns:", list(df.columns))
    return df

# PREPROCESS TIMESTAMPS (Convert to UTC)
def preprocess_timestamps(df):
    print("\n Preprocessing timestamps...")

    # Convert to datetime (UTC)
    df["published_date"] = pd.to_datetime(df["published_date"], utc=True, errors="coerce")
    df["updated_date"] = pd.to_datetime(df["updated_date"], utc=True, errors="coerce")

    # Drop rows missing published_date (rare)
    df = df.dropna(subset=["published_date"])

    # Derived features
    df["published_hour"] = df["published_date"].dt.hour
    df["published_day"] = df["published_date"].dt.date
    df["weekday"] = df["published_date"].dt.weekday   # 0 = Monday, 6 = Sunday
    df["hour_bucket"] = df["published_date"].dt.floor("H")

    print(" Timestamp preprocessing complete.")
    return df

# MAIN
def main():
    df = load_clean_data()
    df = preprocess_timestamps(df)

    print("\n Data is preprocessed. Ready for analysis.")
    print(df.head())


if __name__ == "__main__":
    main()
