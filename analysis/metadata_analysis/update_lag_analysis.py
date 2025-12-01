"""
This script calculates the time lag between an article’s publication timestamp and its most recent update. 
It identifies which sections update stories quickly (breaking news) and which sections have longer revision 
cycles (features, analysis), visualizing the lag distribution and section-level differences.
"""
import os
import os
import sys
import matplotlib.pyplot as plt
import pandas as pd

# allow imports from parent "analysis" folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common import load_clean_data, preprocess_timestamps

# save plots inside THIS folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PLOTS_DIR = BASE_DIR

def compute_lag(df):
    df["lag_minutes"] = (df["updated_date"] - df["published_date"]).dt.total_seconds() / 60
    df = df[df["lag_minutes"] >= 0]
    return df


def plot_lag_histogram(df):
    plt.figure(figsize=(12, 6))
    df["lag_minutes"].clip(upper=300).hist(bins=40, color="orange")  # cap 5 hrs
    plt.title("Distribution of Update Lag (minutes)")
    plt.xlabel("Minutes between publish and update")
    plt.ylabel("Count")
    path = f"{PLOTS_DIR}/update_lag_hist.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f" Saved: {path}")


def plot_lag_by_section(df):
    section_lag = df.groupby("section")["lag_minutes"].median().sort_values(ascending=False).head(10)

    plt.figure(figsize=(12, 6))
    section_lag.plot(kind="bar", color="red")
    plt.title("Median Update Lag by Section (Top 10)")
    plt.ylabel("Minutes")
    plt.xticks(rotation=45, ha="right")

    path = f"{PLOTS_DIR}/update_lag_by_section.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f" Saved: {path}")


def main():
    df = load_clean_data()
    df = preprocess_timestamps(df)

    df = compute_lag(df)
    plot_lag_histogram(df)
    plot_lag_by_section(df)

    print("\n Update lag analysis complete.")


if __name__ == "__main__":
    main()
