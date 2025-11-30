"""
This script detects unusual spikes in publishing activity by computing z-scores on hourly article counts. 
It identifies surge windows—often corresponding to breaking news events—and highlights them visually on a 
time-series plot.
"""
import os
import sys
import matplotlib.pyplot as plt
import pandas as pd
from scipy.stats import zscore

# allow importing common.py from parent folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common import load_clean_data, preprocess_timestamps

# save plots directly in THIS folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PLOTS_DIR = BASE_DIR


def detect_anomalies(df):
    hourly = df.groupby("hour_bucket").size()
    z = zscore(hourly)

    # anomalies = spikes 2 SD above mean
    anomalies = hourly[(z > 2)]

    return hourly, anomalies
"""
Z-SCORE EXPLANATION
    A z-score measures how many standard deviations a value is from
    the mean of the distribution.
    z = (value - mean) / std
    In the context of hourly article counts:
    z ≈ 0   → hour is "normal" publication volume
    z > 2   → hour has unusually high volume (a publishing surge)

    Why this works:
    Publishing volume follows a noisy but stable distribution
    True breaking news events cause sudden spikes in volume
    These spikes appear as outliers far above the mean

    By computing z-scores across all hourly counts, we can identify
    hours that are statistically abnormal — i.e., moments when the
    newsroom switches into "breaking news" mode.
    scipy.stats.zscore computes this automatically.

We classify anomalies as any hour whose z-score is greater than 2.
    This corresponds to "more than 2 standard deviations above average."
    
    Rationale:
    In a normal distribution, >2 SD events happen <5% of the time
    For newsrooms, these correspond to true surges (major events,
    political decisions, disasters, global crises, etc.)
    
    Adjusting threshold:
    z > 1.5 → more sensitive (detects more surges)
    z > 3   → extremely conservative (only huge spikes)
"""

def plot_anomalies(hourly, anomalies):
    plt.figure(figsize=(14, 6))

    # plot baseline hourly volume
    plt.plot(hourly.index, hourly.values, label="Articles per hour", color="blue")

    # highlight anomalies (spikes)
    plt.scatter(
        anomalies.index,
        anomalies.values,
        color="red",
        label="Detected surge",
        s=80,
        zorder=3
    )

    plt.title("Surge / Anomaly Detection in Publishing Volume")
    plt.xlabel("Hour (UTC)")
    plt.ylabel("Articles Published")
    plt.legend()
    plt.grid(True)

    output_path = os.path.join(PLOTS_DIR, "anomalies.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f" Saved anomaly plot → {output_path}")


def main():
    df = load_clean_data()
    df = preprocess_timestamps(df)

    hourly, anomalies = detect_anomalies(df)

    print(f"\nDetected {len(anomalies)} anomalies.")
    print(anomalies)

    plot_anomalies(hourly, anomalies)

    print("\n Anomaly detection complete.")


if __name__ == "__main__":
    main()