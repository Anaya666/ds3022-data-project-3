"""
This script analyzes newsroom publishing velocity by computing how many articles are published per hour and 
per section across your entire ingestion period. It produces multiple plots that reveal daily cycles, peak 
publication times, and high-activity sections in the NYT news feed.
"""
import sys, os

# Allow this script to import ../common.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import matplotlib.pyplot as plt
import seaborn as sns
from common import load_clean_data, preprocess_timestamps

PLOTS_DIR = os.path.dirname(os.path.abspath(__file__))
os.makedirs(PLOTS_DIR, exist_ok=True)


# 1. OVERALL VELOCITY
def plot_overall_velocity(df):
    print("\nComputing overall article velocity...")

    hourly_counts = df.groupby("hour_bucket").size()

    plt.figure(figsize=(14, 6))
    plt.plot(hourly_counts.index, hourly_counts.values, marker="o")
    plt.title("NYTimes Publishing Velocity — Articles per Hour")
    plt.xlabel("Hour (UTC)")
    plt.ylabel("Articles Published")
    plt.grid(True)

    path = f"{PLOTS_DIR}/overall_velocity.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f" Saved: {path}")


# 2. ORIGINAL MULTI-LINE SECTION VELOCITY
def plot_section_velocity(df):
    print("\nComputing per-section velocity (multi-line)...")

    top_sections = df["section"].value_counts().head(6).index

    plt.figure(figsize=(14, 8))
    for section in top_sections:
        subset = df[df["section"] == section]
        hourly = subset.groupby("hour_bucket").size()
        plt.plot(hourly.index, hourly.values, marker="o", label=section)

    plt.title("NYTimes Publishing Velocity by Section")
    plt.xlabel("Hour (UTC)")
    plt.ylabel("Articles Published")
    plt.legend()
    plt.grid(True)

    path = f"{PLOTS_DIR}/section_velocity.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f" Saved: {path}")


# 3. FACETED LINE CHART — SUPER CLEAR
def plot_section_velocity_faceted(df):
    print("\nComputing per-section velocity (faceted)...")

    top_sections = df["section"].value_counts().head(6).index
    n = len(top_sections)

    fig, axes = plt.subplots(n, 1, figsize=(14, 3*n), sharex=True)

    for ax, section in zip(axes, top_sections):
        subset = df[df["section"] == section]
        hourly = subset.groupby("hour_bucket").size()
        ax.plot(hourly.index, hourly.values, marker="o")
        ax.set_title(section)
        ax.set_ylabel("Articles")
        ax.grid(True)

    plt.xlabel("Hour (UTC)")

    path = f"{PLOTS_DIR}/section_velocity_faceted.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f" Saved: {path}")

def main():
    df = load_clean_data()
    df = preprocess_timestamps(df)

    plot_overall_velocity(df)
    plot_section_velocity(df)
    plot_section_velocity_faceted(df)

    print("\nVelocity analysis complete.")


if __name__ == "__main__":
    main()
