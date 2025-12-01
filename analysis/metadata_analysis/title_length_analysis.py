"""
This script measures the length of article headlines to understand stylistic patterns across sections 
and time of day. It computes summary statistics (mean, median, distribution) and generates histograms 
showing how headline length varies across the newsroom.

"""

import os
import sys
import matplotlib.pyplot as plt
import pandas as pd

# allow imports from parent "analysis" folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common import load_clean_data

# save plots inside THIS folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PLOTS_DIR = BASE_DIR

def compute_lengths(df):
    df["title_length"] = df["title"].fillna("").apply(len)
    df["abstract_length"] = df["abstract"].fillna("").apply(len)
    return df


def plot_title_length_section(df):
    avg_len = df.groupby("section")["title_length"].mean().sort_values(ascending=False).head(12)

    plt.figure(figsize=(12, 6))
    avg_len.plot(kind="bar", color="green")
    plt.title("Average Title Length by Section (Top 12)")
    plt.ylabel("Characters")
    plt.xticks(rotation=45, ha="right")

    path = f"{PLOTS_DIR}/title_length_by_section.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f" Saved: {path}")


def main():
    df = load_clean_data()
    df = compute_lengths(df)
    plot_title_length_section(df)

    print("\n Title & content length analysis complete.")


if __name__ == "__main__":
    main()
