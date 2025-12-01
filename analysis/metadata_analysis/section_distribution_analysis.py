"""
This script examines the distribution of articles across NYT sections, identifying which sections 
publish most frequently and how their relative share changes over time. It generates section-level bar 
charts and time-series curves to compare editorial output across categories like Politics, Business, World, 
and Sports
"""

import os
import sys
import matplotlib.pyplot as plt
import pandas as pd
from ast import literal_eval

# allow imports from parent "analysis" folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common import load_clean_data

# save plots inside THIS folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PLOTS_DIR = BASE_DIR

def plot_top_sections(df, top_n=10):
    print(f"\n Computing top {top_n} sections...")

    section_counts = df["section"].value_counts().head(top_n)

    plt.figure(figsize=(12, 6))
    section_counts.plot(kind="bar", color="steelblue")

    plt.title(f"Top {top_n} NYTimes Sections by Article Count")
    plt.xlabel("Section")
    plt.ylabel("Articles")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    path = f"{PLOTS_DIR}/top_sections.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f" Saved: {path}")
    print("\nSection counts:")
    print(section_counts)


def main():
    df = load_clean_data()
    plot_top_sections(df)

    print("\n Section Distribution Analysis complete.")


if __name__ == "__main__":
    main()
