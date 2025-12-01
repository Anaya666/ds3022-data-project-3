"""
This script analyzes NYT metadata facets (des_facet, org_facet, geo_facet) by exploding multi-label 
fields and computing frequency distributions. It reveals which topics, organizations, and locations 
dominate coverage during my ingestion window and produces bar charts summarizing the top facets.
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
# These are NYTimes metadata facet columns
FACETS = ["des_facet", "geo_facet", "org_facet", "per_facet"]

# CLEAN + EXPLODE FACET COLUMN (robust to all formats)
def explode_facet_column(df, column):
    df = df.copy()

    def clean_value(x):
        # Case 0: Null or empty
        if x is None or x == "" or pd.isna(x):
            return []

        # Case 1: Already a Python list
        if isinstance(x, list):
            return x

        # Case 2: String values need parsing
        if isinstance(x, str):
            x = x.strip()

            # Case 2A: Looks like a JSON list
            if x.startswith("[") and x.endswith("]"):
                try:
                    return literal_eval(x)
                except Exception:
                    pass  # fallback to comma split

            # Case 2B: Comma-separated list (most common messy case)
            if "," in x:
                return [item.strip() for item in x.split(",")]

            # Case 2C: Single item string
            return [x]

        # Fallback: wrap anything else as a list
        return [x]

    # Clean the column into consistent lists
    df[column] = df[column].apply(clean_value)

    # Explode into long format
    exploded = df.explode(column)

    # Drop empty strings
    exploded = exploded[exploded[column].notna() & (exploded[column] != "")]
    return exploded

# PLOT TOP FACET VALUES
def plot_top_facet(df, column, top_n=20):
    print(f"\n Computing top {top_n} entries for {column}...")

    # Count the most frequent items
    counts = df[column].value_counts().head(top_n)

    # Plot
    plt.figure(figsize=(12, 6))
    counts.plot(kind="bar", color="purple")

    plt.title(f"Top {top_n} values for {column}")
    plt.xlabel(column)
    plt.ylabel("Count")
    plt.xticks(rotation=60, ha="right")
    plt.tight_layout()

    # Save output
    path = f"{PLOTS_DIR}/facet_{column}.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f" Saved: {path}")
    print(counts)

# MAIN SCRIPT
def main():
    df = load_clean_data()

    for facet in FACETS:
        print(f"\nProcessing facet column: {facet}")
        exploded = explode_facet_column(df, facet)

        if len(exploded) == 0:
            print(f"⚠️ No values found for {facet}")
            continue

        plot_top_facet(exploded, facet)

    print("\n Facet analysis complete.")


if __name__ == "__main__":
    main()
