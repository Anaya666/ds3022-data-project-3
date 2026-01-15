NYTimes Newsroom Streaming Pipeline
Velocity & Metadata Analysis
Project Overview
This project builds an end-to-end streaming data pipeline using the New York Times Newswire API to study newsroom publishing behavior over time. Rather than focusing on article text or NLP, the analysis centers on velocity, timing, and metadata patterns—how often articles are published, which sections spike during breaking news, how quickly articles are updated, and which topics dominate surges.
The project simulates a real-world production ingestion workflow: polling an external API at regular intervals, logging raw JSON data, cleaning and normalizing records, storing them in an analytical database, and extracting insights through time-series analysis and visualization.
Key Questions
How does NYT publishing activity vary by hour, day, and section?
Which sections behave like breaking news desks vs analysis desks?
When do newsroom surges occur, and how large are they?
Which topics, organizations, or geographies dominate spike periods?
How quickly are articles updated after initial publication?
Data Source
New York Times Newswire API
Endpoint:
https://api.nytimes.com/svc/news/v3/content/all/all.json

Only article metadata is used:
publication & update timestamps
section and subsection
byline (author)
descriptive, organizational, and geographic facets
Note: Due to API recency limits and rate restrictions, articles are collected continuously over multiple days rather than via a single historical pull. This reflects real-world production constraints.

Project Structure:
nyt-newsroom-pipeline/
│
├── ingestion/
│   ├── fetch_articles.py        # API polling + raw JSON logging
│   └── prefect_flow.py          # Scheduled Prefect ingestion flow
│
├── processing/
│   ├── clean_articles.py        # JSON normalization & cleaning
│   └── load_to_duckdb.py        # Persist cleaned data to DuckDB
│
├── analysis/
│   ├── velocity_analysis.py     # Time-series velocity metrics
│   └── metadata_analysis.py     # Section, facet, byline analysis
│
├── visualization/
│   ├── plot_velocity.py
│   ├── plot_metadata.py
│   └── figures/                 # Saved charts (PNG)
│
├── data/
│   ├── raw/                     # Raw JSON ingestion logs
│   └── nyt.duckdb               # Analytical database
│
├── docs/
│   ├── project_plan.md
│   └── challenges.md
│
├── .gitignore
├── requirements.txt
└── README.md

Pipeline Architecture
Streaming Ingestion
Polls NYT API at fixed intervals
Appends ingestion timestamps
Deduplicates articles using unique URIs
Stores raw JSON snapshots for reproducibility
Transformation & Storage
Normalizes nested metadata fields
Converts timestamps to timezone-aware datetime
Explodes multi-label facets
Stores data in DuckDB for analytical querying
Analysis
Newsroom velocity (articles/hour, section/hour)
Surge detection via rolling statistics and z-scores
Section-level behavior profiling
Facet, author, and update-lag analysis
Visualization
Velocity line charts
Section × hour heatmaps
Surge detection plots
Facet frequency and author activity charts
Tools & Technologies
Python (pandas, numpy, matplotlib, seaborn)
Prefect for scheduled ingestion & retries
DuckDB for analytical storage
NYTimes API
Git & GitHub for version control
Key Insights (Summary)
Publishing velocity follows strong diurnal patterns with identifiable surge windows
Certain sections exhibit rapid update cycles consistent with breaking news
Topic and geographic facets cluster strongly during spike events
Update lag differentiates real-time reporting from long-form analysis
Metadata alone provides rich signals about newsroom behavior
Limitations
API exposes only a rolling window of recent articles
Rate limits constrain ingestion speed
Dataset size grows over time rather than via bulk historical access
These constraints mirror real production systems and informed the pipeline design.

How to Run
Install dependencies:
pip install -r requirements.txt
Set API key:
export NYT_API_KEY="YOUR_API_KEY"
Run ingestion flow:
python ingestion/prefect_flow.py
Process and analyze data:
python processing/clean_articles.py
python analysis/velocity_analysis.py
python analysis/metadata_analysis.py

