# Team X

## Team members
Anaya Nath

## Data Source

What data source did you work with?
I worked with the New York Times Newswire API, which provides a near–real-time stream of articles published on NYTimes.com. The API exposes metadata for every newly published item: including title, abstract, section, facets (topics, people, organizations, locations), timestamps, and URLs. I ingested thousands of raw records from multiple sections using pagination, stored them in an AWS S3 bucket, and converted them into a structured, cleaned Parquet dataset for analysis.

## Challenges / Obstacles

What challenges did this data choice present in data gathering, processing and analysis, and how did you work through them? What methods and tools did you use to work with this data?

This data source introduced several engineering challenges.
1. First, the API enforces strict rate limits, so large-scale ingestion immediately triggered 429 Too Many Requests errors during pagination. I resolved this by implementing exponential backoff, batching requests by section, and retry logic.
2. Second, the Newswire metadata was highly inconsistent and messy. Facet fields appeared as Python-like lists, comma-separated strings, or irregular free-text values. I built a custom parser that safely standardized all facet formats into proper lists for analysis.
3. The NYT Newswire API only exposes a small rolling window of recent articles, and each section typically stops returning results after 100–300 items. Even with daily ingestion and careful handling of rate limits, it is impossible to bulk-fetch tens of thousands of unique articles in a single run. By running the ingestion pipeline every day, I accumulated ~47,000 articles over time—highlighting the real-world constraint that large-scale volume with this API requires multi-day continuous collection rather than a single historical pull.

## Analysis

Offer a brief analysis of the data with your findings. Keep it to one brief, clear, and meaningful paragraph.

Across more than a thousand NYT articles, several clear behavioral patterns emerged in newsroom publishing. The World and U.S. desks dominated output, both in total volume and in hourly publishing velocity, reflecting where editorial attention is concentrated. Facet analysis showed recurring emphasis on a small set of geopolitical conflicts, major public figures, and institutional actors. Update-lag analysis revealed that certain sections — especially breaking news and politics-adjacent desks — tend to revise articles within minutes of publication, while feature-oriented sections update far less frequently. Title-length analysis showed stylistic differences across desks, and anomaly detection highlighted spikes in publishing that aligned with major global events. Together, these results reveal how the newsroom allocates attention, responds to events, and structures its real-time coverage.

## Plot / Visualization

Include at least one compelling plot or visualization of your work. Add images in your subdirectory and then display them using markdown in your README.md file.

1. ![Overall Publishing Velocity](analysis/velocity_analysis/overall_velocity.png)
This plot shows the number of articles published per hour across all sections, revealing clear peaks where newsroom activity surges and quieter periods where publishing slows.
2. ![Section Velocity](analysis/velocity_analysis/section_velocity.png)
This visualization compares hourly publishing patterns for the top six busiest sections, highlighting how different desks exhibit distinct temporal rhythms and react differently to news cycles.
3. ![Top Sections](analysis/metadata_analysis/top_sections.png)
This bar chart identifies which NYTimes sections produce the highest volume of articles, demonstrating where the newsroom allocates the most editorial resources.
4.![Descriptive Facets](analysis/metadata_analysis/facet_des_facet.png)
This plot shows the most common descriptive topics attached to articles, illustrating the recurring themes and issues that dominate NYT coverage.
5. ![Geographic Facets](analysis/metadata_analysis/facet_geo_facet.png)
This chart highlights the most frequently referenced geographic locations, revealing where global and domestic attention is concentrated in recent news.
6.![Organizational Facets](analysis/metadata_analysis/facet_org_facet.png)
This bar chart lists the organizations most often mentioned in articles, indicating which institutions are central to ongoing news stories.
7. ![Person Facets](analysis/metadata_analysis/facet_per_facet.png)
This visualization displays the most frequently mentioned public figures across the dataset, offering insight into political, cultural, or international actors heavily covered at the moment.
8. ![Update Lag Histogram](analysis/metadata_analysis/update_lag_hist.png)
This histogram shows how long articles take to receive their first update after initial publication, capturing patterns in editorial revisions across all sections.
9. ![Update Lag by Section](analysis/metadata_analysis/update_lag_by_section.png)
This plot compares update speeds across sections, revealing which desks revise stories quickly (e.g., breaking news) versus those that rarely update after publication (e.g., lifestyle or features).
10. ![Title Length](analysis/metadata_analysis/title_length_by_section.png)
This chart visualizes differences in average title length by section, uncovering stylistic tendencies between fast-news desks and long-form features.
11. ![Velocity Anomalies](analysis/velocity_analysis/velocity_anomalies.png)
This anomaly chart highlights hours where article volume significantly deviated from the norm, often aligning with breaking events or major global news.

## GitHub Repository
https://github.com/Anaya666/ds3022-data-project-3


