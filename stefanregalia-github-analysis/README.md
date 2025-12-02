# Team github-analysis

## Team members

Stefan Regalia

## Data Source

I worked with the Authenticated GitHub REST API to examine differences in commit message sentiment analysis, contributor concentration, weekend activity, and formalization ratio (proportion of activity conducted through pull requests compared to commits) over time between 3 performance-related (sci-kit learn, numpy, and polars) and 3 simplicity-related repositories (pandas, matplotlib, and plotly.py).

## Challenges / Obstacles

This data source limits API requests to 5,000 requests per user per hour, so I implemented pagination to extract the last 200 pages for commits and 200 for pull requests per repo, totalling 400 pages * 6 repos = 2400 requests. I also used retry logic with retries = 3 for rate limit handling while ingesting the data. Not all commits contained author information, so I excluded null authors from the contributor concentration analysis. I used a prefect workflow to organize a flow of tasks to ingest 100k+ records of commits and pull requests into a DUCKDB database, where I then performed transformations and aggregations. I then used TextBlob for sentiment analysis and SciPy/NumPy for statistical hypothesis testing with t-tests, Cohen's d effect sizes, and confidence intervals.

## Analysis

I tested four hypotheses, stated in the project repo's ReadMe file, regarding how repository positioning (performance vs. simplicity) affects development culture by examining sentiment analysis in commit messages using polarity (-1 to 1 scale), contributor concentration (Gini coefficient), weekend activity patterns, and formalization ratio. 

First, I hypothesized that performance repositories would have lower sentiment scores due to being more technical, which turned out to be true (performance repo mean: 0.0055, simplicity repo mean: 0.0099). Due to the low number of samples, n = 3, and a p-value of 0.406, this conclusion was not statistically significant. The Cohen's d of -0.757 indicates that the performance sentiment mean is 3/4 of a standard deviation lower than the simplicity sentiment mean, showing that there is an observable difference, but it is not statistically significant. 

Secondly, I hypothesized that performance repositories have a higher contributor concentration than simplicity repositories because they may require a smaller team of expert developers due to high technicality, which turned out to be false (performance Gini: 0.8745, simplicity Gini: 0.8896). A p-value of 0.76 helped me conclude that there was no statistically significant difference between these two means, and a Cohen's d of -0.263 reinforced this conclusion. 

Thirdly, I hypothesized that simplicity repositories have a higher proportion of weekend activity to total activity than performance repositories, as they may be less technical and can be worked on more casually, but this hypothesis was false (performance weekend proportion mean: 0.162, simplicity weekend proportion mean: 0.152). While the mean difference in weekend activity proportion was not statistically significant, with a p-value of 0.425, a Cohen's d of 0.723 reinforces that there is an observable difference between the means of the two groups, as the performance mean is roughly 3/4 of a standard deviation greater than the simplicity mean for weekend activity proportion. 

Lastly, I hypothesized that the ratio of pull requests to commits has increased over time for performance repositories, as opposed to decreasing for simplicity repositories, as the formal and professional usage of technical repos may have increased in recent years due to an increase in machine learning interest. However, this hypothesis was false (performance ratio mean: -0.033, simplicity ratio mean: -0.009), and the difference in mean between the groups was not statistically significant (p-value = 0.58). The Cohen's d was -0.49, indicating a small difference between the two groups. This was also counterintuitive to my hypothesis, as commits were more common than pull requests for both types of repositories.

## Plot / Visualization

Include at least one compelling plot or visualization of your work. Add images in your subdirectory and then display them using markdown in your README.md file.

<img width="2951" height="1757" alt="image" src="https://github.com/user-attachments/assets/3a3c9734-f7d0-4d9a-a02d-b97b67821e59" />
<img width="2951" height="1757" alt="image" src="https://github.com/user-attachments/assets/6d4423e8-5704-4481-a525-ce262789c8b4" />
<img width="2951" height="1758" alt="image" src="https://github.com/user-attachments/assets/1fef1d07-d0ff-456a-b544-1e305f35f963" />
<img width="3551" height="1755" alt="image" src="https://github.com/user-attachments/assets/821eeb8f-a521-4554-bc19-72daf642533a" />





## GitHub Repository

https://github.com/stefanregalia/ds3022-data-project-3
