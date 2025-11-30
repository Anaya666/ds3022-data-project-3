# Team Thomas

## Team members
- Thomas Cusick


## Data Source

What data source did you work with?
- I started by working with Aplaca API (https://docs.alpaca.markets/docs/about-market-data-api) for the stream portion of my project and then used yfinance (https://pypi.org/project/yfinance/) for the historical part and for the visualizations.

## Challenges / Obstacles

What challenges did this data choice present in data gathering, processing and analysis, and how did you work through them? What methods and tools did you use to work with this data?
- Alpaca API was awesome to work with, but I was not able to achieve the desirable scale for this project with that API alone. I made a stream directory with complete consumer, producer, and clean/visualize scripts. However, the API only runs when the market is open (so 9:30am-4pm Monday-Friday and not on holidays.) so achieving the desirable scale for this project proved difficult, given break and trying to do work 'after hours' and on weekends. I knew the point of this project was to stream ingest so I still was able to prove what I have learned in that directory. In terms of scale, I then switched to yfinance and thought it would be interesting to try to compare numbers from the 2008 and then 2020 recession to current numbers in order to try to dig into the idea of whether we're in a recession or not right now. History repeats itself, so data and numbers and comparisons can prove very useful when trying to look at possible events and comparing indicators where relevant. 
- In the stream folder I stored the incoming datapoints in DuckDB for them to then be cleaned and used for visualizations once the consumer and producer scripts are halted.
- In the historical folder, I pulled the data from yfinance into DuckDB for it to them be cleaned and visualized. 
- I was confident in using DuckDB because of our previous assignments and processing large quantities of data using SQL makes the most sense to me.   


## Analysis

Offer a brief analysis of the data with your findings. Keep it to one brief, clear, and meaningful paragraph.
- The data reveals a dramatic transformation in market behavior over the past two decades. While average returns have increased from 16.8% to 21.0% in the current period, volatility has surged significantly (from 0.22 to 0.28), indicating markets are simultaneously more rewarding and riskier. The normalized performance chart shows extreme divergence among stocks, with a few names, likely mega-cap tech companies—achieving astronomical returns (100,000+ normalized gains) while most remain clustered near baseline. Notably, the COVID-19 crisis produced a sharp but brief drawdown (-12.7%) that recovered quickly, contrasting with the 2008 financial crisis's deeper and more prolonged decline (-25.1%). The volatility chart demonstrates that despite three major crises since 2000, markets have maintained relatively elevated baseline volatility in recent years, suggesting structural changes in market dynamics—possibly driven by algorithmic trading, concentrated performance in tech stocks, and increased retail participation.

## Plot / Visualization

Include at least one compelling plot or visualization of your work. Add images in your subdirectory and then display them using markdown in your README.md file.
![All performance chart](sample/visualizations/All_performance.png)
*Figure 1: Normalized performance across the sample.*

![Current vs 2008](sample/visualizations/current_vs_2008.png)
*Figure 2: Current period vs 2008 comparison.*

![Drawdown analysis](sample/visualizations/drawdown_analysis.png)
*Figure 3: Drawdown comparison across crisis periods.*

![Market volatility](sample/visualizations/market_volatility.png)
*Figure 4: Rolling volatility over time.*

![Recession comparison](sample/visualizations/recession_comparison.png)
*Figure 5: Recession period comparisons.*



## GitHub Repository

https://github.com/tcusick8/alpaca_yfinance_DP3/tree/main
