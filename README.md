# StockTerminal
### Platform for low latency and  slicing and dicing of data in near real time

## Motivation
Platforms like the bloomberg terminal are expensive. This pipeline can help democratize stock and financial data for amateur traders by helping them make buy/sell decisions faster which means they are making more money

## Architecture
![](images/stockpipeline.png)

## Challenges
Selecting a suitable database/datastore to ensure pipleline meets the low latency standards was challenging.

## Future work
1. The existing pipeline can be combined with historical (batch) data that can provide richer and more granular visualizations
For example, Along with portfolios being tracked day over day and week over week, it can be tracked month over month and year over year as well.
  More interestingly we could track how real world events correlates to stock market swings
2. Investigate whether memsql could be replaced with another open source database/datastore which could provide the same low latency benefits

