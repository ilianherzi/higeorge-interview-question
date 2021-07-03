# Solution
**TL;DR** For the problem of an ETL framework for rental price data, I designed a schema that aggregated by zip code, summed and counted prices to deal with repeat rows, and visualized rental prices accross zip codes for a single day and through time for a single zip code. I used pandas and sqlalchemy but would've used pyspark if I had a distributed environment
> For this question, because of a lack of resources, I extracted, transformed, and loaded a fraction of the dataset. If I had been given more compute, I would've parallelized the ETL pipeline using python's multiprocessing toolkit; the one caveat with this approach is there will be duplicate zip codes that need to be combined at the final stage of extraction.

> My schema is designed for efficency and space by storing only the zip code, price, count and visualization related features like lat and long. I hosted a postgresql server on my local machine (can be hosted anywhere though) and used pandas and sqlalchemy as the primary toolkits. I opted to reduce the space of the loaded database by ignoring source addresses in a local zip code at the cost of flexibility for computing metrics other than the mean (e.g. median or std), which we wouldn't be able to calculate with this schema. 

> Duplicate zip codes are handled on an online process using hashing. After a bit of EDA I noticed that the CSV is fairly linear in that multiple sources for the same addresses have similar ids. This means that while the cost of these updates in the worst case is O(N^2) where N is the number of rows, in reality this is much closer to O(1) when the chunk size is large enough (the order of 10\*\*3). Zip codes with missing price data are not loaded into the table.

> Finally for my visualizations, I used seaborn and folium. Folium is a helpful tool when trying to analyze the spread of rental prices on a particular date. Seaborn is more helpful when selecting a single zip code and analyzing it's change in rental price over time. Examples of both visualizations are attached below. Note however, for the geographical visualization is not optimized. 

> Given access to a distributed environment, I would've used pyspark instead of pandas and sqlalchemy. I've given an example of what that would look like below just as a proof of concept. In an industrial enviornment this pipeline should also use a workflow automation tool to detect changes as the CSV grows, like Airflow.


## To run the ETL pipeline 
Specify the source csv via and postgresqldb url 
```bash
python interview_0.py --csv_file=/path/to/csv --postgrep_url='postgresql://url/'

```
## To run the visualizations
See the interview_0.ipynb which contains the visualization functions
