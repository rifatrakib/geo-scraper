# GeoScraper

This project consists of a collection of scrapers dedicated to browse and collect data for `geospatial analysis` using different tools like Python, R, Microsoft Excel, Tableau, Power BI, etc. and some of which will be implemented later in different repositories (to be linked later).


## Objective

Geospatial data are becoming more and more essential for people all around the world and suitable data for electronic visualization are becoming more and more accessible and useful everyday. The collection of scraper collects most of the data in `GeoJSON` format which is a widely accepted standard for `geospatial data visualization` on many platforms. The scraper also has a report generation script integrated to it which will generate real-time reports on the network performance where the scraper is being run for monitoring the scraper health and performance. In the end, it will also save the last frame of the report for further analysis on daily reports accumulated over time.

Scraped data, logs, and reports are stored in a way that will keep track of daily information accumulation which is useful in order to analyze application and implications of the product in use in the long run and customize according to the analyzed results.

There is one spider dedicated for logging the user into the system followed by the remaining spiders which will collect data from the target website or websites. The user only needs to have the credentials as environment variables available to the scraper environment in the right format.


## Tools

This collection of scraper is built solely on Python and a few modules publicly available to everyone. The list goes as follows:

* Python
* Scrapy
* Pandas
* Matplotlib

The user will also need to install `MongoDB` on the machine where the scraper will run, or have a cluster of `MongoDB` databases hosted on a remote machine. If using `MongoDB` to store collected and pre-processed data is not desired, it can be turned of from the `geo/settings.py` script.
