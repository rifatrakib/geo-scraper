import scrapy

from geo import settings


class ReportsSpider(scrapy.Spider):
    name = "reports"

    def start_requests(self):
        survey_names_url = settings.SURVEY_NAMES_URL
        yield scrapy.Request(url=survey_names_url, callback=self.parse)

    def parse(self, response):
        print({"data": response.json()})
