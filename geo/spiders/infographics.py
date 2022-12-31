import scrapy

from geo import settings
from geo.utils import extract_data_from_stored_records


class InfographicsSpider(scrapy.Spider):
    name = "infographics"

    def start_requests(self):
        connector = settings.THRESHOLDS_CONNECTOR
        identifier = settings.THRESHOLDS_IDENTIFIER
        base_url = ""  # to be scripted later
        records = extract_data_from_stored_records(connector, identifier)
        for record in records:
            parameter = record[identifier]
            yield scrapy.Request(
                url=f"{base_url}/{parameter}",
                callback=self.parse,
            )

    def parse(self, response):
        print(response.json())
