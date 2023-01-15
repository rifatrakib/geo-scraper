import scrapy

from geo import settings
from geo.utils import extract_data_from_stored_records


class InfographicsSpider(scrapy.Spider):
    name = "infographics"

    def start_requests(self):
        connector = settings.THRESHOLDS_CONNECTOR
        identifier = settings.THRESHOLDS_IDENTIFIER
        flat_info_url = settings.FLAT_INFO_URL
        query_parameter = settings.FLAT_INFO_PARAM
        records = extract_data_from_stored_records(connector, identifier)
        for record in records:
            value = record[identifier]
            # request for flat info
            yield scrapy.Request(
                url=f"{flat_info_url}?{query_parameter}={value}",
                callback=self.parse,
            )

    def parse(self, response):
        print(response.json())
