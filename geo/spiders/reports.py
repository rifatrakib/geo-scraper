import scrapy

from geo import settings
from geo.utils import extract_data_from_stored_records


class ReportsSpider(scrapy.Spider):
    name = "reports"

    def start_requests(self):
        survey_names_url = settings.SURVEY_NAMES_URL
        yield scrapy.Request(url=survey_names_url, callback=self.parse)

    def parse(self, response):
        layers = response.json()["layers"]
        connector = settings.THRESHOLDS_CONNECTOR
        identifier = settings.THRESHOLDS_IDENTIFIER
        survey_report_url = settings.SURVEY_REPORT_URL
        records = extract_data_from_stored_records(connector, identifier)

        for record in records:
            value = record[identifier]

            # request for report data
            for layer in layers:
                yield scrapy.Request(
                    url=f"{survey_report_url}/{value}/{layer}",
                    callback=self.parse_report,
                    cb_kwargs={"id": value},
                )

    def parse_report(self, response, **kwargs):
        print({**kwargs, "data": response.json()})
