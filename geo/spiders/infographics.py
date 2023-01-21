import scrapy

from geo import settings
from geo.utils import extract_data_from_stored_records


class InfographicsSpider(scrapy.Spider):
    name = "infographics"

    def start_requests(self):
        connector = settings.THRESHOLDS_CONNECTOR
        identifier = settings.THRESHOLDS_IDENTIFIER
        records = extract_data_from_stored_records(connector, identifier)

        flat_info_url = settings.FLAT_INFO_URL
        flat_info_query_parameter = settings.FLAT_INFO_PARAM
        warning_url = settings.WARNING_URL
        warning_query_parameter = settings.WARNING_PARAM
        purpose_url = settings.PURPOSE_URL
        purpose_query_parameter = settings.PURPOSE_PARAM
        plan_url = settings.PLAN_URL
        plan_query_parameter = settings.PLAN_PARAM

        for record in records:
            value = record[identifier]
            short_value = "-".join(value.split("-")[:3]).lstrip("0")

            # request for flat info
            yield scrapy.Request(
                url=f"{flat_info_url}?{flat_info_query_parameter}={value}",
                callback=self.parse_flat_info,
                cb_kwargs={"id": value},
            )

            # request for warning
            yield scrapy.Request(
                url=f"{warning_url}?{warning_query_parameter}={short_value}",
                callback=self.parse_warning,
                cb_kwargs={"id": value},
            )

            # request for purpose
            yield scrapy.Request(
                url=f"{purpose_url}?{purpose_query_parameter}={short_value}",
                callback=self.parse_purpose,
                cb_kwargs={"id": value},
            )

            # request for plan
            yield scrapy.Request(
                url=f"{plan_url}?{plan_query_parameter}={short_value}",
                callback=self.parse_plan,
                cb_kwargs={"id": value},
            )

    def parse_flat_info(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_warning(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_purpose(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_plan(self, response, **kwargs):
        print({**kwargs, "data": response.json()})
