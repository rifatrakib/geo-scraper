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

        dwellers_url = settings.DWELLERS_URL
        apartment_url = settings.APARTMENT_URL
        subleters_url = settings.SUBLETERS_URL

        external_links_url = settings.EXTERNAL_LINKS_URL
        external_links_param = settings.EXTERNAL_LINKS_PARAM

        flat_registration_url = settings.FLAT_REGISTRATION_URL
        flat_location_url = settings.FLAT_LOCATION_URL

        land_establishments_url = settings.LAND_ESTABLISHMENTS_URL
        land_flats_url = settings.LAND_FLATS_URL
        land_utilities_url = settings.LAND_UTILITIES_URL
        land_attachments_url = settings.LAND_ATTACHMENTS_URL
        land_registration_url = settings.LAND_REGISTRATION_URL

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

            # request for dwellers
            yield scrapy.Request(
                url=f"{dwellers_url}/{value}",
                callback=self.parse_dwellers,
                cb_kwargs={"id": value},
            )

            # request for apartment
            yield scrapy.Request(
                url=f"{apartment_url}/{value}",
                callback=self.parse_apartments,
                cb_kwargs={"id": value},
            )

            # request for subleters
            yield scrapy.Request(
                url=f"{subleters_url}/{value}",
                callback=self.parse_subleters,
                cb_kwargs={"id": value},
            )

            # request for external links
            yield scrapy.Request(
                url=f"{external_links_url}?{external_links_param}={value.lstrip('0')}",
                callback=self.parse_external_links,
                cb_kwargs={"id": value},
            )

            # request for flat registration
            yield scrapy.Request(
                url=f"{flat_registration_url}/{value}",
                callback=self.parse_flat_registration,
                cb_kwargs={"id": value},
            )

            # request for flat location
            yield scrapy.Request(
                url=f"{flat_location_url}/{value}",
                callback=self.parse_flat_location,
                cb_kwargs={"id": value},
            )

            # request for land establishments
            yield scrapy.Request(
                url=f"{land_establishments_url}/{value}",
                callback=self.parse_land_establishments,
                cb_kwargs={"id": value},
            )

            # request for land flats
            yield scrapy.Request(
                url=f"{land_flats_url}/{value}",
                callback=self.parse_land_flats,
                cb_kwargs={"id": value},
            )

            # request for land utilities
            yield scrapy.Request(
                url=f"{land_utilities_url}/{value}",
                callback=self.parse_land_utilities,
                cb_kwargs={"id": value},
            )

            # request for land attachments
            yield scrapy.Request(
                url=f"{land_attachments_url}/{value}",
                callback=self.parse_land_attachments,
                cb_kwargs={"id": value},
            )

            # request for land registration
            yield scrapy.Request(
                url=f"{land_registration_url}/{value}",
                callback=self.parse_land_registration,
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

    def parse_dwellers(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_apartments(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_subleters(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_external_links(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_flat_registration(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_flat_location(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_land_establishments(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_land_flats(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_land_utilities(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_land_attachments(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_land_registration(self, response, **kwargs):
        print({**kwargs, "data": response.json()})
