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
        land_address_url = settings.LAND_ADDRESS_URL

        for record in records:
            value = record[identifier]
            short_value = "-".join(value.split("-")[:3]).lstrip("0")

            # request for flat info
            yield scrapy.Request(
                url=f"{flat_info_url}?{flat_info_query_parameter}={value}",
                callback=self.parse_flat_info,
                cb_kwargs={"identifier": value},
            )

            # request for warning
            yield scrapy.Request(
                url=f"{warning_url}?{warning_query_parameter}={short_value}",
                callback=self.parse_warning,
                cb_kwargs={"identifier": value},
            )

            # request for purpose
            yield scrapy.Request(
                url=f"{purpose_url}?{purpose_query_parameter}={short_value}",
                callback=self.parse_purpose,
                cb_kwargs={"identifier": value},
            )

            # request for plan
            yield scrapy.Request(
                url=f"{plan_url}?{plan_query_parameter}={short_value}",
                callback=self.parse_plan,
                cb_kwargs={"identifier": value},
            )

            # request for dwellers
            yield scrapy.Request(
                url=f"{dwellers_url}/{value}",
                callback=self.parse_dwellers,
                cb_kwargs={"identifier": value},
            )

            # request for apartment
            yield scrapy.Request(
                url=f"{apartment_url}/{value}",
                callback=self.parse_apartments,
                cb_kwargs={"identifier": value},
            )

            # request for subleters
            yield scrapy.Request(
                url=f"{subleters_url}/{value}",
                callback=self.parse_subleters,
                cb_kwargs={"identifier": value},
            )

            # request for external links
            yield scrapy.Request(
                url=f"{external_links_url}?{external_links_param}={value.lstrip('0')}",
                callback=self.parse_external_links,
                cb_kwargs={"identifier": value},
            )

            # request for flat registration
            yield scrapy.Request(
                url=f"{flat_registration_url}/{value}",
                callback=self.parse_flat_registration,
                cb_kwargs={"identifier": value},
            )

            # request for flat location
            yield scrapy.Request(
                url=f"{flat_location_url}/{value}",
                callback=self.parse_flat_location,
                cb_kwargs={"identifier": value},
            )

            # request for land establishments
            yield scrapy.Request(
                url=f"{land_establishments_url}/{value}",
                callback=self.parse_land_establishments,
                cb_kwargs={"identifier": value},
            )

            # request for land flats
            yield scrapy.Request(
                url=f"{land_flats_url}/{value}",
                callback=self.parse_land_flats,
                cb_kwargs={"identifier": value},
            )

            # request for land utilities
            yield scrapy.Request(
                url=f"{land_utilities_url}/{value}",
                callback=self.parse_land_utilities,
                cb_kwargs={"identifier": value},
            )

            # request for land attachments
            yield scrapy.Request(
                url=f"{land_attachments_url}/{value}",
                callback=self.parse_land_attachments,
                cb_kwargs={"identifier": value},
            )

            # request for land registration
            yield scrapy.Request(
                url=f"{land_registration_url}/{value}",
                callback=self.parse_land_registration,
                cb_kwargs={"identifier": value},
            )

            # request for land address
            yield scrapy.Request(
                url=f"{land_address_url}/{value}",
                callback=self.parse_land_address,
                cb_kwargs={"identifier": value},
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
        records = response.json()
        print({**kwargs, "data": records})

        owner_information_url = settings.OWNER_INFORMATION_URL
        owner_path = settings.OWNER_PATH

        keys = owner_path.split(".")
        ids = set()
        for doc in records:
            items = doc[keys[0]][keys[1]]
            for item in items:
                id = item[keys[2]][keys[3]]
                ids.add(str(id))

        ids = ",".join(list(ids))
        yield scrapy.Request(
            url=f"{owner_information_url}/{ids}",
            callback=self.parse_owner_information,
            cb_kwargs=kwargs,
        )

    def parse_land_utilities(self, response, **kwargs):
        records = response.json()
        print({**kwargs, "data": records})

        flat_price_estimation_url = settings.FLAT_PRICE_ESTIMATION_URL
        flat_price_estimation_param = settings.FLAT_PRICE_ESTIMATION_PARAM
        flat_sale_ads_url = settings.FLAT_SALE_ADS_URL
        flat_sale_ads_param = settings.FLAT_SALE_ADS_PARAM
        flat_building_identifier = settings.FLAT_BUILDING_IDENTIFIER
        building_details_url = settings.BUILDING_DETAILS_URL
        building_details_param = settings.BUILDING_DETAILS_PARAM

        param = ",".join([str(record[flat_building_identifier]) for record in records])

        # request for flat price estimation
        yield scrapy.Request(
            url=f"{flat_price_estimation_url}?{flat_price_estimation_param}={param}",
            callback=self.parse_flat_price_estimation,
            cb_kwargs=kwargs,
        )

        # request for flat sale ads
        yield scrapy.Request(
            url=f"{flat_sale_ads_url}?{flat_sale_ads_param}={param}",
            callback=self.parse_flat_sale_ads,
            cb_kwargs=kwargs,
        )

        building_ids = set([str(record[building_details_param]) for record in records])
        for building_id in building_ids:
            yield scrapy.Request(
                url=f"{building_details_url}/{building_id}",
                callback=self.parse_building_details,
                cb_kwargs=kwargs,
            )

    def parse_land_attachments(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_land_registration(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_land_address(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_flat_price_estimation(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_flat_sale_ads(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_owner_information(self, response, **kwargs):
        print({**kwargs, "data": response.json()})

    def parse_building_details(self, response, **kwargs):
        print({**kwargs, "data": response.json()})
