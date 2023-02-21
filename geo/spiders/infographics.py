import json
from datetime import datetime

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
        data = response.json()
        for item in data:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_warning(self, response, **kwargs):
        records = response.json()
        record = {**records, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

    def parse_purpose(self, response, **kwargs):
        data = response.json()
        for item in data:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_plan(self, response, **kwargs):
        records = response.json()
        record = {**records, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

    def parse_dwellers(self, response, **kwargs):
        data = response.json()
        for item in data:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_apartments(self, response, **kwargs):
        data = response.json()
        for item in data.get("data", []):
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_subleters(self, response, **kwargs):
        data = response.json()
        for item in data.get("data", []):
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_external_links(self, response, **kwargs):
        records = response.json()
        record = {**records, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

    def parse_flat_registration(self, response, **kwargs):
        records = response.json()
        record = {**records, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

    def parse_flat_location(self, response, **kwargs):
        records = response.json()
        record = {**records, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

    def parse_land_establishments(self, response, **kwargs):
        data = response.json()
        for item in data:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_land_flats(self, response, **kwargs):
        records = response.json()
        for item in records:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

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
        # request for owner information
        yield scrapy.Request(
            url=f"{owner_information_url}/{ids}",
            callback=self.parse_owner_information,
            cb_kwargs=kwargs,
        )

    def parse_land_utilities(self, response, **kwargs):
        records = response.json()
        for item in records:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

        flat_price_estimation_url = settings.FLAT_PRICE_ESTIMATION_URL
        flat_price_estimation_param = settings.FLAT_PRICE_ESTIMATION_PARAM
        flat_sale_ads_url = settings.FLAT_SALE_ADS_URL
        flat_sale_ads_param = settings.FLAT_SALE_ADS_PARAM
        flat_building_identifier = settings.FLAT_BUILDING_IDENTIFIER
        land_building_identifier = settings.LAND_BUILDING_IDENTIFIER
        building_details_url = settings.BUILDING_DETAILS_URL
        building_details_param = settings.BUILDING_DETAILS_PARAM
        flat_info_url = settings.FLAT_INFO_URL
        flat_info_query_parameter = settings.FLAT_INFO_PARAM
        latest_dealings_url = settings.LATEST_DEALINGS_URL
        historical_dealings_url = settings.HISTORICAL_DEALINGS_URL
        land_registration_url = settings.LAND_REGISTRATION_URL
        flat_evaluation_url = settings.FLAT_EVALUATION_URL
        flat_evaluation_param = settings.FLAT_EVALUATION_PARAM
        flat_utilities_url = settings.FLAT_UTILITIES_URL
        flat_association_url = settings.FLAT_ASSOCIATION_URL
        flat_ads_connection_url = settings.FLAT_ADS_CONNECTION_URL
        flat_association_param = settings.FLAT_ASSOCIATION_PARAM

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

        lands = [record[land_building_identifier] for record in records]
        param = ",".join(lands)

        # request for flat information
        yield scrapy.Request(
            url=f"{flat_info_url}?{flat_info_query_parameter}={param}",
            callback=self.parse_flat_info,
            cb_kwargs=kwargs,
        )

        lands.append(kwargs["identifier"])
        param = ",".join(lands)

        # request for flat latest dealings
        yield scrapy.Request(
            url=f"{latest_dealings_url}/{param}",
            callback=self.parse_latest_dealings,
            cb_kwargs=kwargs,
        )

        for land in lands:
            # request for historical dealings
            yield scrapy.Request(
                url=f"{historical_dealings_url}/{land}",
                callback=self.parse_historical_dealings,
                cb_kwargs=kwargs,
            )

            # request for land registration
            if land != kwargs["identifier"]:
                yield scrapy.Request(
                    url=f"{land_registration_url}/{land}",
                    callback=self.parse_land_registration,
                    cb_kwargs={"identifier": land},
                )

        building_ids = set([str(record[building_details_param]) for record in records])
        for building_id in building_ids:
            # request for building details
            yield scrapy.Request(
                url=f"{building_details_url}/{building_id}",
                callback=self.parse_building_details,
                cb_kwargs=kwargs,
            )

        flat_ids = set([record[flat_building_identifier] for record in records])
        for flat_id in flat_ids:
            # request for flat evaluation
            yield scrapy.Request(
                url=f"{flat_evaluation_url}?{flat_evaluation_param}={flat_id}",
                callback=self.parse_flat_evaluation_information,
                cb_kwargs={**kwargs, "UNIT_ID": flat_id},
            )

            # request for flat utilities
            yield scrapy.Request(
                url=f"{flat_utilities_url}/{flat_id}",
                callback=self.parse_flat_utilities,
                cb_kwargs={**kwargs, "UNIT_ID": flat_id},
            )

            # request for flat association
            yield scrapy.Request(
                url=f"{flat_association_url}?{flat_association_param}={flat_id}",
                callback=self.parse_flat_associations,
                cb_kwargs={**kwargs, "UNIT_ID": flat_id},
            )

            # request for flat ads connection
            yield scrapy.Request(
                url=f"{flat_ads_connection_url}?{flat_association_param}={flat_id}",
                callback=self.parse_flat_ads_connections,
                cb_kwargs={**kwargs, "UNIT_ID": flat_id},
            )

    def parse_land_attachments(self, response, **kwargs):
        data = response.json()
        yield {"data": data, **kwargs, "metadata": response.meta}

    def parse_land_registration(self, response, **kwargs):
        records = response.json()
        for item in records:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_land_address(self, response, **kwargs):
        records = response.json()
        data = records.get("property_details", {})
        record = {**data, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

    def parse_flat_price_estimation(self, response, **kwargs):
        records = response.json()
        for item in records:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_flat_sale_ads(self, response, **kwargs):
        records = response.json()
        for item in records:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_owner_information(self, response, **kwargs):
        records = response.json()
        for item in records:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_building_details(self, response, **kwargs):
        records = response.json()
        record = {**records, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

    def parse_latest_dealings(self, response, **kwargs):
        records = response.json()
        for item in records:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_historical_dealings(self, response, **kwargs):
        records = response.json()
        for item in records:
            record = {**item, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_flat_evaluation_information(self, response, **kwargs):
        records = response.json()
        data = records.get("valuation", {})
        record = {**data, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

        index_estimation_url = settings.INDEX_ESTIMATION_URL
        index_estimation_param = settings.INDEX_ESTIMATION_PARAM
        index_estimation_source = settings.INDEX_ESTIMATION_SOURCE

        price_distribution_url = settings.PRICE_DISTRIBUTION_URL
        price_distribution_param = settings.PRICE_DISTRIBUTION_PARAM
        price_distribution_source = settings.PRICE_DISTRIBUTION_SOURCE

        adjacent_lands_url = settings.ADJACENT_LANDS_URL
        adjacent_lands_source = settings.ADJACENT_LANDS_SOURCE

        index_keys = index_estimation_source.split(".")
        if records.get(index_keys[0], None) and records[index_keys[0]].get(index_keys[1], None):
            index_param = records[index_keys[0]][index_keys[1]]
            # request for index estimation
            yield scrapy.Request(
                url=f"{index_estimation_url}?{index_estimation_param}={index_param}",
                callback=self.parse_index_estimation,
                cb_kwargs=kwargs,
            )

        price_keys = price_distribution_source.split(".")
        if records.get(price_keys[0], None) and records[price_keys[0]].get(price_keys[1], None):
            price_param = records[price_keys[0]][price_keys[1]]
            # request for price distribution
            yield scrapy.Request(
                url=f"{price_distribution_url}?{index_estimation_param}={index_param}&{price_distribution_param}={price_param}",
                callback=self.parse_price_distribution,
                cb_kwargs=kwargs,
            )

        adjacent_keys = adjacent_lands_source.split(",")
        if records.get(index_keys[0], None):
            payload = {}
            for key in adjacent_keys:
                payload[key] = records[index_keys[0]][key]
            # request for adjacent lands
            yield scrapy.Request(
                url=adjacent_lands_url,
                method="POST",
                body=json.dumps(payload),
                callback=self.parse_adjacent_lands,
                cb_kwargs=kwargs,
            )

    def parse_index_estimation(self, response, **kwargs):
        records = response.json()
        data = records.get("index_valuation", {})
        record = {**data, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

    def parse_price_distribution(self, response, **kwargs):
        records = response.json()
        data = records.get("price_distribution", {})
        record = {**data, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

    def parse_adjacent_lands(self, response, **kwargs):
        records = response.json()
        record = {**records, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

    def parse_flat_utilities(self, response, **kwargs):
        records = response.json()
        record = {**records, **kwargs, "metadata": response.meta}
        record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
        yield record

    def parse_flat_associations(self, response, **kwargs):
        records = response.json()
        if records:
            record = {**records, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record

    def parse_flat_ads_connections(self, response, **kwargs):
        records = response.json()
        if records:
            record = {**records, **kwargs, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record
