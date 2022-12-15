import json
from datetime import datetime

import scrapy

from geo import settings


class LandsSpider(scrapy.Spider):
    name = "lands"

    def start_requests(self):
        payload = {"default": True, "cacheable": True}
        yield scrapy.Request(
            url=settings.LANDS_COUNTER,
            method="POST",
            body=json.dumps(payload),
            callback=self.parse_count,
        )

    def parse_count(self, response):
        response = response.json()
        if response["success"]:
            number_of_pages = response["count"] // 50 + 1

            for i in range(1, number_of_pages + 1):
                payload = {"page": i, "cacheable": True}
                yield scrapy.Request(
                    url=settings.LANDS_PAGES,
                    method="POST",
                    body=json.dumps(payload),
                    callback=self.parse,
                )

    def parse(self, response):
        adverts = response.json()
        for advert in adverts:
            record = {**advert, "metadata": response.meta}
            record["metadata"]["yield_time"] = datetime.utcnow().isoformat()
            yield record
