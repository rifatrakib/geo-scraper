import json
from datetime import datetime

import scrapy

from geo import settings


class FindersSpider(scrapy.Spider):
    name = "finders"

    def __init__(self, category, **kwargs):
        self.category = category

    def start_requests(self):
        payload = {"default": True, "cacheable": True}
        base_url = settings.HOME_URL
        counter = settings.COUNTER_PATH.replace("(placeholder)", self.category)
        url = f"{base_url}/{counter}"
        yield scrapy.Request(
            url=url,
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
                    url=settings.ADVERTS_PAGES,
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
