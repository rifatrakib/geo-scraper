import json

import scrapy
from scrapy.http import FormRequest

from geo import settings


class LandsSpider(scrapy.Spider):
    name = "authentication"
    custom_settings = {
        "SPIDER_MIDDLEWARES": {},
        "DOWNLOADER_MIDDLEWARES": {},
        "ITEM_PIPELINES": {},
    }

    def start_requests(self):
        payload = {"email": settings.EMAIL, "password": settings.PASSWORD}
        yield FormRequest(
            url=settings.SIGNIN_PATH,
            formdata=payload,
            callback=self.parse,
        )

    def parse(self, response):
        cookies = response.headers.to_unicode_dict()["set-cookie"].split(";")
        with open("keys/credentials.json") as reader:
            credentials = json.loads(reader.read())

        for cookie in cookies:
            splits = cookie.split("=")
            if splits[0] == "auth_token":
                credentials["cookies"]["auth_token"] = "=".join(splits[1:])

        with open("keys/credentials.json", "w") as writer:
            writer.write(json.dumps(credentials, indent=4))
