from datetime import datetime
from pathlib import Path

from scrapy import signals

from services.utils import log_writer, prepare_json_credentials, read_json_credentials


class GeoSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.
        if response.status == 200:
            response.meta["response_in"] = datetime.utcnow().isoformat()

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.
        if response.status == 200:
            response.meta["response_out"] = datetime.utcnow().isoformat()

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        location = "logs/meta"
        Path(location).mkdir(parents=True, exist_ok=True)
        file_path = f"{location}/{spider.name}.log"
        string = (
            f"ERROR: {str(exception)} occurred for "
            f"{response.request.url} while running {spider.name} spider "
            f"at {datetime.utcnow().isoformat()}\n"
        )
        log_writer(file_path, string)

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.
        location = "logs/meta"
        Path(location).mkdir(parents=True, exist_ok=True)

        # Must return only requests (not items).
        for r in start_requests:
            file_path = f"{location}/{spider.name}.log"
            string = f"INFO: {r.method} request fired for {spider.name} with {r.url} at {datetime.utcnow().isoformat()}\n"
            log_writer(file_path, string)
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class GeoDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.
        location = "keys/credentials"
        Path(location).mkdir(parents=True, exist_ok=True)

        if not Path(f"{location}/{spider.name}.sh").exists():
            prepare_json_credentials(spider.name)

        credentials = read_json_credentials(spider.name)

        for key, value in credentials["headers"].items():
            request.headers[key] = value

        for key, value in credentials["cookies"].items():
            request.cookies[key] = value

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.
        location = "logs/fails"
        Path(location).mkdir(parents=True, exist_ok=True)

        if (
            response.status == 200
            and response.headers.get("content-type") == "application/jsonsuccess" in response.json()
            and not response.json()["success"]
        ):
            file_path = f"{location}/{spider.name}.log"
            string = f"request to {request.url} failed with {response.json()} at {datetime.utcnow().isoformat()}\n"
            log_writer(file_path, string)

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        location = "logs/fails"
        Path(location).mkdir(parents=True, exist_ok=True)

        file_path = f"{location}/{spider.name}.log"
        string = f"ERROR: request to {request.url} failed with {str(exception)} at {datetime.utcnow().isoformat()}\n"
        log_writer(file_path, string)

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
