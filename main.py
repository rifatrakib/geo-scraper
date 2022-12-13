import os

from dotenv import load_dotenv

from services.utils import run_spider

load_dotenv()


def initialize_scraping_session():
    spiders = os.environ.get("INSTALLED_SPIDERS", None).split(",")
    for spider in spiders:
        run_spider(spider)


if __name__ == "__main__":
    initialize_scraping_session()
