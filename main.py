import os
import time
from datetime import datetime
from multiprocessing import Process
from pathlib import Path

from dotenv import load_dotenv

from services.reporter import scraper_report
from services.utils import run_spider

load_dotenv()


def run_processes(spider):
    p1 = Process(target=run_spider, args=[spider])
    p2 = Process(target=scraper_report, args=[spider])
    p1.start()

    date = datetime.utcnow().date().isoformat()
    while not Path(f"data/meta/{spider}-{date}.csv").exists():
        time.sleep(5)
        continue

    while True:
        with open(f"data/meta/{spider}-{date}.csv") as reader:
            if len(reader.readlines()) > 0:
                break
            time.sleep(2)

    p2.start()


def initialize_scraping_session():
    spiders = os.environ.get("INSTALLED_SPIDERS", None).split(",")
    for spider in spiders:
        run_processes(spider)


if __name__ == "__main__":
    initialize_scraping_session()
